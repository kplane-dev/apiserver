package smoke

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestNoIdentityMetadataInEtcd creates objects through the API, then reads the
// raw etcd values to verify that cluster identity is NOT stored as labels or
// annotations on the object. Identity is derived entirely from the etcd key path.
func TestNoIdentityMetadataInEtcd(t *testing.T) {
	etcdEndpoints := os.Getenv("ETCD_ENDPOINTS")
	if etcdEndpoints == "" {
		t.Skip("ETCD_ENDPOINTS not set")
	}

	prefix := fmt.Sprintf("/registry-etcd-meta-%d", time.Now().UnixNano())
	s := startAPIServerWithOptions(t, etcdEndpoints, apiserverOptions{
		etcdPrefix: prefix,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	clusters := []string{s.root, "c-" + randSuffix(3)}
	cmNames := map[string]string{}

	for _, cid := range clusters {
		cs := kubeClientForCluster(t, s, cid)
		if err := waitForNamespace(ctx, cs, "default"); err != nil {
			t.Fatalf("cluster=%s wait namespace: %v", cid, err)
		}

		cmName := "etcd-check-" + randSuffix(4)
		cmNames[cid] = cmName

		_, err := cs.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:   cmName,
				Labels: map[string]string{"user-label": "yes"},
			},
			Data: map[string]string{"hello": "world"},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("cluster=%s create: %v", cid, err)
		}
	}

	// Now read raw etcd to inspect stored objects
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	defer etcdClient.Close()

	// List all keys under the prefix to find our configmaps
	resp, err := etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("etcd get: %v", err)
	}

	for _, cid := range clusters {
		cmName := cmNames[cid]
		found := false

		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			if !strings.Contains(key, cmName) {
				continue
			}
			found = true

			t.Logf("cluster=%s key=%s (len=%d)", cid, key, len(kv.Value))

			// The value is protobuf-encoded, but labels/annotations would still
			// appear as readable strings. Check raw bytes for identity keywords.
			raw := string(kv.Value)
			identityKeywords := []string{
				"clusterid", "cluster-id", "cluster_id",
				"kplane", "storagekey", "storage-key",
				"multicluster", "identity",
			}
			for _, keyword := range identityKeywords {
				if strings.Contains(strings.ToLower(raw), keyword) {
					t.Errorf("cluster=%s cm=%s: raw etcd value contains identity keyword %q", cid, cmName, keyword)
				}
			}

			// Also try to find the JSON metadata section and parse it.
			// Protobuf values have the JSON-like metadata embedded.
			// Look for labels/annotations that shouldn't be there.
			if idx := strings.Index(raw, `"labels"`); idx >= 0 {
				// Extract a chunk around the labels for inspection
				end := idx + 200
				if end > len(raw) {
					end = len(raw)
				}
				chunk := raw[idx:end]
				// The only label should be "user-label"
				if strings.Contains(chunk, "cluster") {
					t.Errorf("cluster=%s cm=%s: etcd labels section contains 'cluster': ...%s...", cid, cmName, chunk)
				}
			}

			// Try JSON decode as a secondary check (works if stored as JSON)
			var obj map[string]interface{}
			if json.Unmarshal(kv.Value, &obj) == nil {
				meta, _ := obj["metadata"].(map[string]interface{})
				if meta != nil {
					labels, _ := meta["labels"].(map[string]interface{})
					annotations, _ := meta["annotations"].(map[string]interface{})
					for k := range labels {
						if k != "user-label" && isIdentityMetadata(k) {
							t.Errorf("cluster=%s cm=%s: etcd JSON has identity label %q", cid, cmName, k)
						}
					}
					for k := range annotations {
						if isIdentityMetadata(k) {
							t.Errorf("cluster=%s cm=%s: etcd JSON has identity annotation %q", cid, cmName, k)
						}
					}
				}
			}
		}

		if !found {
			t.Errorf("cluster=%s: configmap %s not found in etcd under prefix %s", cid, cmName, prefix)
		}
	}

	// Verify the key path contains cluster identity (structural check)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.Contains(key, "configmaps") {
			continue
		}
		for _, cid := range clusters {
			if strings.Contains(key, cmNames[cid]) {
				if !strings.Contains(key, "/clusters/"+cid+"/") {
					t.Errorf("expected key to contain /clusters/%s/, got: %s", cid, key)
				} else {
					t.Logf("confirmed: cluster=%s identity is in key path: %s", cid, key)
				}
			}
		}
	}
}
