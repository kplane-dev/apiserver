package smoke

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSnapshotEndpoint creates a few resources in a VCP and verifies that
// GET /clusters/{cid}/control-plane/snapshot returns them from the live
// informer cache.
func TestSnapshotEndpoint(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	const cid = "snap-vcp"
	s.waitReady(t, cid)

	cs := kubeClientForCluster(t, s, cid)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create a couple of objects so the cache has something to show.
	if err := waitForNamespace(ctx, cs, "default"); err != nil {
		t.Fatalf("default namespace never appeared: %v", err)
	}
	cmName := "snap-cm-" + randSuffix(4)
	if _, err := cs.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmName},
		Data:       map[string]string{"hello": "kplane"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create configmap: %v", err)
	}

	// Listing once warms the MultiClusterInformer for configmaps so it
	// shows up in the snapshot. The snapshot endpoint deliberately only
	// reports live MCIs to avoid surprise watches.
	if _, err := cs.CoreV1().ConfigMaps("default").List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("list configmaps to warm cache: %v", err)
	}
	if _, err := cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("list namespaces to warm cache: %v", err)
	}

	// Give the informer a brief moment to absorb the writes.
	time.Sleep(500 * time.Millisecond)

	snap := mustFetchSnapshot(t, s, cid, "")
	if snap.Cluster != cid {
		t.Fatalf("snapshot cluster mismatch: got %q want %q", snap.Cluster, cid)
	}
	if snap.LiveResources == 0 {
		t.Fatalf("expected at least one live resource, got 0")
	}

	found := false
	for _, r := range snap.Resources {
		if r.Resource == "configmaps" {
			for _, item := range r.Items {
				name, _ := nestedString(item, "metadata", "name")
				if name == cmName {
					found = true
					break
				}
			}
		}
	}
	if !found {
		body, _ := json.MarshalIndent(snap, "", "  ")
		t.Fatalf("snapshot did not contain configmap %q\nresponse:\n%s", cmName, string(body))
	}

	// resource= filter narrows the response.
	filtered := mustFetchSnapshot(t, s, cid, "?resource=configmaps")
	for _, r := range filtered.Resources {
		if r.Resource != "configmaps" {
			t.Fatalf("filter=configmaps returned unexpected resource %q", r.Resource)
		}
	}
}

type snapshotResp struct {
	Cluster       string `json:"cluster"`
	SnapshotTime  string `json:"snapshotTime"`
	LiveResources int    `json:"liveResources"`
	Resources     []struct {
		Group     string                   `json:"group"`
		Resource  string                   `json:"resource"`
		Synced    bool                     `json:"synced"`
		ItemCount int                      `json:"itemCount"`
		Items     []map[string]interface{} `json:"items"`
	} `json:"resources"`
}

func mustFetchSnapshot(t *testing.T, s *testAPIServer, cid, query string) snapshotResp {
	t.Helper()
	url := fmt.Sprintf("%s/snapshot%s", s.clusterURL(cid), query)
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer smoketoken")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("fetch snapshot: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("snapshot status=%d body=%s", resp.StatusCode, string(body))
	}
	var out snapshotResp
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode snapshot: %v\nbody=%s", err, string(body))
	}
	return out
}

func nestedString(m map[string]interface{}, path ...string) (string, bool) {
	var cur interface{} = m
	for _, p := range path {
		mp, ok := cur.(map[string]interface{})
		if !ok {
			return "", false
		}
		cur, ok = mp[p]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}
