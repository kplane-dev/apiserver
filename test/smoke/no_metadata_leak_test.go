package smoke

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TestNoMetadataLeakOnExternalObjects verifies that cluster identity metadata
// (labels, annotations) never appears on objects returned through the API.
// The ObjectWithClusterIdentity envelope must be fully stripped before objects
// reach the client via Create response, Get, List, and Watch.
func TestNoMetadataLeakOnExternalObjects(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	clusters := []string{s.root, "c-" + randSuffix(3), "c-" + randSuffix(3)}

	for _, cid := range clusters {
		cs := kubeClientForCluster(t, s, cid)
		ns := "default"
		if err := waitForNamespace(ctx, cs, ns); err != nil {
			t.Fatalf("cluster=%s wait namespace: %v", cid, err)
		}

		cmName := "leak-test-" + randSuffix(4)

		// Start a watch before creating so we can catch the ADDED event.
		watcher, err := cs.CoreV1().ConfigMaps(ns).Watch(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("cluster=%s watch: %v", cid, err)
		}

		// --- Create response ---
		created, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:   cmName,
				Labels: map[string]string{"user-label": "yes"},
			},
			Data: map[string]string{"k": "v"},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("cluster=%s create: %v", cid, err)
		}
		assertNoIdentityMetadata(t, cid, "create-response", created)

		// --- Get ---
		got, err := cs.CoreV1().ConfigMaps(ns).Get(ctx, cmName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("cluster=%s get: %v", cid, err)
		}
		assertNoIdentityMetadata(t, cid, "get", got)

		// --- List ---
		list, err := cs.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("cluster=%s list: %v", cid, err)
		}
		for i := range list.Items {
			if list.Items[i].Name == cmName {
				assertNoIdentityMetadata(t, cid, "list", &list.Items[i])
			}
		}

		// --- Watch ---
		deadline := time.After(10 * time.Second)
	watchLoop:
		for {
			select {
			case evt := <-watcher.ResultChan():
				if evt.Type == watch.Bookmark || evt.Object == nil {
					continue
				}
				cm, ok := evt.Object.(*corev1.ConfigMap)
				if !ok {
					continue
				}
				if cm.Name == cmName {
					assertNoIdentityMetadata(t, cid, "watch", cm)
					break watchLoop
				}
			case <-deadline:
				t.Fatalf("cluster=%s timed out waiting for watch event for %s", cid, cmName)
			}
		}
		watcher.Stop()

		// --- Update response ---
		got.Data["k"] = "v2"
		updated, err := cs.CoreV1().ConfigMaps(ns).Update(ctx, got, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("cluster=%s update: %v", cid, err)
		}
		assertNoIdentityMetadata(t, cid, "update-response", updated)

		// Cleanup
		_ = cs.CoreV1().ConfigMaps(ns).Delete(ctx, cmName, metav1.DeleteOptions{})
	}
}

// assertNoIdentityMetadata checks that a ConfigMap returned by the API has no
// cluster-identity-related labels or annotations. The only label allowed is
// the one explicitly set by the test ("user-label").
func assertNoIdentityMetadata(t *testing.T, cluster, path string, cm *corev1.ConfigMap) {
	t.Helper()

	// Check labels — only "user-label" should be present
	for k := range cm.Labels {
		if k == "user-label" {
			continue
		}
		if isIdentityMetadata(k) {
			t.Errorf("cluster=%s path=%s: unexpected identity label %q=%q on %s", cluster, path, k, cm.Labels[k], cm.Name)
		}
	}

	// Check annotations — none expected from identity system
	for k := range cm.Annotations {
		if isIdentityMetadata(k) {
			t.Errorf("cluster=%s path=%s: unexpected identity annotation %q=%q on %s", cluster, path, k, cm.Annotations[k], cm.Name)
		}
	}
}

func isIdentityMetadata(key string) bool {
	suspects := []string{
		"cluster", "clusterid", "cluster-id", "cluster_id",
		"kplane", "multicluster", "storage-key", "storagekey",
		"identity", "vcp",
	}
	lower := strings.ToLower(key)
	for _, s := range suspects {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}
