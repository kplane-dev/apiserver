package smoke

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestListAfterRestart reproduces the cacher keying bug where LIST returns
// empty for some VCPs after an apiserver restart.
//
// Root cause: the reflector's WatchList uses a temporaryStore keyed by
// DeletionHandlingMetaNamespaceKeyFunc (namespace/name). When multiple VCPs
// have same-named objects (every VCP has "default", "kube-system", etc.),
// the temporaryStore deduplicates them. Only the last VCP's objects survive.
// Replace sends the deduplicated set to the watchCache, so most VCPs
// get empty LIST results.
//
// This test does NOT require any etcd manipulation — a simple restart
// with existing multi-VCP data triggers the bug.
func TestListAfterRestart(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	if strings.TrimSpace(etcd) == "" {
		t.Skip("ETCD_ENDPOINTS is not set")
	}

	prefix := randomEtcdPrefix("restart")

	// Start apiserver and create VCPs
	s := startAPIServerWithOptions(t, etcd, apiserverOptions{etcdPrefix: prefix})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clusters := []string{"vcp-alpha", "vcp-beta", "vcp-gamma"}
	for _, cid := range clusters {
		cs := kubeClientForCluster(t, s, cid)
		if err := waitForNamespace(ctx, cs, "default"); err != nil {
			t.Fatalf("cluster %s: bootstrap failed: %v", cid, err)
		}
	}

	// Verify all VCPs work before restart
	for _, cid := range clusters {
		verifyListAndGet(ctx, t, s, cid, "before-restart")
	}
	verifyListAndGet(ctx, t, s, s.root, "before-restart-root")
	t.Log("pre-restart: all VCPs return namespaces via LIST")

	// Restart apiserver — same etcd, no data manipulation
	s.Stop()
	s2 := startAPIServerWithOptions(t, etcd, apiserverOptions{
		etcdPrefix: prefix,
		port:       s.port,
	})

	// Wait for VCPs to be accessible
	time.Sleep(5 * time.Second)

	// Verify all VCPs still work after restart
	for _, cid := range clusters {
		verifyListAndGet(ctx, t, s2, cid, "after-restart")
	}
	verifyListAndGet(ctx, t, s2, s2.root, "after-restart-root")
	t.Log("post-restart: all VCPs return namespaces via LIST")
}

func verifyListAndGet(ctx context.Context, t *testing.T, s *testAPIServer, clusterID, phase string) {
	t.Helper()
	cs := kubeClientForCluster(t, s, clusterID)

	nsList, err := cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("[%s] cluster %s: LIST error: %v", phase, clusterID, err)
	}

	ns, getErr := cs.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{})

	if len(nsList.Items) == 0 {
		if getErr == nil && ns != nil {
			t.Fatalf("[%s] cluster %s: BUG — LIST returned 0 namespaces but GET 'default' succeeded (uid=%s rv=%s). "+
				"The cacher's WatchList temporaryStore deduplicates objects by namespace/name across clusters.",
				phase, clusterID, ns.UID, ns.ResourceVersion)
		}
		t.Fatalf("[%s] cluster %s: LIST returned 0 namespaces, GET also failed: %v", phase, clusterID, getErr)
	}

	t.Logf("[%s] cluster %s: LIST=%d namespaces", phase, clusterID, len(nsList.Items))
}

func randomEtcdPrefix(label string) string {
	return "/registry-" + label + "-" + randSuffix(4)
}
