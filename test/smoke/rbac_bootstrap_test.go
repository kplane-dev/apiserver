package smoke

import (
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRBACBootstrapPerCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	clusterID := "vc-rbac-bootstrap"
	cs := kubeClientForCluster(t, s, clusterID)

	// Trigger a request in the target cluster, which runs our per-cluster bootstrap callback.
	if _, err := cs.Discovery().ServerVersion(); err != nil {
		t.Fatalf("trigger request failed: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := cs.RbacV1().ClusterRoles().Get(t.Context(), "cluster-admin", metav1.GetOptions{})
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for RBAC bootstrap in cluster=%s: %v\nlogs:\n%s", clusterID, err, s.logs())
		}
		time.Sleep(250 * time.Millisecond)
	}
}
