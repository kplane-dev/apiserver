package smoke

import (
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSystemNamespacesBootstrapPerCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	clusterID := "vc-bootstrap"
	cs := kubeClientForCluster(t, s, clusterID)
	want := []string{"default", "kube-system", "kube-public", "kube-node-lease"}

	deadline := time.Now().Add(10 * time.Second)
	for {
		missing := ""
		for _, ns := range want {
			if _, err := cs.CoreV1().Namespaces().Get(t.Context(), ns, metav1.GetOptions{}); err != nil {
				missing = ns
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for system namespace bootstrap in cluster=%s\nlogs:\n%s", clusterID, s.logs())
		}
		time.Sleep(250 * time.Millisecond)
	}
}
