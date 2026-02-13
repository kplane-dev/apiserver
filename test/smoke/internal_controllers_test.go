package smoke

import (
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterAuthInfoControllerPerCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	clusterID := "vc-authinfo-bootstrap"
	cs := kubeClientForCluster(t, s, clusterID)
	if _, err := cs.Discovery().ServerVersion(); err != nil {
		t.Fatalf("trigger request failed: %v", err)
	}

	deadline := time.Now().Add(12 * time.Second)
	for {
		// Root apiserver startup logs one instance; non-root bootstrap should start another.
		if strings.Count(s.logs(), "Starting cluster_authentication_trust_controller controller") >= 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for per-cluster cluster_authentication_trust_controller start in cluster=%s\nlogs:\n%s", clusterID, s.logs())
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestLegacyTokenTrackingControllerPerCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	clusterID := "vc-legacy-token-bootstrap"
	cs := kubeClientForCluster(t, s, clusterID)
	if _, err := cs.Discovery().ServerVersion(); err != nil {
		t.Fatalf("trigger request failed: %v", err)
	}

	deadline := time.Now().Add(12 * time.Second)
	for {
		cm, err := cs.CoreV1().ConfigMaps("kube-system").Get(t.Context(), "kube-apiserver-legacy-service-account-token-tracking", metav1.GetOptions{})
		if err == nil && cm.Data != nil && cm.Data["since"] != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for legacy token tracking configmap in cluster=%s: %v\nlogs:\n%s", clusterID, err, s.logs())
		}
		time.Sleep(250 * time.Millisecond)
	}
}
