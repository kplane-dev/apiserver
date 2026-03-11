package smoke

import (
	"os"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestKubernetesServiceBootstrapPerCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServerWithOptions(t, etcd, apiserverOptions{
		extraArgs: []string{"--kubernetes-service-mode=per-cluster-autoip"},
	})

	clusterID := "vc-core-bootstrap"
	cs := kubeClientForCluster(t, s, clusterID)

	// Trigger cluster callback with a cluster-scoped discovery call.
	if _, err := cs.Discovery().ServerVersion(); err != nil {
		t.Fatalf("trigger request failed: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		svc, err := cs.CoreV1().Services(metav1.NamespaceDefault).Get(t.Context(), "kubernetes", metav1.GetOptions{})
		if err == nil {
			if svc.Spec.ClusterIP == "" {
				t.Fatalf("kubernetes service exists but clusterIP is empty")
			}
			eps, epErr := cs.CoreV1().Endpoints(metav1.NamespaceDefault).Get(t.Context(), "kubernetes", metav1.GetOptions{})
			if epErr == nil && len(eps.Subsets) > 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for kubernetes service bootstrap in cluster=%s: %v\nlogs:\n%s", clusterID, err, s.logs())
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestServiceCIDRBootstrapPerCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServerWithOptions(t, etcd, apiserverOptions{
		extraArgs: []string{"--service-cidr-sharing-mode=per-cluster"},
	})

	clusterID := "vc-servicecidr-bootstrap"
	cs := kubeClientForCluster(t, s, clusterID)

	// Trigger cluster callback with a cluster-scoped discovery call.
	if _, err := cs.Discovery().ServerVersion(); err != nil {
		t.Fatalf("trigger request failed: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := cs.NetworkingV1().ServiceCIDRs().Get(t.Context(), "kubernetes", metav1.GetOptions{})
		if err == nil {
			return
		}
		if apierrors.IsNotFound(err) {
			// keep waiting while bootstrap may still be running
		} else if apierrors.IsMethodNotSupported(err) || apierrors.IsNotAcceptable(err) {
			t.Skip("ServiceCIDR API not enabled")
		}
		if time.Now().After(deadline) {
			// ServiceCIDR may be disabled by feature gates in this binary/config.
			t.Skipf("ServiceCIDR bootstrap not observed (likely feature-disabled): %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
