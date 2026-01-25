package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Ensure NamespaceLifecycle uses cluster-scoped namespace data instead of root cache.
func TestNamespaceLifecycle_VirtualClusterNamespace(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	csA := kubeClientForCluster(t, s, clusterA)
	csRoot := kubeClientForCluster(t, s, s.root)

	ns := "ns-" + randSuffix(4)
	_, err := csA.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create namespace: %v", clusterA, err)
	}

	// Root should not see virtual cluster namespaces.
	_, err = csRoot.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("expected root to not have namespace %q, got: %v", ns, err)
	}

	saName := "sa-" + randSuffix(4)
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := csA.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: saName},
		}, metav1.CreateOptions{})
		if err == nil {
			return true, nil
		}
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	})
	if err != nil {
		t.Fatalf("cluster=%s create serviceaccount in namespace %q: %v", clusterA, ns, err)
	}
}
