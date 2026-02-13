package bootstrap

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSystemNamespaceBootstrapperEnsureCreatesOnce(t *testing.T) {
	cs := fake.NewSimpleClientset()
	var clientCalls int32
	b := NewSystemNamespaceBootstrapper(SystemNamespaceOptions{
		ClientForCluster: func(clusterID string) (kubernetes.Interface, error) {
			atomic.AddInt32(&clientCalls, 1)
			return cs, nil
		},
		Namespaces: []string{"default", "kube-system", "kube-public"},
		Timeout:    2 * time.Second,
	})

	b.Ensure("vc-1")
	b.Ensure("vc-1")

	if got := atomic.LoadInt32(&clientCalls); got != 1 {
		t.Fatalf("expected one client creation call, got %d", got)
	}
	for _, ns := range []string{"default", "kube-system", "kube-public"} {
		if _, err := cs.CoreV1().Namespaces().Get(t.Context(), ns, metav1.GetOptions{}); err != nil {
			t.Fatalf("expected namespace %q to exist: %v", ns, err)
		}
	}
}

func TestSystemNamespaceBootstrapperEnsureRetriesAfterFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	var calls int32
	b := NewSystemNamespaceBootstrapper(SystemNamespaceOptions{
		ClientForCluster: func(clusterID string) (kubernetes.Interface, error) {
			n := atomic.AddInt32(&calls, 1)
			if n == 1 {
				return nil, fmt.Errorf("temporary client error")
			}
			return cs, nil
		},
		Namespaces: []string{"default"},
		Timeout:    2 * time.Second,
	})

	b.Ensure("vc-2")
	if _, err := cs.CoreV1().Namespaces().Get(t.Context(), "default", metav1.GetOptions{}); err == nil {
		t.Fatalf("default namespace should not exist after failed first attempt")
	}

	b.Ensure("vc-2")
	if _, err := cs.CoreV1().Namespaces().Get(t.Context(), "default", metav1.GetOptions{}); err != nil {
		t.Fatalf("default namespace should exist after retry: %v", err)
	}
}

func TestSystemNamespaceBootstrapperEnsureSkipsEmptyCluster(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	var calls int32
	b := NewSystemNamespaceBootstrapper(SystemNamespaceOptions{
		ClientForCluster: func(clusterID string) (kubernetes.Interface, error) {
			atomic.AddInt32(&calls, 1)
			return cs, nil
		},
		Namespaces: []string{"kube-system"},
	})

	b.Ensure("")
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("expected no client calls for empty cluster, got %d", got)
	}
}
