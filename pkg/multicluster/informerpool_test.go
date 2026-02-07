package multicluster

import (
	"testing"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInformerPool_ReusesFactoryPerCluster(t *testing.T) {
	pool := NewInformerPool(InformerPoolOptions{
		ClientForCluster: func(clusterID string) (kubernetes.Interface, error) {
			return fake.NewSimpleClientset(), nil
		},
		StartOnGet: false,
	})

	_, f1, _, err := pool.Get("c1")
	if err != nil {
		t.Fatalf("Get c1: %v", err)
	}
	_, f1b, _, err := pool.Get("c1")
	if err != nil {
		t.Fatalf("Get c1 again: %v", err)
	}
	if f1 != f1b {
		t.Fatalf("expected same informer factory for same cluster")
	}

	_, f2, _, err := pool.Get("c2")
	if err != nil {
		t.Fatalf("Get c2: %v", err)
	}
	if f1 == f2 {
		t.Fatalf("expected different informer factories for different clusters")
	}

	if _, ok := f1.(informers.SharedInformerFactory); !ok {
		t.Fatalf("expected shared informer factory type")
	}
}
