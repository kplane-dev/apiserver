package multicluster

import (
	"testing"

	"k8s.io/client-go/rest"
)

func TestClientPoolReusesClientPerCluster(t *testing.T) {
	base := &rest.Config{Host: "https://example.com"}
	pool := NewClientPool(base, DefaultOptions.PathPrefix, DefaultOptions.ControlPlaneSegment)

	c1, err := pool.KubeClientForCluster("c1")
	if err != nil {
		t.Fatalf("KubeClientForCluster c1: %v", err)
	}
	c1b, err := pool.KubeClientForCluster("c1")
	if err != nil {
		t.Fatalf("KubeClientForCluster c1 second: %v", err)
	}
	if c1 != c1b {
		t.Fatalf("expected client reuse for same cluster")
	}

	c2, err := pool.KubeClientForCluster("c2")
	if err != nil {
		t.Fatalf("KubeClientForCluster c2: %v", err)
	}
	if c1 == c2 {
		t.Fatalf("expected different client instances for different clusters")
	}
}
