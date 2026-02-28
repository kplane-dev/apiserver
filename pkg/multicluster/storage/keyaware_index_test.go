package storage

import "testing"

func TestClusterScopedObjectKey(t *testing.T) {
	got := ClusterScopedObjectKey("c1", "default", "obj")
	want := "c1/default/obj"
	if got != want {
		t.Fatalf("key=%q want=%q", got, want)
	}
}

