package storage

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

func TestNewEntryWatch_EmitsRuntimeObjects(t *testing.T) {
	fw := watch.NewFake()
	defer fw.Stop()

	out := NewEntryWatch(fw)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm"}}
	fw.Add(&InternalEntry{Object: cm, ClusterID: "c1", StorageKey: "/registry/configmaps/clusters/c1/default/cm"})

	evt, ok := <-out.ResultChan()
	if !ok {
		t.Fatalf("expected watch event")
	}
	if evt.Type != watch.Added {
		t.Fatalf("event type=%s want=%s", evt.Type, watch.Added)
	}
	got, ok := evt.Object.(*corev1.ConfigMap)
	if !ok {
		t.Fatalf("expected ConfigMap object, got %T", evt.Object)
	}
	if got.Name != "cm" {
		t.Fatalf("name=%q want=cm", got.Name)
	}
}

