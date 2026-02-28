package scopedinformer

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	mcstorage "github.com/kplane-dev/apiserver/pkg/multicluster/storage"
)

func TestObjectCluster_FromInternalEntryClusterID(t *testing.T) {
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm"}}
	entry := &mcstorage.InternalEntry{
		Object:    obj,
		ClusterID: "c1",
	}
	if got := ObjectCluster(entry); got != "c1" {
		t.Fatalf("expected c1 from internal entry, got %q", got)
	}
}

func TestObjectCluster_FromInternalEntryWithoutClusterID(t *testing.T) {
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm"}}
	entry := &mcstorage.InternalEntry{
		Object: obj,
	}
	if got := ObjectCluster(entry); got != "" {
		t.Fatalf("expected empty cluster from internal entry without ClusterID, got %q", got)
	}
}

func TestObjectCluster_FromTombstoneInternalEntry(t *testing.T) {
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm"}}
	entry := &mcstorage.InternalEntry{
		Object:    obj,
		ClusterID: "c3",
	}
	tombstone := cache.DeletedFinalStateUnknown{Obj: entry}
	if got := ObjectCluster(tombstone); got != "c3" {
		t.Fatalf("expected c3 from tombstone internal entry, got %q", got)
	}
}

func TestObjectCluster_NonEntryHasNoFallback(t *testing.T) {
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm"}}
	if got := ObjectCluster(obj); got != "" {
		t.Fatalf("expected empty cluster for non-entry object, got %q", got)
	}
}

func TestFilteredByCluster_UnwrapsInternalEntry(t *testing.T) {
	indexer := cache.NewIndexer(func(obj interface{}) (string, error) {
		if e, ok := obj.(*mcstorage.InternalEntry); ok && e != nil {
			if e.StorageKey != "" {
				return e.StorageKey, nil
			}
			return "entry/default/cm", nil
		}
		return cache.MetaNamespaceKeyFunc(obj)
	}, cache.Indexers{
		ClusterIndexName: func(obj interface{}) ([]string, error) {
			return []string{ObjectCluster(obj)}, nil
		},
	})

	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "default"}}
	entry := &mcstorage.InternalEntry{Object: obj, ClusterID: "c5"}
	if err := indexer.Add(entry); err != nil {
		t.Fatalf("add: %v", err)
	}
	items := FilteredByCluster(indexer, "c5")
	if len(items) != 1 {
		t.Fatalf("expected one item, got %d", len(items))
	}
	if _, ok := items[0].(*corev1.ConfigMap); !ok {
		t.Fatalf("expected unwrapped ConfigMap, got %T", items[0])
	}
}
