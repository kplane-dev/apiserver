package multicluster

import (
	"context"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
)

type fakeStorage struct{}

func (f *fakeStorage) Versioner() storage.Versioner { return nil }
func (f *fakeStorage) Create(context.Context, string, runtime.Object, runtime.Object, uint64) error {
	return nil
}
func (f *fakeStorage) Delete(context.Context, string, runtime.Object, *storage.Preconditions, storage.ValidateObjectFunc, runtime.Object, storage.DeleteOptions) error {
	return nil
}
func (f *fakeStorage) Watch(context.Context, string, storage.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}
func (f *fakeStorage) Get(context.Context, string, storage.GetOptions, runtime.Object) error {
	return nil
}
func (f *fakeStorage) GetList(context.Context, string, storage.ListOptions, runtime.Object) error {
	return nil
}
func (f *fakeStorage) GuaranteedUpdate(context.Context, string, runtime.Object, bool, *storage.Preconditions, storage.UpdateFunc, runtime.Object) error {
	return nil
}
func (f *fakeStorage) Stats(context.Context) (storage.Stats, error) { return storage.Stats{}, nil }
func (f *fakeStorage) ReadinessCheck() error                        { return nil }
func (f *fakeStorage) RequestWatchProgress(context.Context) error   { return nil }
func (f *fakeStorage) GetCurrentResourceVersion(context.Context) (uint64, error) {
	return 0, nil
}
func (f *fakeStorage) SetKeysFunc(storage.KeysFunc) {}
func (f *fakeStorage) CompactRevision() int64       { return 0 }

func TestRewriteKey_ClusterInsertion(t *testing.T) {
	cs := &clusteredStorage{
		resourcePrefix: "/registry/pods",
		options:        Options{DefaultCluster: "root"},
	}
	if got := cs.rewriteKey("c1", "/registry/pods/default/nginx"); got != "/registry/pods/clusters/c1/default/nginx" {
		t.Fatalf("unexpected key: %s", got)
	}
	if got := cs.rewriteKey("c1", "/registry/pods/default"); got != "/registry/pods/clusters/c1/default" {
		t.Fatalf("unexpected list key: %s", got)
	}
	if got := cs.rewriteKey("c1", "/registry/pods"); got != "/registry/pods/clusters/c1" {
		t.Fatalf("unexpected root key: %s", got)
	}
	if got := cs.rewriteKey("c1", "/registry/pods/clusters/c1/default/nginx"); got != "/registry/pods/clusters/c1/default/nginx" {
		t.Fatalf("unexpected already-clustered key: %s", got)
	}
}

func TestRewriteKey_ClusterScoped(t *testing.T) {
	cs := &clusteredStorage{
		resourcePrefix: "/registry/nodes",
		options:        Options{DefaultCluster: "root"},
	}
	if got := cs.rewriteKey("c2", "/registry/nodes/node-1"); got != "/registry/nodes/clusters/c2/node-1" {
		t.Fatalf("unexpected key: %s", got)
	}
}

func TestEnsureStore_SinglePerResource(t *testing.T) {
	var calls int32
	base := func(
		_ *storagebackend.ConfigForResource,
		_ string,
		_ func(obj runtime.Object) (string, error),
		_ func() runtime.Object,
		_ func() runtime.Object,
		_ storage.AttrFunc,
		_ storage.IndexerFuncs,
		_ *cache.Indexers,
	) (storage.Interface, factory.DestroyFunc, error) {
		atomic.AddInt32(&calls, 1)
		return &fakeStorage{}, func() {}, nil
	}

	cs, destroy, err := newClusteredStorage(
		base,
		&storagebackend.ConfigForResource{},
		"/registry/pods",
		func(obj runtime.Object) (string, error) { return "/registry/pods/default/nginx", nil },
		func() runtime.Object { return &corev1.Pod{} },
		func() runtime.Object { return &corev1.PodList{} },
		nil,
		nil,
		nil,
		Options{DefaultCluster: "root"},
	)
	if err != nil {
		t.Fatalf("newClusteredStorage: %v", err)
	}
	defer destroy()

	ctxA := WithCluster(context.Background(), "c1", false)
	ctxB := WithCluster(context.Background(), "c2", false)

	_, keyA, err := cs.(*clusteredStorage).storeAndKey(ctxA, "/registry/pods/default/nginx")
	if err != nil {
		t.Fatalf("storeAndKey c1: %v", err)
	}
	_, keyB, err := cs.(*clusteredStorage).storeAndKey(ctxB, "/registry/pods/default/nginx")
	if err != nil {
		t.Fatalf("storeAndKey c2: %v", err)
	}
	if keyA == keyB {
		t.Fatalf("expected different keys for different clusters, got %s", keyA)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected base to be called once, got %d", got)
	}
}

func TestStoreAndKey_AllClustersRoot(t *testing.T) {
	var calls int32
	base := func(
		_ *storagebackend.ConfigForResource,
		_ string,
		_ func(obj runtime.Object) (string, error),
		_ func() runtime.Object,
		_ func() runtime.Object,
		_ storage.AttrFunc,
		_ storage.IndexerFuncs,
		_ *cache.Indexers,
	) (storage.Interface, factory.DestroyFunc, error) {
		atomic.AddInt32(&calls, 1)
		return &fakeStorage{}, func() {}, nil
	}

	cs, destroy, err := newClusteredStorage(
		base,
		&storagebackend.ConfigForResource{},
		"/registry/pods",
		func(obj runtime.Object) (string, error) { return "/registry/pods/default/nginx", nil },
		func() runtime.Object { return &corev1.Pod{} },
		func() runtime.Object { return &corev1.PodList{} },
		nil,
		nil,
		nil,
		Options{DefaultCluster: "root"},
	)
	if err != nil {
		t.Fatalf("newClusteredStorage: %v", err)
	}
	defer destroy()

	ctxAll := WithCluster(context.Background(), "", true)
	_, key, err := cs.(*clusteredStorage).storeAndKey(ctxAll, "/registry/pods")
	if err != nil {
		t.Fatalf("storeAndKey all: %v", err)
	}
	if key != "/registry/pods/clusters" {
		t.Fatalf("unexpected all-clusters key: %s", key)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected base to be called once, got %d", got)
	}
}
