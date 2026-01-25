package multicluster

import (
	"context"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
)

// RESTOptionsDecorator wraps the underlying getter to inject a decorator that
// wraps the storage.Interface with our key-rewriting adapter so keys include cluster.

type RESTOptionsDecorator struct {
	Delegate generic.RESTOptionsGetter
	Options  Options
}

func (w RESTOptionsDecorator) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	opts, err := w.Delegate.GetRESTOptions(resource, example)
	if err != nil {
		return opts, err
	}
	base := opts.Decorator
	if base == nil {
		base = generic.UndecoratedStorage
	}
	layout := KeyLayout{Options: w.Options}
	opts.Decorator = func(
		config *storagebackend.ConfigForResource,
		resourcePrefix string,
		keyFunc func(obj runtime.Object) (string, error),
		newFunc func() runtime.Object,
		newListFunc func() runtime.Object,
		getAttrsFunc storage.AttrFunc,
		trigger storage.IndexerFuncs,
		indexers *cache.Indexers,
	) (storage.Interface, factory.DestroyFunc, error) {
		return newClusteredStorage(
			base,
			config,
			resourcePrefix,
			keyFunc,
			newFunc,
			newListFunc,
			getAttrsFunc,
			trigger,
			indexers,
			w.Options,
			layout,
		)
	}
	return opts, nil
}

type clusteredStorage struct {
	base           generic.StorageDecorator
	config         *storagebackend.ConfigForResource
	resourcePrefix string
	keyFunc        func(obj runtime.Object) (string, error)
	newFunc        func() runtime.Object
	newListFunc    func() runtime.Object
	getAttrsFunc   storage.AttrFunc
	trigger        storage.IndexerFuncs
	indexers       *cache.Indexers
	options        Options
	layout         KeyLayout

	mu       sync.Mutex
	stores   map[string]storage.Interface
	destroys map[string]factory.DestroyFunc
	global   storage.Interface
}

func newClusteredStorage(
	base generic.StorageDecorator,
	config *storagebackend.ConfigForResource,
	resourcePrefix string,
	keyFunc func(obj runtime.Object) (string, error),
	newFunc func() runtime.Object,
	newListFunc func() runtime.Object,
	getAttrsFunc storage.AttrFunc,
	trigger storage.IndexerFuncs,
	indexers *cache.Indexers,
	options Options,
	layout KeyLayout,
) (storage.Interface, factory.DestroyFunc, error) {
	cs := &clusteredStorage{
		base:           base,
		config:         config,
		resourcePrefix: resourcePrefix,
		keyFunc:        keyFunc,
		newFunc:        newFunc,
		newListFunc:    newListFunc,
		getAttrsFunc:   getAttrsFunc,
		trigger:        trigger,
		indexers:       indexers,
		options:        options,
		layout:         layout,
		stores:         map[string]storage.Interface{},
		destroys:       map[string]factory.DestroyFunc{},
	}
	return cs, cs.destroy, nil
}

func (c *clusteredStorage) Versioner() storage.Versioner {
	store, err := c.storeForCluster(c.defaultCluster())
	if err != nil {
		return nil
	}
	return store.Versioner()
}

func (c *clusteredStorage) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.Create(ctx, key, obj, out, ttl)
}

func (c *clusteredStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts)
}

func (c *clusteredStorage) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return store.Watch(ctx, key, opts)
}

func (c *clusteredStorage) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.Get(ctx, key, opts, objPtr)
}

func (c *clusteredStorage) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.GetList(ctx, key, opts, listObj)
}

func (c *clusteredStorage) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, precond *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, precond, tryUpdate, cachedExistingObject)
}

func (c *clusteredStorage) Stats(ctx context.Context) (storage.Stats, error) {
	store, _, err := c.storeAndKey(ctx, "")
	if err != nil {
		return storage.Stats{}, err
	}
	return store.Stats(ctx)
}

func (c *clusteredStorage) ReadinessCheck() error {
	store, err := c.storeForCluster(c.defaultCluster())
	if err != nil {
		return err
	}
	return store.ReadinessCheck()
}

func (c *clusteredStorage) RequestWatchProgress(ctx context.Context) error {
	store, _, err := c.storeAndKey(ctx, "")
	if err != nil {
		return err
	}
	return store.RequestWatchProgress(ctx)
}

func (c *clusteredStorage) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	store, _, err := c.storeAndKey(ctx, "")
	if err != nil {
		return 0, err
	}
	return store.GetCurrentResourceVersion(ctx)
}

func (c *clusteredStorage) SetKeysFunc(keysFunc storage.KeysFunc) {
	store, err := c.storeForCluster(c.defaultCluster())
	if err != nil {
		return
	}
	store.SetKeysFunc(keysFunc)
}

func (c *clusteredStorage) CompactRevision() int64 {
	store, err := c.storeForCluster(c.defaultCluster())
	if err != nil {
		return 0
	}
	return store.CompactRevision()
}

func (c *clusteredStorage) destroy() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, destroy := range c.destroys {
		destroy()
	}
}

func (c *clusteredStorage) storeAndKey(ctx context.Context, key string) (storage.Interface, string, error) {
	cid, all, _ := FromContext(ctx)
	if all {
		store, err := c.globalStore()
		return store, key, err
	}
	if cid == "" {
		cid = c.defaultCluster()
	}
	store, err := c.storeForCluster(cid)
	if err != nil {
		return nil, key, err
	}
	return store, c.rewriteKey(cid, key), nil
}

func (c *clusteredStorage) defaultCluster() string {
	if c.options.DefaultCluster != "" {
		return c.options.DefaultCluster
	}
	return DefaultClusterName
}

func (c *clusteredStorage) rewriteKey(cluster, key string) string {
	return injectClusterSegment(key, cluster, c.layout)
}

func (c *clusteredStorage) storeForCluster(cluster string) (storage.Interface, error) {
	cluster = strings.TrimSpace(cluster)
	if cluster == "" {
		cluster = c.defaultCluster()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if store, ok := c.stores[cluster]; ok {
		return store, nil
	}
	cfg := *c.config
	clusterPrefix := injectClusterSegment(c.resourcePrefix, cluster, c.layout)
	keyFunc := func(obj runtime.Object) (string, error) {
		key, err := c.keyFunc(obj)
		if err != nil {
			return "", err
		}
		return injectClusterSegment(key, cluster, c.layout), nil
	}
	store, destroy, err := c.base(&cfg, clusterPrefix, keyFunc, c.newFunc, c.newListFunc, c.getAttrsFunc, c.trigger, c.indexers)
	if err != nil {
		return nil, err
	}
	c.stores[cluster] = store
	c.destroys[cluster] = destroy
	return store, nil
}

func (c *clusteredStorage) globalStore() (storage.Interface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.global != nil {
		return c.global, nil
	}
	cfg := *c.config
	store, destroy, err := c.base(&cfg, c.resourcePrefix, c.keyFunc, c.newFunc, c.newListFunc, c.getAttrsFunc, c.trigger, c.indexers)
	if err != nil {
		return nil, err
	}
	c.global = store
	c.destroys["__global__"] = destroy
	return store, nil
}

func injectClusterSegment(key, cluster string, layout KeyLayout) string {
	if cluster == "" {
		cluster = DefaultClusterName
	}
	leadingSlash := strings.HasPrefix(key, "/")
	trimmed := strings.TrimPrefix(key, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		if leadingSlash {
			return "/"
		}
		return key
	}
	// Compute position after kind-root: /<prefix>/<group?>/<resource>/... => insert after resource
	resIdx := 0
	p := layout.Options.EtcdPrefix
	if p == "" {
		p = DefaultEtcdPrefix
	}
	if parts[0] == strings.TrimPrefix(p, "/") { // starts with etcd prefix sans leading '/'
		if len(parts) >= 3 && strings.Contains(parts[1], ".") {
			resIdx = 2
		} else {
			resIdx = 1
		}
	} else if strings.Contains(parts[0], ".") {
		resIdx = 1
	} else {
		resIdx = 0
	}
	if resIdx >= len(parts) {
		resIdx = len(parts) - 1
	}
	insertPos := resIdx + 1
	if insertPos < len(parts) && parts[insertPos] == cluster {
		if leadingSlash {
			return "/" + strings.Join(parts, "/")
		}
		return strings.Join(parts, "/")
	}
	newParts := append(append([]string{}, parts[:insertPos]...), append([]string{cluster}, parts[insertPos:]...)...)
	if leadingSlash {
		return "/" + strings.Join(newParts, "/")
	}
	return strings.Join(newParts, "/")
}

func matchesCluster(obj runtime.Object, cid, annotationKey string) bool {
	if annotationKey == "" {
		annotationKey = DefaultClusterAnnotation
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return false
	}
	anns := acc.GetAnnotations()
	if anns == nil {
		return false
	}
	return anns[annotationKey] == cid
}

func filterListByCluster(listObj runtime.Object, cid, annotationKey string) error {
	items, err := meta.ExtractList(listObj)
	if err != nil {
		return err
	}
	filtered := make([]runtime.Object, 0, len(items))
	for _, it := range items {
		if matchesCluster(it, cid, annotationKey) {
			filtered = append(filtered, it)
		}
	}
	return meta.SetList(listObj, filtered)
}
