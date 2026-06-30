package multicluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"

	"github.com/kplane-dev/apiserver/pkg/multicluster/internalcap"
	mcstorage "github.com/kplane-dev/apiserver/pkg/multicluster/storage"
	extstorage "github.com/kplane-dev/storage"
)

// RESTOptionsDecorator wraps the underlying getter to inject a decorator that
// wraps the storage.Interface with our key-rewriting adapter so keys include cluster.

type RESTOptionsDecorator struct {
	Delegate generic.RESTOptionsGetter
	Options  Options
}

var (
	ensureStoreSeq   uint64
	ensureStoreTotal = metrics.NewCounterVec(&metrics.CounterOpts{
		Name: "mc_storage_ensure_store_total",
		Help: "Number of base stores created by the multicluster storage decorator.",
	}, []string{"server", "resourcePrefix"})
	baseDecoratorTotal = metrics.NewCounterVec(&metrics.CounterOpts{
		Name: "mc_storage_base_decorator_total",
		Help: "Number of base storage decorator invocations by server and resource prefix.",
	}, []string{"server", "resourcePrefix"})
	debugStoreAndKey = os.Getenv("MC_STOREANDKEY_DEBUG") == "1"

	// ErrAllClustersScopeForbidden is returned when all-clusters scope is requested
	// without trusted internal capability.
	ErrAllClustersScopeForbidden = errors.New("all-clusters scope is internal-only")
	// ErrAllClustersMutationForbidden is returned when write operations attempt all-clusters scope.
	ErrAllClustersMutationForbidden = errors.New("mutating operations are not allowed for all-clusters scope")
)

func init() {
	legacyregistry.MustRegister(ensureStoreTotal, baseDecoratorTotal)
}

func (w RESTOptionsDecorator) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	opts, err := w.Delegate.GetRESTOptions(resource, example)
	if err != nil {
		return opts, err
	}
	// Use StorageWithClusterIdentity as the base decorator instead of the
	// default StorageWithCacher. This configures the cacher pipeline with
	// identity hooks (IdentityFromKey, WrapWatchObject, UnwrapObject) and
	// sets WrapDecodedObject on the etcd3 storage config so that decoded
	// objects carry their storage key through the watch.Event boundary.
	base := extstorage.StorageWithClusterIdentity(extstorage.DecoratorConfig{
		KeyLayout:      extstorage.DefaultKeyLayout(),
		GroupResource:  resource,
		BackendFactory: w.Options.BackendFactory,
	})
	base = wrapBaseDecorator(base, w.Options)
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
		)
	}
	return opts, nil
}

func wrapBaseDecorator(base generic.StorageDecorator, opts Options) generic.StorageDecorator {
	return func(
		config *storagebackend.ConfigForResource,
		resourcePrefix string,
		keyFunc func(obj runtime.Object) (string, error),
		newFunc func() runtime.Object,
		newListFunc func() runtime.Object,
		getAttrsFunc storage.AttrFunc,
		trigger storage.IndexerFuncs,
		indexers *cache.Indexers,
	) (storage.Interface, factory.DestroyFunc, error) {
		server := opts.ServerName
		if server == "" {
			server = "unknown"
		}
		baseDecoratorTotal.WithLabelValues(server, resourcePrefix).Inc()
		klog.V(2).Infof("mc.baseDecorator server=%s resourcePrefix=%s", server, resourcePrefix)
		return base(config, resourcePrefix, keyFunc, newFunc, newListFunc, getAttrsFunc, trigger, indexers)
	}
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

	mu        sync.Mutex
	store     storage.Interface
	destroyFn factory.DestroyFunc
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
	}
	if options.InformerRegistry != nil {
		options.InformerRegistry.RegisterStorage(config.GroupResource, cs)
	}
	return cs, cs.destroy, nil
}

func (c *clusteredStorage) Versioner() storage.Versioner {
	store, err := c.ensureStore()
	if err != nil {
		return nil
	}
	return store.Versioner()
}

func (c *clusteredStorage) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if err := c.rejectAllClustersMutation(ctx); err != nil {
		return err
	}
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.Create(ctx, key, obj, out, ttl)
}

func (c *clusteredStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	if err := c.rejectAllClustersMutation(ctx); err != nil {
		return err
	}
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
	w, err := store.Watch(ctx, key, opts)
	if err != nil {
		return nil, err
	}
	return mcstorage.NewEntryWatch(w), nil
}

func (c *clusteredStorage) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	store, rewrittenKey, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	if entry, ok := objPtr.(*mcstorage.InternalEntry); ok && entry != nil && entry.Object != nil {
		if err := store.Get(ctx, rewrittenKey, opts, entry.Object); err != nil {
			return err
		}
		entry.StorageKey = rewrittenKey
		if cid := extstorage.DefaultKeyLayout().ClusterFromKey(rewrittenKey); cid != "" {
			entry.ClusterID = cid
		}
		return nil
	}
	return store.Get(ctx, rewrittenKey, opts, objPtr)
}

func (c *clusteredStorage) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.GetList(ctx, key, opts, listObj)
}

func (c *clusteredStorage) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, precond *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	if err := c.rejectAllClustersMutation(ctx); err != nil {
		return err
	}
	store, rewrittenKey, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	return store.GuaranteedUpdate(ctx, rewrittenKey, destination, ignoreNotFound, precond, tryUpdate, cachedExistingObject)
}

func (c *clusteredStorage) Stats(ctx context.Context) (storage.Stats, error) {
	store, _, err := c.storeAndKey(ctx, "")
	if err != nil {
		return storage.Stats{}, err
	}
	return store.Stats(ctx)
}

func (c *clusteredStorage) ReadinessCheck() error {
	store, err := c.ensureStore()
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

func (c *clusteredStorage) EnableResourceSizeEstimation(keysFunc storage.KeysFunc) error {
	store, err := c.ensureStore()
	if err != nil {
		return err
	}
	return store.EnableResourceSizeEstimation(keysFunc)
}

func (c *clusteredStorage) CompactRevision() int64 {
	store, err := c.ensureStore()
	if err != nil {
		return 0
	}
	return store.CompactRevision()
}

func (c *clusteredStorage) destroy() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destroyFn != nil {
		c.destroyFn()
	}
}

func (c *clusteredStorage) storeAndKey(ctx context.Context, key string) (storage.Interface, string, error) {
	cid, scope, _ := FromContextScope(ctx)
	if scope != ResourceScopeCrossClusterRead && HasInternalCrossClusterCapability(ctx) {
		scope = ResourceScopeCrossClusterRead
		ctx = internalcap.WithAllClustersCapability(ctx)
	}
	if cid == "" {
		cid = c.defaultCluster()
	}
	store, err := c.ensureStore()
	if err != nil {
		return nil, key, err
	}
	if scope == ResourceScopeCrossClusterRead {
		if !internalcap.HasAllClustersCapability(ctx) {
			return nil, key, ErrAllClustersScopeForbidden
		}
		rewritten := c.kindRootPrefix()
		fullKey := strings.TrimSuffix(c.config.Prefix, "/") + "/" + strings.TrimPrefix(rewritten, "/")
		if debugStoreAndKey {
			fmt.Fprintf(os.Stderr, "mc.storeAndKey scope=%s store=%T/%p resourcePrefix=%s key=%s cid=%s rewritten=%s etcdPrefix=%s fullKey=%s\n",
				scope, store, store, c.resourcePrefix, key, cid, rewritten, c.config.Prefix, fullKey,
			)
		}
		return store, rewritten, nil
	}
	rewritten := c.rewriteKey(cid, key)
	fullKey := strings.TrimSuffix(c.config.Prefix, "/") + "/" + strings.TrimPrefix(rewritten, "/")
	if debugStoreAndKey {
		fmt.Fprintf(os.Stderr, "mc.storeAndKey scope=%s store=%T/%p resourcePrefix=%s key=%s cid=%s rewritten=%s etcdPrefix=%s fullKey=%s\n",
			scope, store, store, c.resourcePrefix, key, cid, rewritten, c.config.Prefix, fullKey,
		)
	}
	return store, rewritten, nil
}

func (c *clusteredStorage) rejectAllClustersMutation(ctx context.Context) error {
	_, scope, _ := FromContextScope(ctx)
	if scope == ResourceScopeCrossClusterRead {
		return ErrAllClustersMutationForbidden
	}
	return nil
}

func (c *clusteredStorage) defaultCluster() string {
	if c.options.DefaultCluster != "" {
		return c.options.DefaultCluster
	}
	return DefaultClusterName
}


func (c *clusteredStorage) clusterFromObject(obj runtime.Object) string {
	if entry, ok := obj.(*mcstorage.InternalEntry); ok && entry != nil {
		if entry.ClusterID != "" {
			return entry.ClusterID
		}
		if entry.StorageKey != "" {
			if cid := extstorage.DefaultKeyLayout().ClusterFromKey(entry.StorageKey); cid != "" {
				return cid
			}
		}
		if entry.Object != nil {
			obj = entry.Object
		}
	}
	return c.defaultCluster()
}


func (c *clusteredStorage) rewriteKey(cluster, key string) string {
	if cluster == "" {
		cluster = DefaultClusterName
	}
	rp := strings.TrimSuffix(c.resourcePrefix, "/")
	if key == "" {
		return rp + "/clusters/" + cluster
	}
	if strings.HasPrefix(key, rp+"/clusters/") {
		return key
	}
	if strings.HasPrefix(key, rp) {
		return rp + "/clusters/" + cluster + strings.TrimPrefix(key, rp)
	}
	return key
}

func (c *clusteredStorage) kindRootPrefix() string {
	return strings.TrimSuffix(c.resourcePrefix, "/") + "/clusters"
}

func (c *clusteredStorage) ensureStore() (storage.Interface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store != nil {
		return c.store, nil
	}
	cfg := *c.config
	kindRootPrefix := c.kindRootPrefix()
	server := c.options.ServerName
	if server == "" {
		server = "unknown"
	}
	seq := atomic.AddUint64(&ensureStoreSeq, 1)
	stack := debug.Stack()
	stackHash := sha256.Sum256(stack)
	ensureStoreTotal.WithLabelValues(server, c.resourcePrefix).Inc()
	klog.Infof("mc.ensureStore #%d server=%s resourcePrefix=%s kindRoot=%s etcdPrefix=%s stack=%s",
		seq, server, c.resourcePrefix, kindRootPrefix, cfg.Prefix, hex.EncodeToString(stackHash[:8]),
	)
	keyFunc := func(obj runtime.Object) (string, error) {
		if entry, ok := obj.(*mcstorage.InternalEntry); ok && entry != nil {
			if entry.StorageKey != "" {
				return entry.StorageKey, nil
			}
			if entry.Object != nil {
				obj = entry.Object
			}
		}
		key, err := c.keyFunc(obj)
		if err != nil {
			return "", err
		}
		// The watchcache indexes by keyFunc output, so we must include the cluster
		// derived from the stored object to avoid cross-cluster list/watch misses.
		return c.rewriteKey(c.clusterFromObject(obj), key), nil
	}
	store, destroy, err := c.base(&cfg, kindRootPrefix, keyFunc, c.newFunc, c.newListFunc, c.getAttrsFunc, c.trigger, c.indexers)
	if err != nil {
		return nil, err
	}
	c.store = store
	c.destroyFn = destroy
	return c.store, nil
}
