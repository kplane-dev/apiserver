package multicluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/generic"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"

	"github.com/kplane-dev/apiserver/pkg/multicluster/internalcap"
	mcstorage "github.com/kplane-dev/apiserver/pkg/multicluster/storage"
	"github.com/kplane-dev/apiserver/pkg/multicluster/storagekeyaware"
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
	base := opts.Decorator
	if base == nil {
		base = generic.UndecoratedStorage
	}
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

	etcdMu     sync.Mutex
	etcdClient *clientv3.Client
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
	c.enforceObjectClusterIdentity(obj, c.clusterFromContext(ctx))
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	if err := store.Create(ctx, key, obj, out, ttl); err != nil {
		return err
	}
	c.sanitizeResponseObject(ctx, out)
	return nil
}

func (c *clusteredStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	if err := c.rejectAllClustersMutation(ctx); err != nil {
		return err
	}
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	if err := store.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts); err != nil {
		return err
	}
	c.sanitizeResponseObject(ctx, out)
	return nil
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
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	if err := store.Get(ctx, key, opts, objPtr); err != nil {
		return err
	}
	c.sanitizeResponseObject(ctx, objPtr)
	return nil
}

func (c *clusteredStorage) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	store, key, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	if err := store.GetList(ctx, key, opts, listObj); err != nil {
		return err
	}
	c.sanitizeResponseList(ctx, listObj)
	return nil
}

func (c *clusteredStorage) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, precond *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	if err := c.rejectAllClustersMutation(ctx); err != nil {
		return err
	}
	store, rewrittenKey, err := c.storeAndKey(ctx, key)
	if err != nil {
		return err
	}
	// CRD and other internal controllers run via loopback context without an
	// explicit cluster in the URL. Resolve cluster from durable storage identity
	// before writing status so updates route to the owning cluster keyspace.
	if isInternalLoopbackContext(ctx) {
		ctxCID, _, _ := FromContextScope(ctx)
		if ctxCID == "" {
			hintCID := c.clusterHintFromDestination(destination)
			if hintCID == "" {
				hintCID = c.clusterHintFromDestination(cachedExistingObject)
			}
			if hintCID == "" {
				hintCID = c.clusterHintFromKey(key, precond)
			}
			// Never route CRD controller status writes to a guessed/default cluster.
			// If attribution is unresolved, force retry so correctness stays key-derived.
			if hintCID == "" && strings.Trim(c.resourcePrefix, "/") == "apiextensions.k8s.io/customresourcedefinitions" {
				return fmt.Errorf("unable to resolve CRD cluster from storage identity for key %q", key)
			}
			if hintCID != "" {
				rewrittenKey = c.rewriteKey(hintCID, key)
			}
		}
	}
	cid := c.clusterFromContext(ctx)
	wrappedUpdate := func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		outObj, ttl, err := tryUpdate(input, res)
		if err != nil || outObj == nil {
			return outObj, ttl, err
		}
		c.enforceObjectClusterIdentity(outObj, cid)
		return outObj, ttl, nil
	}
	if err := store.GuaranteedUpdate(ctx, rewrittenKey, destination, ignoreNotFound, precond, wrappedUpdate, cachedExistingObject); err != nil {
		return err
	}
	c.sanitizeResponseObject(ctx, destination)
	return nil
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

func (c *clusteredStorage) SetKeysFunc(keysFunc storage.KeysFunc) {
	store, err := c.ensureStore()
	if err != nil {
		return
	}
	store.SetKeysFunc(keysFunc)
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

func (c *clusteredStorage) clusterFromContext(ctx context.Context) string {
	cid, _, _ := FromContextScope(ctx)
	if cid == "" {
		cid = c.defaultCluster()
	}
	return cid
}

func (c *clusteredStorage) enforceObjectClusterIdentity(obj runtime.Object, cid string) {
	SetObjectClusterIdentity(obj, cid)
}

func (c *clusteredStorage) sanitizeResponseObject(ctx context.Context, obj runtime.Object) {
	if obj == nil || c.exposeInternalIdentity(ctx) {
		return
	}
	ClearObjectClusterIdentity(obj)
}

func (c *clusteredStorage) sanitizeResponseList(ctx context.Context, listObj runtime.Object) {
	if listObj == nil || c.exposeInternalIdentity(ctx) {
		return
	}
	_ = meta.EachListItem(listObj, func(obj runtime.Object) error {
		ClearObjectClusterIdentity(obj)
		return nil
	})
}

func (c *clusteredStorage) exposeInternalIdentity(ctx context.Context) bool {
	_, scope, _ := FromContextScope(ctx)
	return scope == ResourceScopeCrossClusterRead
}

func (c *clusteredStorage) clusterFromObject(obj runtime.Object) string {
	if entry, ok := obj.(*mcstorage.InternalEntry); ok && entry != nil {
		if entry.ClusterID != "" {
			return entry.ClusterID
		}
		if entry.StorageKey != "" {
			resolver := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: c.kindRootPrefix()}
			if cid, ok := resolver.ClusterFromStorageKey(entry.StorageKey); ok && cid != "" {
				return cid
			}
		}
		if entry.Object != nil {
			obj = entry.Object
		}
	}
	if c.requiresStorageClusterLookup() {
		if cid := c.lookupClusterFromStorage(obj); cid != "" {
			return cid
		}
	}
	return c.defaultCluster()
}

func (c *clusteredStorage) requiresStorageClusterLookup() bool {
	rp := strings.Trim(c.resourcePrefix, "/")
	switch rp {
	case "apiextensions.k8s.io/customresourcedefinitions",
		"serviceaccounts",
		"secrets",
		"configmaps",
		"pods",
		"nodes",
		"endpointslices",
		"services/specs",
		"services/endpoints",
		"namespaces",
		"validatingwebhookconfigurations",
		"mutatingwebhookconfigurations":
		return true
	default:
		return false
	}
}

func (c *clusteredStorage) lookupClusterFromStorage(obj runtime.Object) string {
	if obj == nil {
		return ""
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	name := accessor.GetName()
	if name == "" {
		return ""
	}
	namespace := accessor.GetNamespace()
	wantRV := int64(0)
	if rv := accessor.GetResourceVersion(); rv != "" {
		if parsed, err := strconv.ParseInt(rv, 10, 64); err == nil {
			wantRV = parsed
		}
	}
	cli, err := c.getETCDClient()
	if err != nil {
		return ""
	}
	storagePrefix := strings.TrimSuffix(c.config.Prefix, "/") + "/" + strings.TrimPrefix(c.kindRootPrefix(), "/")
	queryPrefix := strings.TrimSuffix(storagePrefix, "/") + "/"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	matchCID := ""
	matchScore := 0
	ambiguous := false
	scan := func(kvs []*mvccpb.KeyValue) {
		for _, kv := range kvs {
			key := c.storageKeyFromETCDKey(string(kv.Key))
			if key == "" {
				continue
			}
			cid, ok := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: c.kindRootPrefix()}.ClusterFromStorageKey(key)
			if !ok || cid == "" {
				continue
			}
			ns, nm, ok := objectPathFromStorageKey(key, c.kindRootPrefix()+"/")
			if !ok || nm != name || ns != namespace {
				continue
			}
			score := 1
			if wantRV > 0 && kv.ModRevision == wantRV {
				score = 2
			}
			if score > matchScore {
				matchCID = cid
				matchScore = score
				if score == 1 {
					ambiguous = false
				}
				if score == 2 {
					return
				}
			} else if score == matchScore && score == 1 && matchCID != "" && matchCID != cid {
				ambiguous = true
			}
		}
	}
	if wantRV > 0 {
		if resp, err := cli.Get(
			ctx,
			queryPrefix,
			clientv3.WithPrefix(),
			clientv3.WithKeysOnly(),
			clientv3.WithMinModRev(wantRV),
			clientv3.WithMaxModRev(wantRV),
		); err == nil {
			scan(resp.Kvs)
		}
		if matchScore == 2 {
			return matchCID
		}
	}
	resp, err := cli.Get(ctx, queryPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return ""
	}
	for _, kv := range resp.Kvs {
		key := c.storageKeyFromETCDKey(string(kv.Key))
		if key == "" {
			continue
		}
		cid, ok := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: c.kindRootPrefix()}.ClusterFromStorageKey(key)
		if !ok || cid == "" {
			continue
		}
		ns, nm, ok := objectPathFromStorageKey(key, c.kindRootPrefix()+"/")
		if !ok || nm != name || ns != namespace {
			continue
		}
		score := 1
		if wantRV > 0 && kv.ModRevision == wantRV {
			score = 2
		}
		if score > matchScore {
			matchCID = cid
			matchScore = score
			if score == 2 {
				break
			}
		}
	}
	if wantRV == 0 && ambiguous {
		return ""
	}
	return matchCID
}

func (c *clusteredStorage) clusterHintFromDestination(obj runtime.Object) string {
	if obj == nil {
		return ""
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	name := accessor.GetName()
	if name == "" {
		return ""
	}
	namespace := accessor.GetNamespace()
	wantRV := int64(0)
	if rv := accessor.GetResourceVersion(); rv != "" {
		if parsed, err := strconv.ParseInt(rv, 10, 64); err == nil {
			wantRV = parsed
		}
	}
	cli, err := c.getETCDClient()
	if err != nil {
		return ""
	}
	storagePrefix := strings.TrimSuffix(c.config.Prefix, "/") + "/" + strings.TrimPrefix(c.kindRootPrefix(), "/")
	queryPrefix := strings.TrimSuffix(storagePrefix, "/") + "/"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	matchCID := ""
	matchScore := 0
	ambiguous := false
	scan := func(kvs []*mvccpb.KeyValue) {
		for _, kv := range kvs {
			key := c.storageKeyFromETCDKey(string(kv.Key))
			if key == "" {
				continue
			}
			cid, ok := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: c.kindRootPrefix()}.ClusterFromStorageKey(key)
			if !ok || cid == "" {
				continue
			}
			ns, nm, ok := objectPathFromStorageKey(key, c.kindRootPrefix()+"/")
			if !ok || nm != name || ns != namespace {
				continue
			}
			score := 1
			if wantRV > 0 && kv.ModRevision == wantRV {
				score = 2
			}
			if score > matchScore {
				matchCID = cid
				matchScore = score
				if score == 1 {
					ambiguous = false
				}
				if score == 2 {
					return
				}
			} else if score == matchScore && score == 1 && matchCID != "" && matchCID != cid {
				ambiguous = true
			}
		}
	}
	if wantRV > 0 {
		if resp, err := cli.Get(
			ctx,
			queryPrefix,
			clientv3.WithPrefix(),
			clientv3.WithKeysOnly(),
			clientv3.WithMinModRev(wantRV),
			clientv3.WithMaxModRev(wantRV),
		); err == nil {
			scan(resp.Kvs)
		}
		if matchScore == 2 {
			return matchCID
		}
	}
	resp, err := cli.Get(ctx, queryPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return ""
	}
	for _, kv := range resp.Kvs {
		key := c.storageKeyFromETCDKey(string(kv.Key))
		if key == "" {
			continue
		}
		cid, ok := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: c.kindRootPrefix()}.ClusterFromStorageKey(key)
		if !ok || cid == "" {
			continue
		}
		ns, nm, ok := objectPathFromStorageKey(key, c.kindRootPrefix()+"/")
		if !ok || nm != name || ns != namespace {
			continue
		}
		score := 1
		if wantRV > 0 && kv.ModRevision == wantRV {
			score = 2
		}
		if score > matchScore {
			matchCID = cid
			matchScore = score
			if score == 2 {
				break
			}
		}
	}
	if wantRV == 0 && ambiguous {
		return ""
	}
	return matchCID
}

func (c *clusteredStorage) clusterHintFromKey(key string, precond *storage.Preconditions) string {
	if key == "" {
		return ""
	}
	base := strings.TrimSuffix(c.resourcePrefix, "/")
	if !strings.HasPrefix(key, base+"/") {
		return ""
	}
	rest := strings.TrimPrefix(key, base+"/")
	parts := strings.Split(rest, "/")
	var namespace, name string
	switch len(parts) {
	case 1:
		name = parts[0]
	default:
		namespace, name = parts[len(parts)-2], parts[len(parts)-1]
	}
	if name == "" {
		return ""
	}
	wantRV := int64(0)
	if precond != nil && precond.ResourceVersion != nil {
		if parsed, err := strconv.ParseInt(*precond.ResourceVersion, 10, 64); err == nil {
			wantRV = parsed
		}
	}
	cli, err := c.getETCDClient()
	if err != nil {
		return ""
	}
	storagePrefix := strings.TrimSuffix(c.config.Prefix, "/") + "/" + strings.TrimPrefix(c.kindRootPrefix(), "/")
	queryPrefix := strings.TrimSuffix(storagePrefix, "/") + "/"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	matchCID, matchScore := "", 0
	ambiguous := false
	scan := func(kvs []*mvccpb.KeyValue) {
		for _, kv := range kvs {
			storageKey := c.storageKeyFromETCDKey(string(kv.Key))
			if storageKey == "" {
				continue
			}
			cid, ok := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: c.kindRootPrefix()}.ClusterFromStorageKey(storageKey)
			if !ok || cid == "" {
				continue
			}
			ns, nm, ok := objectPathFromStorageKey(storageKey, c.kindRootPrefix()+"/")
			if !ok || nm != name || ns != namespace {
				continue
			}
			score := 1
			if wantRV > 0 && kv.ModRevision == wantRV {
				score = 2
			}
			if score > matchScore {
				matchCID, matchScore = cid, score
				if score == 1 {
					ambiguous = false
				}
				if score == 2 {
					return
				}
			} else if score == matchScore && score == 1 && matchCID != "" && matchCID != cid {
				ambiguous = true
			}
		}
	}
	if wantRV > 0 {
		if resp, err := cli.Get(
			ctx,
			queryPrefix,
			clientv3.WithPrefix(),
			clientv3.WithKeysOnly(),
			clientv3.WithMinModRev(wantRV),
			clientv3.WithMaxModRev(wantRV),
		); err == nil {
			scan(resp.Kvs)
		}
		if matchScore == 2 {
			return matchCID
		}
	}
	resp, err := cli.Get(ctx, queryPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return ""
	}
	for _, kv := range resp.Kvs {
		storageKey := c.storageKeyFromETCDKey(string(kv.Key))
		if storageKey == "" {
			continue
		}
		cid, ok := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: c.kindRootPrefix()}.ClusterFromStorageKey(storageKey)
		if !ok || cid == "" {
			continue
		}
		ns, nm, ok := objectPathFromStorageKey(storageKey, c.kindRootPrefix()+"/")
		if !ok || nm != name || ns != namespace {
			continue
		}
		score := 1
		if wantRV > 0 && kv.ModRevision == wantRV {
			score = 2
		}
		if score > matchScore {
			matchCID, matchScore = cid, score
			if score == 2 {
				break
			}
		}
	}
	if wantRV == 0 && ambiguous {
		return ""
	}
	return matchCID
}

func isInternalLoopbackContext(ctx context.Context) bool {
	u, ok := apirequest.UserFrom(ctx)
	if !ok || u == nil {
		return false
	}
	return u.GetName() == "system:apiserver"
}

func objectPathFromStorageKey(storageKey, rootPrefix string) (namespace, name string, ok bool) {
	if !strings.HasPrefix(storageKey, rootPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(storageKey, rootPrefix)
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return "", "", false
	}
	// parts[0] is cluster ID.
	switch len(parts) {
	case 2:
		return "", parts[1], true
	default:
		return parts[1], parts[2], true
	}
}

func (c *clusteredStorage) storageKeyFromETCDKey(etcdKey string) string {
	if etcdKey == "" {
		return ""
	}
	prefix := strings.TrimSuffix(c.config.Prefix, "/")
	if prefix == "" {
		return etcdKey
	}
	trimmed := strings.TrimPrefix(etcdKey, prefix)
	if trimmed == etcdKey {
		return ""
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return trimmed
}

func (c *clusteredStorage) getETCDClient() (*clientv3.Client, error) {
	c.etcdMu.Lock()
	defer c.etcdMu.Unlock()
	if c.etcdClient != nil {
		return c.etcdClient, nil
	}
	if len(c.config.Transport.ServerList) == 0 {
		if env := strings.TrimSpace(os.Getenv("ETCD_ENDPOINTS")); env != "" {
			for _, ep := range strings.Split(env, ",") {
				ep = strings.TrimSpace(ep)
				if ep != "" {
					c.config.Transport.ServerList = append(c.config.Transport.ServerList, ep)
				}
			}
		}
	}
	if len(c.config.Transport.ServerList) == 0 {
		return nil, fmt.Errorf("no etcd servers configured")
	}
	cfg := clientv3.Config{
		Endpoints:   append([]string{}, c.config.Transport.ServerList...),
		DialTimeout: 5 * time.Second,
	}
	if c.config.Transport.CertFile != "" || c.config.Transport.KeyFile != "" || c.config.Transport.TrustedCAFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      c.config.Transport.CertFile,
			KeyFile:       c.config.Transport.KeyFile,
			TrustedCAFile: c.config.Transport.TrustedCAFile,
		}
		tlsCfg, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	c.etcdClient = cli
	return c.etcdClient, nil
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
	c.store = storagekeyaware.New(store, kindRootPrefix, c.config.Prefix, c.config.Transport, nil)
	c.destroyFn = destroy
	return c.store, nil
}
