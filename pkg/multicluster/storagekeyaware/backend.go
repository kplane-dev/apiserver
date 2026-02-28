package storagekeyaware

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	mcstorage "github.com/kplane-dev/apiserver/pkg/multicluster/storage"
)

// Backend is a local fork boundary for key-aware storage behavior.
// It currently forwards to the delegate while centralizing hooks for
// InternalEntry-based list/watch handling.
type Backend struct {
	delegate       storage.Interface
	kindRootPrefix string
	etcdPrefix     string
	transport      storagebackend.TransportConfig
	resolver       mcstorage.PlacementResolver
	remember       func(runtime.Object, string)

	clientMu sync.Mutex
	client   *clientv3.Client

	revCluster sync.Map // map[string]string
}

func New(
	delegate storage.Interface,
	kindRootPrefix string,
	etcdPrefix string,
	transport storagebackend.TransportConfig,
	remember func(runtime.Object, string),
) storage.Interface {
	if delegate == nil {
		return nil
	}
	trimmedRoot := strings.TrimSuffix(kindRootPrefix, "/")
	return &Backend{
		delegate:       delegate,
		kindRootPrefix: trimmedRoot,
		etcdPrefix:     strings.TrimSuffix(etcdPrefix, "/"),
		transport:      transport,
		resolver:       mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: trimmedRoot},
		remember:       remember,
	}
}

func (b *Backend) Versioner() storage.Versioner { return b.delegate.Versioner() }

func (b *Backend) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if err := b.delegate.Create(ctx, key, obj, out, ttl); err != nil {
		return err
	}
	if cid, ok := b.resolver.ClusterFromStorageKey(key); ok {
		b.rememberCluster(out, cid)
	}
	return nil
}

func (b *Backend) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	return b.delegate.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts)
}

func (b *Backend) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	source, err := b.delegate.Watch(ctx, key, opts)
	if err != nil {
		return nil, err
	}
	return &entryWrapWatch{
		source: source,
		backend: b,
		key:    key,
	}, nil
}

func (b *Backend) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	if entry, ok := objPtr.(*mcstorage.InternalEntry); ok && entry != nil && entry.Object != nil {
		if err := b.delegate.Get(ctx, key, opts, entry.Object); err != nil {
			return err
		}
		entry.StorageKey = key
		if cid, ok := b.resolver.ClusterFromStorageKey(key); ok {
			entry.ClusterID = cid
			b.rememberCluster(entry.Object, cid)
		}
		return nil
	}
	if err := b.delegate.Get(ctx, key, opts, objPtr); err != nil {
		return err
	}
	if cid, ok := b.resolver.ClusterFromStorageKey(key); ok {
		b.rememberCluster(objPtr, cid)
	}
	return nil
}

func (b *Backend) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	if err := b.delegate.GetList(ctx, key, opts, listObj); err != nil {
		return err
	}
	// Rebuild revision->cluster memory from durable keyspace on targeted
	// cross-cluster relists. We intentionally keep this narrow to avoid
	// penalizing startup/list performance for unrelated resources.
	if b.isCrossClusterKey(key) && shouldPrimeListIdentity(listObj) {
		revToCluster, err := b.revisionClusterIndex(ctx, key)
		if err == nil {
			b.rememberListClusters(listObj, revToCluster)
		}
	}
	return nil
}

func (b *Backend) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	wrapped := func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		out, ttl, err := tryUpdate(input, res)
		if err != nil || out == nil {
			return out, ttl, err
		}
		if cid, ok := b.resolver.ClusterFromStorageKey(key); ok {
			b.rememberCluster(out, cid)
		}
		return out, ttl, nil
	}
	if err := b.delegate.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, wrapped, cachedExistingObject); err != nil {
		return err
	}
	if cid, ok := b.resolver.ClusterFromStorageKey(key); ok {
		b.rememberCluster(destination, cid)
	}
	return nil
}

func (b *Backend) ReadinessCheck() error { return b.delegate.ReadinessCheck() }

func (b *Backend) RequestWatchProgress(ctx context.Context) error {
	return b.delegate.RequestWatchProgress(ctx)
}

func (b *Backend) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	return b.delegate.GetCurrentResourceVersion(ctx)
}

func (b *Backend) SetKeysFunc(keysFunc storage.KeysFunc) { b.delegate.SetKeysFunc(keysFunc) }

func (b *Backend) GetListWithAlloc(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object, growSlice bool) error {
	if alloc, ok := b.delegate.(interface {
		GetListWithAlloc(context.Context, string, storage.ListOptions, runtime.Object, bool) error
	}); ok {
		return alloc.GetListWithAlloc(ctx, key, opts, listObj, growSlice)
	}
	return b.delegate.GetList(ctx, key, opts, listObj)
}

func (b *Backend) Stats(ctx context.Context) (storage.Stats, error) { return b.delegate.Stats(ctx) }

func (b *Backend) CompactRevision() int64 { return b.delegate.CompactRevision() }

type entryWrapWatch struct {
	source  watch.Interface
	backend *Backend
	key     string
}

func (w *entryWrapWatch) Stop() {
	if w.source != nil {
		w.source.Stop()
	}
}

func (w *entryWrapWatch) ResultChan() <-chan watch.Event {
	ch := make(chan watch.Event)
	go func() {
		defer close(ch)
		scopeClusterID, _ := w.backend.resolver.ClusterFromStorageKey(w.key)
		for evt := range w.source.ResultChan() {
			obj := evt.Object
			if obj == nil {
				continue
			}
			storageKey := objectStorageKey(w.key, obj, !w.backend.isCrossClusterKey(w.key))
			clusterID := scopeClusterID
			if clusterID == "" {
				if cid, ok := w.backend.resolver.ClusterFromStorageKey(storageKey); ok {
					clusterID = cid
				}
			}
			if clusterID == "" {
				// Keep identity derivation strictly key-based: scope key for scoped
				// watches, object storage key for cross-cluster watches.
				// If neither yields a cluster, leave it unset.
			}
			w.backend.rememberCluster(obj, clusterID)
			ch <- evt
		}
	}()
	return ch
}

func (b *Backend) rememberCluster(obj runtime.Object, clusterID string) {
	if obj == nil || clusterID == "" {
		return
	}
	if b.remember != nil {
		b.remember(obj, clusterID)
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	rv := accessor.GetResourceVersion()
	if rv == "" {
		return
	}
	b.revCluster.Store(rv, clusterID)
}

func (b *Backend) rememberListClusters(listObj runtime.Object, revToCluster map[string]string) {
	if listObj == nil || len(revToCluster) == 0 {
		return
	}
	_ = meta.EachListItem(listObj, func(obj runtime.Object) error {
		accessor, err := meta.Accessor(obj)
		if err != nil {
			return nil
		}
		rv := accessor.GetResourceVersion()
		if rv == "" {
			return nil
		}
		cid := revToCluster[objectIdentityKey(rv, accessor.GetNamespace(), accessor.GetName())]
		if cid == "" {
			cid = revToCluster[rv]
		}
		if cid == "" {
			return nil
		}
		b.rememberCluster(obj, cid)
		return nil
	})
}

func (b *Backend) isCrossClusterKey(key string) bool {
	trimmed := strings.TrimSuffix(key, "/")
	return trimmed == b.kindRootPrefix
}

func (b *Backend) revisionClusterIndex(ctx context.Context, key string) (map[string]string, error) {
	cli, err := b.getETCDClient()
	if err != nil {
		return nil, err
	}
	fullPrefix := b.fullETCDPrefixForKey(key)
	if fullPrefix == "" {
		return nil, fmt.Errorf("empty etcd prefix")
	}
	queryPrefix := fullPrefix
	if !strings.HasSuffix(queryPrefix, "/") {
		queryPrefix += "/"
	}
	etcdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cli.Get(etcdCtx, queryPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(resp.Kvs))
	storagePrefix := strings.TrimSuffix(key, "/") + "/"
	for _, kv := range resp.Kvs {
		storageKey := b.storageKeyFromETCDKey(string(kv.Key))
		if storageKey == "" {
			continue
		}
		cid, ok := b.resolver.ClusterFromStorageKey(storageKey)
		if !ok || cid == "" {
			continue
		}
		rv := strconv.FormatInt(kv.ModRevision, 10)
		out[rv] = cid
		if ns, name, ok := objectPathFromStorageKey(storageKey, storagePrefix); ok {
			out[objectIdentityKey(rv, ns, name)] = cid
		}
	}
	return out, nil
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

func objectIdentityKey(rv, namespace, name string) string {
	return rv + "|" + namespace + "|" + name
}

func (b *Backend) fullETCDPrefixForKey(storageKey string) string {
	if storageKey == "" {
		return ""
	}
	return strings.TrimSuffix(b.etcdPrefix, "/") + "/" + strings.TrimPrefix(storageKey, "/")
}

func (b *Backend) storageKeyFromETCDKey(etcdKey string) string {
	if etcdKey == "" {
		return ""
	}
	prefix := strings.TrimSuffix(b.etcdPrefix, "/")
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

func (b *Backend) getETCDClient() (*clientv3.Client, error) {
	b.clientMu.Lock()
	defer b.clientMu.Unlock()
	if b.client != nil {
		return b.client, nil
	}
	if len(b.transport.ServerList) == 0 {
		return nil, fmt.Errorf("no etcd servers configured")
	}
	cfg := clientv3.Config{
		Endpoints:   append([]string{}, b.transport.ServerList...),
		DialTimeout: 5 * time.Second,
	}
	if b.transport.CertFile != "" || b.transport.KeyFile != "" || b.transport.TrustedCAFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile: b.transport.CertFile,
			KeyFile:  b.transport.KeyFile,
			TrustedCAFile: b.transport.TrustedCAFile,
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
	b.client = cli
	return b.client, nil
}

func shouldPrimeListIdentity(listObj runtime.Object) bool {
	if listObj == nil {
		return false
	}
	// Shared auth projection relies on cluster attribution for RBAC resources.
	// Prime only these lists to make restart/relist deterministic without
	// introducing broad etcd key-scan overhead.
	t := reflect.TypeOf(listObj).String()
	return strings.HasSuffix(t, ".RoleList") ||
		strings.HasSuffix(t, ".RoleBindingList") ||
		strings.HasSuffix(t, ".ClusterRoleList") ||
		strings.HasSuffix(t, ".ClusterRoleBindingList")
}

func objectStorageKey(requestKey string, obj runtime.Object, appendObjectPath bool) string {
	if strings.TrimSpace(requestKey) == "" || obj == nil {
		return ""
	}
	if !appendObjectPath {
		return strings.TrimSuffix(requestKey, "/")
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return requestKey
	}
	name := accessor.GetName()
	if name == "" {
		return requestKey
	}
	base := strings.TrimSuffix(requestKey, "/")
	namespace := accessor.GetNamespace()
	if namespace == "" {
		if strings.HasSuffix(base, "/"+name) {
			return base
		}
		return base + "/" + name
	}
	if strings.HasSuffix(base, "/"+namespace+"/"+name) {
		return base
	}
	return base + "/" + namespace + "/" + name
}
