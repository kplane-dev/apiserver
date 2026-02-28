package scopedinformer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	mcstorage "github.com/kplane-dev/apiserver/pkg/multicluster/storage"
)

const ClusterIndexName = "mc.cluster"

func NewAllClustersKubeClient(base *rest.Config) (kubernetes.Interface, error) {
	if base == nil {
		return nil, fmt.Errorf("base loopback config is required")
	}
	cfg := rest.CopyConfig(base)
	cfg.Impersonate.UserName = mc.DefaultInternalCrossClusterUser
	cfg.Impersonate.Groups = []string{"system:authenticated", "system:masters"}
	if cfg.Impersonate.Extra == nil {
		cfg.Impersonate.Extra = map[string][]string{}
	}
	cfg.Impersonate.Extra[mc.DefaultInternalCrossClusterCapability] = []string{"true"}
	if cfg.UserAgent == "" {
		cfg.UserAgent = mc.DefaultInternalCrossClusterUserAgent
	} else {
		cfg.UserAgent = mc.DefaultInternalCrossClusterUserAgent + " " + cfg.UserAgent
	}
	return kubernetes.NewForConfig(cfg)
}

func EnsureClusterIndex(inf cache.SharedIndexInformer) error {
	return inf.AddIndexers(cache.Indexers{
		ClusterIndexName: func(obj interface{}) ([]string, error) {
			cid := ObjectCluster(obj)
			if cid == "" {
				return nil, nil
			}
			return []string{cid}, nil
		},
	})
}

// ClusterEntryTransform wraps informer objects into InternalEntry so cluster
// metadata can be carried outside model fields.
func ClusterEntryTransform() cache.TransformFunc {
	return func(obj interface{}) (interface{}, error) {
		if entry, ok := obj.(*mcstorage.InternalEntry); ok && entry != nil {
			return entry, nil
		}
		ro, ok := obj.(runtime.Object)
		if !ok || ro == nil {
			return obj, nil
		}
		return &mcstorage.InternalEntry{
			Object: ro,
		}, nil
	}
}

func ObjectCluster(obj interface{}) string {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if entry, ok := obj.(*mcstorage.InternalEntry); ok && entry != nil {
		if entry.ClusterID != "" {
			return entry.ClusterID
		}
		if entry.StorageKey != "" {
			if cid := clusterFromStorageKey(entry.StorageKey); cid != "" {
				return cid
			}
		}
		return ""
	}
	return ""
}

func clusterFromStorageKey(storageKey string) string {
	i := strings.Index(storageKey, "/clusters/")
	if i <= 0 {
		return ""
	}
	root := strings.TrimSuffix(storageKey[:i+len("/clusters")], "/")
	resolver := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: root}
	cid, ok := resolver.ClusterFromStorageKey(storageKey)
	if !ok {
		return ""
	}
	return cid
}

func UnwrapObject(obj interface{}) interface{} {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if entry, ok := obj.(*mcstorage.InternalEntry); ok && entry != nil {
		if entry.Object != nil {
			return entry.Object
		}
		return nil
	}
	return obj
}

func FilteredByCluster(indexer cache.Indexer, clusterID string) []interface{} {
	items, err := indexer.ByIndex(ClusterIndexName, clusterID)
	if err != nil {
		return nil
	}
	out := make([]interface{}, 0, len(items))
	for _, item := range items {
		unwrapped := UnwrapObject(item)
		if unwrapped == nil {
			continue
		}
		out = append(out, unwrapped)
	}
	return out
}

func NewFilteredSharedIndexInformer(shared cache.SharedIndexInformer, clusterID string) cache.SharedIndexInformer {
	return &filteredSharedIndexInformer{shared: shared, clusterID: clusterID}
}

type filteredSharedIndexInformer struct {
	shared    cache.SharedIndexInformer
	clusterID string
}

func (f *filteredSharedIndexInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return f.shared.AddEventHandler(f.wrap(handler))
}

func (f *filteredSharedIndexInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return f.shared.AddEventHandlerWithResyncPeriod(f.wrap(handler), resyncPeriod)
}

func (f *filteredSharedIndexInformer) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return f.shared.AddEventHandlerWithOptions(f.wrap(handler), options)
}

func (f *filteredSharedIndexInformer) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return f.shared.RemoveEventHandler(handle)
}

func (f *filteredSharedIndexInformer) GetStore() cache.Store { return f.shared.GetStore() }
func (f *filteredSharedIndexInformer) GetController() cache.Controller {
	return f.shared.GetController()
}
func (f *filteredSharedIndexInformer) Run(stopCh <-chan struct{})         {}
func (f *filteredSharedIndexInformer) RunWithContext(ctx context.Context) {}
func (f *filteredSharedIndexInformer) HasSynced() bool                    { return f.shared.HasSynced() }
func (f *filteredSharedIndexInformer) LastSyncResourceVersion() string {
	return f.shared.LastSyncResourceVersion()
}
func (f *filteredSharedIndexInformer) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	return f.shared.SetWatchErrorHandler(handler)
}
func (f *filteredSharedIndexInformer) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	return f.shared.SetWatchErrorHandlerWithContext(handler)
}
func (f *filteredSharedIndexInformer) SetTransform(handler cache.TransformFunc) error {
	return f.shared.SetTransform(handler)
}
func (f *filteredSharedIndexInformer) IsStopped() bool { return f.shared.IsStopped() }
func (f *filteredSharedIndexInformer) AddIndexers(indexers cache.Indexers) error {
	return f.shared.AddIndexers(indexers)
}
func (f *filteredSharedIndexInformer) GetIndexer() cache.Indexer { return f.shared.GetIndexer() }

func (f *filteredSharedIndexInformer) wrap(handler cache.ResourceEventHandler) cache.ResourceEventHandler {
	if handler == nil {
		return cache.ResourceEventHandlerFuncs{}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if ObjectCluster(obj) == f.clusterID {
				if unwrapped := UnwrapObject(obj); unwrapped != nil {
					handler.OnAdd(unwrapped, false)
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldMatch := ObjectCluster(oldObj) == f.clusterID
			newMatch := ObjectCluster(newObj) == f.clusterID
			switch {
			case oldMatch && newMatch:
				oldUnwrapped := UnwrapObject(oldObj)
				newUnwrapped := UnwrapObject(newObj)
				if oldUnwrapped != nil && newUnwrapped != nil {
					handler.OnUpdate(oldUnwrapped, newUnwrapped)
				}
			case !oldMatch && newMatch:
				if unwrapped := UnwrapObject(newObj); unwrapped != nil {
					handler.OnAdd(unwrapped, false)
				}
			case oldMatch && !newMatch:
				if unwrapped := UnwrapObject(oldObj); unwrapped != nil {
					handler.OnDelete(unwrapped)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if ObjectCluster(obj) == f.clusterID {
				if unwrapped := UnwrapObject(obj); unwrapped != nil {
					handler.OnDelete(unwrapped)
				}
			}
		},
	}
}

var _ cache.SharedIndexInformer = (*filteredSharedIndexInformer)(nil)
