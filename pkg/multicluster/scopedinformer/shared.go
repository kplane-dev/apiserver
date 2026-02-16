package scopedinformer

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
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

func EnsureClusterIndex(inf cache.SharedIndexInformer, clusterLabelKey string) error {
	return inf.AddIndexers(cache.Indexers{
		ClusterIndexName: func(obj interface{}) ([]string, error) {
			cid := ObjectCluster(obj, clusterLabelKey)
			if cid == "" {
				return nil, nil
			}
			return []string{cid}, nil
		},
	})
}

func ObjectCluster(obj interface{}, clusterLabelKey string) string {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return acc.GetLabels()[clusterLabelKey]
}

func FilteredByCluster(indexer cache.Indexer, clusterID string) []interface{} {
	items, err := indexer.ByIndex(ClusterIndexName, clusterID)
	if err != nil {
		return nil
	}
	return items
}

func NewFilteredSharedIndexInformer(shared cache.SharedIndexInformer, clusterID, clusterLabelKey string) cache.SharedIndexInformer {
	return &filteredSharedIndexInformer{shared: shared, clusterID: clusterID, clusterLabelKey: clusterLabelKey}
}

type filteredSharedIndexInformer struct {
	shared          cache.SharedIndexInformer
	clusterID       string
	clusterLabelKey string
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
			if ObjectCluster(obj, f.clusterLabelKey) == f.clusterID {
				handler.OnAdd(obj, false)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldMatch := ObjectCluster(oldObj, f.clusterLabelKey) == f.clusterID
			newMatch := ObjectCluster(newObj, f.clusterLabelKey) == f.clusterID
			switch {
			case oldMatch && newMatch:
				handler.OnUpdate(oldObj, newObj)
			case !oldMatch && newMatch:
				handler.OnAdd(newObj, false)
			case oldMatch && !newMatch:
				handler.OnDelete(oldObj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if ObjectCluster(obj, f.clusterLabelKey) == f.clusterID {
				handler.OnDelete(obj)
			}
		},
	}
}

var _ cache.SharedIndexInformer = (*filteredSharedIndexInformer)(nil)
