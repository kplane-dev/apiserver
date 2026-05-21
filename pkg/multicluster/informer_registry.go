package multicluster

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	"github.com/kplane-dev/informer"
)

// InformerRegistry bridges storage creation (lazy, per-resource) and informer
// consumption (by managers). It creates MultiClusterInformer instances on demand,
// backed by the raw cacher for each resource type.
type InformerRegistry struct {
	ctx       context.Context
	mu        sync.Mutex
	storages  map[schema.GroupResource]*clusteredStorage
	informers map[schema.GroupResource]*informer.MultiClusterInformer
}

// NewInformerRegistry creates a new InformerRegistry. The context controls the
// lifetime of all MultiClusterInformers created by this registry.
func NewInformerRegistry(ctx context.Context) *InformerRegistry {
	return &InformerRegistry{
		ctx:       ctx,
		storages:  make(map[schema.GroupResource]*clusteredStorage),
		informers: make(map[schema.GroupResource]*informer.MultiClusterInformer),
	}
}

// RegisterStorage is called by newClusteredStorage() to register the storage
// for a resource type. The actual cacher is created lazily via ensureStore().
func (r *InformerRegistry) RegisterStorage(gr schema.GroupResource, cs *clusteredStorage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.storages[gr] = cs
	klog.V(2).Infof("mc.informerRegistry registered storage for %s", gr)
}

// ListRegistered returns the GroupResources for which storage has been
// registered. Storage registration happens lazily as the apiserver wires up
// each resource, so this set grows over the server lifetime. The returned
// slice is a copy and safe to retain.
func (r *InformerRegistry) ListRegistered() []schema.GroupResource {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]schema.GroupResource, 0, len(r.storages))
	for gr := range r.storages {
		out = append(out, gr)
	}
	return out
}

// ListLive returns the GroupResources for which a MultiClusterInformer has
// already been created (i.e. at least one consumer has read or watched the
// resource). Reading from these MCIs does not start additional watches.
func (r *InformerRegistry) ListLive() []schema.GroupResource {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]schema.GroupResource, 0, len(r.informers))
	for gr := range r.informers {
		out = append(out, gr)
	}
	return out
}

// Peek returns the MultiClusterInformer for a resource if one has already been
// created, without triggering cacher/MCI creation. Returns (nil, false) if no
// MCI exists yet for the resource.
func (r *InformerRegistry) Peek(gr schema.GroupResource) (*informer.MultiClusterInformer, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mci, ok := r.informers[gr]
	return mci, ok
}

// Get returns (or creates) the MultiClusterInformer for a resource.
// Forces cacher creation via ensureStore() if needed.
func (r *InformerRegistry) Get(gr schema.GroupResource) (*informer.MultiClusterInformer, error) {
	r.mu.Lock()
	if mci, ok := r.informers[gr]; ok {
		r.mu.Unlock()
		return mci, nil
	}
	cs, ok := r.storages[gr]
	r.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("no registered storage for %s", gr)
	}

	// ensureStore() acquires its own lock; call outside our lock to avoid nesting.
	store, err := cs.ensureStore()
	if err != nil {
		return nil, fmt.Errorf("failed to ensure store for %s: %w", gr, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after re-acquiring lock.
	if mci, ok := r.informers[gr]; ok {
		return mci, nil
	}

	mci := informer.New(informer.Config{
		Storage:        store,
		ResourcePrefix: cs.kindRootPrefix(),
		GroupResource:  gr,
	})

	go mci.Run(r.ctx)

	r.informers[gr] = mci
	klog.V(2).Infof("mc.informerRegistry created MultiClusterInformer for %s prefix=%s", gr, cs.kindRootPrefix())
	return mci, nil
}
