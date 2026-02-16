package generic

import (
	"fmt"
	"sort"
	"sync"

	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	"k8s.io/client-go/informers"
	admissionregistrationlisters "k8s.io/client-go/listers/admissionregistration/v1"
)

type accessorCacheEntry struct {
	resourceVersion string
	accessors       []webhook.WebhookAccessor
}

type mutatingConfigSource struct {
	lister    admissionregistrationlisters.MutatingWebhookConfigurationLister
	hasSynced func() bool

	mu    sync.RWMutex
	cache map[string]accessorCacheEntry
}

type validatingConfigSource struct {
	lister    admissionregistrationlisters.ValidatingWebhookConfigurationLister
	hasSynced func() bool

	mu    sync.RWMutex
	cache map[string]accessorCacheEntry
}

// NewListBackedMutatingWebhookSource returns a webhook source backed by lister reads.
// Unlike upstream configuration managers, this source does not register per-plugin
// informer handlers, which avoids O(clusters) listener fanout.
func NewListBackedMutatingWebhookSource(f informers.SharedInformerFactory) Source {
	inf := f.Admissionregistration().V1().MutatingWebhookConfigurations()
	return &mutatingConfigSource{
		lister:    inf.Lister(),
		hasSynced: inf.Informer().HasSynced,
		cache:     map[string]accessorCacheEntry{},
	}
}

// NewListBackedValidatingWebhookSource returns a webhook source backed by lister reads.
// Unlike upstream configuration managers, this source does not register per-plugin
// informer handlers, which avoids O(clusters) listener fanout.
func NewListBackedValidatingWebhookSource(f informers.SharedInformerFactory) Source {
	inf := f.Admissionregistration().V1().ValidatingWebhookConfigurations()
	return &validatingConfigSource{
		lister:    inf.Lister(),
		hasSynced: inf.Informer().HasSynced,
		cache:     map[string]accessorCacheEntry{},
	}
}

func (s *mutatingConfigSource) Webhooks() []webhook.WebhookAccessor {
	configs, err := s.lister.List(labels.Everything())
	if err != nil {
		return nil
	}
	sort.SliceStable(configs, func(i, j int) bool { return configs[i].Name < configs[j].Name })
	total := 0
	for _, cfg := range configs {
		total += len(cfg.Webhooks)
	}
	out := make([]webhook.WebhookAccessor, 0, total)
	for _, cfg := range configs {
		out = append(out, s.accessorsForMutatingConfig(cfg)...)
	}
	return out
}

func (s *mutatingConfigSource) HasSynced() bool {
	return s.hasSynced()
}

func (s *mutatingConfigSource) accessorsForMutatingConfig(cfg *v1.MutatingWebhookConfiguration) []webhook.WebhookAccessor {
	s.mu.RLock()
	cached, ok := s.cache[cfg.Name]
	s.mu.RUnlock()
	if ok && cached.resourceVersion == cfg.ResourceVersion {
		return cached.accessors
	}
	names := map[string]int{}
	accessors := make([]webhook.WebhookAccessor, 0, len(cfg.Webhooks))
	for i := range cfg.Webhooks {
		n := cfg.Webhooks[i].Name
		uid := fmt.Sprintf("%s/%s/%d", cfg.Name, n, names[n])
		names[n]++
		accessors = append(accessors, webhook.NewMutatingWebhookAccessor(uid, cfg.Name, &cfg.Webhooks[i]))
	}
	s.mu.Lock()
	s.cache[cfg.Name] = accessorCacheEntry{
		resourceVersion: cfg.ResourceVersion,
		accessors:       accessors,
	}
	s.mu.Unlock()
	return accessors
}

func (s *validatingConfigSource) Webhooks() []webhook.WebhookAccessor {
	configs, err := s.lister.List(labels.Everything())
	if err != nil {
		return nil
	}
	sort.SliceStable(configs, func(i, j int) bool { return configs[i].Name < configs[j].Name })
	total := 0
	for _, cfg := range configs {
		total += len(cfg.Webhooks)
	}
	out := make([]webhook.WebhookAccessor, 0, total)
	for _, cfg := range configs {
		out = append(out, s.accessorsForValidatingConfig(cfg)...)
	}
	return out
}

func (s *validatingConfigSource) HasSynced() bool {
	return s.hasSynced()
}

func (s *validatingConfigSource) accessorsForValidatingConfig(cfg *v1.ValidatingWebhookConfiguration) []webhook.WebhookAccessor {
	s.mu.RLock()
	cached, ok := s.cache[cfg.Name]
	s.mu.RUnlock()
	if ok && cached.resourceVersion == cfg.ResourceVersion {
		return cached.accessors
	}
	names := map[string]int{}
	accessors := make([]webhook.WebhookAccessor, 0, len(cfg.Webhooks))
	for i := range cfg.Webhooks {
		n := cfg.Webhooks[i].Name
		uid := fmt.Sprintf("%s/%s/%d", cfg.Name, n, names[n])
		names[n]++
		accessors = append(accessors, webhook.NewValidatingWebhookAccessor(uid, cfg.Name, &cfg.Webhooks[i]))
	}
	s.mu.Lock()
	s.cache[cfg.Name] = accessorCacheEntry{
		resourceVersion: cfg.ResourceVersion,
		accessors:       accessors,
	}
	s.mu.Unlock()
	return accessors
}

