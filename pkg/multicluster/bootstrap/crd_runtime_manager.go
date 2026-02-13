package bootstrap

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"golang.org/x/sync/singleflight"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
)

const (
	servesLookupTimeout = 2 * time.Second
	servesCacheTTL      = 5 * time.Second
	watchRetryBackoff   = 500 * time.Millisecond
)

var (
	crdRuntimeMetricsOnce = sync.Once{}
	crdRuntimeCreateTotal = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_runtime_create_total", Help: "Per-cluster CRD runtime creations."}, []string{"status"})
	crdServesLookupTotal  = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_serves_lookup_total", Help: "CRD serves-group-version lookups."}, []string{"result"})
	crdServesCacheHit     = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_serves_cache_hit_total", Help: "CRD serves cache hits."}, []string{"result"})
	crdServesCacheMiss    = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_serves_cache_miss_total", Help: "CRD serves cache misses."}, []string{"result"})
	crdServesLookupLat    = metrics.NewHistogramVec(&metrics.HistogramOpts{Name: "mc_crd_serves_lookup_latency_seconds", Help: "CRD serves lookup latency.", Buckets: metrics.ExponentialBuckets(0.001, 2, 12)}, []string{"result"})
)

type CRDRuntimeManagerOptions struct {
	BaseLoopbackClientConfig *rest.Config
	PathPrefix, ControlPlaneSegment, DefaultCluster string
	Delegate                                        http.Handler
	CRDRESTOptionsGetter                            genericregistry.RESTOptionsGetter
	Admission                                       admission.Interface
	ServiceResolver                                 webhook.ServiceResolver
	AuthResolverWrapper                             webhook.AuthenticationInfoResolverWrapper
	MasterCount                                     int
	Authorizer                                      authorizer.Authorizer
	RequestTimeout, MinRequestTimeout               time.Duration
	StaticOpenAPISpec                               map[string]*spec.Schema
	MaxRequestBodyBytes                             int64
}

type servesCacheEntry struct {
	served bool
	exp    time.Time
}

type clusterState struct {
	rt *apiextensionsapiserver.ClusterScopedCRDRuntime
	cs *apiextensionsclient.Clientset
}

type CRDRuntimeManager struct {
	opts CRDRuntimeManagerOptions

	mu      sync.Mutex
	runtimes map[string]*apiextensionsapiserver.ClusterScopedCRDRuntime
	clients  map[string]*apiextensionsclient.Clientset
	started  map[string]bool

	cache      map[string]servesCacheEntry
	clusterKeys map[string]map[string]struct{}
	createSF   singleflight.Group
}

func NewCRDRuntimeManager(opts CRDRuntimeManagerOptions) *CRDRuntimeManager {
	crdRuntimeMetricsOnce.Do(func() {
		legacyregistry.MustRegister(crdRuntimeCreateTotal, crdServesLookupTotal, crdServesCacheHit, crdServesCacheMiss, crdServesLookupLat)
	})
	if opts.BaseLoopbackClientConfig != nil {
		opts.BaseLoopbackClientConfig = rest.CopyConfig(opts.BaseLoopbackClientConfig)
	}
	return &CRDRuntimeManager{
		opts:       opts,
		runtimes:   map[string]*apiextensionsapiserver.ClusterScopedCRDRuntime{},
		clients:    map[string]*apiextensionsclient.Clientset{},
		started:    map[string]bool{},
		cache:      map[string]servesCacheEntry{},
		clusterKeys: map[string]map[string]struct{}{},
	}
}

func (m *CRDRuntimeManager) Runtime(clusterID string, stopCh <-chan struct{}) (http.Handler, error) {
	if m == nil || clusterID == "" || clusterID == m.opts.DefaultCluster || m.opts.BaseLoopbackClientConfig == nil {
		return nil, nil
	}
	rt, _, err := m.ensureClusterState(clusterID, stopCh)
	if err != nil || rt == nil {
		return nil, err
	}
	rt.Start(stopCh)
	return rt.Handler(), nil
}

func (m *CRDRuntimeManager) ServesGroupVersion(clusterID, group, version string, stopCh <-chan struct{}) (bool, error) {
	if m == nil || clusterID == "" || clusterID == m.opts.DefaultCluster || group == "" || version == "" {
		return false, nil
	}
	start := time.Now()
	key := clusterID + "\x00" + group + "\x00" + version
	if served, ok := m.getCache(key); ok {
		r := result(served)
		crdServesCacheHit.WithLabelValues(r).Inc()
		crdServesLookupTotal.WithLabelValues(r).Inc()
		crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
		return served, nil
	}
	crdServesCacheMiss.WithLabelValues("miss").Inc()
	_, cs, err := m.ensureClusterState(clusterID, stopCh)
	if err != nil {
		crdServesLookupTotal.WithLabelValues("error").Inc()
		crdServesLookupLat.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), servesLookupTimeout)
	defer cancel()
	list, err := cs.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		crdServesLookupTotal.WithLabelValues("error").Inc()
		crdServesLookupLat.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return false, err
	}
	served := false
	for i := range list.Items {
		crd := &list.Items[i]
		if crd.Spec.Group != group || !isCRDEstablished(crd) {
			continue
		}
		for _, v := range crd.Spec.Versions {
			if v.Name == version && v.Served {
				served = true
				break
			}
		}
		if served {
			break
		}
	}
	m.setCache(clusterID, key, served)
	r := result(served)
	crdServesLookupTotal.WithLabelValues(r).Inc()
	crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
	return served, nil
}

func (m *CRDRuntimeManager) ensureClusterState(clusterID string, stopCh <-chan struct{}) (*apiextensionsapiserver.ClusterScopedCRDRuntime, *apiextensionsclient.Clientset, error) {
	m.mu.Lock()
	if rt, ok := m.runtimes[clusterID]; ok {
		cs := m.clients[clusterID]
		m.mu.Unlock()
		return rt, cs, nil
	}
	m.mu.Unlock()
	v, err, _ := m.createSF.Do(clusterID, func() (any, error) {
		m.mu.Lock()
		if rt, ok := m.runtimes[clusterID]; ok {
			cs := m.clients[clusterID]
			m.mu.Unlock()
			return clusterState{rt: rt, cs: cs}, nil
		}
		m.mu.Unlock()
		cfg := rest.CopyConfig(m.opts.BaseLoopbackClientConfig)
		host, err := mc.ClusterHost(cfg.Host, mc.Options{PathPrefix: m.opts.PathPrefix, ControlPlaneSegment: m.opts.ControlPlaneSegment}, clusterID)
		if err != nil {
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, fmt.Errorf("build cluster host: %w", err)
		}
		cfg.Host = host
		rt, err := apiextensionsapiserver.NewClusterScopedCRDRuntime(apiextensionsapiserver.ClusterScopedCRDConfig{
			LoopbackClientConfig: cfg, Delegate: m.opts.Delegate, CRDRESTOptionsGetter: m.opts.CRDRESTOptionsGetter,
			Admission: m.opts.Admission, ServiceResolver: m.opts.ServiceResolver, AuthResolverWrapper: m.opts.AuthResolverWrapper,
			MasterCount: m.opts.MasterCount, Authorizer: m.opts.Authorizer, RequestTimeout: m.opts.RequestTimeout, MinRequestTimeout: m.opts.MinRequestTimeout,
			StaticOpenAPISpec: m.opts.StaticOpenAPISpec, MaxRequestBodyBytes: m.opts.MaxRequestBodyBytes,
		})
		if err != nil {
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, err
		}
		cs, err := apiextensionsclient.NewForConfig(rest.CopyConfig(cfg))
		if err != nil {
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, err
		}
		rt.Start(stopCh)
		m.mu.Lock()
		m.runtimes[clusterID] = rt
		m.clients[clusterID] = cs
		if !m.started[clusterID] {
			m.started[clusterID] = true
			go m.watchUpdates(clusterID, cs, stopCh)
		}
		m.mu.Unlock()
		crdRuntimeCreateTotal.WithLabelValues("success").Inc()
		return clusterState{rt: rt, cs: cs}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	s := v.(clusterState)
	return s.rt, s.cs, nil
}

func (m *CRDRuntimeManager) watchUpdates(clusterID string, cs *apiextensionsclient.Clientset, stopCh <-chan struct{}) {
	for {
		select { case <-stopCh: return; default: }
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		w, err := cs.ApiextensionsV1().CustomResourceDefinitions().Watch(ctx, metav1.ListOptions{AllowWatchBookmarks: true, ResourceVersion: "0"})
		if err != nil {
			cancel()
			select { case <-stopCh: return; case <-time.After(watchRetryBackoff): continue }
		}
		m.invalidateCluster(clusterID)
		closed := false
		for !closed {
			select {
			case <-stopCh:
				w.Stop(); cancel(); return
			case _, ok := <-w.ResultChan():
				if !ok { closed = true; break }
				m.invalidateCluster(clusterID)
			}
		}
		w.Stop(); cancel()
		select { case <-stopCh: return; case <-time.After(watchRetryBackoff): }
	}
}

func (m *CRDRuntimeManager) getCache(key string) (bool, bool) {
	now := time.Now()
	m.mu.Lock(); defer m.mu.Unlock()
	v, ok := m.cache[key]
	if !ok || now.After(v.exp) {
		delete(m.cache, key)
		return false, false
	}
	return v.served, true
}

func (m *CRDRuntimeManager) setCache(clusterID, key string, served bool) {
	m.mu.Lock(); defer m.mu.Unlock()
	m.cache[key] = servesCacheEntry{served: served, exp: time.Now().Add(servesCacheTTL)}
	if m.clusterKeys[clusterID] == nil {
		m.clusterKeys[clusterID] = map[string]struct{}{}
	}
	m.clusterKeys[clusterID][key] = struct{}{}
}

func (m *CRDRuntimeManager) invalidateCluster(clusterID string) {
	m.mu.Lock(); defer m.mu.Unlock()
	for k := range m.clusterKeys[clusterID] {
		delete(m.cache, k)
	}
	delete(m.clusterKeys, clusterID)
}

func isCRDEstablished(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, c := range crd.Status.Conditions {
		if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}
	return false
}

func result(served bool) string {
	if served {
		return "served"
	}
	return "not_served"
}

