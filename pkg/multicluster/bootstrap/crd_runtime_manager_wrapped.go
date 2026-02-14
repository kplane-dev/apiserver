package bootstrap

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
)

const (
	servesLookupTimeout = 2 * time.Second
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
	BaseAPIExtensionsConfig   *apiextensionsapiserver.Config
	APIExtensionsInformerPool *mc.APIExtensionsInformerPool
	PathPrefix                string
	ControlPlaneSegment       string
	DefaultCluster            string
	Delegate                  http.Handler
}

type runtimeEntry struct {
	handler http.Handler
	server  *server.GenericAPIServer
	cancel  context.CancelFunc
}

type clusterState struct {
	r runtimeEntry
	c apiextensionsclient.Interface
}

type CRDRuntimeManager struct {
	opts CRDRuntimeManagerOptions

	mu sync.Mutex

	runtimes map[string]runtimeEntry
	clients  map[string]apiextensionsclient.Interface
	createSF singleflight.Group

	// Informer-backed serves index state.
	informerStarted map[string]bool
	clusterSynced   map[string]bool
	serves          map[string]bool
	clusterKeys     map[string]map[string]struct{}
	crdKeys         map[string]map[string][]string
	informerSF      singleflight.Group
}

func NewCRDRuntimeManager(opts CRDRuntimeManagerOptions) *CRDRuntimeManager {
	crdRuntimeMetricsOnce.Do(func() {
		legacyregistry.MustRegister(crdRuntimeCreateTotal, crdServesLookupTotal, crdServesCacheHit, crdServesCacheMiss, crdServesLookupLat)
	})
	return &CRDRuntimeManager{
		opts:            opts,
		runtimes:        map[string]runtimeEntry{},
		clients:         map[string]apiextensionsclient.Interface{},
		informerStarted: map[string]bool{},
		clusterSynced:   map[string]bool{},
		serves:          map[string]bool{},
		clusterKeys:     map[string]map[string]struct{}{},
		crdKeys:         map[string]map[string][]string{},
	}
}

func (m *CRDRuntimeManager) Runtime(clusterID string, stopCh <-chan struct{}) (http.Handler, error) {
	if m == nil || clusterID == "" || clusterID == m.opts.DefaultCluster || m.opts.BaseAPIExtensionsConfig == nil {
		return nil, nil
	}
	state, err := m.ensureClusterState(clusterID, stopCh)
	if err != nil {
		return nil, err
	}
	return state.r.handler, nil
}

func (m *CRDRuntimeManager) ServesGroupVersion(clusterID, group, version string, stopCh <-chan struct{}) (bool, error) {
	if m == nil || clusterID == "" || clusterID == m.opts.DefaultCluster || group == "" || version == "" {
		return false, nil
	}
	start := time.Now()
	key := clusterID + "\x00" + group + "\x00" + version
	if served, ok := m.lookupFromInformerIndex(clusterID, key); ok {
		r := result(served)
		crdServesCacheHit.WithLabelValues(r).Inc()
		crdServesLookupTotal.WithLabelValues(r).Inc()
		crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
		return served, nil
	}
	crdServesCacheMiss.WithLabelValues("miss").Inc()

	// Prefer shared informer-backed state for served checks.
	if err := m.ensureInformerState(clusterID, stopCh); err == nil {
		if served, ok := m.lookupFromInformerIndex(clusterID, key); ok {
			r := result(served)
			crdServesCacheHit.WithLabelValues(r).Inc()
			crdServesLookupTotal.WithLabelValues(r).Inc()
			crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
			return served, nil
		}
	}

	// Fallback to direct API list if informer state is unavailable.
	state, err := m.ensureClusterState(clusterID, stopCh)
	if err != nil {
		crdServesLookupTotal.WithLabelValues("error").Inc()
		crdServesLookupLat.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return false, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), servesLookupTimeout)
	defer cancel()
	list, err := state.c.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
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
	r := result(served)
	crdServesLookupTotal.WithLabelValues(r).Inc()
	crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
	return served, nil
}

func (m *CRDRuntimeManager) ensureClusterState(clusterID string, stopCh <-chan struct{}) (clusterState, error) {
	m.mu.Lock()
	if r, ok := m.runtimes[clusterID]; ok {
		c := m.clients[clusterID]
		m.mu.Unlock()
		return clusterState{r: r, c: c}, nil
	}
	m.mu.Unlock()

	v, err, _ := m.createSF.Do(clusterID, func() (any, error) {
		m.mu.Lock()
		if r, ok := m.runtimes[clusterID]; ok {
			c := m.clients[clusterID]
			m.mu.Unlock()
			return clusterState{r: r, c: c}, nil
		}
		m.mu.Unlock()

		if m.opts.BaseAPIExtensionsConfig == nil || m.opts.BaseAPIExtensionsConfig.GenericConfig == nil {
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, fmt.Errorf("base apiextensions config is required")
		}
		baseGeneric := *m.opts.BaseAPIExtensionsConfig.GenericConfig
		loopback := rest.CopyConfig(baseGeneric.LoopbackClientConfig)
		host, err := mc.ClusterHost(loopback.Host, mc.Options{
			PathPrefix:          m.opts.PathPrefix,
			ControlPlaneSegment: m.opts.ControlPlaneSegment,
		}, clusterID)
		if err != nil {
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, fmt.Errorf("build cluster host: %w", err)
		}
		loopback.Host = host
		baseGeneric.LoopbackClientConfig = loopback

		baseCfg := *m.opts.BaseAPIExtensionsConfig
		baseCfg.GenericConfig = &baseGeneric
		completed := baseCfg.Complete()
		delegate := m.opts.Delegate
		if delegate == nil {
			delegate = http.NotFoundHandler()
		}
		crdServer, err := completed.New(server.NewEmptyDelegateWithCustomHandler(delegate))
		if err != nil {
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, err
		}
		runCtx, cancel := context.WithCancel(context.Background())
		go crdServer.GenericAPIServer.RunPostStartHooks(runCtx)

		cs, err := apiextensionsclient.NewForConfig(rest.CopyConfig(loopback))
		if err != nil {
			cancel()
			crdServer.GenericAPIServer.Destroy()
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, err
		}
		entry := runtimeEntry{
			handler: crdServer.GenericAPIServer.Handler.NonGoRestfulMux,
			server:  crdServer.GenericAPIServer,
			cancel:  cancel,
		}
		if stopCh != nil {
			go func() {
				<-stopCh
				cancel()
				crdServer.GenericAPIServer.Destroy()
			}()
		}

		m.mu.Lock()
		m.runtimes[clusterID] = entry
		m.clients[clusterID] = cs
		m.mu.Unlock()

		crdRuntimeCreateTotal.WithLabelValues("success").Inc()
		return clusterState{r: entry, c: cs}, nil
	})
	if err != nil {
		return clusterState{}, err
	}
	state, ok := v.(clusterState)
	if !ok {
		return clusterState{}, fmt.Errorf("unexpected cluster state type %T", v)
	}
	return state, nil
}

func (m *CRDRuntimeManager) lookupFromInformerIndex(clusterID, key string) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.clusterSynced[clusterID] {
		return false, false
	}
	_, served := m.serves[key]
	return served, true
}

func (m *CRDRuntimeManager) ensureInformerState(clusterID string, stopCh <-chan struct{}) error {
	if m.opts.APIExtensionsInformerPool == nil {
		return fmt.Errorf("apiextensions informer pool not configured")
	}
	m.mu.Lock()
	if m.informerStarted[clusterID] && m.clusterSynced[clusterID] {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	_, err, _ := m.informerSF.Do(clusterID, func() (any, error) {
		m.mu.Lock()
		if m.informerStarted[clusterID] && m.clusterSynced[clusterID] {
			m.mu.Unlock()
			return nil, nil
		}
		m.mu.Unlock()

		cs, factory, poolStopCh, err := m.opts.APIExtensionsInformerPool.Get(clusterID)
		if err != nil {
			return nil, err
		}
		crdInformer := factory.Apiextensions().V1().CustomResourceDefinitions().Informer()
		_, err = crdInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				m.onCRDUpsert(clusterID, obj)
			},
			UpdateFunc: func(_, newObj interface{}) {
				m.onCRDUpsert(clusterID, newObj)
			},
			DeleteFunc: func(obj interface{}) {
				m.onCRDDelete(clusterID, obj)
			},
		})
		if err != nil {
			return nil, err
		}

		stop := poolStopCh
		if stop == nil {
			stop = stopCh
		}
		if stop == nil {
			return nil, fmt.Errorf("missing stop channel for apiextensions informer")
		}
		factory.Start(stop)
		if !cache.WaitForCacheSync(stop, crdInformer.HasSynced) {
			return nil, fmt.Errorf("failed waiting for CRD informer sync for cluster=%s", clusterID)
		}

		m.rebuildClusterIndex(clusterID, crdInformer.GetStore().List())

		m.mu.Lock()
		m.informerStarted[clusterID] = true
		m.clusterSynced[clusterID] = true
		if _, ok := m.clients[clusterID]; !ok {
			m.clients[clusterID] = cs
		}
		m.mu.Unlock()
		return nil, nil
	})
	return err
}

func (m *CRDRuntimeManager) rebuildClusterIndex(clusterID string, objs []interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k := range m.clusterKeys[clusterID] {
		delete(m.serves, k)
	}
	m.clusterKeys[clusterID] = map[string]struct{}{}
	m.crdKeys[clusterID] = map[string][]string{}

	for _, obj := range objs {
		crd, ok := crdFromObj(obj)
		if !ok {
			continue
		}
		keys := servedKeysForCRD(clusterID, crd)
		m.setCRDKeysLocked(clusterID, crd.Name, keys)
	}
}

func (m *CRDRuntimeManager) onCRDUpsert(clusterID string, obj interface{}) {
	crd, ok := crdFromObj(obj)
	if !ok {
		return
	}
	keys := servedKeysForCRD(clusterID, crd)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCRDKeysLocked(clusterID, crd.Name, keys)
}

func (m *CRDRuntimeManager) onCRDDelete(clusterID string, obj interface{}) {
	crd, ok := crdFromObj(obj)
	if !ok {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCRDKeysLocked(clusterID, crd.Name, nil)
}

func (m *CRDRuntimeManager) setCRDKeysLocked(clusterID, crdName string, keys []string) {
	if m.clusterKeys[clusterID] == nil {
		m.clusterKeys[clusterID] = map[string]struct{}{}
	}
	if m.crdKeys[clusterID] == nil {
		m.crdKeys[clusterID] = map[string][]string{}
	}
	for _, old := range m.crdKeys[clusterID][crdName] {
		delete(m.serves, old)
		delete(m.clusterKeys[clusterID], old)
	}
	if len(keys) == 0 {
		delete(m.crdKeys[clusterID], crdName)
		return
	}
	m.crdKeys[clusterID][crdName] = keys
	for _, k := range keys {
		m.serves[k] = true
		m.clusterKeys[clusterID][k] = struct{}{}
	}
}

func servedKeysForCRD(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) []string {
	if crd == nil || !isCRDEstablished(crd) {
		return nil
	}
	keys := make([]string, 0, len(crd.Spec.Versions))
	for _, v := range crd.Spec.Versions {
		if !v.Served {
			continue
		}
		keys = append(keys, clusterID+"\x00"+crd.Spec.Group+"\x00"+v.Name)
	}
	return keys
}

func crdFromObj(obj interface{}) (*apiextensionsv1.CustomResourceDefinition, bool) {
	if obj == nil {
		return nil, false
	}
	if crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition); ok {
		return crd, true
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if crd, ok := tombstone.Obj.(*apiextensionsv1.CustomResourceDefinition); ok {
			return crd, true
		}
	}
	return nil, false
}

func (m *CRDRuntimeManager) invalidateCluster(clusterID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.clusterKeys[clusterID] {
		delete(m.serves, k)
	}
	delete(m.clusterKeys, clusterID)
	delete(m.crdKeys, clusterID)
	delete(m.clusterSynced, clusterID)
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
