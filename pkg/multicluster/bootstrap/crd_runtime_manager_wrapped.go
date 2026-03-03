package bootstrap

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/apimachinery/pkg/util/validation/field"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	mcinformer "github.com/kplane-dev/informer"
	mcstorage "github.com/kplane-dev/storage"
)

var (
	crdRuntimeMetricsOnce = sync.Once{}
	crdRuntimeCreateTotal = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_runtime_create_total", Help: "Shared CRD runtime creations."}, []string{"status"})
	crdServesLookupTotal  = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_serves_lookup_total", Help: "CRD serves-group-version lookups."}, []string{"result"})
	crdServesCacheHit     = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_serves_cache_hit_total", Help: "CRD serves cache hits."}, []string{"result"})
	crdServesCacheMiss    = metrics.NewCounterVec(&metrics.CounterOpts{Name: "mc_crd_serves_cache_miss_total", Help: "CRD serves cache misses."}, []string{"result"})
	crdServesLookupLat    = metrics.NewHistogramVec(&metrics.HistogramOpts{Name: "mc_crd_serves_lookup_latency_seconds", Help: "CRD serves lookup latency.", Buckets: metrics.ExponentialBuckets(0.001, 2, 12)}, []string{"result"})
)

type CRDRuntimeManagerOptions struct {
	BaseAPIExtensionsConfig *apiextensionsapiserver.Config
	InformerRegistry        *mc.InformerRegistry
	PathPrefix              string
	ControlPlaneSegment     string
	DefaultCluster          string
	Delegate                http.Handler
}

type runtimeEntry struct {
	handler http.Handler
	server  *server.GenericAPIServer
	cancel  context.CancelFunc
}

type CRDRuntimeManager struct {
	opts CRDRuntimeManagerOptions

	mu sync.Mutex

	sharedRuntime *runtimeEntry
	clients       map[string]apiextensionsclient.Interface
	apiClientPool *mc.APIExtensionsClientPool
	runtimeSF     singleflight.Group
	clientSF      singleflight.Group
	sharedCRDSF   singleflight.Group

	// Informer-backed serves index state.
	servesIndex       *CRDServesIndex
	sharedProjection  *crdProjectionStore
	sharedStarted     bool
	crdMCI            *mcinformer.MultiClusterInformer
	crdQueue          workqueue.TypedRateLimitingInterface[string]
	crdWorkersStarted bool
}

func NewCRDRuntimeManager(opts CRDRuntimeManagerOptions) *CRDRuntimeManager {
	crdRuntimeMetricsOnce.Do(func() {
		legacyregistry.MustRegister(crdRuntimeCreateTotal, crdServesLookupTotal, crdServesCacheHit, crdServesCacheMiss, crdServesLookupLat)
	})
	return &CRDRuntimeManager{
		opts:             opts,
		clients:          map[string]apiextensionsclient.Interface{},
		servesIndex:      NewCRDServesIndex(),
		sharedProjection: newCRDProjectionStore(),
		crdQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "mc_shared_crd_status"},
		),
	}
}

// Start initializes the shared CRD informer/workers used for status and delete reconciliation.
func (m *CRDRuntimeManager) Runtime(clusterID string, stopCh <-chan struct{}) (http.Handler, error) {
	if m == nil || clusterID == "" || clusterID == m.opts.DefaultCluster || m.opts.BaseAPIExtensionsConfig == nil {
		return nil, nil
	}
	entry, err := m.ensureSharedRuntime(stopCh)
	if err != nil {
		return nil, err
	}
	return entry.handler, nil
}

func (m *CRDRuntimeManager) ServesGroupVersion(clusterID, group, version string, stopCh <-chan struct{}) (bool, error) {
	if m == nil || clusterID == "" || clusterID == m.opts.DefaultCluster || group == "" || version == "" {
		return false, nil
	}
	start := time.Now()
	if served, ok := m.lookupFromInformerIndex(clusterID, group, version); ok {
		r := result(served)
		crdServesCacheHit.WithLabelValues(r).Inc()
		crdServesLookupTotal.WithLabelValues(r).Inc()
		crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
		return served, nil
	}
	crdServesCacheMiss.WithLabelValues("miss").Inc()

	if err := m.ensureSharedCRDState(stopCh); err != nil {
		crdServesLookupTotal.WithLabelValues("error").Inc()
		crdServesLookupLat.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return false, err
	}
	if served, ok := m.lookupFromInformerIndex(clusterID, group, version); ok {
		r := result(served)
		crdServesCacheHit.WithLabelValues(r).Inc()
		crdServesLookupTotal.WithLabelValues(r).Inc()
		crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
		return served, nil
	}
	// Reconcile from shared projection, then briefly wait for projection updates
	// to absorb fresh CRD install/update events before returning not served.
	if served, ok := m.lookupFromSharedProjection(clusterID, group, version); ok {
		r := result(served)
		crdServesCacheMiss.WithLabelValues("projection").Inc()
		crdServesLookupTotal.WithLabelValues(r).Inc()
		crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
		return served, nil
	}
	if served, ok := m.waitForSharedProjection(clusterID, group, version, 2*time.Second); ok {
		r := result(served)
		crdServesCacheMiss.WithLabelValues("projection-wait").Inc()
		crdServesLookupTotal.WithLabelValues(r).Inc()
		crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
		return served, nil
	}
	r := result(false)
	crdServesLookupTotal.WithLabelValues(r).Inc()
	crdServesLookupLat.WithLabelValues(r).Observe(time.Since(start).Seconds())
	return false, nil
}

func (m *CRDRuntimeManager) ensureSharedRuntime(stopCh <-chan struct{}) (runtimeEntry, error) {
	m.mu.Lock()
	if m.sharedRuntime != nil {
		entry := *m.sharedRuntime
		m.mu.Unlock()
		return entry, nil
	}
	m.mu.Unlock()

	v, err, _ := m.runtimeSF.Do("shared", func() (any, error) {
		m.mu.Lock()
		if m.sharedRuntime != nil {
			entry := *m.sharedRuntime
			m.mu.Unlock()
			return entry, nil
		}
		m.mu.Unlock()

		if m.opts.BaseAPIExtensionsConfig == nil || m.opts.BaseAPIExtensionsConfig.GenericConfig == nil {
			crdRuntimeCreateTotal.WithLabelValues("error").Inc()
			return nil, fmt.Errorf("base apiextensions config is required")
		}
		baseGeneric := *m.opts.BaseAPIExtensionsConfig.GenericConfig
		if baseGeneric.LoopbackClientConfig != nil {
			baseGeneric.LoopbackClientConfig = allClustersLoopbackConfig(baseGeneric.LoopbackClientConfig)
		}
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

		entry := runtimeEntry{
			handler: crdServer.GenericAPIServer.Handler.Director,
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
		m.sharedRuntime = &entry
		m.mu.Unlock()

		crdRuntimeCreateTotal.WithLabelValues("success").Inc()
		return entry, nil
	})
	if err != nil {
		return runtimeEntry{}, err
	}
	entry, ok := v.(runtimeEntry)
	if !ok {
		return runtimeEntry{}, fmt.Errorf("unexpected runtime entry type %T", v)
	}
	return entry, nil
}

func (m *CRDRuntimeManager) ensureClusterClient(clusterID string) (apiextensionsclient.Interface, error) {
	m.mu.Lock()
	if c, ok := m.clients[clusterID]; ok {
		m.mu.Unlock()
		return c, nil
	}
	if m.apiClientPool == nil {
		base := m.baseLoopbackConfig()
		if base == nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("base apiextensions loopback config is required")
		}
		m.apiClientPool = mc.NewAPIExtensionsClientPool(base, m.opts.PathPrefix, m.opts.ControlPlaneSegment)
	}
	pool := m.apiClientPool
	m.mu.Unlock()

	v, err, _ := m.clientSF.Do(clusterID, func() (any, error) {
		m.mu.Lock()
		if c, ok := m.clients[clusterID]; ok {
			m.mu.Unlock()
			return c, nil
		}
		p := m.apiClientPool
		m.mu.Unlock()

		if p == nil {
			p = pool
		}
		cs, err := p.APIExtensionsClientForCluster(clusterID)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.clients[clusterID] = cs
		m.mu.Unlock()
		return cs, nil
	})
	if err != nil {
		return nil, err
	}
	cs, ok := v.(apiextensionsclient.Interface)
	if !ok {
		return nil, fmt.Errorf("unexpected apiextensions client type %T", v)
	}
	return cs, nil
}

func (m *CRDRuntimeManager) lookupFromInformerIndex(clusterID, group, version string) (bool, bool) {
	return m.servesIndex.Lookup(clusterID, group, version)
}

func (m *CRDRuntimeManager) lookupFromSharedProjection(clusterID, group, version string) (bool, bool) {
	crds := m.sharedProjection.List(clusterID)
	objs := make([]interface{}, 0, len(crds))
	served := false
	for _, crd := range crds {
		if crd == nil {
			continue
		}
		objs = append(objs, crd)
		if !isCRDEstablished(crd) || crd.Spec.Group != group {
			continue
		}
		for _, v := range crd.Spec.Versions {
			if v.Served && v.Name == version {
				served = true
				break
			}
		}
	}
	m.rebuildClusterIndex(clusterID, objs)
	return served, true
}

func (m *CRDRuntimeManager) waitForSharedProjection(clusterID, group, version string, timeout time.Duration) (bool, bool) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if served, ok := m.lookupFromInformerIndex(clusterID, group, version); ok {
			return served, true
		}
		if served, ok := m.lookupFromSharedProjection(clusterID, group, version); ok && served {
			return true, true
		}
		time.Sleep(25 * time.Millisecond)
	}
	if served, ok := m.lookupFromInformerIndex(clusterID, group, version); ok {
		return served, true
	}
	if served, ok := m.lookupFromSharedProjection(clusterID, group, version); ok {
		return served, true
	}
	return false, false
}

func (m *CRDRuntimeManager) ensureSharedCRDState(stopCh <-chan struct{}) error {
	m.mu.Lock()
	if m.sharedStarted {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	_, err, _ := m.sharedCRDSF.Do("shared", func() (any, error) {
		m.mu.Lock()
		if m.sharedStarted {
			m.mu.Unlock()
			return nil, nil
		}
		m.mu.Unlock()

		if m.opts.InformerRegistry == nil {
			return nil, fmt.Errorf("InformerRegistry is required for shared CRD state")
		}

		mci, err := m.opts.InformerRegistry.Get(schema.GroupResource{
			Group:    "apiextensions.k8s.io",
			Resource: "customresourcedefinitions",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get MCI for CRDs: %w", err)
		}

		mci.AddEventHandler(mcinformer.MultiClusterEventHandlerFuncs{
			AddFunc: func(obj *mcstorage.ObjectWithClusterIdentity, _ bool) {
				m.onMCICRDUpsert(obj)
			},
			UpdateFunc: func(_, newObj *mcstorage.ObjectWithClusterIdentity) {
				m.onMCICRDUpsert(newObj)
			},
			DeleteFunc: func(obj *mcstorage.ObjectWithClusterIdentity) {
				m.onMCICRDDelete(obj)
			},
		})

		ctx := context.Background()
		if stopCh != nil {
			ctx = wait.ContextForChannel(stopCh)
		}
		for !mci.HasSynced() {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}

		// Backfill existing objects (late handler registration misses existing items).
		for _, clusterID := range mci.Clusters() {
			objs := mci.List(clusterID)
			for _, obj := range objs {
				crd, ok := toVersionedCRD(obj)
				if !ok {
					continue
				}
				m.sharedProjection.Upsert(clusterID, crd)
				m.servesIndex.UpsertCRD(clusterID, crd)
			}
		}

		workerStopCh := stopCh
		if workerStopCh == nil {
			workerStopCh = make(chan struct{})
		}
		m.startSharedCRDWorkers(workerStopCh)

		m.mu.Lock()
		m.crdMCI = mci
		m.sharedStarted = true
		m.mu.Unlock()
		return nil, nil
	})
	return err
}

func (m *CRDRuntimeManager) rebuildClusterIndex(clusterID string, objs []interface{}) {
	m.servesIndex.RebuildCluster(clusterID, objs)
}

// toVersionedCRD converts an internal CRD type to its versioned equivalent.
// The cacher stores internal types; MCI event handlers receive them wrapped
// in cachingObject envelopes. This function unwraps and converts using the
// apiextensions scheme (legacyscheme doesn't register apiextensions types).
func toVersionedCRD(obj interface{}) (*apiextensionsv1.CustomResourceDefinition, bool) {
	// Unwrap cacher's cachingObject envelope if present.
	if co, ok := obj.(runtime.CacheableObject); ok {
		obj = co.GetObject()
	}
	// Fast path: already versioned
	if crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition); ok {
		return crd, true
	}
	rObj, ok := obj.(runtime.Object)
	if !ok {
		return nil, false
	}
	out := &apiextensionsv1.CustomResourceDefinition{}
	if err := apiextensionsapiserver.Scheme.Convert(rObj, out, nil); err != nil {
		return nil, false
	}
	return out, true
}

func (m *CRDRuntimeManager) onMCICRDUpsert(obj *mcstorage.ObjectWithClusterIdentity) {
	clusterID := obj.ClusterID
	if clusterID == "" {
		clusterID = m.opts.DefaultCluster
	}
	if clusterID == "" {
		return
	}
	crd, ok := toVersionedCRD(obj.Object)
	if !ok {
		return
	}
	m.sharedProjection.Upsert(clusterID, crd)
	m.servesIndex.UpsertCRD(clusterID, crd)
	m.enqueueCRDStatus(clusterID, crd.Name)
}

func (m *CRDRuntimeManager) onMCICRDDelete(obj *mcstorage.ObjectWithClusterIdentity) {
	clusterID := obj.ClusterID
	if clusterID == "" {
		clusterID = m.opts.DefaultCluster
	}
	if clusterID == "" {
		return
	}
	crd, ok := toVersionedCRD(obj.Object)
	if !ok {
		return
	}
	m.sharedProjection.Delete(clusterID, crd.Name)
	m.servesIndex.DeleteCRD(clusterID, crd)
}

func (m *CRDRuntimeManager) EnsureCluster(clusterID string, stopCh <-chan struct{}) error {
	if m == nil || clusterID == "" {
		return nil
	}
	if clusterID != m.opts.DefaultCluster {
		if _, err := m.ensureSharedRuntime(stopCh); err != nil {
			return err
		}
	}
	if err := m.ensureSharedCRDState(stopCh); err != nil {
		return err
	}
	_, err := m.ensureClusterClient(clusterID)
	return err
}

func (m *CRDRuntimeManager) CRDGetterForRequest(ctx context.Context, name string) (*apiextensionsv1.CustomResourceDefinition, error) {
	clusterID := m.clusterFromContext(ctx)
	if err := m.ensureSharedCRDState(nil); err != nil {
		return nil, err
	}
	if crd, ok := m.sharedProjection.Get(clusterID, name); ok {
		return crd, nil
	}
	return nil, apierrors.NewNotFound(apiextensionsv1.Resource("customresourcedefinitions"), name)
}

func (m *CRDRuntimeManager) CRDListerForRequest(ctx context.Context) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	clusterID := m.clusterFromContext(ctx)
	if err := m.ensureSharedCRDState(nil); err != nil {
		return nil, err
	}
	return m.sharedProjection.List(clusterID), nil
}

func (m *CRDRuntimeManager) clusterFromContext(ctx context.Context) string {
	cid, _, _ := mc.FromContext(ctx)
	if cid == "" {
		cid = m.opts.DefaultCluster
	}
	if cid == "" {
		cid = mc.DefaultClusterName
	}
	return cid
}

func ServedKeysForCRD(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) []string {
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

type crdProjectionStore struct {
	mu        sync.RWMutex
	byCluster map[string]map[string]*apiextensionsv1.CustomResourceDefinition
}

func newCRDProjectionStore() *crdProjectionStore {
	return &crdProjectionStore{
		byCluster: map[string]map[string]*apiextensionsv1.CustomResourceDefinition{},
	}
}

func (s *crdProjectionStore) Upsert(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) {
	if s == nil || clusterID == "" || crd == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clusterMap, ok := s.byCluster[clusterID]
	if !ok {
		clusterMap = map[string]*apiextensionsv1.CustomResourceDefinition{}
		s.byCluster[clusterID] = clusterMap
	}
	clusterMap[crd.Name] = crd.DeepCopy()
}

func (s *crdProjectionStore) Delete(clusterID, name string) {
	if s == nil || clusterID == "" || name == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clusterMap, ok := s.byCluster[clusterID]
	if !ok {
		return
	}
	delete(clusterMap, name)
	if len(clusterMap) == 0 {
		delete(s.byCluster, clusterID)
	}
}

func (s *crdProjectionStore) Get(clusterID, name string) (*apiextensionsv1.CustomResourceDefinition, bool) {
	if s == nil || clusterID == "" || name == "" {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	clusterMap, ok := s.byCluster[clusterID]
	if !ok {
		return nil, false
	}
	crd, ok := clusterMap[name]
	if !ok || crd == nil {
		return nil, false
	}
	return crd.DeepCopy(), true
}

func (s *crdProjectionStore) List(clusterID string) []*apiextensionsv1.CustomResourceDefinition {
	if s == nil || clusterID == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	clusterMap, ok := s.byCluster[clusterID]
	if !ok {
		return nil
	}
	out := make([]*apiextensionsv1.CustomResourceDefinition, 0, len(clusterMap))
	for _, crd := range clusterMap {
		if crd == nil {
			continue
		}
		out = append(out, crd.DeepCopy())
	}
	return out
}

func (m *CRDRuntimeManager) baseLoopbackConfig() *rest.Config {
	if m == nil || m.opts.BaseAPIExtensionsConfig == nil || m.opts.BaseAPIExtensionsConfig.GenericConfig == nil {
		return nil
	}
	return m.opts.BaseAPIExtensionsConfig.GenericConfig.LoopbackClientConfig
}

func allClustersLoopbackConfig(base *rest.Config) *rest.Config {
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
	return cfg
}

const sharedCRDNamePrefix = "__mc_shared_crd__"

// EncodeSharedCRDResourceName returns a cluster-unique resource token that
// preserves the "<encodedResource>.<group>" CRD name relation used by
// upstream CRD handler lookup.
func EncodeSharedCRDResourceName(clusterID, resource string) string {
	if clusterID == "" || resource == "" {
		return resource
	}
	prefix := sharedCRDNamePrefix + clusterID + "__"
	if strings.HasPrefix(resource, prefix) {
		return resource
	}
	return prefix + resource
}

const sharedCRDWorkerCount = 6

func (m *CRDRuntimeManager) startSharedCRDWorkers(stopCh <-chan struct{}) {
	m.mu.Lock()
	if m.crdWorkersStarted {
		m.mu.Unlock()
		return
	}
	m.crdWorkersStarted = true
	m.mu.Unlock()
	for i := 0; i < sharedCRDWorkerCount; i++ {
		go func() {
			for {
				key, quit := m.crdQueue.Get()
				if quit {
					return
				}
				func() {
					defer m.crdQueue.Done(key)
					if err := m.reconcileCRDStatusKey(key); err != nil {
						klog.Errorf("mc.crd shared status reconcile failed key=%s err=%v", key, err)
						m.crdQueue.AddRateLimited(key)
						return
					}
					m.crdQueue.Forget(key)
				}()
			}
		}()
	}
	go func() {
		<-stopCh
		m.crdQueue.ShutDown()
	}()
}

func (m *CRDRuntimeManager) enqueueCRDStatus(clusterID, name string) {
	if clusterID == "" || name == "" {
		return
	}
	m.crdQueue.Add(clusterID + "\x00" + name)
}

func splitCRDStatusKey(key string) (string, string, bool) {
	clusterID, name, ok := strings.Cut(key, "\x00")
	if !ok || clusterID == "" || name == "" {
		return "", "", false
	}
	return clusterID, name, true
}

func (m *CRDRuntimeManager) reconcileCRDStatusKey(key string) error {
	clusterID, name, ok := splitCRDStatusKey(key)
	if !ok {
		return nil
	}
	_, found := m.sharedProjection.Get(clusterID, name)
	if !found {
		return nil
	}
	cs, err := m.ensureClusterClient(clusterID)
	if err != nil {
		return err
	}
	live, err := cs.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if live.DeletionTimestamp != nil && hasCRDCleanupFinalizer(live) {
		return m.reconcileDeletingCRD(clusterID, live)
	}
	// Let upstream finalizer/status controllers own terminating CRDs.
	// Overwriting status during deletion can stall customresourcecleanup.
	if live.DeletionTimestamp != nil {
		return nil
	}
	desired := live.DeepCopy()
	desired.Status.AcceptedNames = desired.Spec.Names

	namesAccepted := apiextensionsv1.CustomResourceDefinitionCondition{
		Type:    apiextensionsv1.NamesAccepted,
		Status:  apiextensionsv1.ConditionTrue,
		Reason:  "NoConflicts",
		Message: "cluster-scoped naming accepted",
	}
	apiextensionshelpers.SetCRDCondition(desired, namesAccepted)

	established := apiextensionsv1.CustomResourceDefinitionCondition{
		Type:    apiextensionsv1.Established,
		Status:  apiextensionsv1.ConditionTrue,
		Reason:  "InitialNamesAccepted",
		Message: "the initial names have been accepted",
	}
	if desired.Spec.Conversion != nil &&
		desired.Spec.Conversion.Webhook != nil &&
		desired.Spec.Conversion.Webhook.ClientConfig != nil &&
		len(webhook.ValidateCABundle(field.NewPath(""), desired.Spec.Conversion.Webhook.ClientConfig.CABundle)) > 0 {
		established.Status = apiextensionsv1.ConditionFalse
		established.Reason = "InvalidCABundle"
		established.Message = "The conversion webhook CABundle is invalid"
	}
	apiextensionshelpers.SetCRDCondition(desired, established)

	if equality.Semantic.DeepEqual(live.Status, desired.Status) {
		return nil
	}
	_, err = cs.ApiextensionsV1().CustomResourceDefinitions().UpdateStatus(context.TODO(), desired, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if apierrors.IsConflict(err) {
		return err
	}
	return err
}

func hasCRDCleanupFinalizer(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, f := range crd.Finalizers {
		if f == apiextensionsv1.CustomResourceCleanupFinalizer {
			return true
		}
	}
	return false
}

func (m *CRDRuntimeManager) reconcileDeletingCRD(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) error {
	cs, err := m.ensureClusterClient(clusterID)
	if err != nil {
		return err
	}

	live, err := cs.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), crd.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if live.DeletionTimestamp == nil || !hasCRDCleanupFinalizer(live) {
		return nil
	}

	inProgress := live.DeepCopy()
	apiextensionshelpers.SetCRDCondition(inProgress, apiextensionsv1.CustomResourceDefinitionCondition{
		Type:    apiextensionsv1.Terminating,
		Status:  apiextensionsv1.ConditionTrue,
		Reason:  "InstanceDeletionInProgress",
		Message: "CustomResource deletion is in progress",
	})
	if _, err := cs.ApiextensionsV1().CustomResourceDefinitions().UpdateStatus(context.TODO(), inProgress, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return nil
		}
		return err
	}

	cond, delErr := m.deleteCRInstances(clusterID, live)

	live, err = cs.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), crd.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	next := live.DeepCopy()
	apiextensionshelpers.SetCRDCondition(next, cond)
	if delErr != nil {
		if _, err := cs.ApiextensionsV1().CustomResourceDefinitions().UpdateStatus(context.TODO(), next, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			klog.Errorf("mc.crd status update failed while deleting instances cluster=%s crd=%s err=%v", clusterID, crd.Name, err)
		}
		return delErr
	}

	apiextensionshelpers.CRDRemoveFinalizer(next, apiextensionsv1.CustomResourceCleanupFinalizer)
	if _, err := cs.ApiextensionsV1().CustomResourceDefinitions().UpdateStatus(context.TODO(), next, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
	return nil
}

func (m *CRDRuntimeManager) deleteCRInstances(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) (apiextensionsv1.CustomResourceDefinitionCondition, error) {
	dc, err := m.dynamicClientForCluster(clusterID)
	if err != nil {
		return deletingCond("InstanceDeletionFailed", fmt.Sprintf("could not create dynamic client: %v", err)), err
	}

	storageVersion, err := apiextensionshelpers.GetCRDStorageVersion(crd)
	if err != nil {
		return deletingCond("InstanceDeletionFailed", fmt.Sprintf("could not resolve storage version: %v", err)), err
	}
	resource := crd.Status.AcceptedNames.Plural
	if resource == "" {
		resource = crd.Spec.Names.Plural
	}
	gvr := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  storageVersion,
		Resource: resource,
	}
	rc := dc.Resource(gvr)

	ctx := context.TODO()
	list, err := rc.List(ctx, metav1.ListOptions{})
	if err != nil {
		return deletingCond("InstanceDeletionFailed", fmt.Sprintf("could not list instances: %v", err)), err
	}

	if crd.Spec.Scope == apiextensionsv1.NamespaceScoped {
		seen := sets.New[string]()
		for _, item := range list.Items {
			ns := item.GetNamespace()
			if ns == "" || seen.Has(ns) {
				continue
			}
			seen.Insert(ns)
			if err := rc.Namespace(ns).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return deletingCond("InstanceDeletionFailed", fmt.Sprintf("could not issue all deletes: %v", err)), err
			}
		}
	} else {
		if err := rc.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return deletingCond("InstanceDeletionFailed", fmt.Sprintf("could not issue all deletes: %v", err)), err
		}
	}

	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
		l, err := rc.List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		return len(l.Items) == 0, nil
	})
	if err != nil {
		return deletingCond("InstanceDeletionCheck", fmt.Sprintf("could not confirm zero CustomResources remaining: %v", err)), err
	}
	return deletingCond("InstanceDeletionCompleted", "removed all instances"), nil
}

func deletingCond(reason, message string) apiextensionsv1.CustomResourceDefinitionCondition {
	status := apiextensionsv1.ConditionTrue
	if reason == "InstanceDeletionCompleted" {
		status = apiextensionsv1.ConditionFalse
	}
	return apiextensionsv1.CustomResourceDefinitionCondition{
		Type:    apiextensionsv1.Terminating,
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}

func (m *CRDRuntimeManager) dynamicClientForCluster(clusterID string) (dynamic.Interface, error) {
	base := m.baseLoopbackConfig()
	if base == nil {
		return nil, fmt.Errorf("base apiextensions loopback config is required")
	}
	cfg := rest.CopyConfig(base)
	host, err := mc.ClusterHost(cfg.Host, mc.Options{
		PathPrefix:          m.opts.PathPrefix,
		ControlPlaneSegment: m.opts.ControlPlaneSegment,
	}, clusterID)
	if err != nil {
		return nil, err
	}
	cfg.Host = host
	return dynamic.NewForConfig(cfg)
}

