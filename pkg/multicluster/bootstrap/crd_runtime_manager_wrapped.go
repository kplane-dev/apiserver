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
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/apimachinery/pkg/util/validation/field"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
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
	servesIndex      *CRDServesIndex
	sharedProjection *crdProjectionStore
	sharedStarted    bool
	sharedFactory    apiextensionsinformers.SharedInformerFactory
	sharedStopCh     <-chan struct{}
	sharedOwnedStop  chan struct{}
	crdQueue         workqueue.TypedRateLimitingInterface[string]
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
	// No fallback direct API lookup: shared projection is the source of truth.
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

		base := m.baseLoopbackConfig()
		if base == nil {
			return nil, fmt.Errorf("base apiextensions loopback config is required for shared CRD informer")
		}
		cs, err := allClustersAPIExtensionsClient(base)
		if err != nil {
			return nil, err
		}

		factory := apiextensionsinformers.NewSharedInformerFactory(cs, 0)
		crdInformer := factory.Apiextensions().V1().CustomResourceDefinitions().Informer()
		if err := crdInformer.SetTransform(transformCRDForShared(mc.DefaultClusterAnnotation)); err != nil {
			return nil, err
		}
		_, err = crdInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				m.onSharedCRDUpsert(obj)
			},
			UpdateFunc: func(_, newObj interface{}) {
				m.onSharedCRDUpsert(newObj)
			},
			DeleteFunc: func(obj interface{}) {
				m.onSharedCRDDelete(obj)
			},
		})
		if err != nil {
			return nil, err
		}

		startStop := m.sharedStartStopCh(stopCh)
		factory.Start(startStop)
		if !cache.WaitForCacheSync(startStop, crdInformer.HasSynced) {
			return nil, fmt.Errorf("failed waiting for shared CRD informer sync")
		}
		m.startSharedCRDWorkers(startStop)

		m.rebuildSharedProjection(crdInformer.GetStore().List())

		m.mu.Lock()
		m.sharedFactory = factory
		m.sharedStarted = true
		m.mu.Unlock()
		return nil, nil
	})
	return err
}

func (m *CRDRuntimeManager) rebuildClusterIndex(clusterID string, objs []interface{}) {
	m.servesIndex.RebuildCluster(clusterID, objs)
}

func (m *CRDRuntimeManager) rebuildSharedProjection(objs []interface{}) {
	decodedObjs := make([]interface{}, 0, len(objs))
	byCluster := map[string][]interface{}{}
	for _, obj := range objs {
		crd, ok := crdFromObj(obj)
		if !ok {
			continue
		}
		clusterID := objectClusterID(crd, mc.DefaultClusterAnnotation)
		if clusterID == "" {
			continue
		}
		decoded := decodeSharedCRD(clusterID, crd)
		decodedObjs = append(decodedObjs, decoded)
		byCluster[clusterID] = append(byCluster[clusterID], decoded)
	}
	m.sharedProjection.ReplaceAll(decodedObjs, mc.DefaultClusterAnnotation)
	for clusterID, clusterObjs := range byCluster {
		m.servesIndex.RebuildCluster(clusterID, clusterObjs)
	}
}

func (m *CRDRuntimeManager) onSharedCRDUpsert(obj interface{}) {
	crd, ok := crdFromObj(obj)
	if !ok {
		return
	}
	clusterID := objectClusterID(crd, mc.DefaultClusterAnnotation)
	if clusterID == "" {
		return
	}
	crd = decodeSharedCRD(clusterID, crd)
	m.sharedProjection.Upsert(clusterID, crd)
	m.servesIndex.UpsertCRD(clusterID, crd)
	m.enqueueCRDStatus(clusterID, crd.Name)
}

func (m *CRDRuntimeManager) onSharedCRDDelete(obj interface{}) {
	crd, ok := crdFromObj(obj)
	if !ok {
		return
	}
	clusterID := objectClusterID(crd, mc.DefaultClusterAnnotation)
	if clusterID == "" {
		return
	}
	crd = decodeSharedCRD(clusterID, crd)
	m.sharedProjection.Delete(clusterID, crd.Name)
	m.servesIndex.DeleteCRD(clusterID, crd)
}

func (m *CRDRuntimeManager) EnsureCluster(clusterID string, stopCh <-chan struct{}) error {
	if m == nil || clusterID == "" || clusterID == m.opts.DefaultCluster {
		return nil
	}
	if _, err := m.ensureSharedRuntime(stopCh); err != nil {
		return err
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

func (s *crdProjectionStore) ReplaceAll(objs []interface{}, clusterLabelKey string) {
	if s == nil {
		return
	}
	next := map[string]map[string]*apiextensionsv1.CustomResourceDefinition{}
	for _, obj := range objs {
		crd, ok := crdFromObj(obj)
		if !ok {
			continue
		}
		clusterID := objectClusterID(crd, clusterLabelKey)
		if clusterID == "" {
			continue
		}
		clusterMap, ok := next[clusterID]
		if !ok {
			clusterMap = map[string]*apiextensionsv1.CustomResourceDefinition{}
			next[clusterID] = clusterMap
		}
		clusterMap[crd.Name] = crd.DeepCopy()
	}
	s.mu.Lock()
	s.byCluster = next
	s.mu.Unlock()
}

func (m *CRDRuntimeManager) baseLoopbackConfig() *rest.Config {
	if m == nil || m.opts.BaseAPIExtensionsConfig == nil || m.opts.BaseAPIExtensionsConfig.GenericConfig == nil {
		return nil
	}
	return m.opts.BaseAPIExtensionsConfig.GenericConfig.LoopbackClientConfig
}

func allClustersAPIExtensionsClient(base *rest.Config) (apiextensionsclient.Interface, error) {
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
	return apiextensionsclient.NewForConfig(cfg)
}

func (m *CRDRuntimeManager) sharedStartStopCh(stopCh <-chan struct{}) <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sharedStopCh != nil {
		return m.sharedStopCh
	}
	if stopCh != nil {
		m.sharedStopCh = stopCh
		return m.sharedStopCh
	}
	m.sharedOwnedStop = make(chan struct{})
	m.sharedStopCh = m.sharedOwnedStop
	return m.sharedStopCh
}

func objectClusterID(obj interface{}, clusterLabelKey string) string {
	if clusterLabelKey == "" {
		clusterLabelKey = mc.DefaultClusterAnnotation
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return accessor.GetLabels()[clusterLabelKey]
}

const sharedCRDNamePrefix = "__mc_shared_crd__"

func transformCRDForShared(clusterLabelKey string) cache.TransformFunc {
	return func(obj interface{}) (interface{}, error) {
		crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
		if !ok || crd == nil {
			return obj, nil
		}
		clusterID := objectClusterID(crd, clusterLabelKey)
		if clusterID == "" {
			return obj, nil
		}
		cp := crd.DeepCopy()
		cp.Name = encodeSharedCRDName(clusterID, cp.Name)
		return cp, nil
	}
}

func encodeSharedCRDName(clusterID, name string) string {
	if clusterID == "" || name == "" {
		return name
	}
	prefix := sharedCRDNamePrefix + clusterID + "__"
	if strings.HasPrefix(name, prefix) {
		return name
	}
	return prefix + name
}

func decodeSharedCRDName(clusterID, name string) string {
	if clusterID == "" || name == "" {
		return name
	}
	prefix := sharedCRDNamePrefix + clusterID + "__"
	return strings.TrimPrefix(name, prefix)
}

func decodeSharedCRD(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.CustomResourceDefinition {
	if crd == nil {
		return nil
	}
	cp := crd.DeepCopy()
	cp.Name = decodeSharedCRDName(clusterID, cp.Name)
	return cp
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
	crd, found := m.sharedProjection.Get(clusterID, name)
	if !found || crd == nil {
		return nil
	}
	desired := crd.DeepCopy()
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

	if equality.Semantic.DeepEqual(crd.Status, desired.Status) {
		return nil
	}

	cs, err := m.ensureClusterClient(clusterID)
	if err != nil {
		return err
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
