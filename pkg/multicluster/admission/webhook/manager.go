package webhook

import (
	"fmt"
	"reflect"
	"sync"

	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/scopedinformer"
)

type Options struct {
	// BaseLoopbackClientConfig is the apiserver loopback config for the *root* server.
	// We will clone it and rewrite Host to include the cluster path prefix.
	BaseLoopbackClientConfig *rest.Config

	// AuthWrapper is used by upstream webhook plugin to build transport + dialer config.
	AuthWrapper webhookutil.AuthenticationInfoResolverWrapper

	// EnableAggregatorRouting chooses endpoint-slice resolver vs ClusterIP resolver,
	// mirroring kube-apiserver behavior.
	EnableAggregatorRouting bool

	// Hostname is used for loopback service resolution (kubernetes.default.svc).
	Hostname string

	// PathPrefix and ControlPlaneSegment define the cluster URL form.
	PathPrefix          string
	ControlPlaneSegment string

	// CelRuntime provides shared CEL env/compiler caching for matchConditions.
	CelRuntime *CelRuntime

	// ClientPool caches per-cluster loopback clients.
	ClientPool *mc.ClientPool

	// InformerPool shares informer factories across managers per cluster.
	InformerPool *mc.InformerPool
}

type Manager struct {
	opts Options

	mu       sync.Mutex
	clusters map[string]*clusterEnv

	sharedOnce sync.Once
	sharedErr  error
	shared     clientgoinformers.SharedInformerFactory
	sharedStop <-chan struct{}
	sharedOwn  chan struct{}
	sharedSync chan struct{}
}

type clusterEnv struct {
	cid string

	stopCh <-chan struct{}
	ownCh  chan struct{}
	synced chan struct{}

	clientset kubernetes.Interface
	informers clientgoinformers.SharedInformerFactory

	serviceResolver webhookutil.ServiceResolver
}

func NewManager(opts Options) *Manager {
	if opts.ClientPool == nil && opts.BaseLoopbackClientConfig != nil {
		opts.ClientPool = mc.NewClientPool(opts.BaseLoopbackClientConfig, opts.PathPrefix, opts.ControlPlaneSegment)
	}
	return &Manager{
		opts:     opts,
		clusters: map[string]*clusterEnv{},
	}
}

func (m *Manager) envForCluster(clusterID string) (*clusterEnv, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.clusters[clusterID]; ok {
		return e, nil
	}

	if m.opts.ClientPool == nil {
		return nil, mc.ErrMissingClientFactory
	}
	cs, err := m.opts.ClientPool.KubeClientForCluster(clusterID)
	if err != nil {
		return nil, err
	}
	scoped, err := m.scopedWebhookFactory(clusterID)
	if err != nil {
		return nil, err
	}
	stopCh := make(chan struct{})

	sr := newDirectServiceResolver(
		scoped.Core().V1().Services().Lister(),
		scoped.Discovery().V1().EndpointSlices().Lister(),
		m.opts.EnableAggregatorRouting,
		m.opts.Hostname,
	)

	e := &clusterEnv{
		cid:             clusterID,
		stopCh:          stopCh,
		ownCh:           stopCh,
		synced:          m.sharedSync,
		clientset:       cs,
		informers:       scoped,
		serviceResolver: sr,
	}

	// Warm required informers (must happen before Start()).
	_ = scoped.Core().V1().Namespaces().Informer()
	_ = scoped.Core().V1().Services().Informer()
	_ = scoped.Discovery().V1().EndpointSlices().Informer()
	_ = scoped.Admissionregistration().V1().MutatingWebhookConfigurations().Informer()
	_ = scoped.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer()
	scoped.Start(stopCh)

	m.clusters[clusterID] = e
	return e, nil
}

func (m *Manager) scopedWebhookFactory(clusterID string) (clientgoinformers.SharedInformerFactory, error) {
	shared, err := m.ensureSharedFactory()
	if err != nil {
		return nil, err
	}
	return newScopedFactory(clusterID, mc.DefaultClusterAnnotation, shared), nil
}

func (m *Manager) ensureSharedFactory() (clientgoinformers.SharedInformerFactory, error) {
	m.sharedOnce.Do(func() {
		if m.opts.BaseLoopbackClientConfig == nil {
			m.sharedErr = fmt.Errorf("base loopback config is required for shared webhook factory")
			return
		}
		cs, err := scopedinformer.NewAllClustersKubeClient(m.opts.BaseLoopbackClientConfig)
		if err != nil {
			m.sharedErr = err
			return
		}
		factory := clientgoinformers.NewSharedInformerFactory(cs, 0)
		webhookInformers := []cache.SharedIndexInformer{
			factory.Core().V1().Namespaces().Informer(),
			factory.Core().V1().Services().Informer(),
			factory.Discovery().V1().EndpointSlices().Informer(),
			factory.Admissionregistration().V1().MutatingWebhookConfigurations().Informer(),
			factory.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer(),
		}
		for _, inf := range webhookInformers {
			if err := scopedinformer.EnsureClusterIndex(inf, mc.DefaultClusterAnnotation); err != nil {
				m.sharedErr = err
				return
			}
		}
		if m.sharedStop == nil {
			m.sharedOwn = make(chan struct{})
			m.sharedStop = m.sharedOwn
		}
		factory.Start(m.sharedStop)
		// One shared cache-sync signal for all clusters; scoped informers are projections over shared caches.
		m.sharedSync = make(chan struct{})
		go func() {
			ok := factory.WaitForCacheSync(m.sharedStop)
			if allSynced(ok) {
				close(m.sharedSync)
			}
		}()
		m.shared = factory
	})
	if m.sharedErr != nil {
		return nil, m.sharedErr
	}
	return m.shared, nil
}

func allSynced(m map[reflect.Type]bool) bool {
	for _, v := range m {
		if !v {
			return false
		}
	}
	return true
}

// StopCluster is test-oriented cleanup; production can leave informers running.
func (m *Manager) StopCluster(clusterID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.clusters[clusterID]; ok {
		if e.ownCh != nil {
			close(e.ownCh)
		}
		delete(m.clusters, clusterID)
	}
}
