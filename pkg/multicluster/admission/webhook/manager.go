package webhook

import (
	"reflect"
	"sync"

	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
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
}

type clusterEnv struct {
	cid string

	stopCh <-chan struct{}
	synced chan struct{}

	okMu sync.Mutex
	ok   bool

	clientset kubernetes.Interface
	informers clientgoinformers.SharedInformerFactory

	serviceResolver webhookutil.ServiceResolver
}

func NewManager(opts Options) *Manager {
	if opts.ClientPool == nil && opts.BaseLoopbackClientConfig != nil {
		opts.ClientPool = mc.NewClientPool(opts.BaseLoopbackClientConfig, opts.PathPrefix, opts.ControlPlaneSegment)
	}
	if opts.InformerPool == nil && opts.ClientPool != nil {
		opts.InformerPool = mc.NewInformerPoolFromClientPool(opts.ClientPool, 0, nil)
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

	if m.opts.InformerPool == nil {
		return nil, mc.ErrMissingClientFactory
	}
	cs, inf, stopCh, err := m.opts.InformerPool.Get(clusterID)
	if err != nil {
		return nil, err
	}

	sr := newDirectServiceResolver(cs, m.opts.EnableAggregatorRouting, m.opts.Hostname)

	e := &clusterEnv{
		cid:             clusterID,
		stopCh:          stopCh,
		synced:          make(chan struct{}),
		clientset:       cs,
		informers:       inf,
		serviceResolver: sr,
	}

	// Warm required informers (must happen before Start()).
	_ = inf.Core().V1().Namespaces().Informer()
	_ = inf.Core().V1().Services().Informer()
	_ = inf.Discovery().V1().EndpointSlices().Informer()
	_ = inf.Admissionregistration().V1().MutatingWebhookConfigurations().Informer()
	_ = inf.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer()
	inf.Start(stopCh)

	// Start informers for resources needed by webhook admission.
	go func() {
		ok := inf.WaitForCacheSync(e.stopCh)
		e.okMu.Lock()
		e.ok = allSynced(ok)
		e.okMu.Unlock()
		close(e.synced)
	}()

	m.clusters[clusterID] = e
	return e, nil
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
	if _, ok := m.clusters[clusterID]; ok {
		delete(m.clusters, clusterID)
	}
	if m.opts.InformerPool != nil {
		m.opts.InformerPool.StopCluster(clusterID)
	}
}
