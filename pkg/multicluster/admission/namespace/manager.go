package namespace

import (
	"sync"

	"github.com/kplane-dev/apiserver/pkg/multicluster/scopedinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
)

type Options struct {
	// BaseLoopbackClientConfig is the apiserver loopback config for the root server.
	// We clone it and rewrite Host to include the cluster path prefix.
	BaseLoopbackClientConfig *rest.Config

	// PathPrefix and ControlPlaneSegment define the cluster URL form.
	PathPrefix          string
	ControlPlaneSegment string

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
	shared     informers.SharedInformerFactory
	sharedStop <-chan struct{}
	sharedOwn  chan struct{}
}

type clusterEnv struct {
	stopCh chan struct{}
	cid    string

	clientset kubernetes.Interface
	informers informers.SharedInformerFactory
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
	scoped, err := m.scopedNamespaceFactory(clusterID)
	if err != nil {
		return nil, err
	}
	stopCh := make(chan struct{})

	e := &clusterEnv{
		cid:       clusterID,
		stopCh:    stopCh,
		clientset: cs,
		informers: scoped,
	}

	// Warm the namespaces informer (used by NamespaceLifecycle).
	_ = scoped.Core().V1().Namespaces().Informer()
	scoped.Start(stopCh)
	m.clusters[clusterID] = e
	return e, nil
}

func (m *Manager) scopedNamespaceFactory(clusterID string) (informers.SharedInformerFactory, error) {
	shared, err := m.ensureSharedFactory()
	if err != nil {
		return nil, err
	}
	return newScopedFactory(clusterID, mc.DefaultClusterAnnotation, shared), nil
}

func (m *Manager) ensureSharedFactory() (informers.SharedInformerFactory, error) {
	m.sharedOnce.Do(func() {
		if m.opts.BaseLoopbackClientConfig == nil {
			m.sharedErr = mc.ErrMissingClientFactory
			return
		}
		cs, err := scopedinformer.NewAllClustersKubeClient(m.opts.BaseLoopbackClientConfig)
		if err != nil {
			m.sharedErr = err
			return
		}
		factory := informers.NewSharedInformerFactory(cs, 0)
		if err := factory.Core().V1().Namespaces().Informer().SetTransform(transformNamespaceForShared(mc.DefaultClusterAnnotation)); err != nil {
			m.sharedErr = err
			return
		}
		if err := scopedinformer.EnsureClusterIndex(factory.Core().V1().Namespaces().Informer(), mc.DefaultClusterAnnotation); err != nil {
			m.sharedErr = err
			return
		}
		if m.sharedStop == nil {
			m.sharedOwn = make(chan struct{})
			m.sharedStop = m.sharedOwn
		}
		factory.Start(m.sharedStop)
		m.shared = factory
	})
	if m.sharedErr != nil {
		return nil, m.sharedErr
	}
	return m.shared, nil
}

// StopCluster is test-oriented cleanup; production can leave informers running.
func (m *Manager) StopCluster(clusterID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.clusters[clusterID]; ok {
		if e.stopCh != nil {
			close(e.stopCh)
		}
		delete(m.clusters, clusterID)
	}
}
