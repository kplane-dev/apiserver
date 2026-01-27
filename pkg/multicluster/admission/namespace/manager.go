package namespace

import (
	"sync"

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
}

type clusterEnv struct {
	stopCh <-chan struct{}
	cid    string

	clientset kubernetes.Interface
	informers informers.SharedInformerFactory
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

	e := &clusterEnv{
		cid:       clusterID,
		stopCh:    stopCh,
		clientset: cs,
		informers: inf,
	}

	// Warm the namespaces informer (used by NamespaceLifecycle).
	_ = inf.Core().V1().Namespaces().Informer()
	m.clusters[clusterID] = e
	return e, nil
}
