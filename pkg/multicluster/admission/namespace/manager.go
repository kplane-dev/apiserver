package namespace

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/typedinformer"
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

	// InformerRegistry provides MultiClusterInformers for resource types.
	InformerRegistry *mc.InformerRegistry
}

type Manager struct {
	opts Options

	mu       sync.Mutex
	clusters map[string]*clusterEnv
}

type clusterEnv struct {
	cid string

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

	nsMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Resource: "namespaces"})
	if err != nil {
		return nil, err
	}

	factory := typedinformer.NewMCIFactory(typedinformer.MCIFactoryConfig{
		ClusterID:  clusterID,
		Namespaces: nsMCI,
	})

	e := &clusterEnv{
		cid:       clusterID,
		clientset: cs,
		informers: factory,
	}

	m.clusters[clusterID] = e
	return e, nil
}

// StopCluster is test-oriented cleanup; production can leave informers running.
func (m *Manager) StopCluster(clusterID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clusters, clusterID)
}
