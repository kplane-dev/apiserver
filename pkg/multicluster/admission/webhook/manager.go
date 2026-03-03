package webhook

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/typedinformer"
	mcinformer "github.com/kplane-dev/informer"
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

	nsMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Resource: "namespaces"})
	if err != nil {
		return nil, err
	}
	servicesMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Resource: "services"})
	if err != nil {
		return nil, err
	}
	endpointSlicesMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Group: "discovery.k8s.io", Resource: "endpointslices"})
	if err != nil {
		return nil, err
	}
	mutatingMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "mutatingwebhookconfigurations"})
	if err != nil {
		return nil, err
	}
	validatingMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "validatingwebhookconfigurations"})
	if err != nil {
		return nil, err
	}

	factory := typedinformer.NewMCIFactory(typedinformer.MCIFactoryConfig{
		ClusterID:          clusterID,
		Namespaces:         nsMCI,
		Services:           servicesMCI,
		EndpointSlices:     endpointSlicesMCI,
		MutatingWebhooks:   mutatingMCI,
		ValidatingWebhooks: validatingMCI,
	})

	sr := newDirectServiceResolver(
		typedinformer.NewServiceLister(servicesMCI, clusterID),
		typedinformer.NewEndpointSliceLister(endpointSlicesMCI, clusterID),
		m.opts.EnableAggregatorRouting,
		m.opts.Hostname,
	)

	synced := make(chan struct{})
	e := &clusterEnv{
		cid:             clusterID,
		synced:          synced,
		clientset:       cs,
		informers:       factory,
		serviceResolver: sr,
	}

	// Close synced channel when all MCIs have completed initial sync.
	mcis := []*mcinformer.MultiClusterInformer{nsMCI, servicesMCI, endpointSlicesMCI, mutatingMCI, validatingMCI}
	go func() {
		for {
			allSynced := true
			for _, mci := range mcis {
				if !mci.HasSynced() {
					allSynced = false
					break
				}
			}
			if allSynced {
				close(synced)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	m.clusters[clusterID] = e
	return e, nil
}

// StopCluster is test-oriented cleanup; production can leave informers running.
func (m *Manager) StopCluster(clusterID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clusters, clusterID)
}
