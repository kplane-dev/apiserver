package webhook

import (
	"net/url"
	"reflect"
	"strings"
	"sync"

	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
}

type Manager struct {
	opts Options

	mu       sync.Mutex
	clusters map[string]*clusterEnv
}

type clusterEnv struct {
	cid string

	stopCh chan struct{}
	synced chan struct{}

	okMu sync.Mutex
	ok   bool

	clientset kubernetes.Interface
	informers clientgoinformers.SharedInformerFactory

	serviceResolver webhookutil.ServiceResolver
}

func NewManager(opts Options) *Manager {
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

	cfg := rest.CopyConfig(m.opts.BaseLoopbackClientConfig)
	host, err := withClusterPrefix(cfg.Host, m.opts.PathPrefix, clusterID, m.opts.ControlPlaneSegment)
	if err != nil {
		return nil, err
	}
	cfg.Host = host

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	inf := clientgoinformers.NewSharedInformerFactory(cs, 0)

	sr := newDirectServiceResolver(cs, m.opts.EnableAggregatorRouting, m.opts.Hostname)

	e := &clusterEnv{
		cid:             clusterID,
		stopCh:          make(chan struct{}),
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

	// Start informers for resources needed by webhook admission.
	inf.Start(e.stopCh)

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

func withClusterPrefix(host, pathPrefix, clusterID, controlPlaneSegment string) (string, error) {
	u, err := url.Parse(host)
	if err != nil {
		return "", err
	}
	pp := pathPrefix
	if pp == "" {
		pp = "/clusters/"
	}
	seg := controlPlaneSegment
	if seg == "" {
		seg = "control-plane"
	}
	// ensure pp ends with "/"
	if !strings.HasSuffix(pp, "/") {
		pp = pp + "/"
	}
	// join onto existing path
	basePath := strings.TrimRight(u.Path, "/")
	u.Path = basePath + pp + clusterID + "/" + seg
	return u.String(), nil
}

// StopCluster is test-oriented cleanup; production can leave informers running.
func (m *Manager) StopCluster(clusterID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.clusters[clusterID]; ok && e.stopCh != nil {
		close(e.stopCh)
		delete(m.clusters, clusterID)
	}
}
