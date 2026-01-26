package auth

import (
	"context"
	"fmt"
	"sync"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/server/egressselector"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/controller/serviceaccount"
	"k8s.io/kubernetes/pkg/features"
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
	"k8s.io/kubernetes/plugin/pkg/auth/authenticator/token/bootstrap"
)

// Options configures per-cluster auth construction.
type Options struct {
	BaseLoopbackClientConfig *rest.Config
	PathPrefix               string
	ControlPlaneSegment      string
	Authentication           *kubeoptions.BuiltInAuthenticationOptions
	Authorization            *kubeoptions.BuiltInAuthorizationOptions
	EgressSelector           *egressselector.EgressSelector
	APIServerID              string
	ClientPool               *mc.ClientPool
	InformerPool             *mc.InformerPool
}

// Manager builds per-cluster authenticators and authorizers on demand.
type Manager struct {
	ctx  context.Context
	opts Options

	mu       sync.Mutex
	clusters map[string]*clusterEnv
}

type clusterEnv struct {
	cid string

	clientset kubernetes.Interface
	informers clientgoinformers.SharedInformerFactory

	authenticator authenticator.Request
	authorizer    authorizer.Authorizer
	ruleResolver  authorizer.RuleResolver
}

// NewManager constructs a per-cluster auth manager.
func NewManager(ctx context.Context, opts Options) *Manager {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.ClientPool == nil && opts.BaseLoopbackClientConfig != nil {
		opts.ClientPool = mc.NewClientPool(opts.BaseLoopbackClientConfig, opts.PathPrefix, opts.ControlPlaneSegment)
	}
	if opts.InformerPool == nil && opts.ClientPool != nil {
		opts.InformerPool = mc.NewInformerPoolFromClientPool(opts.ClientPool, 0, nil)
	}
	return &Manager{
		ctx:      ctx,
		opts:     opts,
		clusters: map[string]*clusterEnv{},
	}
}

// AuthenticatorForCluster returns the authenticator for a cluster.
func (m *Manager) AuthenticatorForCluster(clusterID string) (authenticator.Request, error) {
	env, err := m.envForCluster(clusterID)
	if err != nil {
		return nil, err
	}
	return env.authenticator, nil
}

// AuthorizerForCluster returns the authorizer + rule resolver for a cluster.
func (m *Manager) AuthorizerForCluster(clusterID string) (authorizer.Authorizer, authorizer.RuleResolver, error) {
	env, err := m.envForCluster(clusterID)
	if err != nil {
		return nil, nil, err
	}
	return env.authorizer, env.ruleResolver, nil
}

// StopCluster shuts down per-cluster informers (test helper).
func (m *Manager) StopCluster(clusterID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	env, ok := m.clusters[clusterID]
	if !ok {
		return
	}
	delete(m.clusters, clusterID)
	if m.opts.InformerPool != nil {
		m.opts.InformerPool.StopCluster(clusterID)
	} else {
		_ = env
	}
}

func (m *Manager) envForCluster(clusterID string) (*clusterEnv, error) {
	m.mu.Lock()
	if env, ok := m.clusters[clusterID]; ok {
		m.mu.Unlock()
		return env, nil
	}

	if m.opts.InformerPool == nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("loopback informer pool is required for cluster auth")
	}

	cs, informers, _, err := m.opts.InformerPool.Get(clusterID)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}

	authn, err := buildAuthenticator(m.ctx, m.opts, cs, informers)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	authz, resolver, err := buildAuthorizer(m.ctx, m.opts, informers)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}

	env := &clusterEnv{
		cid:           clusterID,
		clientset:     cs,
		informers:     informers,
		authenticator: authn,
		authorizer:    authz,
		ruleResolver:  resolver,
	}

	m.clusters[clusterID] = env
	m.mu.Unlock()
	return env, nil
}

func buildAuthenticator(ctx context.Context, opts Options, clientset kubernetes.Interface, informers clientgoinformers.SharedInformerFactory) (authenticator.Request, error) {
	if opts.Authentication == nil {
		return nil, nil
	}
	authConfig, err := opts.Authentication.ToAuthenticationConfig()
	if err != nil {
		return nil, err
	}

	if opts.Authentication.ServiceAccounts != nil && opts.Authentication.ServiceAccounts.OptionalTokenGetter != nil {
		authConfig.ServiceAccountTokenGetter = opts.Authentication.ServiceAccounts.OptionalTokenGetter(informers)
	} else {
		var nodeLister v1.NodeLister
		if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenNodeBindingValidation) {
			nodeLister = informers.Core().V1().Nodes().Lister()
		}

		authConfig.ServiceAccountTokenGetter = serviceaccount.NewGetterFromClient(
			clientset,
			informers.Core().V1().Secrets().Lister(),
			informers.Core().V1().ServiceAccounts().Lister(),
			informers.Core().V1().Pods().Lister(),
			nodeLister,
		)
	}
	authConfig.SecretsWriter = clientset.CoreV1()

	if authConfig.BootstrapToken {
		authConfig.BootstrapTokenAuthenticator = bootstrap.NewTokenAuthenticator(
			informers.Core().V1().Secrets().Lister().Secrets(metav1.NamespaceSystem),
		)
	}

	if opts.EgressSelector != nil {
		egressDialer, err := opts.EgressSelector.Lookup(egressselector.ControlPlane.AsNetworkContext())
		if err != nil {
			return nil, err
		}
		authConfig.CustomDial = egressDialer
		authConfig.EgressLookup = opts.EgressSelector.Lookup
	}

	authenticator, _, _, _, err := authConfig.New(ctx)
	if err != nil {
		return nil, err
	}
	return authenticator, nil
}

func buildAuthorizer(ctx context.Context, opts Options, informers clientgoinformers.SharedInformerFactory) (authorizer.Authorizer, authorizer.RuleResolver, error) {
	if opts.Authorization == nil {
		return nil, nil, nil
	}
	authzConfig, err := opts.Authorization.ToAuthorizationConfig(informers)
	if err != nil {
		return nil, nil, err
	}
	if authzConfig == nil {
		return nil, nil, nil
	}
	if opts.EgressSelector != nil {
		egressDialer, err := opts.EgressSelector.Lookup(egressselector.ControlPlane.AsNetworkContext())
		if err != nil {
			return nil, nil, err
		}
		authzConfig.CustomDial = egressDialer
	}
	return authzConfig.New(ctx, opts.APIServerID)
}
