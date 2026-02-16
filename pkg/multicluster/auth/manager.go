package auth

import (
	"context"
	"fmt"
	"strings"
	"sync"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/scopedinformer"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	authzunion "k8s.io/apiserver/pkg/authorization/union"
	"k8s.io/apiserver/pkg/server/egressselector"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller/serviceaccount"
	"k8s.io/kubernetes/pkg/features"
	rbacregistryvalidation "k8s.io/kubernetes/pkg/registry/rbac/validation"
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
	rbacauthorizer "k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac"
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

	sharedOnce sync.Once
	sharedErr  error
	sharedAuth informers.SharedInformerFactory
	rbacStore  *rbacProjectionStore
	sharedStop <-chan struct{}
	sharedOwn  chan struct{}
}

type clusterEnv struct {
	cid string

	clientset kubernetes.Interface
	informers informers.SharedInformerFactory

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
	_, ok := m.clusters[clusterID]
	if !ok {
		return
	}
	delete(m.clusters, clusterID)
}

func (m *Manager) envForCluster(clusterID string) (*clusterEnv, error) {
	m.mu.Lock()
	if env, ok := m.clusters[clusterID]; ok {
		m.mu.Unlock()
		return env, nil
	}

	if m.opts.ClientPool == nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("loopback client pool is required for cluster auth")
	}

	cs, err := m.opts.ClientPool.KubeClientForCluster(clusterID)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}

	var (
		scopedFactory informers.SharedInformerFactory
		authn         authenticator.Request
	)
	var (
		authz    authorizer.Authorizer
		resolver authorizer.RuleResolver
	)
	if m.useSharedRBACAuthorizer() {
		listers, err := m.coreListersForCluster(clusterID)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		authn, err = buildAuthenticatorWithCoreListers(m.ctx, m.opts, cs, listers)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		authz, resolver, err = m.buildSharedRBACAuthorizerForCluster(clusterID)
	} else {
		scopedFactory, err = m.scopedAuthFactory(clusterID)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		authn, err = buildAuthenticator(m.ctx, m.opts, cs, scopedFactory)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		authz, resolver, err = buildAuthorizer(m.ctx, m.opts, scopedFactory)
	}
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}

	env := &clusterEnv{
		cid:           clusterID,
		clientset:     cs,
		informers:     scopedFactory,
		authenticator: authn,
		authorizer:    authz,
		ruleResolver:  resolver,
	}

	m.clusters[clusterID] = env
	m.mu.Unlock()
	return env, nil
}

func (m *Manager) scopedAuthFactory(clusterID string) (informers.SharedInformerFactory, error) {
	shared, err := m.ensureSharedAuthFactory()
	if err != nil {
		return nil, err
	}
	return newScopedFactory(clusterID, mc.DefaultClusterAnnotation, shared, m.rbacStore), nil
}

func (m *Manager) ensureSharedAuthFactory() (informers.SharedInformerFactory, error) {
	m.sharedOnce.Do(func() {
		if m.opts.BaseLoopbackClientConfig == nil {
			m.sharedErr = fmt.Errorf("base loopback config is required for shared auth factory")
			return
		}
		cs, err := scopedinformer.NewAllClustersKubeClient(m.opts.BaseLoopbackClientConfig)
		if err != nil {
			m.sharedErr = err
			return
		}
		factory := informers.NewSharedInformerFactory(cs, 0)
		// Warm and index shared auth-critical informers once.
		authInformers := []cache.SharedIndexInformer{
			factory.Core().V1().Secrets().Informer(),
			factory.Core().V1().ServiceAccounts().Informer(),
			factory.Core().V1().Pods().Informer(),
			factory.Core().V1().Nodes().Informer(),
			factory.Rbac().V1().Roles().Informer(),
			factory.Rbac().V1().RoleBindings().Informer(),
			factory.Rbac().V1().ClusterRoles().Informer(),
			factory.Rbac().V1().ClusterRoleBindings().Informer(),
		}
		for _, inf := range authInformers {
			if err := scopedinformer.EnsureClusterIndex(inf, mc.DefaultClusterAnnotation); err != nil {
				m.sharedErr = err
				return
			}
		}
		rbacStore := newRBACProjectionStore(mc.DefaultClusterAnnotation)
		if err := registerRBACProjectionHandlers(
			rbacStore,
			factory.Rbac().V1().Roles().Informer(),
			factory.Rbac().V1().RoleBindings().Informer(),
			factory.Rbac().V1().ClusterRoles().Informer(),
			factory.Rbac().V1().ClusterRoleBindings().Informer(),
		); err != nil {
			m.sharedErr = err
			return
		}
		if m.sharedStop == nil {
			m.sharedOwn = make(chan struct{})
			m.sharedStop = m.sharedOwn
		}
		factory.Start(m.sharedStop)
		m.sharedAuth = factory
		m.rbacStore = rbacStore
	})
	if m.sharedErr != nil {
		return nil, m.sharedErr
	}
	return m.sharedAuth, nil
}

type coreAuthListers struct {
	secrets         corelisters.SecretLister
	serviceAccounts corelisters.ServiceAccountLister
	pods            corelisters.PodLister
	nodes           corelisters.NodeLister
}

func (m *Manager) coreListersForCluster(clusterID string) (*coreAuthListers, error) {
	shared, err := m.ensureSharedAuthFactory()
	if err != nil {
		return nil, err
	}
	return &coreAuthListers{
		secrets: &scopedSecretLister{
			indexer:         shared.Core().V1().Secrets().Informer().GetIndexer(),
			clusterID:       clusterID,
			clusterLabelKey: mc.DefaultClusterAnnotation,
		},
		serviceAccounts: &scopedServiceAccountLister{
			indexer:         shared.Core().V1().ServiceAccounts().Informer().GetIndexer(),
			clusterID:       clusterID,
			clusterLabelKey: mc.DefaultClusterAnnotation,
		},
		pods: &scopedPodLister{
			indexer:         shared.Core().V1().Pods().Informer().GetIndexer(),
			clusterID:       clusterID,
			clusterLabelKey: mc.DefaultClusterAnnotation,
		},
		nodes: &scopedNodeLister{
			indexer:         shared.Core().V1().Nodes().Informer().GetIndexer(),
			clusterID:       clusterID,
			clusterLabelKey: mc.DefaultClusterAnnotation,
		},
	}, nil
}

func (m *Manager) useSharedRBACAuthorizer() bool {
	if m == nil || m.opts.Authorization == nil {
		return false
	}
	if len(m.opts.Authorization.Modes) != 1 {
		return false
	}
	return strings.EqualFold(m.opts.Authorization.Modes[0], "RBAC")
}

func (m *Manager) buildSharedRBACAuthorizerForCluster(clusterID string) (authorizer.Authorizer, authorizer.RuleResolver, error) {
	if _, err := m.ensureSharedAuthFactory(); err != nil {
		return nil, nil, err
	}
	if m.rbacStore == nil {
		return nil, nil, fmt.Errorf("shared RBAC projection store is not initialized")
	}
	resolver := &clusterAwareRBACDataSource{
		store:          m.rbacStore,
		defaultCluster: clusterID,
		fixedCluster:   clusterID,
	}
	rbacAuthz := rbacauthorizer.New(resolver, resolver, resolver, resolver)
	superuser := authorizerfactory.NewPrivilegedGroups(user.SystemPrivilegedGroup)
	// Match upstream shape: privileged groups short-circuit before RBAC checks.
	return authzunion.New(superuser, rbacAuthz), rbacAuthz, nil
}

type clusterAwareRBACDataSource struct {
	store          *rbacProjectionStore
	defaultCluster string
	fixedCluster   string
}

func (s *clusterAwareRBACDataSource) clusterFromContext(ctx context.Context) string {
	if s != nil && s.fixedCluster != "" {
		return s.fixedCluster
	}
	cid, _, _ := mc.FromContext(ctx)
	if cid != "" {
		return cid
	}
	if s != nil && s.defaultCluster != "" {
		return s.defaultCluster
	}
	return mc.DefaultClusterName
}

func (s *clusterAwareRBACDataSource) GetRole(ctx context.Context, namespace, name string) (*rbacv1.Role, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("rbac projection store is not initialized")
	}
	clusterID := s.clusterFromContext(ctx)
	for _, role := range s.store.listRoles(clusterID) {
		if role == nil {
			continue
		}
		if role.Namespace == namespace && role.Name == name {
			return role, nil
		}
	}
	return nil, fmt.Errorf("role %s/%s not found", namespace, name)
}

func (s *clusterAwareRBACDataSource) ListRoleBindings(ctx context.Context, namespace string) ([]*rbacv1.RoleBinding, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("rbac projection store is not initialized")
	}
	clusterID := s.clusterFromContext(ctx)
	items := s.store.listRoleBindings(clusterID)
	out := make([]*rbacv1.RoleBinding, 0, len(items))
	for _, rb := range items {
		if rb == nil || rb.Namespace != namespace {
			continue
		}
		out = append(out, rb)
	}
	return out, nil
}

func (s *clusterAwareRBACDataSource) GetClusterRole(ctx context.Context, name string) (*rbacv1.ClusterRole, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("rbac projection store is not initialized")
	}
	clusterID := s.clusterFromContext(ctx)
	role := s.store.getClusterRole(clusterID, name)
	if role == nil {
		return nil, fmt.Errorf("clusterrole %s not found", name)
	}
	return role, nil
}

func (s *clusterAwareRBACDataSource) ListClusterRoleBindings(ctx context.Context) ([]*rbacv1.ClusterRoleBinding, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("rbac projection store is not initialized")
	}
	clusterID := s.clusterFromContext(ctx)
	return s.store.listClusterRoleBindings(clusterID), nil
}

var _ rbacregistryvalidation.RoleGetter = (*clusterAwareRBACDataSource)(nil)
var _ rbacregistryvalidation.RoleBindingLister = (*clusterAwareRBACDataSource)(nil)
var _ rbacregistryvalidation.ClusterRoleGetter = (*clusterAwareRBACDataSource)(nil)
var _ rbacregistryvalidation.ClusterRoleBindingLister = (*clusterAwareRBACDataSource)(nil)

func buildAuthenticator(ctx context.Context, opts Options, clientset kubernetes.Interface, informers informers.SharedInformerFactory) (authenticator.Request, error) {
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

func buildAuthenticatorWithCoreListers(ctx context.Context, opts Options, clientset kubernetes.Interface, listers *coreAuthListers) (authenticator.Request, error) {
	if opts.Authentication == nil {
		return nil, nil
	}
	if listers == nil {
		return nil, fmt.Errorf("core auth listers are required")
	}
	if opts.Authentication.ServiceAccounts != nil && opts.Authentication.ServiceAccounts.OptionalTokenGetter != nil {
		return nil, fmt.Errorf("optional token getter requires informer factory path")
	}
	authConfig, err := opts.Authentication.ToAuthenticationConfig()
	if err != nil {
		return nil, err
	}

	var nodeLister v1.NodeLister
	if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenNodeBindingValidation) {
		nodeLister = listers.nodes
	}
	authConfig.ServiceAccountTokenGetter = serviceaccount.NewGetterFromClient(
		clientset,
		listers.secrets,
		listers.serviceAccounts,
		listers.pods,
		nodeLister,
	)
	authConfig.SecretsWriter = clientset.CoreV1()

	if authConfig.BootstrapToken {
		authConfig.BootstrapTokenAuthenticator = bootstrap.NewTokenAuthenticator(
			listers.secrets.Secrets(metav1.NamespaceSystem),
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

func buildAuthorizer(ctx context.Context, opts Options, informers informers.SharedInformerFactory) (authorizer.Authorizer, authorizer.RuleResolver, error) {
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
