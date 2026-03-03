package auth

import (
	"context"
	"fmt"
	"sync"
	"time"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/typedinformer"
	mcinformer "github.com/kplane-dev/informer"
	mcstorage "github.com/kplane-dev/storage"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	authzunion "k8s.io/apiserver/pkg/authorization/union"
	"k8s.io/apiserver/pkg/server/egressselector"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/controller/serviceaccount"
	"k8s.io/kubernetes/pkg/features"
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
	rbacregistryvalidation "k8s.io/kubernetes/pkg/registry/rbac/validation"
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
	InformerRegistry         *mc.InformerRegistry
}

// Manager builds per-cluster authenticators and authorizers on demand.
type Manager struct {
	ctx  context.Context
	opts Options

	mu       sync.Mutex
	clusters map[string]*clusterEnv

	rbacOnce  sync.Once
	rbacErr   error
	rbacStore *rbacProjectionStore
}

type clusterEnv struct {
	cid string

	clientset kubernetes.Interface

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
	m.mu.Unlock()

	cs, err := m.opts.ClientPool.KubeClientForCluster(clusterID)
	if err != nil {
		return nil, err
	}

	// Build authenticator with MCI-backed core listers.
	listers, err := m.coreListersForCluster(clusterID)
	if err != nil {
		return nil, err
	}
	authn, err := buildAuthenticatorWithCoreListers(m.ctx, m.opts, cs, listers)
	if err != nil {
		return nil, err
	}

	// Build authorizer via RBAC projection store with MCI event handlers.
	if err := m.ensureRBACProjection(); err != nil {
		return nil, err
	}
	authz, ruleResolver := m.buildRBACAuthorizerForCluster(clusterID)

	env := &clusterEnv{
		cid:           clusterID,
		clientset:     cs,
		authenticator: authn,
		authorizer:    authz,
		ruleResolver:  ruleResolver,
	}

	m.mu.Lock()
	if existing, ok := m.clusters[clusterID]; ok {
		m.mu.Unlock()
		return existing, nil
	}
	m.clusters[clusterID] = env
	m.mu.Unlock()
	return env, nil
}

func (m *Manager) coreListersForCluster(clusterID string) (*coreAuthListers, error) {
	if m.opts.InformerRegistry == nil {
		return nil, fmt.Errorf("informer registry is required for core auth listers")
	}
	secretsMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Resource: "secrets"})
	if err != nil {
		return nil, err
	}
	saMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Resource: "serviceaccounts"})
	if err != nil {
		return nil, err
	}
	podsMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Resource: "pods"})
	if err != nil {
		return nil, err
	}
	nodesMCI, err := m.opts.InformerRegistry.Get(schema.GroupResource{Resource: "nodes"})
	if err != nil {
		return nil, err
	}
	return &coreAuthListers{
		secrets:         typedinformer.NewSecretLister(secretsMCI, clusterID),
		serviceAccounts: typedinformer.NewServiceAccountLister(saMCI, clusterID),
		pods:            typedinformer.NewPodLister(podsMCI, clusterID),
		nodes:           typedinformer.NewNodeLister(nodesMCI, clusterID),
	}, nil
}

// ensureRBACProjection registers MCI event handlers on RBAC resource types
// to incrementally populate the projection store.
func (m *Manager) ensureRBACProjection() error {
	m.rbacOnce.Do(func() {
		if m.opts.InformerRegistry == nil {
			m.rbacErr = fmt.Errorf("informer registry is required for RBAC projection")
			return
		}
		m.rbacStore = newRBACProjectionStore()

		rbacResources := []schema.GroupResource{
			{Group: "rbac.authorization.k8s.io", Resource: "roles"},
			{Group: "rbac.authorization.k8s.io", Resource: "rolebindings"},
			{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"},
			{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"},
		}

		mcis := make([]*mcinformer.MultiClusterInformer, 0, len(rbacResources))
		for _, gr := range rbacResources {
			mci, err := m.opts.InformerRegistry.Get(gr)
			if err != nil {
				m.rbacErr = fmt.Errorf("failed to get MCI for %s: %w", gr, err)
				return
			}
			mci.AddEventHandler(mcinformer.MultiClusterEventHandlerFuncs{
				AddFunc: func(obj *mcstorage.ObjectWithClusterIdentity, _ bool) {
					m.rbacStore.upsertWithCluster(obj.Object, obj.ClusterID)
				},
				UpdateFunc: func(_, newObj *mcstorage.ObjectWithClusterIdentity) {
					m.rbacStore.upsertWithCluster(newObj.Object, newObj.ClusterID)
				},
				DeleteFunc: func(obj *mcstorage.ObjectWithClusterIdentity) {
					m.rbacStore.deleteWithCluster(obj.Object, obj.ClusterID)
				},
			})
			mcis = append(mcis, mci)
		}

		// Wait for all MCIs to complete their initial list before backfilling.
		// HasSynced becomes true after the cacher sends all existing objects
		// via the initial watch stream.
		for _, mci := range mcis {
			for !mci.HasSynced() {
				select {
				case <-m.ctx.Done():
					m.rbacErr = m.ctx.Err()
					return
				default:
					time.Sleep(10 * time.Millisecond)
				}
			}
		}

		// Backfill existing objects — event handlers registered above only
		// receive future events; objects already in the store must be seeded.
		for _, mci := range mcis {
			for _, clusterID := range mci.Clusters() {
				for _, obj := range mci.List(clusterID) {
					m.rbacStore.upsertWithCluster(obj, clusterID)
				}
			}
		}
	})
	return m.rbacErr
}

func (m *Manager) buildRBACAuthorizerForCluster(clusterID string) (authorizer.Authorizer, authorizer.RuleResolver) {
	resolver := &clusterAwareRBACDataSource{
		store:          m.rbacStore,
		defaultCluster: clusterID,
		fixedCluster:   clusterID,
	}
	rbacAuthz := rbacauthorizer.New(resolver, resolver, resolver, resolver)
	superuser := authorizerfactory.NewPrivilegedGroups(user.SystemPrivilegedGroup)
	return authzunion.New(superuser, rbacAuthz), rbacAuthz
}

type coreAuthListers struct {
	secrets         corelisters.SecretLister
	serviceAccounts corelisters.ServiceAccountLister
	pods            corelisters.PodLister
	nodes           corelisters.NodeLister
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

	var nodeLister corelisters.NodeLister
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
