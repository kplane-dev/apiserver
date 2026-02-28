package auth

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/scopedinformer"
	mcstorage "github.com/kplane-dev/apiserver/pkg/multicluster/storage"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	"k8s.io/apiserver/pkg/storage/storagebackend"
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
	EtcdPrefix               string
	EtcdTransport            storagebackend.TransportConfig
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
	rbacDirty  chan struct{}

	etcdMu     sync.Mutex
	etcdClient *clientv3.Client
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
	m.mu.Unlock()

	cs, err := m.opts.ClientPool.KubeClientForCluster(clusterID)
	if err != nil {
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
		_, coreFactory, _, err := m.coreAuthFactoryForCluster(clusterID)
		if err != nil {
			return nil, err
		}
		authn, err = buildAuthenticator(m.ctx, m.opts, cs, coreFactory)
		if err != nil {
			return nil, err
		}
		authz, resolver, err = m.buildSharedRBACAuthorizerForCluster(clusterID)
	} else {
		scopedFactory, err = m.scopedAuthFactory(clusterID)
		if err != nil {
			return nil, err
		}
		authn, err = buildAuthenticator(m.ctx, m.opts, cs, scopedFactory)
		if err != nil {
			return nil, err
		}
		authz, resolver, err = buildAuthorizer(m.ctx, m.opts, scopedFactory)
	}
	if err != nil {
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

	m.mu.Lock()
	if existing, ok := m.clusters[clusterID]; ok {
		m.mu.Unlock()
		return existing, nil
	}
	m.clusters[clusterID] = env
	m.mu.Unlock()
	return env, nil
}

func (m *Manager) coreAuthFactoryForCluster(clusterID string) (kubernetes.Interface, informers.SharedInformerFactory, <-chan struct{}, error) {
	if m.opts.InformerPool == nil {
		return nil, nil, nil, fmt.Errorf("informer pool is required for core authn in shared RBAC mode")
	}
	return m.opts.InformerPool.Get(clusterID)
}

func (m *Manager) scopedAuthFactory(clusterID string) (informers.SharedInformerFactory, error) {
	shared, err := m.ensureSharedAuthFactory()
	if err != nil {
		return nil, err
	}
	return newScopedFactory(clusterID, shared, m.rbacStore), nil
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
		}
		for _, inf := range authInformers {
			if err := inf.SetTransform(scopedinformer.ClusterEntryTransform()); err != nil {
				m.sharedErr = err
				return
			}
			if err := scopedinformer.EnsureClusterIndex(inf); err != nil {
				m.sharedErr = err
				return
			}
		}
		rbacStore := newRBACProjectionStore()
		m.rbacDirty = make(chan struct{}, 1)
		for _, inf := range []cache.SharedIndexInformer{
			factory.Rbac().V1().Roles().Informer(),
			factory.Rbac().V1().RoleBindings().Informer(),
			factory.Rbac().V1().ClusterRoles().Informer(),
			factory.Rbac().V1().ClusterRoleBindings().Informer(),
		} {
			_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					_ = obj
					m.markRBACProjectionDirty()
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					_, _ = oldObj, newObj
					m.markRBACProjectionDirty()
				},
				DeleteFunc: func(obj interface{}) {
					_ = obj
					m.markRBACProjectionDirty()
				},
			})
			if err != nil {
				m.sharedErr = err
				return
			}
		}
		if m.sharedStop == nil {
			m.sharedOwn = make(chan struct{})
			m.sharedStop = m.sharedOwn
		}
		factory.Start(m.sharedStop)
		m.sharedAuth = factory
		m.rbacStore = rbacStore
		go m.runRBACProjectionRebuildWorker()
		m.markRBACProjectionDirty()
	})
	if m.sharedErr != nil {
		return nil, m.sharedErr
	}
	return m.sharedAuth, nil
}

func (m *Manager) runRBACProjectionRebuildWorker() {
	if m == nil || m.sharedStop == nil {
		return
	}
	timer := time.NewTimer(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	pending := false
	for {
		select {
		case <-m.sharedStop:
			return
		case <-m.rbacDirty:
			pending = true
			timer.Reset(150 * time.Millisecond)
		case <-timer.C:
			if !pending {
				continue
			}
			pending = false
			m.rebuildSharedRBACProjection()
		}
	}
}

func (m *Manager) markRBACProjectionDirty() {
	if m == nil || m.rbacDirty == nil {
		return
	}
	select {
	case m.rbacDirty <- struct{}{}:
	default:
	}
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
			indexer:   shared.Core().V1().Secrets().Informer().GetIndexer(),
			clusterID: clusterID,
		},
		serviceAccounts: &scopedServiceAccountLister{
			indexer:   shared.Core().V1().ServiceAccounts().Informer().GetIndexer(),
			clusterID: clusterID,
		},
		pods: &scopedPodLister{
			indexer:   shared.Core().V1().Pods().Informer().GetIndexer(),
			clusterID: clusterID,
		},
		nodes: &scopedNodeLister{
			indexer:   shared.Core().V1().Nodes().Informer().GetIndexer(),
			clusterID: clusterID,
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
	m.rebuildSharedRBACProjection()
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

func (m *Manager) rebuildSharedRBACProjection() {
	if m == nil || m.sharedAuth == nil || m.rbacStore == nil {
		return
	}
	roles := m.sharedAuth.Rbac().V1().Roles().Informer().GetStore().List()
	roleBindings := m.sharedAuth.Rbac().V1().RoleBindings().Informer().GetStore().List()
	clusterRoles := m.sharedAuth.Rbac().V1().ClusterRoles().Informer().GetStore().List()
	clusterRoleBindings := m.sharedAuth.Rbac().V1().ClusterRoleBindings().Informer().GetStore().List()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var (
		roleIndex               map[string]string
		roleBindingIndex        map[string]string
		clusterRoleIndex        map[string]string
		clusterRoleBindingIndex map[string]string
		errRoles, errRB         error
		errCR, errCRB           error
		wg                      sync.WaitGroup
	)
	wg.Add(4)
	go func() {
		defer wg.Done()
		roleIndex, errRoles = m.revisionClusterIndex(ctx, "/roles/clusters")
	}()
	go func() {
		defer wg.Done()
		roleBindingIndex, errRB = m.revisionClusterIndex(ctx, "/rolebindings/clusters")
	}()
	go func() {
		defer wg.Done()
		clusterRoleIndex, errCR = m.revisionClusterIndex(ctx, "/clusterroles/clusters")
	}()
	go func() {
		defer wg.Done()
		clusterRoleBindingIndex, errCRB = m.revisionClusterIndex(ctx, "/clusterrolebindings/clusters")
	}()
	wg.Wait()
	// Do not fall back to UID/RV object-memory attribution when durable key
	// indexing fails; that can leak cross-cluster RBAC permissions.
	if errRoles != nil {
		roleIndex = map[string]string{}
	}
	if errRB != nil {
		roleBindingIndex = map[string]string{}
	}
	if errCR != nil {
		clusterRoleIndex = map[string]string{}
	}
	if errCRB != nil {
		clusterRoleBindingIndex = map[string]string{}
	}
	m.rbacStore.RebuildFromInformerStoresWithResolver(
		roles, roleBindings, clusterRoles, clusterRoleBindings,
		func(rv, namespace, name string) string { return clusterFromRevisionIndex(roleIndex, rv, namespace, name) },
		func(rv, namespace, name string) string { return clusterFromRevisionIndex(roleBindingIndex, rv, namespace, name) },
		func(rv, namespace, name string) string { return clusterFromRevisionIndex(clusterRoleIndex, rv, namespace, name) },
		func(rv, namespace, name string) string { return clusterFromRevisionIndex(clusterRoleBindingIndex, rv, namespace, name) },
	)
}

func clusterFromRevisionIndex(index map[string]string, rv, namespace, name string) string {
	if len(index) == 0 || rv == "" {
		return ""
	}
	return index[rv+"|"+namespace+"|"+name]
}

func (m *Manager) revisionClusterIndex(ctx context.Context, key string) (map[string]string, error) {
	cli, err := m.getETCDClient()
	if err != nil {
		return nil, err
	}
	fullPrefix := strings.TrimSuffix(m.opts.EtcdPrefix, "/") + "/" + strings.TrimPrefix(key, "/")
	queryPrefix := strings.TrimSuffix(fullPrefix, "/") + "/"
	etcdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cli.Get(etcdCtx, queryPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(resp.Kvs))
	resolver := mcstorage.KeyLayoutPlacementResolver{KindRootPrefix: strings.TrimSuffix(key, "/")}
	storagePrefix := strings.TrimSuffix(key, "/") + "/"
	for _, kv := range resp.Kvs {
		etcdKey := string(kv.Key)
		storageKey := strings.TrimPrefix(etcdKey, strings.TrimSuffix(m.opts.EtcdPrefix, "/"))
		if storageKey == etcdKey {
			continue
		}
		if !strings.HasPrefix(storageKey, "/") {
			storageKey = "/" + storageKey
		}
		cid, ok := resolver.ClusterFromStorageKey(storageKey)
		if !ok || cid == "" {
			continue
		}
		rv := strconv.FormatInt(kv.ModRevision, 10)
		out[rv] = cid
		if ns, name, ok := objectPathFromStorageKey(storageKey, storagePrefix); ok {
			out[rv+"|"+ns+"|"+name] = cid
		}
	}
	return out, nil
}

func objectPathFromStorageKey(storageKey, rootPrefix string) (namespace, name string, ok bool) {
	if !strings.HasPrefix(storageKey, rootPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(storageKey, rootPrefix)
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return "", "", false
	}
	switch len(parts) {
	case 2:
		return "", parts[1], true
	default:
		return parts[1], parts[2], true
	}
}

func (m *Manager) getETCDClient() (*clientv3.Client, error) {
	m.etcdMu.Lock()
	defer m.etcdMu.Unlock()
	if m.etcdClient != nil {
		return m.etcdClient, nil
	}
	if len(m.opts.EtcdTransport.ServerList) == 0 {
		return nil, fmt.Errorf("no etcd servers configured")
	}
	cfg := clientv3.Config{
		Endpoints:   append([]string{}, m.opts.EtcdTransport.ServerList...),
		DialTimeout: 5 * time.Second,
	}
	if m.opts.EtcdTransport.CertFile != "" || m.opts.EtcdTransport.KeyFile != "" || m.opts.EtcdTransport.TrustedCAFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      m.opts.EtcdTransport.CertFile,
			KeyFile:       m.opts.EtcdTransport.KeyFile,
			TrustedCAFile: m.opts.EtcdTransport.TrustedCAFile,
		}
		tlsCfg, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	m.etcdClient = cli
	return m.etcdClient, nil
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
