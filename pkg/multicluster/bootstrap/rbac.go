package bootstrap

import (
	"context"
	"fmt"
	"sync"
	"time"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"k8s.io/client-go/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	rbacrest "k8s.io/kubernetes/pkg/registry/rbac/rest"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac/bootstrappolicy"
	"k8s.io/klog/v2"
)

type RBACOptions struct {
	BaseLoopbackClientConfig *rest.Config
	PathPrefix               string
	ControlPlaneSegment      string
	Timeout                  time.Duration
}

// RBACBootstrapper applies upstream default RBAC bootstrap policy per cluster.
// It preserves upstream policy data and reconciliation semantics while making
// the target cluster explicit via a cluster-scoped loopback host.
type RBACBootstrapper struct {
	base                *rest.Config
	pathPrefix          string
	controlPlaneSegment string
	timeout             time.Duration

	mu       sync.Mutex
	done     map[string]struct{}
	inflight map[string]chan struct{}
}

func NewRBACBootstrapper(opts RBACOptions) *RBACBootstrapper {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 35 * time.Second
	}
	base := opts.BaseLoopbackClientConfig
	if base != nil {
		base = rest.CopyConfig(base)
	}
	return &RBACBootstrapper{
		base:                base,
		pathPrefix:          opts.PathPrefix,
		controlPlaneSegment: opts.ControlPlaneSegment,
		timeout:             timeout,
		done:                map[string]struct{}{},
		inflight:            map[string]chan struct{}{},
	}
}

func (b *RBACBootstrapper) Ensure(clusterID string) {
	if b == nil || clusterID == "" || b.base == nil {
		return
	}

	b.mu.Lock()
	if _, ok := b.done[clusterID]; ok {
		b.mu.Unlock()
		return
	}
	if ch, ok := b.inflight[clusterID]; ok {
		b.mu.Unlock()
		<-ch
		return
	}
	ch := make(chan struct{})
	b.inflight[clusterID] = ch
	b.mu.Unlock()

	err := b.bootstrap(clusterID)

	b.mu.Lock()
	delete(b.inflight, clusterID)
	if err == nil {
		b.done[clusterID] = struct{}{}
	}
	close(ch)
	b.mu.Unlock()
	if err != nil {
		klog.Errorf("mc.bootstrap rbac failed cluster=%s: %v", clusterID, err)
	}
}

func (b *RBACBootstrapper) bootstrap(clusterID string) error {
	cfg := rest.CopyConfig(b.base)
	host, err := mc.ClusterHost(cfg.Host, mc.Options{
		PathPrefix:          b.pathPrefix,
		ControlPlaneSegment: b.controlPlaneSegment,
	}, clusterID)
	if err != nil {
		return fmt.Errorf("build cluster host: %w", err)
	}
	cfg.Host = host

	policy := &rbacrest.PolicyData{
		ClusterRoles:               append(bootstrappolicy.ClusterRoles(), bootstrappolicy.ControllerRoles()...),
		ClusterRoleBindings:        append(bootstrappolicy.ClusterRoleBindings(), bootstrappolicy.ControllerRoleBindings()...),
		Roles:                      bootstrappolicy.NamespaceRoles(),
		RoleBindings:               bootstrappolicy.NamespaceRoleBindings(),
		ClusterRolesToAggregate:    bootstrappolicy.ClusterRolesToAggregate(),
		ClusterRoleBindingsToSplit: bootstrappolicy.ClusterRoleBindingsToSplit(),
	}
	hook := policy.EnsureRBACPolicy()
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	return hook(genericapiserver.PostStartHookContext{
		LoopbackClientConfig: cfg,
		Context:              ctx,
	})
}
