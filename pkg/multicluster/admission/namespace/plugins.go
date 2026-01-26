package namespace

import (
	"context"
	"fmt"
	"sync"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	upstream "k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
)

// LifecyclePlugin dispatches NamespaceLifecycle per cluster.
type LifecyclePlugin struct {
	*admission.Handler
	opts mc.Options
	mgr  *Manager

	mu    sync.Mutex
	cache map[string]*upstream.Lifecycle
}

func NewLifecycle(opts mc.Options, mgr *Manager) *LifecyclePlugin {
	return &LifecyclePlugin{
		Handler: admission.NewHandler(admission.Create, admission.Update, admission.Delete),
		opts:    opts,
		mgr:     mgr,
		cache:   map[string]*upstream.Lifecycle{},
	}
}

func (p *LifecyclePlugin) Admit(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	return p.forCluster(ctx).Admit(ctx, attr, o)
}

func (p *LifecyclePlugin) ValidateInitialization() error {
	if p.mgr == nil {
		return fmt.Errorf("missing manager")
	}
	return nil
}

func (p *LifecyclePlugin) forCluster(ctx context.Context) *upstream.Lifecycle {
	cid, _, _ := mc.FromContext(ctx)
	if cid == "" {
		cid = p.opts.DefaultCluster
	}
	if cid == "" {
		cid = mc.DefaultClusterName
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.cache[cid]; ok {
		return existing
	}

	env, err := p.mgr.envForCluster(cid)
	if err != nil {
		panic(err)
	}

	plugin, err := upstream.NewLifecycle(sets.NewString(metav1.NamespaceDefault, metav1.NamespaceSystem, metav1.NamespacePublic))
	if err != nil {
		panic(err)
	}
	plugin.SetExternalKubeClientSet(env.clientset)
	plugin.SetExternalKubeInformerFactory(env.informers)
	// Avoid deadlock on initial cache sync for per-cluster namespace lifecycle.
	plugin.SetReadyFunc(func() bool { return true })
	if err := plugin.ValidateInitialization(); err != nil {
		panic(err)
	}

	p.cache[cid] = plugin
	return plugin
}
