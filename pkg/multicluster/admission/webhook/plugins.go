package webhook

import (
	"context"
	"io"
	"sync"
	"time"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	mcmutating "github.com/kplane-dev/apiserver/pkg/multicluster/admission/webhook/mutating"
	mcvalidating "github.com/kplane-dev/apiserver/pkg/multicluster/admission/webhook/validating"

	"k8s.io/apiserver/pkg/admission"
)

// MutatingPlugin dispatches upstream MutatingAdmissionWebhook per cluster.
type MutatingPlugin struct {
	*admission.Handler
	opts mc.Options
	mgr  *Manager

	mu    sync.Mutex
	cache map[string]mutatingEntry
}

// ValidatingPlugin dispatches upstream ValidatingAdmissionWebhook per cluster.
type ValidatingPlugin struct {
	*admission.Handler
	opts mc.Options
	mgr  *Manager

	mu    sync.Mutex
	cache map[string]validatingEntry
}

type mutatingEntry struct {
	env    *clusterEnv
	plugin *mcmutating.Plugin
}

type validatingEntry struct {
	env    *clusterEnv
	plugin *mcvalidating.Plugin
}

func NewMutating(opts mc.Options, mgr *Manager) *MutatingPlugin {
	return &MutatingPlugin{
		Handler: admission.NewHandler(admission.Connect, admission.Create, admission.Delete, admission.Update),
		opts:    opts,
		mgr:     mgr,
		cache:   map[string]mutatingEntry{},
	}
}

func NewValidating(opts mc.Options, mgr *Manager) *ValidatingPlugin {
	return &ValidatingPlugin{
		Handler: admission.NewHandler(admission.Connect, admission.Create, admission.Delete, admission.Update),
		opts:    opts,
		mgr:     mgr,
		cache:   map[string]validatingEntry{},
	}
}

func (p *MutatingPlugin) Admit(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	env, plugin := p.forCluster(ctx)
	waitForWebhookCaches(ctx, attr, env)
	return plugin.Admit(ctx, attr, o)
}

func (p *ValidatingPlugin) Validate(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	env, plugin := p.forCluster(ctx)
	waitForWebhookCaches(ctx, attr, env)
	return plugin.Validate(ctx, attr, o)
}

func (p *MutatingPlugin) forCluster(ctx context.Context) (*clusterEnv, *mcmutating.Plugin) {
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
		return existing.env, existing.plugin
	}

	env, err := p.mgr.envForCluster(cid)
	if err != nil {
		// Admission interface doesn't allow returning init error cleanly here;
		// panic is acceptable since this indicates server misconfiguration.
		panic(err)
	}

	plugin, err := mcmutating.NewMutatingWebhook(io.Reader(nil))
	if err != nil {
		panic(err)
	}
	if p.mgr.opts.CelRuntime != nil {
		compiler, err := p.mgr.opts.CelRuntime.BaseCompiler()
		if err != nil {
			panic(err)
		}
		plugin.SetConditionCompiler(compiler)
	}
	plugin.SetExternalKubeClientSet(env.clientset)
	plugin.SetExternalKubeInformerFactory(env.informers)
	plugin.SetServiceResolver(env.serviceResolver)
	plugin.SetAuthenticationInfoResolverWrapper(p.mgr.opts.AuthWrapper)
	// Never gate apiserver readiness on these per-cluster informers.
	plugin.SetReadyFunc(func() bool { return true })
	if err := plugin.ValidateInitialization(); err != nil {
		panic(err)
	}

	p.cache[cid] = mutatingEntry{env: env, plugin: plugin}
	return env, plugin
}

func (p *ValidatingPlugin) forCluster(ctx context.Context) (*clusterEnv, *mcvalidating.Plugin) {
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
		return existing.env, existing.plugin
	}

	env, err := p.mgr.envForCluster(cid)
	if err != nil {
		panic(err)
	}

	plugin, err := mcvalidating.NewValidatingAdmissionWebhook(io.Reader(nil))
	if err != nil {
		panic(err)
	}
	if p.mgr.opts.CelRuntime != nil {
		compiler, err := p.mgr.opts.CelRuntime.BaseCompiler()
		if err != nil {
			panic(err)
		}
		plugin.SetConditionCompiler(compiler)
	}
	plugin.SetExternalKubeClientSet(env.clientset)
	plugin.SetExternalKubeInformerFactory(env.informers)
	plugin.SetServiceResolver(env.serviceResolver)
	plugin.SetAuthenticationInfoResolverWrapper(p.mgr.opts.AuthWrapper)
	// Never gate apiserver readiness on these per-cluster informers.
	plugin.SetReadyFunc(func() bool { return true })
	if err := plugin.ValidateInitialization(); err != nil {
		panic(err)
	}

	p.cache[cid] = validatingEntry{env: env, plugin: plugin}
	return env, plugin
}

func waitForWebhookCaches(ctx context.Context, attr admission.Attributes, env *clusterEnv) {
	if env == nil {
		return
	}
	// Avoid impacting control plane startup/leader-election, etc.
	if attr.GetNamespace() == "kube-system" && attr.GetResource().Resource == "leases" {
		return
	}
	// If caches are already synced, we're good.
	select {
	case <-env.synced:
		return
	default:
	}

	// Wait briefly for informer initial list+watch to complete so webhook config + services are visible.
	// Fail-open if it doesn't happen quickly (prevents deadlocks during early startup).
	timeout := 10 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		rem := time.Until(dl)
		if rem > 0 && rem < timeout {
			timeout = rem
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-env.synced:
	case <-timer.C:
	case <-ctx.Done():
	}
}
