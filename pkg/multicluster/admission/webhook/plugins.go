package webhook

import (
	"context"
	"io"
	"sync"
	"time"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"

	"k8s.io/apiserver/pkg/admission"
	upstreammutating "k8s.io/apiserver/pkg/admission/plugin/webhook/mutating"
	upstreamvalidating "k8s.io/apiserver/pkg/admission/plugin/webhook/validating"
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
	plugin *upstreammutating.Plugin
}

type validatingEntry struct {
	env    *clusterEnv
	plugin *upstreamvalidating.Plugin
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

func (p *MutatingPlugin) forCluster(ctx context.Context) (*clusterEnv, *upstreammutating.Plugin) {
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

	plugin, err := upstreammutating.NewMutatingWebhook(io.Reader(nil))
	if err != nil {
		panic(err)
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

func (p *ValidatingPlugin) forCluster(ctx context.Context) (*clusterEnv, *upstreamvalidating.Plugin) {
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

	plugin, err := upstreamvalidating.NewValidatingAdmissionWebhook(io.Reader(nil))
	if err != nil {
		panic(err)
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
