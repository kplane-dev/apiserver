package webhook

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

type EnvKey struct {
	ClusterID  string
	GVK        schema.GroupVersionKind
	CRDVersion string
	SchemaHash string
	EnvType    environment.Type
}

type CelRuntime struct {
	baseOnce sync.Once
	base     *environment.EnvSet
	baseErr  error

	mu        sync.RWMutex
	compilers map[EnvKey]cel.ConditionCompiler

	buildTotal    *metrics.CounterVec
	cacheHitTotal *metrics.CounterVec
	cacheSize     *metrics.Gauge
}

var (
	celMetricsOnce sync.Once
	celBuildTotal  = metrics.NewCounterVec(&metrics.CounterOpts{
		Name: "mc_cel_env_build_total",
		Help: "Number of CEL env/compiler builds by reason.",
	}, []string{"reason"})
	celCacheHitTotal = metrics.NewCounterVec(&metrics.CounterOpts{
		Name: "mc_cel_env_cache_hit_total",
		Help: "Number of CEL env/compiler cache hits by reason.",
	}, []string{"reason"})
	celCacheSize = metrics.NewGauge(&metrics.GaugeOpts{
		Name: "mc_cel_env_cache_size",
		Help: "Number of cached CEL env/compiler entries.",
	})
)

func NewCelRuntime() *CelRuntime {
	celMetricsOnce.Do(func() {
		legacyregistry.MustRegister(celBuildTotal, celCacheHitTotal, celCacheSize)
	})
	r := &CelRuntime{
		compilers:     map[EnvKey]cel.ConditionCompiler{},
		buildTotal:    celBuildTotal,
		cacheHitTotal: celCacheHitTotal,
		cacheSize:     celCacheSize,
	}
	return r
}

func (r *CelRuntime) BaseCompiler() (cel.ConditionCompiler, error) {
	return r.CompilerFor(EnvKey{}, nil)
}

func (r *CelRuntime) CompilerFor(key EnvKey, extendOpts []environment.VersionedOptions) (cel.ConditionCompiler, error) {
	reason := "overlay"
	if key == (EnvKey{}) {
		reason = "base"
	}
	r.mu.RLock()
	if compiler, ok := r.compilers[key]; ok {
		r.mu.RUnlock()
		r.cacheHitTotal.WithLabelValues(reason).Inc()
		return compiler, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if compiler, ok := r.compilers[key]; ok {
		r.cacheHitTotal.WithLabelValues(reason).Inc()
		return compiler, nil
	}

	base, err := r.baseEnvSet()
	if err != nil {
		return nil, err
	}
	env := base
	if len(extendOpts) > 0 {
		extended, err := base.Extend(extendOpts...)
		if err != nil {
			return nil, err
		}
		env = extended
	}
	compiler := cel.NewConditionCompiler(env)
	r.compilers[key] = compiler
	r.cacheSize.Set(float64(len(r.compilers)))
	r.buildTotal.WithLabelValues(reason).Inc()
	return compiler, nil
}

func (r *CelRuntime) CacheSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.compilers)
}

func (r *CelRuntime) baseEnvSet() (*environment.EnvSet, error) {
	r.baseOnce.Do(func() {
		r.base = environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion(), utilfeature.DefaultFeatureGate.Enabled(features.StrictCostEnforcementForWebhooks))
	})
	return r.base, r.baseErr
}
