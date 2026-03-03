/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"
	"net/http"
	"reflect"
	"strings"

	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	namespaceplugin "k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfilters "k8s.io/apiserver/pkg/endpoints/filters"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/server"
	serverfilters "k8s.io/apiserver/pkg/server/filters"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/klog/v2"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	"k8s.io/kubernetes/pkg/features"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"

	"github.com/kplane-dev/apiserver/cmd/apiserver/app/options"
	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	mca "github.com/kplane-dev/apiserver/pkg/multicluster/admission"
	mcnsl "github.com/kplane-dev/apiserver/pkg/multicluster/admission/namespace"
	mcwh "github.com/kplane-dev/apiserver/pkg/multicluster/admission/webhook"
	mcauth "github.com/kplane-dev/apiserver/pkg/multicluster/auth"
	mcbootstrap "github.com/kplane-dev/apiserver/pkg/multicluster/bootstrap"
)

type Config struct {
	Options options.CompletedOptions

	Aggregator    *aggregatorapiserver.Config
	KubeAPIs      *controlplane.Config
	ApiExtensions *apiextensionsapiserver.Config

	ExtraConfig
}

type ExtraConfig struct {
}

type completedConfig struct {
	Options options.CompletedOptions

	Aggregator    aggregatorapiserver.CompletedConfig
	KubeAPIs      controlplane.CompletedConfig
	ApiExtensions apiextensionsapiserver.CompletedConfig

	ExtraConfig
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

func (c *Config) Complete() (CompletedConfig, error) {
	return CompletedConfig{&completedConfig{
		Options: c.Options,

		Aggregator:    c.Aggregator.Complete(),
		KubeAPIs:      c.KubeAPIs.Complete(),
		ApiExtensions: c.ApiExtensions.Complete(),

		ExtraConfig: c.ExtraConfig,
	}}, nil
}

// NewConfig creates all the resources for running kube-apiserver, but runs none of them.
func NewConfig(opts options.CompletedOptions) (*Config, error) {
	c := &Config{
		Options: opts,
	}

	// We provide cluster-aware webhook admission below; disable upstream single-cluster webhooks.
	if opts.Admission != nil && opts.Admission.GenericAdmission != nil {
		opts.Admission.GenericAdmission.DisablePlugins = append(
			opts.Admission.GenericAdmission.DisablePlugins,
			"MutatingAdmissionWebhook",
			"ValidatingAdmissionWebhook",
			namespaceplugin.PluginName,
		)
	}

	genericConfig, versionedInformers, storageFactory, err := controlplaneapiserver.BuildGenericConfig(
		opts.CompletedOptions,
		[]*runtime.Scheme{legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme},
		controlplane.DefaultAPIResourceConfigSource(),
		generatedopenapi.GetOpenAPIDefinitions,
	)
	if err != nil {
		return nil, err
	}

	// Install multicluster request routing early in the handler chain
	mcOpts := mc.DefaultOptions
	mcOpts.EtcdPrefix = storageFactory.StorageConfig.Prefix
	if opts.RootControlPlaneName != "" {
		mcOpts.DefaultCluster = opts.RootControlPlaneName
	}
	clientPool := mc.NewClientPool(genericConfig.LoopbackClientConfig, mcOpts.PathPrefix, mcOpts.ControlPlaneSegment)
	informerRegistry := mc.NewInformerRegistry(wait.ContextForChannel(genericConfig.DrainedNotify()))
	mcOpts.InformerRegistry = informerRegistry
	var crdRuntimeMgr *mcbootstrap.CRDRuntimeManager
	systemNamespaceBootstrapper := mcbootstrap.NewSystemNamespaceBootstrapper(mcbootstrap.SystemNamespaceOptions{
		ClientForCluster: clientPool.KubeClientForCluster,
		Namespaces:       opts.SystemNamespaces,
	})
	serviceCIDRBootstrapper := mcbootstrap.NewServiceCIDRBootstrapper(mcbootstrap.ServiceCIDROptions{
		ClientForCluster: clientPool.KubeClientForCluster,
		PrimaryRange:     opts.PrimaryServiceClusterIPRange,
		SecondaryRange:   opts.SecondaryServiceClusterIPRange,
		Enabled:          utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) && opts.ServiceCIDRSharingMode == options.ServiceCIDRSharingModePerCluster,
	})
	rbacBootstrapper := mcbootstrap.NewRBACBootstrapper(mcbootstrap.RBACOptions{
		BaseLoopbackClientConfig: genericConfig.LoopbackClientConfig,
		PathPrefix:               mcOpts.PathPrefix,
		ControlPlaneSegment:      mcOpts.ControlPlaneSegment,
	})
	genericConfig.BuildHandlerChainFunc = func(h http.Handler, conf *server.Config) http.Handler {
		ex := mc.PathExtractor{PathPrefix: mcOpts.PathPrefix, ControlPlaneSegment: mcOpts.ControlPlaneSegment}
		base := withVersionOverride(server.DefaultBuildHandlerChain(h, conf))
		dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cid, _, _ := mc.FromContext(r.Context())
			if cid != "" && cid != mcOpts.DefaultCluster && crdRuntimeMgr != nil {
				if group, version, ok := apisGroupVersionFromPath(r.URL.Path); ok {
					served, err := crdRuntimeMgr.ServesGroupVersion(cid, group, version, genericConfig.DrainedNotify())
					if err != nil {
						klog.Errorf("mc.crdRuntime lookup failed at kube cluster=%s path=%s err=%v", cid, r.URL.Path, err)
						http.Error(w, "cluster CRD runtime unavailable", http.StatusServiceUnavailable)
						return
					}
					if !served {
						base.ServeHTTP(w, r)
						return
					}
					if h, err := crdRuntimeMgr.Runtime(cid, genericConfig.DrainedNotify()); err == nil && h != nil {
						wrapClusterCRDHandler(h, conf, cid, false).ServeHTTP(w, r)
						return
					}
					klog.Errorf("mc.crdRuntime unresolved at kube cluster=%s path=%s", cid, r.URL.Path)
					http.Error(w, "cluster CRD runtime unavailable", http.StatusServiceUnavailable)
					return
				}
			}
			base.ServeHTTP(w, r)
		})
		return mc.WithClusterRouting(dispatch, ex, mcOpts)
	}

	authManager := mcauth.NewManager(wait.ContextForChannel(genericConfig.DrainedNotify()), mcauth.Options{
		BaseLoopbackClientConfig: genericConfig.LoopbackClientConfig,
		PathPrefix:               mcOpts.PathPrefix,
		ControlPlaneSegment:      mcOpts.ControlPlaneSegment,
		Authentication:           opts.Authentication,
		Authorization:            opts.Authorization,
		EgressSelector:           genericConfig.EgressSelector,
		APIServerID:              genericConfig.APIServerID,
		ClientPool:               clientPool,
		InformerRegistry:         informerRegistry,
	})
	if genericConfig.Authentication.Authenticator != nil {
		genericConfig.Authentication.Authenticator = mcauth.NewClusterAuthenticator(mcOpts.DefaultCluster, genericConfig.Authentication.Authenticator, authManager)
	}
	if genericConfig.Authorization.Authorizer != nil {
		// Route root and tenant authorization through the same multicluster
		// manager-backed path to avoid root-only stale lister divergence.
		clusterAuthorizer := mcauth.NewClusterAuthorizer(mcOpts.DefaultCluster, nil, nil, authManager)
		genericConfig.Authorization.Authorizer = clusterAuthorizer
		genericConfig.RuleResolver = clusterAuthorizer
	}

	// Decorate storage to inject cluster-aware key rewriting and filtering
	if genericConfig.RESTOptionsGetter != nil {
		genericConfig.RESTOptionsGetter = decorateRESTOptionsGetter("controlplane", genericConfig.RESTOptionsGetter, mcOpts)
	}

	// Cluster-aware namespace lifecycle (per-cluster client + informer).
	mcNamespaceMgr := mcnsl.NewManager(mcnsl.Options{
		BaseLoopbackClientConfig: genericConfig.LoopbackClientConfig,
		PathPrefix:               mcOpts.PathPrefix,
		ControlPlaneSegment:      mcOpts.ControlPlaneSegment,
		ClientPool:               clientPool,
		InformerRegistry:         informerRegistry,
	})
	mcNamespaceLifecycle := mcnsl.NewLifecycle(mcOpts, mcNamespaceMgr)

	// Wrap admission: mutating first, namespace lifecycle, then existing chain, then validating
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := genericConfig.AdmissionControl
		chain := []admission.Interface{mut, mcNamespaceLifecycle}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, val)
		genericConfig.AdmissionControl = admission.NewChainHandler(chain...)
	}

	kubeAPIs, serviceResolver, pluginInitializer, err := CreateKubeAPIServerConfig(opts, genericConfig, versionedInformers, storageFactory)
	if err != nil {
		return nil, err
	}
	c.KubeAPIs = kubeAPIs
	if c.KubeAPIs.ControlPlane.Generic.RESTOptionsGetter != nil {
		c.KubeAPIs.ControlPlane.Generic.RESTOptionsGetter = decorateRESTOptionsGetter("controlplane", c.KubeAPIs.ControlPlane.Generic.RESTOptionsGetter, mcOpts)
	}
	targetPort := 443
	if opts.SecureServing != nil && opts.SecureServing.BindPort > 0 {
		targetPort = opts.SecureServing.BindPort
	}
	stopChForCluster := func(clusterID string) (<-chan struct{}, error) {
		return genericConfig.DrainedNotify(), nil
	}
	internalControllerMgr := mcbootstrap.NewInternalControllerManager(mcbootstrap.InternalControllerOptions{
		ClientForCluster: clientPool.KubeClientForCluster,
		StopChForCluster: stopChForCluster,
		ClusterAuthInfo:  kubeAPIs.ControlPlane.Extra.ClusterAuthenticationInfo,
	})
	kubeServiceControllerMgr := mcbootstrap.NewKubernetesServiceControllerManager(mcbootstrap.KubernetesServiceControllerOptions{
		ClientForCluster: clientPool.KubeClientForCluster,
		StopChForCluster: stopChForCluster,
		PublicIP:         kubeAPIs.ControlPlane.Generic.PublicAddress,
		ServicePort:      443,
		TargetPort:       targetPort,
		NodePort:         opts.KubernetesServiceNodePort,
	})
	mcOpts.OnClusterSelected = func(clusterID string) {
		// Preserve upstream root bootstrap as-is; only add multicluster bootstrap for non-root VCPs.
		if clusterID == mcOpts.DefaultCluster {
			return
		}
		// Run asynchronously to avoid recursive request deadlocks when bootstrap logic
		// uses cluster-scoped clients that route back through this same middleware.
		go systemNamespaceBootstrapper.Ensure(clusterID)
		go serviceCIDRBootstrapper.Ensure(clusterID)
		go rbacBootstrapper.Ensure(clusterID)
		go internalControllerMgr.Ensure(clusterID)
		if opts.KubernetesServiceMode == options.KubernetesServiceModePerClusterAutoIP {
			go kubeServiceControllerMgr.Ensure(clusterID)
		}
	}

	// Cluster-aware webhook admission (per-cluster clients + informers, no global cross-cluster view).
	authWrapper := webhook.NewDefaultAuthenticationInfoResolverWrapper(
		kubeAPIs.ControlPlane.ProxyTransport,
		kubeAPIs.ControlPlane.Generic.EgressSelector,
		kubeAPIs.ControlPlane.Generic.LoopbackClientConfig,
		kubeAPIs.ControlPlane.Generic.TracerProvider,
	)
	celRuntime := mcwh.NewCelRuntime()
	mcWebhookMgr := mcwh.NewManager(mcwh.Options{
		BaseLoopbackClientConfig: kubeAPIs.ControlPlane.Generic.LoopbackClientConfig,
		AuthWrapper:              authWrapper,
		EnableAggregatorRouting:  opts.EnableAggregatorRouting,
		Hostname:                 kubeAPIs.ControlPlane.Generic.LoopbackClientConfig.Host,
		PathPrefix:               mcOpts.PathPrefix,
		ControlPlaneSegment:      mcOpts.ControlPlaneSegment,
		CelRuntime:               celRuntime,
		ClientPool:               clientPool,
		InformerRegistry:         informerRegistry,
	})
	mcMutatingWebhook := mcwh.NewMutating(mcOpts, mcWebhookMgr)
	mcValidatingWebhook := mcwh.NewValidating(mcOpts, mcWebhookMgr)

	// Reinstall admission chain on concrete generics to avoid later overrides
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := c.KubeAPIs.ControlPlane.Generic.AdmissionControl
		chain := []admission.Interface{mut, mcNamespaceLifecycle, mcMutatingWebhook}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, mcValidatingWebhook, val)
		c.KubeAPIs.ControlPlane.Generic.AdmissionControl = admission.NewChainHandler(chain...)
	}

	apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*kubeAPIs.ControlPlane.Generic, kubeAPIs.ControlPlane.VersionedInformers, pluginInitializer, opts.CompletedOptions, opts.MasterCount,
		serviceResolver, webhook.NewDefaultAuthenticationInfoResolverWrapper(kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Generic.EgressSelector, kubeAPIs.ControlPlane.Generic.LoopbackClientConfig, kubeAPIs.ControlPlane.Generic.TracerProvider))
	if err != nil {
		return nil, err
	}
	if apiExtensions.GenericConfig.RESTOptionsGetter != nil {
		apiExtensions.GenericConfig.RESTOptionsGetter = decorateRESTOptionsGetter("apiextensions", apiExtensions.GenericConfig.RESTOptionsGetter, mcOpts)
	}
	if apiExtensions.ExtraConfig.CRDRESTOptionsGetter != nil {
		apiExtensions.ExtraConfig.CRDRESTOptionsGetter = decorateRESTOptionsGetter("apiextensions-crd", apiExtensions.ExtraConfig.CRDRESTOptionsGetter, mcOpts)
	}
	crdRuntimeMgr = mcbootstrap.NewCRDRuntimeManager(mcbootstrap.CRDRuntimeManagerOptions{
		BaseAPIExtensionsConfig: apiExtensions,
		InformerRegistry:        informerRegistry,
		PathPrefix:              mcOpts.PathPrefix,
		ControlPlaneSegment:     mcOpts.ControlPlaneSegment,
		DefaultCluster:          mcOpts.DefaultCluster,
	})
	setCRDRequestResolvers(apiExtensions, crdRuntimeMgr.CRDGetterForRequest, crdRuntimeMgr.CRDListerForRequest)
	crdController := mcbootstrap.NewMulticlusterCRDController(crdRuntimeMgr, mcOpts.DefaultCluster)
	crdController.Start(genericConfig.DrainedNotify())
	prevOnClusterSelected := mcOpts.OnClusterSelected
	mcOpts.OnClusterSelected = func(clusterID string) {
		if prevOnClusterSelected != nil {
			prevOnClusterSelected(clusterID)
		}
		if clusterID == "" || clusterID == mcOpts.DefaultCluster {
			return
		}
		crdController.EnsureCluster(clusterID)
	}
	serveClusterCRD := func(w http.ResponseWriter, r *http.Request, conf *server.Config, clusterID, caller string) bool {
		group, version, ok := apisGroupVersionFromPath(r.URL.Path)
		if !ok {
			return false
		}

		served, err := crdRuntimeMgr.ServesGroupVersion(clusterID, group, version, genericConfig.DrainedNotify())
		if err != nil {
			klog.Errorf("mc.crdRuntime lookup failed at %s cluster=%s path=%s err=%v", caller, clusterID, r.URL.Path, err)
			http.Error(w, "cluster CRD runtime unavailable", http.StatusServiceUnavailable)
			return true
		}
		if !served {
			return false
		}

		h, err := crdRuntimeMgr.Runtime(clusterID, genericConfig.DrainedNotify())
		if err != nil || h == nil {
			klog.Errorf("mc.crdRuntime unresolved at %s cluster=%s path=%s err=%v", caller, clusterID, r.URL.Path, err)
			http.Error(w, "cluster CRD runtime unavailable", http.StatusServiceUnavailable)
			return true
		}
		wrapClusterCRDHandler(h, conf, clusterID, false).ServeHTTP(w, r)
		return true
	}
	// Ensure CRDs are also routed through the multicluster handler
	apiExtensions.GenericConfig.BuildHandlerChainFunc = func(h http.Handler, conf *server.Config) http.Handler {
		ex := mc.PathExtractor{PathPrefix: mcOpts.PathPrefix, ControlPlaneSegment: mcOpts.ControlPlaneSegment}
		base := withVersionOverride(server.DefaultBuildHandlerChain(h, conf))
		dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cid, _, _ := mc.FromContext(r.Context())
			if cid != "" && cid != mcOpts.DefaultCluster {
				if serveClusterCRD(w, r, conf, cid, "apiextensions") {
					return
				}
			}
			base.ServeHTTP(w, r)
		})
		return mc.WithClusterRouting(dispatch, ex, mcOpts)
	}
	// Install admission chain on apiextensions as well
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := apiExtensions.GenericConfig.AdmissionControl
		chain := []admission.Interface{mut, mcNamespaceLifecycle}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, val)
		apiExtensions.GenericConfig.AdmissionControl = admission.NewChainHandler(chain...)
	}
	c.ApiExtensions = apiExtensions

	aggregator, err := controlplaneapiserver.CreateAggregatorConfig(*kubeAPIs.ControlPlane.Generic, opts.CompletedOptions, kubeAPIs.ControlPlane.VersionedInformers, serviceResolver, kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Extra.PeerProxy, pluginInitializer)
	if err != nil {
		return nil, err
	}
	if aggregator.GenericConfig.RESTOptionsGetter != nil {
		aggregator.GenericConfig.RESTOptionsGetter = decorateRESTOptionsGetter("aggregator", aggregator.GenericConfig.RESTOptionsGetter, mcOpts)
	}
	// Ensure aggregator also receives multicluster routing
	aggregator.GenericConfig.BuildHandlerChainFunc = func(h http.Handler, conf *server.Config) http.Handler {
		ex := mc.PathExtractor{PathPrefix: mcOpts.PathPrefix, ControlPlaneSegment: mcOpts.ControlPlaneSegment}
		base := withVersionOverride(server.DefaultBuildHandlerChain(h, conf))
		dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cid, _, _ := mc.FromContext(r.Context())
			if cid != "" && cid != mcOpts.DefaultCluster && crdRuntimeMgr != nil {
				if serveClusterCRD(w, r, conf, cid, "aggregator") {
					return
				}
			}
			base.ServeHTTP(w, r)
		})
		return mc.WithClusterRouting(dispatch, ex, mcOpts)
	}
	// Install admission chain on aggregator
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := aggregator.GenericConfig.AdmissionControl
		chain := []admission.Interface{mut, mcNamespaceLifecycle}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, val)
		aggregator.GenericConfig.AdmissionControl = admission.NewChainHandler(chain...)
	}
	c.Aggregator = aggregator

	return c, nil
}

func setCRDRequestResolvers(
	apiExtensions *apiextensionsapiserver.Config,
	getter func(ctx context.Context, name string) (*apiextensionsv1.CustomResourceDefinition, error),
	lister func(ctx context.Context) ([]*apiextensionsv1.CustomResourceDefinition, error),
) {
	if apiExtensions == nil {
		return
	}
	v := reflect.ValueOf(&apiExtensions.ExtraConfig).Elem()

	getterField := v.FieldByName("CRDGetter")
	if getterField.IsValid() && getterField.CanSet() {
		getterField.Set(reflect.ValueOf(getter))
	} else {
		klog.V(2).Info("apiextensions ExtraConfig has no CRDGetter field; skipping request-scoped CRD getter hook")
	}

	listerField := v.FieldByName("CRDListerForRequest")
	if listerField.IsValid() && listerField.CanSet() {
		listerField.Set(reflect.ValueOf(lister))
	} else {
		klog.V(2).Info("apiextensions ExtraConfig has no CRDListerForRequest field; skipping request-scoped CRD lister hook")
	}
}

func withClusterCRDRequestInfoRewrite(next http.Handler, clusterID string) http.Handler {
	if next == nil || clusterID == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ri, ok := apirequest.RequestInfoFrom(r.Context())
		if !ok || ri == nil || !ri.IsResourceRequest || ri.APIGroup == "" || ri.APIGroup == "apiextensions.k8s.io" {
			next.ServeHTTP(w, r)
			return
		}
		rewritten := *ri
		rewritten.Resource = mcbootstrap.EncodeSharedCRDResourceName(clusterID, ri.Resource)
		ctx := apirequest.WithRequestInfo(r.Context(), &rewritten)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func wrapClusterCRDHandler(next http.Handler, conf *server.Config, clusterID string, rewriteRequestInfo bool) http.Handler {
	if next == nil || conf == nil {
		return next
	}
	h := next
	if rewriteRequestInfo {
		h = withClusterCRDRequestInfoRewrite(h, clusterID)
	}
	h = genericfilters.WithAuthorization(h, conf.Authorization.Authorizer, conf.Serializer)
	failedHandler := genericfilters.Unauthorized(conf.Serializer)
	failedHandler = genericfilters.WithFailedAuthenticationAudit(failedHandler, conf.AuditBackend, conf.AuditPolicyRuleEvaluator)
	h = genericfilters.WithAuthentication(h, conf.Authentication.Authenticator, failedHandler, conf.Authentication.APIAudiences, conf.Authentication.RequestHeaderConfig)
	// RequestInfo must be available before rewrite/authn/authz wrappers execute.
	h = genericfilters.WithRequestInfo(h, conf.RequestInfoResolver)
	h = genericfilters.WithAuditInit(h)
	h = serverfilters.WithPanicRecovery(h, conf.RequestInfoResolver)
	return h
}

func decorateRESTOptionsGetter(server string, getter generic.RESTOptionsGetter, opts mc.Options) generic.RESTOptionsGetter {
	if _, ok := getter.(mc.RESTOptionsDecorator); ok {
		klog.Infof("mc.restOptionsGetter server=%s alreadyDecorated=true", server)
		return getter
	}
	opts.ServerName = server
	decorated := mc.RESTOptionsDecorator{Delegate: getter, Options: opts}
	klog.Infof("mc.restOptionsGetter server=%s decorated=%t", server, true)
	return decorated
}

func apisGroupVersionFromPath(path string) (group, version string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 3 || parts[0] != "apis" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	if parts[1] == "apiextensions.k8s.io" {
		return "", "", false
	}
	return parts[1], parts[2], true
}
