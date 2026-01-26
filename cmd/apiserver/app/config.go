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
	"net/http"

	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	namespaceplugin "k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/klog/v2"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"

	"github.com/kplane-dev/apiserver/cmd/apiserver/app/options"
	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	mca "github.com/kplane-dev/apiserver/pkg/multicluster/admission"
	mcnsl "github.com/kplane-dev/apiserver/pkg/multicluster/admission/namespace"
	mcwh "github.com/kplane-dev/apiserver/pkg/multicluster/admission/webhook"
	mcauth "github.com/kplane-dev/apiserver/pkg/multicluster/auth"
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
	genericConfig.BuildHandlerChainFunc = func(h http.Handler, conf *server.Config) http.Handler {
		ex := mc.PathExtractor{PathPrefix: mcOpts.PathPrefix, ControlPlaneSegment: mcOpts.ControlPlaneSegment}
		return mc.WithClusterRouting(server.DefaultBuildHandlerChain(h, conf), ex, mcOpts)
	}

	authManager := mcauth.NewManager(wait.ContextForChannel(genericConfig.DrainedNotify()), mcauth.Options{
		BaseLoopbackClientConfig: genericConfig.LoopbackClientConfig,
		PathPrefix:               mcOpts.PathPrefix,
		ControlPlaneSegment:      mcOpts.ControlPlaneSegment,
		Authentication:           opts.Authentication,
		Authorization:            opts.Authorization,
		EgressSelector:           genericConfig.EgressSelector,
		APIServerID:              genericConfig.APIServerID,
	})
	if genericConfig.Authentication.Authenticator != nil {
		genericConfig.Authentication.Authenticator = mcauth.NewClusterAuthenticator(mcOpts.DefaultCluster, genericConfig.Authentication.Authenticator, authManager)
	}
	if genericConfig.Authorization.Authorizer != nil {
		clusterAuthorizer := mcauth.NewClusterAuthorizer(mcOpts.DefaultCluster, genericConfig.Authorization.Authorizer, genericConfig.RuleResolver, authManager)
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

	// Cluster-aware webhook admission (per-cluster clients + informers, no global cross-cluster view).
	authWrapper := webhook.NewDefaultAuthenticationInfoResolverWrapper(
		kubeAPIs.ControlPlane.ProxyTransport,
		kubeAPIs.ControlPlane.Generic.EgressSelector,
		kubeAPIs.ControlPlane.Generic.LoopbackClientConfig,
		kubeAPIs.ControlPlane.Generic.TracerProvider,
	)
	mcWebhookMgr := mcwh.NewManager(mcwh.Options{
		BaseLoopbackClientConfig: kubeAPIs.ControlPlane.Generic.LoopbackClientConfig,
		AuthWrapper:              authWrapper,
		EnableAggregatorRouting:  opts.EnableAggregatorRouting,
		Hostname:                 kubeAPIs.ControlPlane.Generic.LoopbackClientConfig.Host,
		PathPrefix:               mcOpts.PathPrefix,
		ControlPlaneSegment:      mcOpts.ControlPlaneSegment,
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
	// Ensure CRDs are also routed through the multicluster handler
	apiExtensions.GenericConfig.BuildHandlerChainFunc = func(h http.Handler, conf *server.Config) http.Handler {
		ex := mc.PathExtractor{PathPrefix: mcOpts.PathPrefix, ControlPlaneSegment: mcOpts.ControlPlaneSegment}
		return mc.WithClusterRouting(server.DefaultBuildHandlerChain(h, conf), ex, mcOpts)
	}
	// Install admission chain on apiextensions as well
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := apiExtensions.GenericConfig.AdmissionControl
		chain := []admission.Interface{mut, mcNamespaceLifecycle, mcMutatingWebhook}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, mcValidatingWebhook, val)
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
		return mc.WithClusterRouting(server.DefaultBuildHandlerChain(h, conf), ex, mcOpts)
	}
	// Install admission chain on aggregator
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := aggregator.GenericConfig.AdmissionControl
		chain := []admission.Interface{mut, mcNamespaceLifecycle, mcMutatingWebhook}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, mcValidatingWebhook, val)
		aggregator.GenericConfig.AdmissionControl = admission.NewChainHandler(chain...)
	}
	c.Aggregator = aggregator

	return c, nil
}

func decorateRESTOptionsGetter(server string, getter generic.RESTOptionsGetter, opts mc.Options) generic.RESTOptionsGetter {
	opts.ServerName = server
	decorated := mc.RESTOptionsDecorator{Delegate: getter, Options: opts}
	klog.Infof("mc.restOptionsGetter server=%s decorated=%t", server, true)
	return decorated
}
