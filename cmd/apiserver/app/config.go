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
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"

	"github.com/kplane-dev/apiserver/cmd/apiserver/app/options"
	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	mca "github.com/kplane-dev/apiserver/pkg/multicluster/admission"
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
	genericConfig.BuildHandlerChainFunc = func(h http.Handler, conf *server.Config) http.Handler {
		ex := mc.PathExtractor{PathPrefix: mcOpts.PathPrefix, ControlPlaneSegment: mcOpts.ControlPlaneSegment}
		return mc.WithClusterRouting(server.DefaultBuildHandlerChain(h, conf), ex, mcOpts)
	}

	// Decorate storage to inject cluster-aware key rewriting and filtering
	if genericConfig.RESTOptionsGetter != nil {
		genericConfig.RESTOptionsGetter = mc.RESTOptionsDecorator{Delegate: genericConfig.RESTOptionsGetter, Options: mcOpts}
	}

	// Wrap admission: mutating first, then existing chain, then validating
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := genericConfig.AdmissionControl
		chain := []admission.Interface{mut}
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
	// Reinstall admission chain on concrete generics to avoid later overrides
	{
		mut := mca.NewMutating(mcOpts)
		val := mca.NewValidating(mcOpts)
		base := c.KubeAPIs.ControlPlane.Generic.AdmissionControl
		chain := []admission.Interface{mut}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, val)
		c.KubeAPIs.ControlPlane.Generic.AdmissionControl = admission.NewChainHandler(chain...)
	}

	apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*kubeAPIs.ControlPlane.Generic, kubeAPIs.ControlPlane.VersionedInformers, pluginInitializer, opts.CompletedOptions, opts.MasterCount,
		serviceResolver, webhook.NewDefaultAuthenticationInfoResolverWrapper(kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Generic.EgressSelector, kubeAPIs.ControlPlane.Generic.LoopbackClientConfig, kubeAPIs.ControlPlane.Generic.TracerProvider))
	if err != nil {
		return nil, err
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
		chain := []admission.Interface{mut}
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
		chain := []admission.Interface{mut}
		if base != nil {
			chain = append(chain, base)
		}
		chain = append(chain, val)
		aggregator.GenericConfig.AdmissionControl = admission.NewChainHandler(chain...)
	}
	c.Aggregator = aggregator

	return c, nil
}
