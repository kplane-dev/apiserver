package typedinformer

import (
	"context"
	"reflect"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	admissionregistrationinformers "k8s.io/client-go/informers/admissionregistration"
	admissionregistrationinformersv1 "k8s.io/client-go/informers/admissionregistration/v1"
	admissionregistrationinformersv1alpha1 "k8s.io/client-go/informers/admissionregistration/v1alpha1"
	admissionregistrationinformersv1beta1 "k8s.io/client-go/informers/admissionregistration/v1beta1"
	apiserverinternalinformers "k8s.io/client-go/informers/apiserverinternal"
	appsinformers "k8s.io/client-go/informers/apps"
	autoscalinginformers "k8s.io/client-go/informers/autoscaling"
	batchinformers "k8s.io/client-go/informers/batch"
	certificatesinformers "k8s.io/client-go/informers/certificates"
	coordinationinformers "k8s.io/client-go/informers/coordination"
	coreinformers "k8s.io/client-go/informers/core"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	discoveryinformers "k8s.io/client-go/informers/discovery"
	discoveryinformersv1 "k8s.io/client-go/informers/discovery/v1"
	discoveryinformersv1beta1 "k8s.io/client-go/informers/discovery/v1beta1"
	eventsinformers "k8s.io/client-go/informers/events"
	extensionsinformers "k8s.io/client-go/informers/extensions"
	flowcontrolinformers "k8s.io/client-go/informers/flowcontrol"
	internalinformers "k8s.io/client-go/informers/internalinterfaces"
	networkinginformers "k8s.io/client-go/informers/networking"
	nodeinformers "k8s.io/client-go/informers/node"
	policyinformers "k8s.io/client-go/informers/policy"
	rbacinformers "k8s.io/client-go/informers/rbac"
	resourceinformers "k8s.io/client-go/informers/resource"
	schedulinginformers "k8s.io/client-go/informers/scheduling"
	storageinformers "k8s.io/client-go/informers/storage"
	storagemigrationinformers "k8s.io/client-go/informers/storagemigration"
	admissionregistrationlisters "k8s.io/client-go/listers/admissionregistration/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"

	mcinformer "github.com/kplane-dev/informer"
)

// MCIFactoryConfig configures the SharedInformerFactory adapter backed by MCIs.
// Only populate the MCIs for resource types your consumers need.
type MCIFactoryConfig struct {
	ClusterID          string
	Namespaces         *mcinformer.MultiClusterInformer
	Services           *mcinformer.MultiClusterInformer
	EndpointSlices     *mcinformer.MultiClusterInformer
	MutatingWebhooks   *mcinformer.MultiClusterInformer
	ValidatingWebhooks *mcinformer.MultiClusterInformer
}

// NewMCIFactory creates a SharedInformerFactory backed by MultiClusterInformers.
// MCIs are already running; Start() and WaitForCacheSync() delegate to MCI HasSynced.
func NewMCIFactory(cfg MCIFactoryConfig) informers.SharedInformerFactory {
	return &mciFactory{cfg: cfg}
}

type mciFactory struct {
	cfg MCIFactoryConfig
}

func (f *mciFactory) Start(_ <-chan struct{}) {}
func (f *mciFactory) Shutdown()              {}

func (f *mciFactory) StartWithContext(_ context.Context) {}

func (f *mciFactory) InformerName() *cache.InformerName {
	return nil
}

func (f *mciFactory) WaitForCacheSync(_ <-chan struct{}) map[reflect.Type]bool {
	out := map[reflect.Type]bool{}
	if f.cfg.Namespaces != nil {
		out[reflect.TypeOf(&corev1.Namespace{})] = f.cfg.Namespaces.HasSynced()
	}
	if f.cfg.Services != nil {
		out[reflect.TypeOf(&corev1.Service{})] = f.cfg.Services.HasSynced()
	}
	if f.cfg.EndpointSlices != nil {
		out[reflect.TypeOf(&discoveryv1.EndpointSlice{})] = f.cfg.EndpointSlices.HasSynced()
	}
	if f.cfg.MutatingWebhooks != nil {
		out[reflect.TypeOf(&admissionregistrationv1.MutatingWebhookConfiguration{})] = f.cfg.MutatingWebhooks.HasSynced()
	}
	if f.cfg.ValidatingWebhooks != nil {
		out[reflect.TypeOf(&admissionregistrationv1.ValidatingWebhookConfiguration{})] = f.cfg.ValidatingWebhooks.HasSynced()
	}
	return out
}

func (f *mciFactory) WaitForCacheSyncWithContext(_ context.Context) cache.SyncResult {
	return cache.SyncResult{}
}

func (f *mciFactory) ForResource(_ schema.GroupVersionResource) (informers.GenericInformer, error) {
	return nil, nil
}

func (f *mciFactory) InformerFor(_ runtime.Object, _ internalinformers.NewInformerFunc) cache.SharedIndexInformer {
	return nil
}

// Group accessors — only Core, Discovery, and Admissionregistration are populated.

func (f *mciFactory) Core() coreinformers.Interface {
	return &mciCoreGroup{f: f}
}

func (f *mciFactory) Discovery() discoveryinformers.Interface {
	return &mciDiscoveryGroup{f: f}
}

func (f *mciFactory) Admissionregistration() admissionregistrationinformers.Interface {
	return &mciAdmissionregistrationGroup{f: f}
}

// Unsupported groups — return nil; will panic if called by consumer code.
func (f *mciFactory) Internal() apiserverinternalinformers.Interface    { return nil }
func (f *mciFactory) Apps() appsinformers.Interface                     { return nil }
func (f *mciFactory) Autoscaling() autoscalinginformers.Interface       { return nil }
func (f *mciFactory) Batch() batchinformers.Interface                   { return nil }
func (f *mciFactory) Certificates() certificatesinformers.Interface     { return nil }
func (f *mciFactory) Coordination() coordinationinformers.Interface     { return nil }
func (f *mciFactory) Events() eventsinformers.Interface                 { return nil }
func (f *mciFactory) Extensions() extensionsinformers.Interface         { return nil }
func (f *mciFactory) Flowcontrol() flowcontrolinformers.Interface       { return nil }
func (f *mciFactory) Networking() networkinginformers.Interface         { return nil }
func (f *mciFactory) Node() nodeinformers.Interface                     { return nil }
func (f *mciFactory) Policy() policyinformers.Interface                 { return nil }
func (f *mciFactory) Rbac() rbacinformers.Interface                     { return nil }
func (f *mciFactory) Resource() resourceinformers.Interface             { return nil }
func (f *mciFactory) Scheduling() schedulinginformers.Interface         { return nil }
func (f *mciFactory) Storage() storageinformers.Interface               { return nil }
func (f *mciFactory) Storagemigration() storagemigrationinformers.Interface { return nil }

// --- Core group ---

type mciCoreGroup struct{ f *mciFactory }

func (g *mciCoreGroup) V1() coreinformersv1.Interface {
	return &mciCoreV1{f: g.f}
}

type mciCoreV1 struct{ f *mciFactory }

func (v *mciCoreV1) Namespaces() coreinformersv1.NamespaceInformer {
	if v.f.cfg.Namespaces == nil {
		return nil
	}
	return &mciTypedInformer[corelisters.NamespaceLister]{
		mci:       v.f.cfg.Namespaces,
		clusterID: v.f.cfg.ClusterID,
		newLister: NewNamespaceLister,
	}
}

func (v *mciCoreV1) Services() coreinformersv1.ServiceInformer {
	if v.f.cfg.Services == nil {
		return nil
	}
	return &mciTypedInformer[corelisters.ServiceLister]{
		mci:       v.f.cfg.Services,
		clusterID: v.f.cfg.ClusterID,
		newLister: NewServiceLister,
	}
}

// Unsupported Core V1 informers — return nil (not called by webhook/namespace plugins).
func (v *mciCoreV1) ComponentStatuses() coreinformersv1.ComponentStatusInformer { return nil }
func (v *mciCoreV1) ConfigMaps() coreinformersv1.ConfigMapInformer             { return nil }
func (v *mciCoreV1) Endpoints() coreinformersv1.EndpointsInformer              { return nil }
func (v *mciCoreV1) Events() coreinformersv1.EventInformer                     { return nil }
func (v *mciCoreV1) LimitRanges() coreinformersv1.LimitRangeInformer           { return nil }
func (v *mciCoreV1) Nodes() coreinformersv1.NodeInformer                       { return nil }
func (v *mciCoreV1) PersistentVolumes() coreinformersv1.PersistentVolumeInformer {
	return nil
}
func (v *mciCoreV1) PersistentVolumeClaims() coreinformersv1.PersistentVolumeClaimInformer {
	return nil
}
func (v *mciCoreV1) Pods() coreinformersv1.PodInformer                                       { return nil }
func (v *mciCoreV1) PodTemplates() coreinformersv1.PodTemplateInformer                       { return nil }
func (v *mciCoreV1) ReplicationControllers() coreinformersv1.ReplicationControllerInformer    { return nil }
func (v *mciCoreV1) ResourceQuotas() coreinformersv1.ResourceQuotaInformer                   { return nil }
func (v *mciCoreV1) Secrets() coreinformersv1.SecretInformer                                  { return nil }
func (v *mciCoreV1) ServiceAccounts() coreinformersv1.ServiceAccountInformer                  { return nil }

// --- Discovery group ---

type mciDiscoveryGroup struct{ f *mciFactory }

func (g *mciDiscoveryGroup) V1() discoveryinformersv1.Interface {
	return &mciDiscoveryV1{f: g.f}
}

func (g *mciDiscoveryGroup) V1beta1() discoveryinformersv1beta1.Interface {
	return nil
}

type mciDiscoveryV1 struct{ f *mciFactory }

func (v *mciDiscoveryV1) EndpointSlices() discoveryinformersv1.EndpointSliceInformer {
	if v.f.cfg.EndpointSlices == nil {
		return nil
	}
	return &mciTypedInformer[discoverylisters.EndpointSliceLister]{
		mci:       v.f.cfg.EndpointSlices,
		clusterID: v.f.cfg.ClusterID,
		newLister: NewEndpointSliceLister,
	}
}

// --- Admissionregistration group ---

type mciAdmissionregistrationGroup struct{ f *mciFactory }

func (g *mciAdmissionregistrationGroup) V1() admissionregistrationinformersv1.Interface {
	return &mciAdmissionregistrationV1{f: g.f}
}

func (g *mciAdmissionregistrationGroup) V1alpha1() admissionregistrationinformersv1alpha1.Interface {
	return nil
}

func (g *mciAdmissionregistrationGroup) V1beta1() admissionregistrationinformersv1beta1.Interface {
	return nil
}

type mciAdmissionregistrationV1 struct{ f *mciFactory }

func (v *mciAdmissionregistrationV1) MutatingWebhookConfigurations() admissionregistrationinformersv1.MutatingWebhookConfigurationInformer {
	if v.f.cfg.MutatingWebhooks == nil {
		return nil
	}
	return &mciTypedInformer[admissionregistrationlisters.MutatingWebhookConfigurationLister]{
		mci:       v.f.cfg.MutatingWebhooks,
		clusterID: v.f.cfg.ClusterID,
		newLister: NewMutatingWebhookConfigurationLister,
	}
}

func (v *mciAdmissionregistrationV1) ValidatingWebhookConfigurations() admissionregistrationinformersv1.ValidatingWebhookConfigurationInformer {
	if v.f.cfg.ValidatingWebhooks == nil {
		return nil
	}
	return &mciTypedInformer[admissionregistrationlisters.ValidatingWebhookConfigurationLister]{
		mci:       v.f.cfg.ValidatingWebhooks,
		clusterID: v.f.cfg.ClusterID,
		newLister: NewValidatingWebhookConfigurationLister,
	}
}

func (v *mciAdmissionregistrationV1) ValidatingAdmissionPolicies() admissionregistrationinformersv1.ValidatingAdmissionPolicyInformer {
	return nil
}

func (v *mciAdmissionregistrationV1) ValidatingAdmissionPolicyBindings() admissionregistrationinformersv1.ValidatingAdmissionPolicyBindingInformer {
	return nil
}

func (v *mciAdmissionregistrationV1) MutatingAdmissionPolicies() admissionregistrationinformersv1.MutatingAdmissionPolicyInformer {
	return nil
}

func (v *mciAdmissionregistrationV1) MutatingAdmissionPolicyBindings() admissionregistrationinformersv1.MutatingAdmissionPolicyBindingInformer {
	return nil
}

// --- Generic typed informer adapter ---

// mciTypedInformer satisfies typed informer interfaces (e.g., coreinformersv1.NamespaceInformer)
// by delegating Informer() to MCI.ForCluster() and Lister() to a typed lister constructor.
type mciTypedInformer[L any] struct {
	mci       *mcinformer.MultiClusterInformer
	clusterID string
	newLister func(*mcinformer.MultiClusterInformer, string) L
}

func (a *mciTypedInformer[L]) Informer() cache.SharedIndexInformer {
	return a.mci.ForCluster(a.clusterID)
}

func (a *mciTypedInformer[L]) Lister() L {
	return a.newLister(a.mci, a.clusterID)
}

var _ informers.SharedInformerFactory = (*mciFactory)(nil)
