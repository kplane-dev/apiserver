package webhook

import (
	"fmt"
	"reflect"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
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

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/scopedinformer"
)

type scopedFactory struct {
	clusterID       string
	clusterLabelKey string
	shared          informers.SharedInformerFactory
}

func newScopedFactory(clusterID, clusterLabelKey string, shared informers.SharedInformerFactory) informers.SharedInformerFactory {
	if clusterLabelKey == "" {
		clusterLabelKey = mc.DefaultClusterAnnotation
	}
	return &scopedFactory{
		clusterID:       clusterID,
		clusterLabelKey: clusterLabelKey,
		shared:          shared,
	}
}

func (f *scopedFactory) Start(stopCh <-chan struct{}) {
	_ = stopCh
}

func (f *scopedFactory) Shutdown() {
	// shared informers are owned by manager-level lifecycle.
}

func (f *scopedFactory) WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool {
	out := f.shared.WaitForCacheSync(stopCh)
	if out == nil {
		out = map[reflect.Type]bool{}
	}
	if f.shared != nil {
		out[reflect.TypeOf(&corev1.Namespace{})] = f.shared.Core().V1().Namespaces().Informer().HasSynced()
		out[reflect.TypeOf(&corev1.Service{})] = f.shared.Core().V1().Services().Informer().HasSynced()
		out[reflect.TypeOf(&discoveryv1.EndpointSlice{})] = f.shared.Discovery().V1().EndpointSlices().Informer().HasSynced()
		out[reflect.TypeOf(&admissionregistrationv1.MutatingWebhookConfiguration{})] = f.shared.Admissionregistration().V1().MutatingWebhookConfigurations().Informer().HasSynced()
		out[reflect.TypeOf(&admissionregistrationv1.ValidatingWebhookConfiguration{})] = f.shared.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer().HasSynced()
	}
	return out
}

func (f *scopedFactory) ForResource(resource schema.GroupVersionResource) (informers.GenericInformer, error) {
	return f.shared.ForResource(resource)
}

func (f *scopedFactory) InformerFor(obj runtime.Object, newFunc internalinformers.NewInformerFunc) cache.SharedIndexInformer {
	return f.shared.InformerFor(obj, newFunc)
}

func (f *scopedFactory) Core() coreinformers.Interface {
	return &scopedCoreGroup{f: f}
}

func (f *scopedFactory) Discovery() discoveryinformers.Interface {
	return &scopedDiscoveryGroup{f: f}
}

func (f *scopedFactory) Admissionregistration() admissionregistrationinformers.Interface {
	return &scopedAdmissionregistrationGroup{f: f}
}

func (f *scopedFactory) Internal() apiserverinternalinformers.Interface { return f.shared.Internal() }
func (f *scopedFactory) Apps() appsinformers.Interface                  { return f.shared.Apps() }
func (f *scopedFactory) Autoscaling() autoscalinginformers.Interface    { return f.shared.Autoscaling() }
func (f *scopedFactory) Batch() batchinformers.Interface                { return f.shared.Batch() }
func (f *scopedFactory) Certificates() certificatesinformers.Interface {
	return f.shared.Certificates()
}
func (f *scopedFactory) Coordination() coordinationinformers.Interface {
	return f.shared.Coordination()
}
func (f *scopedFactory) Events() eventsinformers.Interface           { return f.shared.Events() }
func (f *scopedFactory) Extensions() extensionsinformers.Interface   { return f.shared.Extensions() }
func (f *scopedFactory) Flowcontrol() flowcontrolinformers.Interface { return f.shared.Flowcontrol() }
func (f *scopedFactory) Networking() networkinginformers.Interface   { return f.shared.Networking() }
func (f *scopedFactory) Node() nodeinformers.Interface               { return f.shared.Node() }
func (f *scopedFactory) Policy() policyinformers.Interface           { return f.shared.Policy() }
func (f *scopedFactory) Rbac() rbacinformers.Interface               { return f.shared.Rbac() }
func (f *scopedFactory) Resource() resourceinformers.Interface       { return f.shared.Resource() }
func (f *scopedFactory) Scheduling() schedulinginformers.Interface   { return f.shared.Scheduling() }
func (f *scopedFactory) Storage() storageinformers.Interface         { return f.shared.Storage() }
func (f *scopedFactory) Storagemigration() storagemigrationinformers.Interface {
	return f.shared.Storagemigration()
}

type scopedCoreGroup struct{ f *scopedFactory }

func (g *scopedCoreGroup) V1() coreinformersv1.Interface { return &scopedCoreV1{f: g.f} }

type scopedCoreV1 struct{ f *scopedFactory }

func (v *scopedCoreV1) ComponentStatuses() coreinformersv1.ComponentStatusInformer {
	return v.f.shared.Core().V1().ComponentStatuses()
}
func (v *scopedCoreV1) ConfigMaps() coreinformersv1.ConfigMapInformer {
	return v.f.shared.Core().V1().ConfigMaps()
}
func (v *scopedCoreV1) Endpoints() coreinformersv1.EndpointsInformer {
	return v.f.shared.Core().V1().Endpoints()
}
func (v *scopedCoreV1) Events() coreinformersv1.EventInformer {
	return v.f.shared.Core().V1().Events()
}
func (v *scopedCoreV1) LimitRanges() coreinformersv1.LimitRangeInformer {
	return v.f.shared.Core().V1().LimitRanges()
}
func (v *scopedCoreV1) Namespaces() coreinformersv1.NamespaceInformer {
	base := v.f.shared.Core().V1().Namespaces().Informer()
	return &scopedNamespaceInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedNamespaceLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID},
	}
}
func (v *scopedCoreV1) Nodes() coreinformersv1.NodeInformer { return v.f.shared.Core().V1().Nodes() }
func (v *scopedCoreV1) PersistentVolumes() coreinformersv1.PersistentVolumeInformer {
	return v.f.shared.Core().V1().PersistentVolumes()
}
func (v *scopedCoreV1) PersistentVolumeClaims() coreinformersv1.PersistentVolumeClaimInformer {
	return v.f.shared.Core().V1().PersistentVolumeClaims()
}
func (v *scopedCoreV1) Pods() coreinformersv1.PodInformer { return v.f.shared.Core().V1().Pods() }
func (v *scopedCoreV1) PodTemplates() coreinformersv1.PodTemplateInformer {
	return v.f.shared.Core().V1().PodTemplates()
}
func (v *scopedCoreV1) ReplicationControllers() coreinformersv1.ReplicationControllerInformer {
	return v.f.shared.Core().V1().ReplicationControllers()
}
func (v *scopedCoreV1) ResourceQuotas() coreinformersv1.ResourceQuotaInformer {
	return v.f.shared.Core().V1().ResourceQuotas()
}
func (v *scopedCoreV1) Secrets() coreinformersv1.SecretInformer {
	return v.f.shared.Core().V1().Secrets()
}
func (v *scopedCoreV1) Services() coreinformersv1.ServiceInformer {
	base := v.f.shared.Core().V1().Services().Informer()
	return &scopedServiceInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedServiceLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID},
	}
}
func (v *scopedCoreV1) ServiceAccounts() coreinformersv1.ServiceAccountInformer {
	return v.f.shared.Core().V1().ServiceAccounts()
}

type scopedDiscoveryGroup struct{ f *scopedFactory }

func (g *scopedDiscoveryGroup) V1() discoveryinformersv1.Interface {
	return &scopedDiscoveryV1{f: g.f}
}
func (g *scopedDiscoveryGroup) V1beta1() discoveryinformersv1beta1.Interface {
	return g.f.shared.Discovery().V1beta1()
}

type scopedDiscoveryV1 struct{ f *scopedFactory }

func (v *scopedDiscoveryV1) EndpointSlices() discoveryinformersv1.EndpointSliceInformer {
	base := v.f.shared.Discovery().V1().EndpointSlices().Informer()
	return &scopedEndpointSliceInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedEndpointSliceLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID},
	}
}

type scopedAdmissionregistrationGroup struct{ f *scopedFactory }

func (g *scopedAdmissionregistrationGroup) V1() admissionregistrationinformersv1.Interface {
	return &scopedAdmissionregistrationV1{f: g.f}
}
func (g *scopedAdmissionregistrationGroup) V1alpha1() admissionregistrationinformersv1alpha1.Interface {
	return g.f.shared.Admissionregistration().V1alpha1()
}
func (g *scopedAdmissionregistrationGroup) V1beta1() admissionregistrationinformersv1beta1.Interface {
	return g.f.shared.Admissionregistration().V1beta1()
}

type scopedAdmissionregistrationV1 struct{ f *scopedFactory }

func (v *scopedAdmissionregistrationV1) MutatingWebhookConfigurations() admissionregistrationinformersv1.MutatingWebhookConfigurationInformer {
	base := v.f.shared.Admissionregistration().V1().MutatingWebhookConfigurations().Informer()
	return &scopedMutatingWebhookConfigurationInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedMutatingWebhookConfigurationLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID},
	}
}
func (v *scopedAdmissionregistrationV1) ValidatingWebhookConfigurations() admissionregistrationinformersv1.ValidatingWebhookConfigurationInformer {
	base := v.f.shared.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer()
	return &scopedValidatingWebhookConfigurationInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedValidatingWebhookConfigurationLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID},
	}
}
func (v *scopedAdmissionregistrationV1) ValidatingAdmissionPolicies() admissionregistrationinformersv1.ValidatingAdmissionPolicyInformer {
	return v.f.shared.Admissionregistration().V1().ValidatingAdmissionPolicies()
}
func (v *scopedAdmissionregistrationV1) ValidatingAdmissionPolicyBindings() admissionregistrationinformersv1.ValidatingAdmissionPolicyBindingInformer {
	return v.f.shared.Admissionregistration().V1().ValidatingAdmissionPolicyBindings()
}

func newFilteredSharedIndexInformer(shared cache.SharedIndexInformer, clusterID, clusterLabelKey string) cache.SharedIndexInformer {
	return scopedinformer.NewFilteredSharedIndexInformer(shared, clusterID, clusterLabelKey)
}

func objectCluster(obj interface{}, clusterLabelKey string) string {
	return scopedinformer.ObjectCluster(obj, clusterLabelKey)
}

func filteredByCluster(indexer cache.Indexer, clusterID string) []interface{} {
	return scopedinformer.FilteredByCluster(indexer, clusterID)
}

type scopedNamespaceInformer struct {
	informer cache.SharedIndexInformer
	lister   corelisters.NamespaceLister
}

func (i *scopedNamespaceInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedNamespaceInformer) Lister() corelisters.NamespaceLister { return i.lister }

type scopedServiceInformer struct {
	informer cache.SharedIndexInformer
	lister   corelisters.ServiceLister
}

func (i *scopedServiceInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedServiceInformer) Lister() corelisters.ServiceLister   { return i.lister }

type scopedEndpointSliceInformer struct {
	informer cache.SharedIndexInformer
	lister   discoverylisters.EndpointSliceLister
}

func (i *scopedEndpointSliceInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedEndpointSliceInformer) Lister() discoverylisters.EndpointSliceLister {
	return i.lister
}

type scopedMutatingWebhookConfigurationInformer struct {
	informer cache.SharedIndexInformer
	lister   admissionregistrationlisters.MutatingWebhookConfigurationLister
}

func (i *scopedMutatingWebhookConfigurationInformer) Informer() cache.SharedIndexInformer {
	return i.informer
}
func (i *scopedMutatingWebhookConfigurationInformer) Lister() admissionregistrationlisters.MutatingWebhookConfigurationLister {
	return i.lister
}

type scopedValidatingWebhookConfigurationInformer struct {
	informer cache.SharedIndexInformer
	lister   admissionregistrationlisters.ValidatingWebhookConfigurationLister
}

func (i *scopedValidatingWebhookConfigurationInformer) Informer() cache.SharedIndexInformer {
	return i.informer
}
func (i *scopedValidatingWebhookConfigurationInformer) Lister() admissionregistrationlisters.ValidatingWebhookConfigurationLister {
	return i.lister
}

type scopedNamespaceLister struct {
	indexer   cache.Indexer
	clusterID string
}

func (l *scopedNamespaceLister) List(sel labels.Selector) (ret []*corev1.Namespace, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Namespace)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *scopedNamespaceLister) Get(name string) (*corev1.Namespace, error) {
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Namespace)
		if !ok {
			continue
		}
		if obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("namespace %q not found", name)
}

type scopedServiceLister struct {
	indexer   cache.Indexer
	clusterID string
}

func (l *scopedServiceLister) List(sel labels.Selector) (ret []*corev1.Service, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Service)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *scopedServiceLister) Services(namespace string) corelisters.ServiceNamespaceLister {
	return &scopedServiceNamespaceLister{parent: l, namespace: namespace}
}

type scopedServiceNamespaceLister struct {
	parent    *scopedServiceLister
	namespace string
}

func (l *scopedServiceNamespaceLister) List(sel labels.Selector) (ret []*corev1.Service, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *scopedServiceNamespaceLister) Get(name string) (*corev1.Service, error) {
	all, _ := l.parent.List(labels.Everything())
	for _, obj := range all {
		if obj.Namespace == l.namespace && obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("service %s/%s not found", l.namespace, name)
}

type scopedEndpointSliceLister struct {
	indexer   cache.Indexer
	clusterID string
}

func (l *scopedEndpointSliceLister) List(sel labels.Selector) (ret []*discoveryv1.EndpointSlice, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*discoveryv1.EndpointSlice)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *scopedEndpointSliceLister) EndpointSlices(namespace string) discoverylisters.EndpointSliceNamespaceLister {
	return &scopedEndpointSliceNamespaceLister{parent: l, namespace: namespace}
}

type scopedEndpointSliceNamespaceLister struct {
	parent    *scopedEndpointSliceLister
	namespace string
}

func (l *scopedEndpointSliceNamespaceLister) List(sel labels.Selector) (ret []*discoveryv1.EndpointSlice, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *scopedEndpointSliceNamespaceLister) Get(name string) (*discoveryv1.EndpointSlice, error) {
	all, _ := l.parent.List(labels.Everything())
	for _, obj := range all {
		if obj.Namespace == l.namespace && obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("endpointslice %s/%s not found", l.namespace, name)
}

type scopedMutatingWebhookConfigurationLister struct {
	indexer   cache.Indexer
	clusterID string
}

func (l *scopedMutatingWebhookConfigurationLister) List(sel labels.Selector) (ret []*admissionregistrationv1.MutatingWebhookConfiguration, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*admissionregistrationv1.MutatingWebhookConfiguration)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *scopedMutatingWebhookConfigurationLister) Get(name string) (*admissionregistrationv1.MutatingWebhookConfiguration, error) {
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*admissionregistrationv1.MutatingWebhookConfiguration)
		if !ok {
			continue
		}
		if obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("mutatingwebhookconfiguration %q not found", name)
}

type scopedValidatingWebhookConfigurationLister struct {
	indexer   cache.Indexer
	clusterID string
}

func (l *scopedValidatingWebhookConfigurationLister) List(sel labels.Selector) (ret []*admissionregistrationv1.ValidatingWebhookConfiguration, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*admissionregistrationv1.ValidatingWebhookConfiguration)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *scopedValidatingWebhookConfigurationLister) Get(name string) (*admissionregistrationv1.ValidatingWebhookConfiguration, error) {
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*admissionregistrationv1.ValidatingWebhookConfiguration)
		if !ok {
			continue
		}
		if obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("validatingwebhookconfiguration %q not found", name)
}
