package typedinformer

import (
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	admissionregistrationlisters "k8s.io/client-go/listers/admissionregistration/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api/legacyscheme"

	"github.com/kplane-dev/informer"
)

// convert attempts a direct type assertion, falling back to scheme conversion
// for internal→versioned type conversion since the cacher stores internal types.
func convert[T any, PT interface {
	*T
	runtime.Object
}](obj runtime.Object) (PT, error) {
	if typed, ok := obj.(PT); ok {
		return typed, nil
	}
	out := PT(new(T))
	if err := legacyscheme.Scheme.Convert(obj, out, nil); err != nil {
		return nil, fmt.Errorf("cannot convert %T: %w", obj, err)
	}
	return out, nil
}

// --- Namespace ---

type namespaceLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewNamespaceLister(mci *informer.MultiClusterInformer, cluster string) corelisters.NamespaceLister {
	return &namespaceLister{mci: mci, cluster: cluster}
}

func (l *namespaceLister) List(sel labels.Selector) (ret []*corev1.Namespace, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		ns, err := convert[corev1.Namespace](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(ns.Labels)) {
			ret = append(ret, ns)
		}
	}
	return ret, nil
}

func (l *namespaceLister) Get(name string) (*corev1.Namespace, error) {
	obj, ok := l.mci.Get(l.cluster, "", name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, name)
	}
	return convert[corev1.Namespace](obj)
}

type namespaceInformerAdapter struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewNamespaceInformer(mci *informer.MultiClusterInformer, cluster string) NamespaceInformer {
	return &namespaceInformerAdapter{mci: mci, cluster: cluster}
}

type NamespaceInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() corelisters.NamespaceLister
}

func (a *namespaceInformerAdapter) Informer() cache.SharedIndexInformer {
	return a.mci.ForCluster(a.cluster)
}

func (a *namespaceInformerAdapter) Lister() corelisters.NamespaceLister {
	return NewNamespaceLister(a.mci, a.cluster)
}

// --- Secret ---

type secretLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewSecretLister(mci *informer.MultiClusterInformer, cluster string) corelisters.SecretLister {
	return &secretLister{mci: mci, cluster: cluster}
}

func (l *secretLister) List(sel labels.Selector) (ret []*corev1.Secret, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		s, err := convert[corev1.Secret](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(s.Labels)) {
			ret = append(ret, s)
		}
	}
	return ret, nil
}

func (l *secretLister) Secrets(ns string) corelisters.SecretNamespaceLister {
	return &secretNamespaceLister{parent: l, namespace: ns}
}

type secretNamespaceLister struct {
	parent    *secretLister
	namespace string
}

func (l *secretNamespaceLister) List(sel labels.Selector) (ret []*corev1.Secret, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *secretNamespaceLister) Get(name string) (*corev1.Secret, error) {
	obj, ok := l.parent.mci.Get(l.parent.cluster, l.namespace, name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	return convert[corev1.Secret](obj)
}

// --- ServiceAccount ---

type serviceAccountLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewServiceAccountLister(mci *informer.MultiClusterInformer, cluster string) corelisters.ServiceAccountLister {
	return &serviceAccountLister{mci: mci, cluster: cluster}
}

func (l *serviceAccountLister) List(sel labels.Selector) (ret []*corev1.ServiceAccount, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		sa, err := convert[corev1.ServiceAccount](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(sa.Labels)) {
			ret = append(ret, sa)
		}
	}
	return ret, nil
}

func (l *serviceAccountLister) ServiceAccounts(ns string) corelisters.ServiceAccountNamespaceLister {
	return &serviceAccountNamespaceLister{parent: l, namespace: ns}
}

type serviceAccountNamespaceLister struct {
	parent    *serviceAccountLister
	namespace string
}

func (l *serviceAccountNamespaceLister) List(sel labels.Selector) (ret []*corev1.ServiceAccount, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *serviceAccountNamespaceLister) Get(name string) (*corev1.ServiceAccount, error) {
	obj, ok := l.parent.mci.Get(l.parent.cluster, l.namespace, name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "serviceaccounts"}, name)
	}
	return convert[corev1.ServiceAccount](obj)
}

// --- Pod ---

type podLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewPodLister(mci *informer.MultiClusterInformer, cluster string) corelisters.PodLister {
	return &podLister{mci: mci, cluster: cluster}
}

func (l *podLister) List(sel labels.Selector) (ret []*corev1.Pod, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		p, err := convert[corev1.Pod](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(p.Labels)) {
			ret = append(ret, p)
		}
	}
	return ret, nil
}

func (l *podLister) Pods(ns string) corelisters.PodNamespaceLister {
	return &podNamespaceLister{parent: l, namespace: ns}
}

type podNamespaceLister struct {
	parent    *podLister
	namespace string
}

func (l *podNamespaceLister) List(sel labels.Selector) (ret []*corev1.Pod, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *podNamespaceLister) Get(name string) (*corev1.Pod, error) {
	obj, ok := l.parent.mci.Get(l.parent.cluster, l.namespace, name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
	}
	return convert[corev1.Pod](obj)
}

// --- Node ---

type nodeLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewNodeLister(mci *informer.MultiClusterInformer, cluster string) corelisters.NodeLister {
	return &nodeLister{mci: mci, cluster: cluster}
}

func (l *nodeLister) List(sel labels.Selector) (ret []*corev1.Node, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		n, err := convert[corev1.Node](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(n.Labels)) {
			ret = append(ret, n)
		}
	}
	return ret, nil
}

func (l *nodeLister) Get(name string) (*corev1.Node, error) {
	obj, ok := l.mci.Get(l.cluster, "", name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, name)
	}
	return convert[corev1.Node](obj)
}

// --- Service ---

type serviceLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewServiceLister(mci *informer.MultiClusterInformer, cluster string) corelisters.ServiceLister {
	return &serviceLister{mci: mci, cluster: cluster}
}

func (l *serviceLister) List(sel labels.Selector) (ret []*corev1.Service, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		svc, err := convert[corev1.Service](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(svc.Labels)) {
			ret = append(ret, svc)
		}
	}
	return ret, nil
}

func (l *serviceLister) Services(ns string) corelisters.ServiceNamespaceLister {
	return &serviceNamespaceLister{parent: l, namespace: ns}
}

type serviceNamespaceLister struct {
	parent    *serviceLister
	namespace string
}

func (l *serviceNamespaceLister) List(sel labels.Selector) (ret []*corev1.Service, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *serviceNamespaceLister) Get(name string) (*corev1.Service, error) {
	obj, ok := l.parent.mci.Get(l.parent.cluster, l.namespace, name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "services"}, name)
	}
	return convert[corev1.Service](obj)
}

// --- EndpointSlice ---

type endpointSliceLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewEndpointSliceLister(mci *informer.MultiClusterInformer, cluster string) discoverylisters.EndpointSliceLister {
	return &endpointSliceLister{mci: mci, cluster: cluster}
}

func (l *endpointSliceLister) List(sel labels.Selector) (ret []*discoveryv1.EndpointSlice, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		ep, err := convert[discoveryv1.EndpointSlice](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(ep.Labels)) {
			ret = append(ret, ep)
		}
	}
	return ret, nil
}

func (l *endpointSliceLister) EndpointSlices(ns string) discoverylisters.EndpointSliceNamespaceLister {
	return &endpointSliceNamespaceLister{parent: l, namespace: ns}
}

type endpointSliceNamespaceLister struct {
	parent    *endpointSliceLister
	namespace string
}

func (l *endpointSliceNamespaceLister) List(sel labels.Selector) (ret []*discoveryv1.EndpointSlice, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}

func (l *endpointSliceNamespaceLister) Get(name string) (*discoveryv1.EndpointSlice, error) {
	obj, ok := l.parent.mci.Get(l.parent.cluster, l.namespace, name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "discovery.k8s.io", Resource: "endpointslices"}, name)
	}
	return convert[discoveryv1.EndpointSlice](obj)
}

// --- MutatingWebhookConfiguration ---

type mutatingWebhookConfigurationLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewMutatingWebhookConfigurationLister(mci *informer.MultiClusterInformer, cluster string) admissionregistrationlisters.MutatingWebhookConfigurationLister {
	return &mutatingWebhookConfigurationLister{mci: mci, cluster: cluster}
}

func (l *mutatingWebhookConfigurationLister) List(sel labels.Selector) (ret []*admissionregistrationv1.MutatingWebhookConfiguration, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		mwc, err := convert[admissionregistrationv1.MutatingWebhookConfiguration](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(mwc.Labels)) {
			ret = append(ret, mwc)
		}
	}
	return ret, nil
}

func (l *mutatingWebhookConfigurationLister) Get(name string) (*admissionregistrationv1.MutatingWebhookConfiguration, error) {
	obj, ok := l.mci.Get(l.cluster, "", name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "mutatingwebhookconfigurations"}, name)
	}
	return convert[admissionregistrationv1.MutatingWebhookConfiguration](obj)
}

// --- ValidatingWebhookConfiguration ---

type validatingWebhookConfigurationLister struct {
	mci     *informer.MultiClusterInformer
	cluster string
}

func NewValidatingWebhookConfigurationLister(mci *informer.MultiClusterInformer, cluster string) admissionregistrationlisters.ValidatingWebhookConfigurationLister {
	return &validatingWebhookConfigurationLister{mci: mci, cluster: cluster}
}

func (l *validatingWebhookConfigurationLister) List(sel labels.Selector) (ret []*admissionregistrationv1.ValidatingWebhookConfiguration, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.mci.List(l.cluster) {
		vwc, err := convert[admissionregistrationv1.ValidatingWebhookConfiguration](obj)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(vwc.Labels)) {
			ret = append(ret, vwc)
		}
	}
	return ret, nil
}

func (l *validatingWebhookConfigurationLister) Get(name string) (*admissionregistrationv1.ValidatingWebhookConfiguration, error) {
	obj, ok := l.mci.Get(l.cluster, "", name)
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "validatingwebhookconfigurations"}, name)
	}
	return convert[admissionregistrationv1.ValidatingWebhookConfiguration](obj)
}

// --- Lister interface assertions ---

var (
	_ corelisters.NamespaceLister      = (*namespaceLister)(nil)
	_ corelisters.SecretLister         = (*secretLister)(nil)
	_ corelisters.ServiceAccountLister = (*serviceAccountLister)(nil)
	_ corelisters.PodLister            = (*podLister)(nil)
	_ corelisters.NodeLister           = (*nodeLister)(nil)
	_ corelisters.ServiceLister        = (*serviceLister)(nil)

	_ discoverylisters.EndpointSliceLister = (*endpointSliceLister)(nil)

	_ admissionregistrationlisters.MutatingWebhookConfigurationLister  = (*mutatingWebhookConfigurationLister)(nil)
	_ admissionregistrationlisters.ValidatingWebhookConfigurationLister = (*validatingWebhookConfigurationLister)(nil)
)
