package namespace

import (
	"fmt"
	"reflect"
	"strings"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/scopedinformer"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const sharedNamespaceNamePrefix = "__mcns__"

type scopedFactory struct {
	informers.SharedInformerFactory
	clusterID       string
	clusterLabelKey string
	shared          informers.SharedInformerFactory
}

func newScopedFactory(clusterID, clusterLabelKey string, shared informers.SharedInformerFactory) informers.SharedInformerFactory {
	if clusterLabelKey == "" {
		clusterLabelKey = mc.DefaultClusterAnnotation
	}
	return &scopedFactory{
		SharedInformerFactory: shared,
		clusterID:             clusterID,
		clusterLabelKey:       clusterLabelKey,
		shared:                shared,
	}
}

func (f *scopedFactory) Core() coreinformers.Interface { return &scopedCoreGroup{f: f} }

func (f *scopedFactory) WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool {
	out := f.SharedInformerFactory.WaitForCacheSync(stopCh)
	if out == nil {
		out = map[reflect.Type]bool{}
	}
	out[reflect.TypeOf(&corev1.Namespace{})] = f.shared.Core().V1().Namespaces().Informer().HasSynced()
	return out
}

type scopedCoreGroup struct{ f *scopedFactory }

func (g *scopedCoreGroup) V1() coreinformersv1.Interface { return &scopedCoreV1{f: g.f} }

type scopedCoreV1 struct{ f *scopedFactory }

func (v *scopedCoreV1) ComponentStatuses() coreinformersv1.ComponentStatusInformer {
	return v.f.SharedInformerFactory.Core().V1().ComponentStatuses()
}
func (v *scopedCoreV1) ConfigMaps() coreinformersv1.ConfigMapInformer {
	return v.f.SharedInformerFactory.Core().V1().ConfigMaps()
}
func (v *scopedCoreV1) Endpoints() coreinformersv1.EndpointsInformer {
	return v.f.SharedInformerFactory.Core().V1().Endpoints()
}
func (v *scopedCoreV1) Events() coreinformersv1.EventInformer {
	return v.f.SharedInformerFactory.Core().V1().Events()
}
func (v *scopedCoreV1) LimitRanges() coreinformersv1.LimitRangeInformer {
	return v.f.SharedInformerFactory.Core().V1().LimitRanges()
}
func (v *scopedCoreV1) Namespaces() coreinformersv1.NamespaceInformer {
	base := v.f.shared.Core().V1().Namespaces().Informer()
	return &scopedNamespaceInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedNamespaceLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID},
	}
}
func (v *scopedCoreV1) Nodes() coreinformersv1.NodeInformer {
	return v.f.SharedInformerFactory.Core().V1().Nodes()
}
func (v *scopedCoreV1) PersistentVolumes() coreinformersv1.PersistentVolumeInformer {
	return v.f.SharedInformerFactory.Core().V1().PersistentVolumes()
}
func (v *scopedCoreV1) PersistentVolumeClaims() coreinformersv1.PersistentVolumeClaimInformer {
	return v.f.SharedInformerFactory.Core().V1().PersistentVolumeClaims()
}
func (v *scopedCoreV1) Pods() coreinformersv1.PodInformer {
	return v.f.SharedInformerFactory.Core().V1().Pods()
}
func (v *scopedCoreV1) PodTemplates() coreinformersv1.PodTemplateInformer {
	return v.f.SharedInformerFactory.Core().V1().PodTemplates()
}
func (v *scopedCoreV1) ReplicationControllers() coreinformersv1.ReplicationControllerInformer {
	return v.f.SharedInformerFactory.Core().V1().ReplicationControllers()
}
func (v *scopedCoreV1) ResourceQuotas() coreinformersv1.ResourceQuotaInformer {
	return v.f.SharedInformerFactory.Core().V1().ResourceQuotas()
}
func (v *scopedCoreV1) Secrets() coreinformersv1.SecretInformer {
	return v.f.SharedInformerFactory.Core().V1().Secrets()
}
func (v *scopedCoreV1) Services() coreinformersv1.ServiceInformer {
	return v.f.SharedInformerFactory.Core().V1().Services()
}
func (v *scopedCoreV1) ServiceAccounts() coreinformersv1.ServiceAccountInformer {
	return v.f.SharedInformerFactory.Core().V1().ServiceAccounts()
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

func transformNamespaceForShared(clusterLabelKey string) cache.TransformFunc {
	return func(obj interface{}) (interface{}, error) {
		cid := objectCluster(obj, clusterLabelKey)
		if cid == "" {
			return obj, nil
		}
		ns, ok := obj.(*corev1.Namespace)
		if !ok {
			return obj, nil
		}
		cp := ns.DeepCopy()
		cp.Name = encodeSharedNamespaceName(cid, cp.Name)
		return cp, nil
	}
}

func encodeSharedNamespaceName(clusterID, name string) string {
	if clusterID == "" || name == "" {
		return name
	}
	prefix := sharedNamespaceNamePrefix + clusterID + "__"
	if strings.HasPrefix(name, prefix) {
		return name
	}
	return prefix + name
}

func decodeSharedNamespaceName(clusterID, name string) (string, bool) {
	prefix := sharedNamespaceNamePrefix + clusterID + "__"
	if strings.HasPrefix(name, prefix) {
		return strings.TrimPrefix(name, prefix), true
	}
	return name, false
}

type scopedNamespaceInformer struct {
	informer cache.SharedIndexInformer
	lister   corelisters.NamespaceLister
}

func (i *scopedNamespaceInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedNamespaceInformer) Lister() corelisters.NamespaceLister { return i.lister }

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
			cp := obj.DeepCopy()
			cp.Name, _ = decodeSharedNamespaceName(l.clusterID, cp.Name)
			ret = append(ret, cp)
		}
	}
	return ret, nil
}

func (l *scopedNamespaceLister) Get(name string) (*corev1.Namespace, error) {
	encoded := encodeSharedNamespaceName(l.clusterID, name)
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Namespace)
		if !ok {
			continue
		}
		if obj.Name == name || obj.Name == encoded {
			cp := obj.DeepCopy()
			cp.Name, _ = decodeSharedNamespaceName(l.clusterID, cp.Name)
			return cp, nil
		}
	}
	return nil, fmt.Errorf("namespace %q not found", name)
}

var _ corelisters.NamespaceLister = (*scopedNamespaceLister)(nil)
var _ coreinformersv1.NamespaceInformer = (*scopedNamespaceInformer)(nil)
var _ informers.SharedInformerFactory = (*scopedFactory)(nil)
