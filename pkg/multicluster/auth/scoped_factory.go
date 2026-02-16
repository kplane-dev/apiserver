package auth

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	admissionregistrationinformers "k8s.io/client-go/informers/admissionregistration"
	apiserverinternalinformers "k8s.io/client-go/informers/apiserverinternal"
	appsinformers "k8s.io/client-go/informers/apps"
	autoscalinginformers "k8s.io/client-go/informers/autoscaling"
	batchinformers "k8s.io/client-go/informers/batch"
	certificatesinformers "k8s.io/client-go/informers/certificates"
	coordinationinformers "k8s.io/client-go/informers/coordination"
	coreinformers "k8s.io/client-go/informers/core"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	discoveryinformers "k8s.io/client-go/informers/discovery"
	eventsinformers "k8s.io/client-go/informers/events"
	extensionsinformers "k8s.io/client-go/informers/extensions"
	flowcontrolinformers "k8s.io/client-go/informers/flowcontrol"
	internalinformers "k8s.io/client-go/informers/internalinterfaces"
	networkinginformers "k8s.io/client-go/informers/networking"
	nodeinformers "k8s.io/client-go/informers/node"
	policyinformers "k8s.io/client-go/informers/policy"
	rbacinformers "k8s.io/client-go/informers/rbac"
	rbacinformersv1 "k8s.io/client-go/informers/rbac/v1"
	rbacinformersv1alpha1 "k8s.io/client-go/informers/rbac/v1alpha1"
	rbacinformersv1beta1 "k8s.io/client-go/informers/rbac/v1beta1"
	resourceinformers "k8s.io/client-go/informers/resource"
	schedulinginformers "k8s.io/client-go/informers/scheduling"
	storageinformers "k8s.io/client-go/informers/storage"
	storagemigrationinformers "k8s.io/client-go/informers/storagemigration"
	corelisters "k8s.io/client-go/listers/core/v1"
	rbaclisters "k8s.io/client-go/listers/rbac/v1"
	"k8s.io/client-go/tools/cache"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"github.com/kplane-dev/apiserver/pkg/multicluster/scopedinformer"
)

type scopedFactory struct {
	clusterID       string
	clusterLabelKey string
	shared          informers.SharedInformerFactory
	rbacStore       *rbacProjectionStore
}

func newScopedFactory(clusterID, clusterLabelKey string, shared informers.SharedInformerFactory, rbacStore *rbacProjectionStore) informers.SharedInformerFactory {
	if clusterLabelKey == "" {
		clusterLabelKey = mc.DefaultClusterAnnotation
	}
	if rbacStore == nil {
		rbacStore = newRBACProjectionStore(clusterLabelKey)
	}
	return &scopedFactory{
		clusterID:       clusterID,
		clusterLabelKey: clusterLabelKey,
		shared:          shared,
		rbacStore:       rbacStore,
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
	// shared informers are started/synced by manager startup; include scoped essentials.
	if f.shared != nil {
		out[reflect.TypeOf(&corev1.Secret{})] = f.shared.Core().V1().Secrets().Informer().HasSynced()
		out[reflect.TypeOf(&corev1.ServiceAccount{})] = f.shared.Core().V1().ServiceAccounts().Informer().HasSynced()
		out[reflect.TypeOf(&corev1.Pod{})] = f.shared.Core().V1().Pods().Informer().HasSynced()
		out[reflect.TypeOf(&corev1.Node{})] = f.shared.Core().V1().Nodes().Informer().HasSynced()
		out[reflect.TypeOf(&rbacv1.Role{})] = f.shared.Rbac().V1().Roles().Informer().HasSynced()
		out[reflect.TypeOf(&rbacv1.RoleBinding{})] = f.shared.Rbac().V1().RoleBindings().Informer().HasSynced()
		out[reflect.TypeOf(&rbacv1.ClusterRole{})] = f.shared.Rbac().V1().ClusterRoles().Informer().HasSynced()
		out[reflect.TypeOf(&rbacv1.ClusterRoleBinding{})] = f.shared.Rbac().V1().ClusterRoleBindings().Informer().HasSynced()
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

func (f *scopedFactory) Rbac() rbacinformers.Interface {
	return &scopedRbacGroup{f: f}
}

func (f *scopedFactory) Admissionregistration() admissionregistrationinformers.Interface {
	return f.shared.Admissionregistration()
}
func (f *scopedFactory) Internal() apiserverinternalinformers.Interface { return f.shared.Internal() }
func (f *scopedFactory) Apps() appsinformers.Interface                  { return f.shared.Apps() }
func (f *scopedFactory) Autoscaling() autoscalinginformers.Interface {
	return f.shared.Autoscaling()
}
func (f *scopedFactory) Batch() batchinformers.Interface { return f.shared.Batch() }
func (f *scopedFactory) Certificates() certificatesinformers.Interface {
	return f.shared.Certificates()
}
func (f *scopedFactory) Coordination() coordinationinformers.Interface {
	return f.shared.Coordination()
}
func (f *scopedFactory) Discovery() discoveryinformers.Interface { return f.shared.Discovery() }
func (f *scopedFactory) Events() eventsinformers.Interface       { return f.shared.Events() }
func (f *scopedFactory) Extensions() extensionsinformers.Interface {
	return f.shared.Extensions()
}
func (f *scopedFactory) Flowcontrol() flowcontrolinformers.Interface {
	return f.shared.Flowcontrol()
}
func (f *scopedFactory) Networking() networkinginformers.Interface {
	return f.shared.Networking()
}
func (f *scopedFactory) Node() nodeinformers.Interface     { return f.shared.Node() }
func (f *scopedFactory) Policy() policyinformers.Interface { return f.shared.Policy() }
func (f *scopedFactory) Resource() resourceinformers.Interface {
	return f.shared.Resource()
}
func (f *scopedFactory) Scheduling() schedulinginformers.Interface {
	return f.shared.Scheduling()
}
func (f *scopedFactory) Storage() storageinformers.Interface {
	return f.shared.Storage()
}
func (f *scopedFactory) Storagemigration() storagemigrationinformers.Interface {
	return f.shared.Storagemigration()
}

type scopedCoreGroup struct{ f *scopedFactory }

func (g *scopedCoreGroup) V1() coreinformersv1.Interface {
	return &scopedCoreV1{f: g.f}
}

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
	return v.f.shared.Core().V1().Namespaces()
}
func (v *scopedCoreV1) Nodes() coreinformersv1.NodeInformer {
	base := v.f.shared.Core().V1().Nodes().Informer()
	return &scopedNodeInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedNodeLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID, clusterLabelKey: v.f.clusterLabelKey},
	}
}
func (v *scopedCoreV1) PersistentVolumes() coreinformersv1.PersistentVolumeInformer {
	return v.f.shared.Core().V1().PersistentVolumes()
}
func (v *scopedCoreV1) PersistentVolumeClaims() coreinformersv1.PersistentVolumeClaimInformer {
	return v.f.shared.Core().V1().PersistentVolumeClaims()
}
func (v *scopedCoreV1) Pods() coreinformersv1.PodInformer {
	base := v.f.shared.Core().V1().Pods().Informer()
	return &scopedPodInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedPodLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID, clusterLabelKey: v.f.clusterLabelKey},
	}
}
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
	base := v.f.shared.Core().V1().Secrets().Informer()
	return &scopedSecretInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedSecretLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID, clusterLabelKey: v.f.clusterLabelKey},
	}
}
func (v *scopedCoreV1) Services() coreinformersv1.ServiceInformer {
	return v.f.shared.Core().V1().Services()
}
func (v *scopedCoreV1) ServiceAccounts() coreinformersv1.ServiceAccountInformer {
	base := v.f.shared.Core().V1().ServiceAccounts().Informer()
	return &scopedServiceAccountInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedServiceAccountLister{indexer: base.GetIndexer(), clusterID: v.f.clusterID, clusterLabelKey: v.f.clusterLabelKey},
	}
}

type scopedRbacGroup struct{ f *scopedFactory }

func (g *scopedRbacGroup) V1() rbacinformersv1.Interface {
	return &scopedRbacV1{f: g.f}
}
func (g *scopedRbacGroup) V1alpha1() rbacinformersv1alpha1.Interface {
	return g.f.shared.Rbac().V1alpha1()
}
func (g *scopedRbacGroup) V1beta1() rbacinformersv1beta1.Interface {
	return g.f.shared.Rbac().V1beta1()
}

type scopedRbacV1 struct{ f *scopedFactory }

func (v *scopedRbacV1) ClusterRoleBindings() rbacinformersv1.ClusterRoleBindingInformer {
	base := v.f.shared.Rbac().V1().ClusterRoleBindings().Informer()
	return &scopedClusterRoleBindingInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedClusterRoleBindingLister{store: v.f.rbacStore, clusterID: v.f.clusterID},
	}
}
func (v *scopedRbacV1) ClusterRoles() rbacinformersv1.ClusterRoleInformer {
	base := v.f.shared.Rbac().V1().ClusterRoles().Informer()
	return &scopedClusterRoleInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedClusterRoleLister{store: v.f.rbacStore, clusterID: v.f.clusterID},
	}
}
func (v *scopedRbacV1) RoleBindings() rbacinformersv1.RoleBindingInformer {
	base := v.f.shared.Rbac().V1().RoleBindings().Informer()
	return &scopedRoleBindingInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedRoleBindingLister{store: v.f.rbacStore, clusterID: v.f.clusterID},
	}
}
func (v *scopedRbacV1) Roles() rbacinformersv1.RoleInformer {
	base := v.f.shared.Rbac().V1().Roles().Informer()
	return &scopedRoleInformer{
		informer: newFilteredSharedIndexInformer(base, v.f.clusterID, v.f.clusterLabelKey),
		lister:   &scopedRoleLister{store: v.f.rbacStore, clusterID: v.f.clusterID},
	}
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

type rbacProjectionStore struct {
	mu              sync.RWMutex
	clusterLabelKey string
	clusterRoles    map[string]*rbacv1.ClusterRole
	clusterBindings map[string]*rbacv1.ClusterRoleBinding
	roles           map[string]*rbacv1.Role
	roleBindings    map[string]*rbacv1.RoleBinding
}

func newRBACProjectionStore(clusterLabelKey string) *rbacProjectionStore {
	if clusterLabelKey == "" {
		clusterLabelKey = mc.DefaultClusterAnnotation
	}
	return &rbacProjectionStore{
		clusterLabelKey: clusterLabelKey,
		clusterRoles:    map[string]*rbacv1.ClusterRole{},
		clusterBindings: map[string]*rbacv1.ClusterRoleBinding{},
		roles:           map[string]*rbacv1.Role{},
		roleBindings:    map[string]*rbacv1.RoleBinding{},
	}
}

func registerRBACProjectionHandlers(store *rbacProjectionStore, roleInf, roleBindingInf, clusterRoleInf, clusterRoleBindingInf cache.SharedIndexInformer) error {
	register := func(inf cache.SharedIndexInformer, upsert func(interface{}), del func(interface{})) error {
		_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    upsert,
			UpdateFunc: func(_, newObj interface{}) { upsert(newObj) },
			DeleteFunc: del,
		})
		return err
	}
	if err := register(roleInf, store.upsertRole, store.deleteRole); err != nil {
		return err
	}
	if err := register(roleBindingInf, store.upsertRoleBinding, store.deleteRoleBinding); err != nil {
		return err
	}
	if err := register(clusterRoleInf, store.upsertClusterRole, store.deleteClusterRole); err != nil {
		return err
	}
	if err := register(clusterRoleBindingInf, store.upsertClusterRoleBinding, store.deleteClusterRoleBinding); err != nil {
		return err
	}
	return nil
}

func (s *rbacProjectionStore) objectKey(obj runtime.Object) string {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	clusterID := acc.GetLabels()[s.clusterLabelKey]
	if clusterID == "" {
		return ""
	}
	return clusterID + "/" + acc.GetNamespace() + "/" + acc.GetName()
}

func tombstoneObj(obj interface{}) interface{} {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return tombstone.Obj
	}
	return obj
}

func (s *rbacProjectionStore) upsertClusterRole(obj interface{}) {
	cr, ok := tombstoneObj(obj).(*rbacv1.ClusterRole)
	if !ok || cr == nil {
		return
	}
	key := s.objectKey(cr)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterRoles[key] = cr
}

func (s *rbacProjectionStore) deleteClusterRole(obj interface{}) {
	cr, ok := tombstoneObj(obj).(*rbacv1.ClusterRole)
	if !ok || cr == nil {
		return
	}
	key := s.objectKey(cr)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusterRoles, key)
}

func (s *rbacProjectionStore) upsertClusterRoleBinding(obj interface{}) {
	crb, ok := tombstoneObj(obj).(*rbacv1.ClusterRoleBinding)
	if !ok || crb == nil {
		return
	}
	key := s.objectKey(crb)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterBindings[key] = crb
}

func (s *rbacProjectionStore) deleteClusterRoleBinding(obj interface{}) {
	crb, ok := tombstoneObj(obj).(*rbacv1.ClusterRoleBinding)
	if !ok || crb == nil {
		return
	}
	key := s.objectKey(crb)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusterBindings, key)
}

func (s *rbacProjectionStore) upsertRole(obj interface{}) {
	r, ok := tombstoneObj(obj).(*rbacv1.Role)
	if !ok || r == nil {
		return
	}
	key := s.objectKey(r)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roles[key] = r
}

func (s *rbacProjectionStore) deleteRole(obj interface{}) {
	r, ok := tombstoneObj(obj).(*rbacv1.Role)
	if !ok || r == nil {
		return
	}
	key := s.objectKey(r)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.roles, key)
}

func (s *rbacProjectionStore) upsertRoleBinding(obj interface{}) {
	rb, ok := tombstoneObj(obj).(*rbacv1.RoleBinding)
	if !ok || rb == nil {
		return
	}
	key := s.objectKey(rb)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roleBindings[key] = rb
}

func (s *rbacProjectionStore) deleteRoleBinding(obj interface{}) {
	rb, ok := tombstoneObj(obj).(*rbacv1.RoleBinding)
	if !ok || rb == nil {
		return
	}
	key := s.objectKey(rb)
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.roleBindings, key)
}

func (s *rbacProjectionStore) listClusterRoles(clusterID string) []*rbacv1.ClusterRole {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := clusterID + "/"
	ret := make([]*rbacv1.ClusterRole, 0)
	for k, v := range s.clusterRoles {
		if strings.HasPrefix(k, prefix) {
			ret = append(ret, v)
		}
	}
	return ret
}

func (s *rbacProjectionStore) getClusterRole(clusterID, name string) *rbacv1.ClusterRole {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterRoles[clusterID+"//"+name]
}

func (s *rbacProjectionStore) listClusterRoleBindings(clusterID string) []*rbacv1.ClusterRoleBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := clusterID + "/"
	ret := make([]*rbacv1.ClusterRoleBinding, 0)
	for k, v := range s.clusterBindings {
		if strings.HasPrefix(k, prefix) {
			ret = append(ret, v)
		}
	}
	return ret
}

func (s *rbacProjectionStore) getClusterRoleBinding(clusterID, name string) *rbacv1.ClusterRoleBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterBindings[clusterID+"//"+name]
}

func (s *rbacProjectionStore) listRoles(clusterID string) []*rbacv1.Role {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := clusterID + "/"
	ret := make([]*rbacv1.Role, 0)
	for k, v := range s.roles {
		if strings.HasPrefix(k, prefix) {
			ret = append(ret, v)
		}
	}
	return ret
}

func (s *rbacProjectionStore) listRoleBindings(clusterID string) []*rbacv1.RoleBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := clusterID + "/"
	ret := make([]*rbacv1.RoleBinding, 0)
	for k, v := range s.roleBindings {
		if strings.HasPrefix(k, prefix) {
			ret = append(ret, v)
		}
	}
	return ret
}

// RBAC listers/informers
type scopedClusterRoleInformer struct {
	informer cache.SharedIndexInformer
	lister   rbaclisters.ClusterRoleLister
}

func (i *scopedClusterRoleInformer) Informer() cache.SharedIndexInformer   { return i.informer }
func (i *scopedClusterRoleInformer) Lister() rbaclisters.ClusterRoleLister { return i.lister }

type scopedClusterRoleBindingInformer struct {
	informer cache.SharedIndexInformer
	lister   rbaclisters.ClusterRoleBindingLister
}

func (i *scopedClusterRoleBindingInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedClusterRoleBindingInformer) Lister() rbaclisters.ClusterRoleBindingLister {
	return i.lister
}

type scopedRoleInformer struct {
	informer cache.SharedIndexInformer
	lister   rbaclisters.RoleLister
}

func (i *scopedRoleInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedRoleInformer) Lister() rbaclisters.RoleLister      { return i.lister }

type scopedRoleBindingInformer struct {
	informer cache.SharedIndexInformer
	lister   rbaclisters.RoleBindingLister
}

func (i *scopedRoleBindingInformer) Informer() cache.SharedIndexInformer   { return i.informer }
func (i *scopedRoleBindingInformer) Lister() rbaclisters.RoleBindingLister { return i.lister }

type scopedClusterRoleLister struct {
	store     *rbacProjectionStore
	clusterID string
}

func (l *scopedClusterRoleLister) List(sel labels.Selector) (ret []*rbacv1.ClusterRole, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.store.listClusterRoles(l.clusterID) {
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedClusterRoleLister) Get(name string) (*rbacv1.ClusterRole, error) {
	if obj := l.store.getClusterRole(l.clusterID, name); obj != nil {
		return obj, nil
	}
	return nil, fmt.Errorf("clusterrole %q not found", name)
}

type scopedClusterRoleBindingLister struct {
	store     *rbacProjectionStore
	clusterID string
}

func (l *scopedClusterRoleBindingLister) List(sel labels.Selector) (ret []*rbacv1.ClusterRoleBinding, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.store.listClusterRoleBindings(l.clusterID) {
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedClusterRoleBindingLister) Get(name string) (*rbacv1.ClusterRoleBinding, error) {
	if obj := l.store.getClusterRoleBinding(l.clusterID, name); obj != nil {
		return obj, nil
	}
	return nil, fmt.Errorf("clusterrolebinding %q not found", name)
}

type scopedRoleLister struct {
	store     *rbacProjectionStore
	clusterID string
}

func (l *scopedRoleLister) List(sel labels.Selector) (ret []*rbacv1.Role, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.store.listRoles(l.clusterID) {
		if !sel.Matches(labels.Set(obj.Labels)) {
			continue
		}
		ret = append(ret, obj)
	}
	return ret, nil
}
func (l *scopedRoleLister) Roles(ns string) rbaclisters.RoleNamespaceLister {
	return &scopedRoleNamespaceLister{parent: l, namespace: ns}
}

type scopedRoleNamespaceLister struct {
	parent    *scopedRoleLister
	namespace string
}

func (l *scopedRoleNamespaceLister) List(sel labels.Selector) (ret []*rbacv1.Role, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedRoleNamespaceLister) Get(name string) (*rbacv1.Role, error) {
	all, _ := l.parent.List(labels.Everything())
	for _, obj := range all {
		if obj.Namespace == l.namespace && obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("role %s/%s not found", l.namespace, name)
}

type scopedRoleBindingLister struct {
	store     *rbacProjectionStore
	clusterID string
}

func (l *scopedRoleBindingLister) List(sel labels.Selector) (ret []*rbacv1.RoleBinding, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, obj := range l.store.listRoleBindings(l.clusterID) {
		if !sel.Matches(labels.Set(obj.Labels)) {
			continue
		}
		ret = append(ret, obj)
	}
	return ret, nil
}
func (l *scopedRoleBindingLister) RoleBindings(ns string) rbaclisters.RoleBindingNamespaceLister {
	return &scopedRoleBindingNamespaceLister{parent: l, namespace: ns}
}

type scopedRoleBindingNamespaceLister struct {
	parent    *scopedRoleBindingLister
	namespace string
}

func (l *scopedRoleBindingNamespaceLister) List(sel labels.Selector) (ret []*rbacv1.RoleBinding, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedRoleBindingNamespaceLister) Get(name string) (*rbacv1.RoleBinding, error) {
	all, _ := l.parent.List(labels.Everything())
	for _, obj := range all {
		if obj.Namespace == l.namespace && obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("rolebinding %s/%s not found", l.namespace, name)
}

// Core listers/informers for authn
type scopedSecretInformer struct {
	informer cache.SharedIndexInformer
	lister   corelisters.SecretLister
}

func (i *scopedSecretInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedSecretInformer) Lister() corelisters.SecretLister    { return i.lister }

type scopedServiceAccountInformer struct {
	informer cache.SharedIndexInformer
	lister   corelisters.ServiceAccountLister
}

func (i *scopedServiceAccountInformer) Informer() cache.SharedIndexInformer      { return i.informer }
func (i *scopedServiceAccountInformer) Lister() corelisters.ServiceAccountLister { return i.lister }

type scopedPodInformer struct {
	informer cache.SharedIndexInformer
	lister   corelisters.PodLister
}

func (i *scopedPodInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedPodInformer) Lister() corelisters.PodLister       { return i.lister }

type scopedNodeInformer struct {
	informer cache.SharedIndexInformer
	lister   corelisters.NodeLister
}

func (i *scopedNodeInformer) Informer() cache.SharedIndexInformer { return i.informer }
func (i *scopedNodeInformer) Lister() corelisters.NodeLister      { return i.lister }

type scopedSecretLister struct {
	indexer         cache.Indexer
	clusterID       string
	clusterLabelKey string
}

func (l *scopedSecretLister) List(sel labels.Selector) (ret []*corev1.Secret, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Secret)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedSecretLister) Secrets(ns string) corelisters.SecretNamespaceLister {
	return &scopedSecretNamespaceLister{parent: l, namespace: ns}
}

type scopedSecretNamespaceLister struct {
	parent    *scopedSecretLister
	namespace string
}

func (l *scopedSecretNamespaceLister) List(sel labels.Selector) (ret []*corev1.Secret, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedSecretNamespaceLister) Get(name string) (*corev1.Secret, error) {
	all, _ := l.parent.List(labels.Everything())
	for _, obj := range all {
		if obj.Namespace == l.namespace && obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("secret %s/%s not found", l.namespace, name)
}

type scopedServiceAccountLister struct {
	indexer         cache.Indexer
	clusterID       string
	clusterLabelKey string
}

func (l *scopedServiceAccountLister) List(sel labels.Selector) (ret []*corev1.ServiceAccount, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.ServiceAccount)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedServiceAccountLister) ServiceAccounts(ns string) corelisters.ServiceAccountNamespaceLister {
	return &scopedServiceAccountNamespaceLister{parent: l, namespace: ns}
}

type scopedServiceAccountNamespaceLister struct {
	parent    *scopedServiceAccountLister
	namespace string
}

func (l *scopedServiceAccountNamespaceLister) List(sel labels.Selector) (ret []*corev1.ServiceAccount, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedServiceAccountNamespaceLister) Get(name string) (*corev1.ServiceAccount, error) {
	all, _ := l.parent.List(labels.Everything())
	for _, obj := range all {
		if obj.Namespace == l.namespace && obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("serviceaccount %s/%s not found", l.namespace, name)
}

type scopedPodLister struct {
	indexer         cache.Indexer
	clusterID       string
	clusterLabelKey string
}

func (l *scopedPodLister) List(sel labels.Selector) (ret []*corev1.Pod, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Pod)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedPodLister) Pods(ns string) corelisters.PodNamespaceLister {
	return &scopedPodNamespaceLister{parent: l, namespace: ns}
}

type scopedPodNamespaceLister struct {
	parent    *scopedPodLister
	namespace string
}

func (l *scopedPodNamespaceLister) List(sel labels.Selector) (ret []*corev1.Pod, err error) {
	all, _ := l.parent.List(sel)
	for _, obj := range all {
		if obj.Namespace == l.namespace {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedPodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	all, _ := l.parent.List(labels.Everything())
	for _, obj := range all {
		if obj.Namespace == l.namespace && obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("pod %s/%s not found", l.namespace, name)
}

type scopedNodeLister struct {
	indexer         cache.Indexer
	clusterID       string
	clusterLabelKey string
}

func (l *scopedNodeLister) List(sel labels.Selector) (ret []*corev1.Node, err error) {
	if sel == nil {
		sel = labels.Everything()
	}
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Node)
		if !ok {
			continue
		}
		if sel.Matches(labels.Set(obj.Labels)) {
			ret = append(ret, obj)
		}
	}
	return ret, nil
}
func (l *scopedNodeLister) Get(name string) (*corev1.Node, error) {
	for _, it := range filteredByCluster(l.indexer, l.clusterID) {
		obj, ok := it.(*corev1.Node)
		if !ok {
			continue
		}
		if obj.Name == name {
			return obj, nil
		}
	}
	return nil, fmt.Errorf("node %q not found", name)
}
