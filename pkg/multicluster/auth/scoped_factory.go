package auth

import (
	"strings"
	"sync"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

type rbacProjectionStore struct {
	mu              sync.RWMutex
	clusterRoles    map[string]*rbacv1.ClusterRole
	clusterBindings map[string]*rbacv1.ClusterRoleBinding
	roles           map[string]*rbacv1.Role
	roleBindings    map[string]*rbacv1.RoleBinding
}

func newRBACProjectionStore() *rbacProjectionStore {
	return &rbacProjectionStore{
		clusterRoles:    map[string]*rbacv1.ClusterRole{},
		clusterBindings: map[string]*rbacv1.ClusterRoleBinding{},
		roles:           map[string]*rbacv1.Role{},
		roleBindings:    map[string]*rbacv1.RoleBinding{},
	}
}

func (s *rbacProjectionStore) objectKey(obj runtime.Object, clusterID string) string {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	if clusterID == "" {
		return ""
	}
	return clusterID + "/" + acc.GetNamespace() + "/" + acc.GetName()
}

// toVersionedRBAC converts an internal RBAC type to its versioned equivalent.
// The cacher stores internal types; MCI event handlers receive them wrapped
// in cachingObject envelopes. This function unwraps and converts.
func toVersionedRBAC(obj interface{}) interface{} {
	// Unwrap cacher's cachingObject envelope if present.
	if co, ok := obj.(runtime.CacheableObject); ok {
		obj = co.GetObject()
	}
	// Fast path: already versioned
	switch obj.(type) {
	case *rbacv1.Role, *rbacv1.RoleBinding, *rbacv1.ClusterRole, *rbacv1.ClusterRoleBinding:
		return obj
	}
	rObj, ok := obj.(runtime.Object)
	if !ok {
		return obj
	}
	out, err := legacyscheme.Scheme.ConvertToVersion(rObj, rbacv1.SchemeGroupVersion)
	if err != nil {
		return obj
	}
	return out
}

// upsertWithCluster handles Add/Update events from MCI by type-switching on the
// RBAC object and inserting into the appropriate map.
func (s *rbacProjectionStore) upsertWithCluster(obj interface{}, clusterID string) {
	if clusterID == "" {
		return
	}
	obj = toVersionedRBAC(obj)
	switch o := obj.(type) {
	case *rbacv1.Role:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		s.roles[key] = o
		s.mu.Unlock()
	case *rbacv1.RoleBinding:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		s.roleBindings[key] = o
		s.mu.Unlock()
	case *rbacv1.ClusterRole:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		s.clusterRoles[key] = o
		s.mu.Unlock()
	case *rbacv1.ClusterRoleBinding:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		s.clusterBindings[key] = o
		s.mu.Unlock()
	}
}

// deleteWithCluster handles Delete events from MCI.
func (s *rbacProjectionStore) deleteWithCluster(obj interface{}, clusterID string) {
	if clusterID == "" {
		return
	}
	obj = toVersionedRBAC(obj)
	switch o := obj.(type) {
	case *rbacv1.Role:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		delete(s.roles, key)
		s.mu.Unlock()
	case *rbacv1.RoleBinding:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		delete(s.roleBindings, key)
		s.mu.Unlock()
	case *rbacv1.ClusterRole:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		delete(s.clusterRoles, key)
		s.mu.Unlock()
	case *rbacv1.ClusterRoleBinding:
		if o == nil {
			return
		}
		key := s.objectKey(o, clusterID)
		if key == "" {
			return
		}
		s.mu.Lock()
		delete(s.clusterBindings, key)
		s.mu.Unlock()
	}
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
