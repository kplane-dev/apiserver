package bootstrap

import (
	"fmt"
	"net"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/core/rangeallocation"
	"k8s.io/kubernetes/pkg/registry/core/service/allocator"
	serviceallocator "k8s.io/kubernetes/pkg/registry/core/service/allocator/storage"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
	svcstorage "k8s.io/kubernetes/pkg/registry/core/service/storage"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	netutils "k8s.io/utils/net"
)

// StorageFactory creates a raw storage.Interface for a given resource prefix.
type StorageFactory func(
	config *storagebackend.ConfigForResource,
	newFunc, newListFunc func() runtime.Object,
	resourcePrefix string,
) (storage.Interface, factory.DestroyFunc, error)

// AllocatorRegistryOptions configures the per-cluster allocator registry.
type AllocatorRegistryOptions struct {
	PrimaryServiceCIDR   net.IPNet
	SecondaryServiceCIDR net.IPNet
	NodePortRange        utilnet.PortRange
	// ServiceStorageConfig is the base storage config for the "services" resource.
	ServiceStorageConfig *storagebackend.ConfigForResource
	// BackendFactory creates raw storage.Interface for allocator bitmap persistence.
	// When nil, factory.Create (etcd) is used.
	BackendFactory StorageFactory
}

// clusterRegistries holds the range registries and allocators for one cluster.
type clusterRegistries struct {
	alloc                *svcstorage.Allocators
	clusterIP            rangeallocation.RangeRegistry
	secondaryClusterIP   rangeallocation.RangeRegistry
	nodePort             rangeallocation.RangeRegistry
}

// AllocatorRegistry lazily creates per-cluster service allocators.
type AllocatorRegistry struct {
	opts AllocatorRegistryOptions

	mu    sync.Mutex
	cache map[string]*clusterRegistries
}

// NewAllocatorRegistry creates a new per-cluster allocator registry.
func NewAllocatorRegistry(opts AllocatorRegistryOptions) *AllocatorRegistry {
	return &AllocatorRegistry{
		opts:  opts,
		cache: map[string]*clusterRegistries{},
	}
}

// AllocatorsForCluster returns (lazily creating) allocators for the given cluster.
func (r *AllocatorRegistry) AllocatorsForCluster(clusterID string) (*svcstorage.Allocators, error) {
	cr, err := r.ensureCluster(clusterID)
	if err != nil {
		return nil, err
	}
	return cr.alloc, nil
}

func (r *AllocatorRegistry) ensureCluster(clusterID string) (*clusterRegistries, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cr, ok := r.cache[clusterID]; ok {
		return cr, nil
	}
	cr, err := r.newClusterRegistries(clusterID)
	if err != nil {
		return nil, err
	}
	r.cache[clusterID] = cr
	return cr, nil
}

func (r *AllocatorRegistry) newClusterRegistries(clusterID string) (*clusterRegistries, error) {
	cr := &clusterRegistries{}
	ipAllocs := map[api.IPFamily]ipallocator.Interface{}

	primaryAlloc, primaryReg, err := r.newIPAllocator(clusterID, &r.opts.PrimaryServiceCIDR, "/ranges/serviceips")
	if err != nil {
		return nil, fmt.Errorf("primary IP allocator for cluster %s: %w", clusterID, err)
	}
	ipAllocs[primaryAlloc.IPFamily()] = primaryAlloc
	cr.clusterIP = primaryReg

	if r.opts.SecondaryServiceCIDR.IP != nil {
		secondaryAlloc, secondaryReg, err := r.newIPAllocator(clusterID, &r.opts.SecondaryServiceCIDR, "/ranges/secondaryserviceips")
		if err != nil {
			return nil, fmt.Errorf("secondary IP allocator for cluster %s: %w", clusterID, err)
		}
		ipAllocs[secondaryAlloc.IPFamily()] = secondaryAlloc
		cr.secondaryClusterIP = secondaryReg
	}

	portAlloc, portReg, err := r.newPortAllocator(clusterID)
	if err != nil {
		return nil, fmt.Errorf("port allocator for cluster %s: %w", clusterID, err)
	}
	cr.nodePort = portReg

	defaultFamily := api.IPv4Protocol
	if netutils.IsIPv6CIDR(&r.opts.PrimaryServiceCIDR) {
		defaultFamily = api.IPv6Protocol
	}

	a := svcstorage.MakeAllocators(defaultFamily, ipAllocs, portAlloc)
	cr.alloc = &a
	klog.Infof("mc.allocatorRegistry created allocators for cluster=%s defaultFamily=%s families=%d", clusterID, defaultFamily, len(ipAllocs))
	return cr, nil
}

func (r *AllocatorRegistry) newIPAllocator(clusterID string, cidr *net.IPNet, baseKey string) (*ipallocator.Range, rangeallocation.RangeRegistry, error) {
	clusterBaseKey := clusterRangeKey(clusterID, baseKey)
	var reg rangeallocation.RangeRegistry
	alloc, err := ipallocator.New(cidr, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
		mem := allocator.NewAllocationMapWithOffset(max, rangeSpec, offset)
		store, err := r.createStorage(clusterBaseKey)
		if err != nil {
			return nil, err
		}
		etcd := serviceallocator.NewWithStorage(mem, clusterBaseKey, store, api.Resource("serviceipallocations"))
		// Initialize the bitmap if it doesn't exist yet.
		rangeRegistry, err := etcd.Get()
		if err != nil {
			return nil, err
		}
		rangeRegistry.Range = rangeSpec
		if len(rangeRegistry.ResourceVersion) == 0 {
			if err := etcd.CreateOrUpdate(rangeRegistry); err != nil {
				return nil, err
			}
		}
		reg = etcd
		return etcd, nil
	})
	return alloc, reg, err
}

func (r *AllocatorRegistry) newPortAllocator(clusterID string) (*portallocator.PortAllocator, rangeallocation.RangeRegistry, error) {
	clusterBaseKey := clusterRangeKey(clusterID, "/ranges/servicenodeports")
	var reg rangeallocation.RangeRegistry
	alloc, err := portallocator.New(r.opts.NodePortRange, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
		mem := allocator.NewAllocationMapWithOffset(max, rangeSpec, offset)
		store, err := r.createStorage(clusterBaseKey)
		if err != nil {
			return nil, err
		}
		etcd := serviceallocator.NewWithStorage(mem, clusterBaseKey, store, api.Resource("servicenodeportallocations"))
		reg = etcd
		return etcd, nil
	})
	return alloc, reg, err
}

// clusterIPRegistry returns the RangeRegistry for primary ClusterIP, or nil.
func (r *AllocatorRegistry) clusterIPRegistry(clusterID string) rangeallocation.RangeRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cr, ok := r.cache[clusterID]; ok {
		return cr.clusterIP
	}
	return nil
}

// secondaryClusterIPRegistry returns the RangeRegistry for secondary ClusterIP, or nil.
func (r *AllocatorRegistry) secondaryClusterIPRegistry(clusterID string) rangeallocation.RangeRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cr, ok := r.cache[clusterID]; ok {
		return cr.secondaryClusterIP
	}
	return nil
}

// secondaryCIDR returns a pointer to the secondary CIDR, or nil if not configured.
func (r *AllocatorRegistry) secondaryCIDR() *net.IPNet {
	if r.opts.SecondaryServiceCIDR.IP == nil {
		return &net.IPNet{}
	}
	return &r.opts.SecondaryServiceCIDR
}

// nodePortRegistry returns the RangeRegistry for NodePorts, or nil.
func (r *AllocatorRegistry) nodePortRegistry(clusterID string) rangeallocation.RangeRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cr, ok := r.cache[clusterID]; ok {
		return cr.nodePort
	}
	return nil
}

func (r *AllocatorRegistry) createStorage(baseKey string) (storage.Interface, error) {
	cfg := *r.opts.ServiceStorageConfig
	newFunc := func() runtime.Object { return &api.RangeAllocation{} }
	if r.opts.BackendFactory != nil {
		s, _, err := r.opts.BackendFactory(&cfg, newFunc, nil, baseKey)
		return s, err
	}
	s, _, err := factory.Create(cfg, newFunc, nil, baseKey)
	return s, err
}

// clusterRangeKey prefixes a range key with the cluster path.
// e.g. "/ranges/serviceips" → "/ranges/clusters/<id>/serviceips"
func clusterRangeKey(clusterID, baseKey string) string {
	return "/ranges/clusters/" + clusterID + baseKey[len("/ranges"):]
}
