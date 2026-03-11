package bootstrap

import (
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	serviceipallocatorcontroller "k8s.io/kubernetes/pkg/registry/core/service/ipallocator/controller"
	portallocatorcontroller "k8s.io/kubernetes/pkg/registry/core/service/portallocator/controller"
)

// AllocatorRepairOptions configures per-cluster allocator repair controllers.
type AllocatorRepairOptions struct {
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)
	StopChForCluster func(clusterID string) (<-chan struct{}, error)
	Registry         *AllocatorRegistry
	RepairInterval   time.Duration
}

// AllocatorRepairManager lazily starts per-cluster service allocator repair loops.
type AllocatorRepairManager struct {
	opts AllocatorRepairOptions
	mu   sync.Mutex
	run  map[string]struct{}
}

// NewAllocatorRepairManager creates a new per-cluster allocator repair manager.
func NewAllocatorRepairManager(opts AllocatorRepairOptions) *AllocatorRepairManager {
	if opts.RepairInterval <= 0 {
		opts.RepairInterval = 3 * time.Minute
	}
	return &AllocatorRepairManager{
		opts: opts,
		run:  map[string]struct{}{},
	}
}

// Ensure starts the repair loops for the given cluster if not already running.
func (m *AllocatorRepairManager) Ensure(clusterID string) {
	if m == nil || clusterID == "" || m.opts.Registry == nil {
		return
	}
	m.mu.Lock()
	if _, ok := m.run[clusterID]; ok {
		m.mu.Unlock()
		return
	}
	m.run[clusterID] = struct{}{}
	m.mu.Unlock()

	go m.startRepair(clusterID)
}

func (m *AllocatorRepairManager) startRepair(clusterID string) {
	cs, err := m.opts.ClientForCluster(clusterID)
	if err != nil {
		klog.Errorf("mc.allocatorRepair client failed cluster=%s: %v", clusterID, err)
		return
	}
	stopCh, err := m.opts.StopChForCluster(clusterID)
	if err != nil {
		klog.Errorf("mc.allocatorRepair stop channel failed cluster=%s: %v", clusterID, err)
		return
	}

	// Ensure allocators are initialized before starting repair.
	_, err = m.opts.Registry.AllocatorsForCluster(clusterID)
	if err != nil {
		klog.Errorf("mc.allocatorRepair allocator init failed cluster=%s: %v", clusterID, err)
		return
	}

	regOpts := m.opts.Registry.opts

	// Convert read-only stop channel to a writable one for RunUntil.
	portStopCh := make(chan struct{})
	ipStopCh := make(chan struct{})
	go func() {
		<-stopCh
		close(portStopCh)
		close(ipStopCh)
	}()

	// Node port repair
	portRepair := portallocatorcontroller.NewRepair(
		m.opts.RepairInterval,
		cs.CoreV1(),
		cs.EventsV1(),
		regOpts.NodePortRange,
		m.opts.Registry.nodePortRegistry(clusterID),
	)
	go portRepair.RunUntil(func() {}, portStopCh)

	// ClusterIP repair
	ipRepair := serviceipallocatorcontroller.NewRepair(
		m.opts.RepairInterval,
		cs.CoreV1(),
		cs.EventsV1(),
		&regOpts.PrimaryServiceCIDR,
		m.opts.Registry.clusterIPRegistry(clusterID),
		m.opts.Registry.secondaryCIDR(),
		m.opts.Registry.secondaryClusterIPRegistry(clusterID),
	)
	go ipRepair.RunUntil(func() {}, ipStopCh)

	klog.Infof("mc.allocatorRepair started repair controllers for cluster=%s", clusterID)
}
