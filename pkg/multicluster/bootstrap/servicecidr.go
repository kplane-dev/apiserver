package bootstrap

import (
	"context"
	"net"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	defaultservicecidr "k8s.io/kubernetes/pkg/controlplane/controller/defaultservicecidr"
	"k8s.io/klog/v2"
)

type ServiceCIDROptions struct {
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)
	PrimaryRange     net.IPNet
	SecondaryRange   net.IPNet
	Enabled          bool
	Timeout          time.Duration
}

// ServiceCIDRBootstrapper ensures the default ServiceCIDR object per cluster
// using the upstream controller logic.
type ServiceCIDRBootstrapper struct {
	clientForCluster func(clusterID string) (kubernetes.Interface, error)
	primaryRange     net.IPNet
	secondaryRange   net.IPNet
	enabled          bool
	timeout          time.Duration

	mu       sync.Mutex
	done     map[string]struct{}
	inflight map[string]chan struct{}
}

func NewServiceCIDRBootstrapper(opts ServiceCIDROptions) *ServiceCIDRBootstrapper {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &ServiceCIDRBootstrapper{
		clientForCluster: opts.ClientForCluster,
		primaryRange:     opts.PrimaryRange,
		secondaryRange:   opts.SecondaryRange,
		enabled:          opts.Enabled,
		timeout:          timeout,
		done:             map[string]struct{}{},
		inflight:         map[string]chan struct{}{},
	}
}

func (b *ServiceCIDRBootstrapper) Ensure(clusterID string) {
	if b == nil || !b.enabled || clusterID == "" || b.clientForCluster == nil {
		return
	}
	b.mu.Lock()
	if _, ok := b.done[clusterID]; ok {
		b.mu.Unlock()
		return
	}
	if ch, ok := b.inflight[clusterID]; ok {
		b.mu.Unlock()
		<-ch
		return
	}
	ch := make(chan struct{})
	b.inflight[clusterID] = ch
	b.mu.Unlock()

	err := b.bootstrap(clusterID)

	b.mu.Lock()
	delete(b.inflight, clusterID)
	if err == nil {
		b.done[clusterID] = struct{}{}
	}
	close(ch)
	b.mu.Unlock()
	if err != nil {
		klog.Errorf("mc.bootstrap servicecidr failed cluster=%s: %v", clusterID, err)
	}
}

func (b *ServiceCIDRBootstrapper) bootstrap(clusterID string) error {
	client, err := b.clientForCluster(clusterID)
	if err != nil {
		return err
	}
	ctrl := defaultservicecidr.NewController(b.primaryRange, b.secondaryRange, client)
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	ctrl.Start(ctx)
	return nil
}
