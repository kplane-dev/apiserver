package multicluster

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

type InformerPoolOptions struct {
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)
	ResyncPeriod     time.Duration
	StopCh           <-chan struct{}
	StartOnGet       bool
}

type InformerPool struct {
	opts InformerPoolOptions

	mu       sync.Mutex
	clusters map[string]*informerEntry
}

type informerEntry struct {
	clientset kubernetes.Interface
	factory   informers.SharedInformerFactory
	stopCh    <-chan struct{}
	ownedCh   chan struct{}

	startOnce sync.Once
}

func NewInformerPool(opts InformerPoolOptions) *InformerPool {
	if opts.StartOnGet == false {
		// keep explicit
	} else {
		opts.StartOnGet = true
	}
	return &InformerPool{
		opts:     opts,
		clusters: map[string]*informerEntry{},
	}
}

func NewInformerPoolFromClientPool(pool *ClientPool, resync time.Duration, stopCh <-chan struct{}) *InformerPool {
	return NewInformerPool(InformerPoolOptions{
		ClientForCluster: pool.KubeClientForCluster,
		ResyncPeriod:     resync,
		StopCh:           stopCh,
		StartOnGet:       true,
	})
}

func (p *InformerPool) Get(clusterID string) (kubernetes.Interface, informers.SharedInformerFactory, <-chan struct{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.clusters[clusterID]; ok {
		if p.opts.StartOnGet {
			entry.start()
		}
		return entry.clientset, entry.factory, entry.stopCh, nil
	}
	if p.opts.ClientForCluster == nil {
		return nil, nil, nil, ErrMissingClientFactory
	}
	cs, err := p.opts.ClientForCluster(clusterID)
	if err != nil {
		return nil, nil, nil, err
	}
	factory := informers.NewSharedInformerFactory(cs, p.opts.ResyncPeriod)
	stopCh := p.opts.StopCh
	var ownedCh chan struct{}
	if stopCh == nil {
		ownedCh = make(chan struct{})
		stopCh = ownedCh
	}
	entry := &informerEntry{
		clientset: cs,
		factory:   factory,
		stopCh:    stopCh,
		ownedCh:   ownedCh,
	}
	p.clusters[clusterID] = entry
	if p.opts.StartOnGet {
		entry.start()
	}
	return entry.clientset, entry.factory, entry.stopCh, nil
}

func (e *informerEntry) start() {
	e.startOnce.Do(func() {
		e.factory.Start(e.stopCh)
	})
}

func (p *InformerPool) StopCluster(clusterID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.clusters[clusterID]
	if !ok {
		return
	}
	if entry.ownedCh != nil {
		close(entry.ownedCh)
	}
	delete(p.clusters, clusterID)
}

var ErrMissingClientFactory = fmt.Errorf("missing client factory for informer pool")
