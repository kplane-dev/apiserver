package multicluster

import (
	"fmt"
	"sync"
	"time"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
)

type APIExtensionsInformerPoolOptions struct {
	ClientForCluster func(clusterID string) (apiextensionsclient.Interface, error)
	ResyncPeriod     time.Duration
	StopCh           <-chan struct{}
	StartOnGet       bool
}

type APIExtensionsInformerPool struct {
	opts APIExtensionsInformerPoolOptions

	mu       sync.Mutex
	clusters map[string]*apiExtensionsInformerEntry
}

type apiExtensionsInformerEntry struct {
	clientset apiextensionsclient.Interface
	factory   apiextensionsinformers.SharedInformerFactory
	stopCh    <-chan struct{}
	ownedCh   chan struct{}
}

func NewAPIExtensionsInformerPool(opts APIExtensionsInformerPoolOptions) *APIExtensionsInformerPool {
	if opts.StartOnGet == false {
		// keep explicit
	} else {
		opts.StartOnGet = true
	}
	return &APIExtensionsInformerPool{
		opts:     opts,
		clusters: map[string]*apiExtensionsInformerEntry{},
	}
}

func NewAPIExtensionsInformerPoolFromClientPool(pool *APIExtensionsClientPool, resync time.Duration, stopCh <-chan struct{}) *APIExtensionsInformerPool {
	return NewAPIExtensionsInformerPool(APIExtensionsInformerPoolOptions{
		ClientForCluster: pool.APIExtensionsClientForCluster,
		ResyncPeriod:     resync,
		StopCh:           stopCh,
		StartOnGet:       true,
	})
}

func (p *APIExtensionsInformerPool) Get(clusterID string) (apiextensionsclient.Interface, apiextensionsinformers.SharedInformerFactory, <-chan struct{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.clusters[clusterID]; ok {
		if p.opts.StartOnGet {
			entry.start()
		}
		return entry.clientset, entry.factory, entry.stopCh, nil
	}
	if p.opts.ClientForCluster == nil {
		return nil, nil, nil, ErrMissingAPIExtensionsClientFactory
	}
	cs, err := p.opts.ClientForCluster(clusterID)
	if err != nil {
		return nil, nil, nil, err
	}
	factory := apiextensionsinformers.NewSharedInformerFactory(cs, p.opts.ResyncPeriod)
	stopCh := p.opts.StopCh
	var ownedCh chan struct{}
	if stopCh == nil {
		ownedCh = make(chan struct{})
		stopCh = ownedCh
	}
	entry := &apiExtensionsInformerEntry{
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

func (e *apiExtensionsInformerEntry) start() {
	e.factory.Start(e.stopCh)
}

func (p *APIExtensionsInformerPool) StopCluster(clusterID string) {
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

var ErrMissingAPIExtensionsClientFactory = fmt.Errorf("missing apiextensions client factory for informer pool")
