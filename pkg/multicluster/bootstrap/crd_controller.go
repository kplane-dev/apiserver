package bootstrap

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type MulticlusterCRDController struct {
	runtimeManager *CRDRuntimeManager
	defaultCluster string

	mu       sync.Mutex
	started  bool
	stopCh   <-chan struct{}
	queue    chan string
	enqueued map[string]struct{}
}

func NewMulticlusterCRDController(runtimeManager *CRDRuntimeManager, defaultCluster string) *MulticlusterCRDController {
	return &MulticlusterCRDController{
		runtimeManager: runtimeManager,
		defaultCluster: defaultCluster,
		queue:          make(chan string, 2048),
		enqueued:       map[string]struct{}{},
	}
}

func (c *MulticlusterCRDController) Start(stopCh <-chan struct{}) {
	if c == nil {
		return
	}

	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.stopCh = stopCh
	c.mu.Unlock()

	go c.run()
	if c.defaultCluster != "" {
		c.EnsureCluster(c.defaultCluster)
	}
}

func (c *MulticlusterCRDController) EnsureCluster(clusterID string) {
	if c == nil || clusterID == "" {
		return
	}

	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return
	}
	if _, ok := c.enqueued[clusterID]; ok {
		c.mu.Unlock()
		return
	}
	c.enqueued[clusterID] = struct{}{}
	queue := c.queue
	stopCh := c.stopCh
	c.mu.Unlock()

	select {
	case queue <- clusterID:
	case <-stopCh:
	}
}

func (c *MulticlusterCRDController) run() {
	for {
		select {
		case <-c.stopCh:
			return
		case clusterID := <-c.queue:
			if c.runtimeManager != nil {
				if err := c.runtimeManager.EnsureCluster(clusterID, c.stopCh); err != nil {
					klog.Warningf("mc.crdController ensure cluster failed cluster=%s err=%v (will retry)", clusterID, err)
					// Re-enqueue after a short delay. Storage may not be
					// registered yet if the apiextensions server hasn't
					// finished construction.
					go func() {
						select {
						case <-c.stopCh:
						case <-time.After(500 * time.Millisecond):
							c.mu.Lock()
							delete(c.enqueued, clusterID)
							c.mu.Unlock()
							c.EnsureCluster(clusterID)
						}
					}()
					continue
				}
			}

			c.mu.Lock()
			delete(c.enqueued, clusterID)
			c.mu.Unlock()
		}
	}
}
