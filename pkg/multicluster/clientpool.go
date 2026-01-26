package multicluster

import (
	"fmt"
	"net/http"
	"sync"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ClientPool caches per-cluster REST clients/transports to avoid rebuilding
// client-go machinery for every request.
type ClientPool struct {
	base                *rest.Config
	pathPrefix          string
	controlPlaneSegment string

	mu      sync.Mutex
	clients map[string]*clientEntry
}

type clientEntry struct {
	clientset  *kubernetes.Clientset
	httpClient *http.Client
}

func NewClientPool(base *rest.Config, pathPrefix, controlPlaneSegment string) *ClientPool {
	return &ClientPool{
		base:                base,
		pathPrefix:          pathPrefix,
		controlPlaneSegment: controlPlaneSegment,
		clients:             map[string]*clientEntry{},
	}
}

func (p *ClientPool) KubeClientForCluster(clusterID string) (kubernetes.Interface, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.clients[clusterID]; ok {
		return existing.clientset, nil
	}
	if p.base == nil {
		return nil, fmt.Errorf("base loopback config is required")
	}

	cfg := rest.CopyConfig(p.base)
	host, err := ClusterHost(cfg.Host, Options{
		PathPrefix:          p.pathPrefix,
		ControlPlaneSegment: p.controlPlaneSegment,
	}, clusterID)
	if err != nil {
		return nil, err
	}
	cfg.Host = host

	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Transport: transport}

	cs, err := kubernetes.NewForConfigAndClient(cfg, httpClient)
	if err != nil {
		return nil, err
	}

	p.clients[clusterID] = &clientEntry{
		clientset:  cs,
		httpClient: httpClient,
	}
	return cs, nil
}
