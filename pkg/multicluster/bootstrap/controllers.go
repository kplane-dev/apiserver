package bootstrap

import (
	"context"
	"sync"

	clusterauthenticationtrust "k8s.io/kubernetes/pkg/controlplane/controller/clusterauthenticationtrust"
	legacytokentracking "k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type InternalControllerOptions struct {
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)
	StopChForCluster func(clusterID string) (<-chan struct{}, error)
	ClusterAuthInfo  clusterauthenticationtrust.ClusterAuthenticationInfo
}

type InternalControllerManager struct {
	opts InternalControllerOptions
	mu   sync.Mutex
	run  map[string]struct{}
}

func NewInternalControllerManager(opts InternalControllerOptions) *InternalControllerManager {
	return &InternalControllerManager{opts: opts, run: map[string]struct{}{}}
}

func (m *InternalControllerManager) Ensure(clusterID string) {
	if m == nil || clusterID == "" || m.opts.ClientForCluster == nil || m.opts.StopChForCluster == nil {
		return
	}
	m.mu.Lock()
	if _, ok := m.run[clusterID]; ok {
		m.mu.Unlock()
		return
	}
	m.run[clusterID] = struct{}{}
	m.mu.Unlock()

	cs, err := m.opts.ClientForCluster(clusterID)
	if err != nil {
		klog.Errorf("mc.bootstrap internal controllers client failed cluster=%s: %v", clusterID, err)
		return
	}
	stopCh, err := m.opts.StopChForCluster(clusterID)
	if err != nil {
		klog.Errorf("mc.bootstrap internal controllers stop channel failed cluster=%s: %v", clusterID, err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()

	legacy := legacytokentracking.NewController(cs)
	go legacy.Run(stopCh)

	auth := clusterauthenticationtrust.NewClusterAuthenticationTrustController(m.opts.ClusterAuthInfo, cs)
	if m.opts.ClusterAuthInfo.ClientCA != nil {
		m.opts.ClusterAuthInfo.ClientCA.AddListener(auth)
	}
	if m.opts.ClusterAuthInfo.RequestHeaderCA != nil {
		m.opts.ClusterAuthInfo.RequestHeaderCA.AddListener(auth)
	}
	go auth.Run(ctx, 1)
}
