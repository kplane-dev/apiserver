package bootstrap

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type SystemNamespaceOptions struct {
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)
	Namespaces       []string
	Timeout          time.Duration
}

// SystemNamespaceBootstrapper ensures system namespaces exist in each cluster.
// It runs once per cluster and retries on a later request if a prior attempt failed.
type SystemNamespaceBootstrapper struct {
	clientForCluster func(clusterID string) (kubernetes.Interface, error)
	namespaces       []string
	timeout          time.Duration

	mu       sync.Mutex
	done     map[string]struct{}
	inflight map[string]chan struct{}
}

func NewSystemNamespaceBootstrapper(opts SystemNamespaceOptions) *SystemNamespaceBootstrapper {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &SystemNamespaceBootstrapper{
		clientForCluster: opts.ClientForCluster,
		namespaces:       append([]string(nil), opts.Namespaces...),
		timeout:          timeout,
		done:             map[string]struct{}{},
		inflight:         map[string]chan struct{}{},
	}
}

func (b *SystemNamespaceBootstrapper) Ensure(clusterID string) {
	if clusterID == "" || b == nil || b.clientForCluster == nil || len(b.namespaces) == 0 {
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
		klog.Errorf("mc.bootstrap system namespaces failed cluster=%s: %v", clusterID, err)
	}
}

func (b *SystemNamespaceBootstrapper) bootstrap(clusterID string) error {
	client, err := b.clientForCluster(clusterID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	for _, ns := range b.namespaces {
		if ns == "" {
			continue
		}
		_, err := client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if err == nil {
			continue
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
		_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}
