package bootstrap

import (
	"context"
	"net"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type KubernetesServiceControllerOptions struct {
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)
	StopChForCluster func(clusterID string) (<-chan struct{}, error)
	PublicIP         net.IP
	ServicePort      int
	TargetPort       int
	NodePort         int
	Interval         time.Duration
}

type KubernetesServiceControllerManager struct {
	opts KubernetesServiceControllerOptions
	mu   sync.Mutex
	run  map[string]struct{}
}

func NewKubernetesServiceControllerManager(opts KubernetesServiceControllerOptions) *KubernetesServiceControllerManager {
	if opts.Interval <= 0 {
		opts.Interval = 10 * time.Second
	}
	return &KubernetesServiceControllerManager{opts: opts, run: map[string]struct{}{}}
}

func (m *KubernetesServiceControllerManager) Ensure(clusterID string) {
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
		klog.Errorf("mc.bootstrap kubernetes service controller client failed cluster=%s: %v", clusterID, err)
		return
	}
	stopCh, err := m.opts.StopChForCluster(clusterID)
	if err != nil {
		klog.Errorf("mc.bootstrap kubernetes service controller stop channel failed cluster=%s: %v", clusterID, err)
		return
	}
	c := &kubernetesServiceController{
		client:     cs,
		publicIP:   m.opts.PublicIP,
		servicePort: m.opts.ServicePort,
		targetPort: m.opts.TargetPort,
		nodePort:   m.opts.NodePort,
		interval:   m.opts.Interval,
	}
	go c.Run(stopCh)
}

type kubernetesServiceController struct {
	client      kubernetes.Interface
	publicIP    net.IP
	servicePort int
	targetPort  int
	nodePort    int
	interval    time.Duration
}

func (c *kubernetesServiceController) Run(stopCh <-chan struct{}) {
	wait.NonSlidingUntil(func() {
		if err := c.reconcile(); err != nil {
			klog.Errorf("mc.bootstrap kubernetes service reconcile failed: %v", err)
		}
	}, c.interval, stopCh)
}

func (c *kubernetesServiceController) reconcile() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	port := corev1.ServicePort{
		Protocol:   corev1.ProtocolTCP,
		Port:       int32(c.servicePort),
		Name:       "https",
		TargetPort: intstr.FromInt32(int32(c.targetPort)),
	}
	svcType := corev1.ServiceTypeClusterIP
	if c.nodePort > 0 {
		port.NodePort = int32(c.nodePort)
		svcType = corev1.ServiceTypeNodePort
	}
	svcClient := c.client.CoreV1().Services(metav1.NamespaceDefault)
	svc, err := svcClient.Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		singleStack := corev1.IPFamilyPolicySingleStack
		_, err = svcClient.Create(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kubernetes",
				Namespace: metav1.NamespaceDefault,
				Labels:    map[string]string{"provider": "kubernetes", "component": "apiserver"},
			},
			Spec: corev1.ServiceSpec{
				Ports:           []corev1.ServicePort{port},
				Selector:        nil,
				IPFamilyPolicy:  &singleStack,
				SessionAffinity: corev1.ServiceAffinityNone,
				Type:            svcType,
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	} else {
		updated := svc.DeepCopy()
		if len(updated.Spec.Ports) != 1 || updated.Spec.Ports[0].Port != port.Port || updated.Spec.Ports[0].TargetPort != port.TargetPort || updated.Spec.Ports[0].Protocol != port.Protocol || updated.Spec.Ports[0].Name != port.Name || (c.nodePort > 0 && updated.Spec.Ports[0].NodePort != port.NodePort) || updated.Spec.Type != svcType {
			updated.Spec.Ports = []corev1.ServicePort{port}
			updated.Spec.Type = svcType
			if _, err := svcClient.Update(ctx, updated, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) {
				return err
			}
		}
	}

	epClient := c.client.CoreV1().Endpoints(metav1.NamespaceDefault)
	want := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubernetes",
			Namespace: metav1.NamespaceDefault,
		},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: c.publicIP.String()}},
			Ports:     []corev1.EndpointPort{{Name: "https", Port: int32(c.targetPort), Protocol: corev1.ProtocolTCP}},
		}},
	}
	existing, err := epClient.Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = epClient.Create(ctx, want, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
			return nil
		}
		return err
	}
	existing = existing.DeepCopy()
	existing.Subsets = want.Subsets
	if _, err := epClient.Update(ctx, existing, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(err) {
		return err
	}
	return nil
}
