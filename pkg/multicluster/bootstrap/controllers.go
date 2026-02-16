package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clusterauthenticationtrust "k8s.io/kubernetes/pkg/controlplane/controller/clusterauthenticationtrust"
	legacytokentracking "k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	internalControllerWorkerCount = 4
	legacyTokenDateFormat         = "2006-01-02"
)

type caQueueListener struct {
	onChange func()
}

func (l *caQueueListener) Enqueue() {
	if l != nil && l.onChange != nil {
		l.onChange()
	}
}

func nonEmptyJSONList(v []string) (string, bool) {
	if len(v) == 0 {
		return "", false
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return string(b), true
}

func providerValues(p interface{ Value() []string }) []string {
	if p == nil {
		return nil
	}
	return p.Value()
}

func desiredClusterAuthConfigData(info clusterauthenticationtrust.ClusterAuthenticationInfo) map[string]string {
	data := map[string]string{}
	if info.ClientCA != nil {
		if b := info.ClientCA.CurrentCABundleContent(); len(b) > 0 {
			data["client-ca-file"] = string(b)
		}
	}
	if info.RequestHeaderCA == nil {
		return data
	}
	requestHeaderCA := info.RequestHeaderCA.CurrentCABundleContent()
	if len(requestHeaderCA) == 0 {
		return data
	}
	data["requestheader-client-ca-file"] = string(requestHeaderCA)
	if s, ok := nonEmptyJSONList(providerValues(info.RequestHeaderUsernameHeaders)); ok {
		data["requestheader-username-headers"] = s
	}
	if s, ok := nonEmptyJSONList(providerValues(info.RequestHeaderUIDHeaders)); ok {
		data["requestheader-uid-headers"] = s
	}
	if s, ok := nonEmptyJSONList(providerValues(info.RequestHeaderGroupHeaders)); ok {
		data["requestheader-group-headers"] = s
	}
	if s, ok := nonEmptyJSONList(providerValues(info.RequestHeaderExtraHeaderPrefixes)); ok {
		data["requestheader-extra-headers-prefix"] = s
	}
	if s, ok := nonEmptyJSONList(providerValues(info.RequestHeaderAllowedNames)); ok {
		data["requestheader-allowed-names"] = s
	}
	return data
}

func sameConfigData(lhs, rhs map[string]string) bool {
	if len(lhs) != len(rhs) {
		return false
	}
	for k, v := range lhs {
		if rhs[k] != v {
			return false
		}
	}
	return true
}

func ensureNamespace(ctx context.Context, cs kubernetes.Interface, ns string) error {
	if cs == nil || ns == "" {
		return nil
	}
	_, err := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	_, err = cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func reconcileLegacyTokenTracking(ctx context.Context, cs kubernetes.Interface) error {
	if cs == nil {
		return fmt.Errorf("nil clientset")
	}
	client := cs.CoreV1().ConfigMaps(metav1.NamespaceSystem)
	cm, err := client.Get(ctx, legacytokentracking.ConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceSystem,
				Name:      legacytokentracking.ConfigMapName,
			},
			Data: map[string]string{
				legacytokentracking.ConfigMapDataKey: time.Now().UTC().Format(legacyTokenDateFormat),
			},
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	if _, parseErr := time.Parse(legacyTokenDateFormat, cm.Data[legacytokentracking.ConfigMapDataKey]); parseErr == nil {
		return nil
	}
	copy := cm.DeepCopy()
	if copy.Data == nil {
		copy.Data = map[string]string{}
	}
	copy.Data[legacytokentracking.ConfigMapDataKey] = time.Now().UTC().Format(legacyTokenDateFormat)
	_, err = client.Update(ctx, copy, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func reconcileClusterAuthenticationTrust(ctx context.Context, cs kubernetes.Interface, info clusterauthenticationtrust.ClusterAuthenticationInfo) error {
	if cs == nil {
		return fmt.Errorf("nil clientset")
	}
	if err := ensureNamespace(ctx, cs, "kube-system"); err != nil {
		return err
	}
	desiredData := desiredClusterAuthConfigData(info)
	client := cs.CoreV1().ConfigMaps("kube-system")
	cm, err := client.Get(ctx, "extension-apiserver-authentication", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "kube-system",
				Name:      "extension-apiserver-authentication",
			},
			Data: desiredData,
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	if sameConfigData(cm.Data, desiredData) {
		return nil
	}
	copy := cm.DeepCopy()
	copy.Data = desiredData
	_, err = client.Update(ctx, copy, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func reconcileInternalControllersForCluster(ctx context.Context, cs kubernetes.Interface, info clusterauthenticationtrust.ClusterAuthenticationInfo) error {
	if err := reconcileLegacyTokenTracking(ctx, cs); err != nil {
		return err
	}
	return reconcileClusterAuthenticationTrust(ctx, cs, info)
}

type InternalControllerOptions struct {
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)
	StopChForCluster func(clusterID string) (<-chan struct{}, error)
	ClusterAuthInfo  clusterauthenticationtrust.ClusterAuthenticationInfo
}

type InternalControllerManager struct {
	opts InternalControllerOptions
	mu   sync.Mutex
	run  map[string]struct{}

	stopCh   <-chan struct{}
	started  bool
	queue    workqueue.TypedRateLimitingInterface[string]
	listener *caQueueListener
}

func NewInternalControllerManager(opts InternalControllerOptions) *InternalControllerManager {
	return &InternalControllerManager{
		opts:  opts,
		run:   map[string]struct{}{},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "mc_internal_controllers"},
		),
	}
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
	if !m.started {
		stopCh, err := m.opts.StopChForCluster(clusterID)
		if err != nil {
			m.mu.Unlock()
			klog.Errorf("mc.bootstrap internal controllers stop channel failed cluster=%s: %v", clusterID, err)
			return
		}
		m.stopCh = stopCh
		m.started = true
		m.startWorkersLocked()
	}
	m.mu.Unlock()
	m.queue.Add(clusterID)
}

func (m *InternalControllerManager) startWorkersLocked() {
	stopCh := m.stopCh
	if stopCh == nil {
		return
	}
	for i := 0; i < internalControllerWorkerCount; i++ {
		go wait.Until(func() { m.runWorker() }, time.Second, stopCh)
	}
	go func() {
		<-stopCh
		m.queue.ShutDown()
	}()
	if m.listener == nil {
		m.listener = &caQueueListener{onChange: m.enqueueAllClusters}
		if m.opts.ClusterAuthInfo.ClientCA != nil {
			m.opts.ClusterAuthInfo.ClientCA.AddListener(m.listener)
		}
		if m.opts.ClusterAuthInfo.RequestHeaderCA != nil {
			m.opts.ClusterAuthInfo.RequestHeaderCA.AddListener(m.listener)
		}
	}
}

func (m *InternalControllerManager) enqueueAllClusters() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for cid := range m.run {
		m.queue.Add(cid)
	}
}

func (m *InternalControllerManager) runWorker() {
	for {
		clusterID, quit := m.queue.Get()
		if quit {
			return
		}
		func() {
			defer m.queue.Done(clusterID)
			cs, err := m.opts.ClientForCluster(clusterID)
			if err != nil {
				klog.Errorf("mc.bootstrap internal controllers client failed cluster=%s: %v", clusterID, err)
				m.queue.AddRateLimited(clusterID)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := reconcileInternalControllersForCluster(ctx, cs, m.opts.ClusterAuthInfo); err != nil {
				klog.Errorf("mc.bootstrap internal controllers reconcile failed cluster=%s: %v", clusterID, err)
				m.queue.AddRateLimited(clusterID)
				return
			}
			m.queue.Forget(clusterID)
		}()
	}
}
