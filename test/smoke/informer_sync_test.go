package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// Ensure shared informers can sync and observe namespace events.
// We then create the default ServiceAccount as a simple controller to validate
// the watch/list path for virtual clusters.
func TestInformerSync_DefaultServiceAccount(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	clusterID := "c-" + randSuffix(3)
	clientset := kubeClientForCluster(t, s, clusterID)

	stopCh := make(chan struct{})
	defer close(stopCh)

	factory := informers.NewSharedInformerFactory(clientset, 0)
	nsInformer := factory.Core().V1().Namespaces().Informer()

	nsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			ns, ok := obj.(*corev1.Namespace)
			if !ok {
				return
			}
			_, err := clientset.CoreV1().ServiceAccounts(ns.Name).Create(context.Background(), &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
			}, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				t.Logf("serviceaccount create failed: %v", err)
			}
		},
	})

	factory.Start(stopCh)
	if ok := cache.WaitForCacheSync(stopCh, nsInformer.HasSynced); !ok {
		t.Fatalf("namespace informer failed to sync")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nsName := "ns-" + randSuffix(4)
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: nsName},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := clientset.CoreV1().ServiceAccounts(nsName).Get(ctx, "default", metav1.GetOptions{})
		if err == nil {
			return true, nil
		}
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	})
	if err != nil {
		t.Fatalf("default serviceaccount not created: %v", err)
	}
}
