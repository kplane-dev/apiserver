package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
)

func TestListWatchIsolationAcrossClusters(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	clusterB := "c-" + randSuffix(3)
	csA := kubeClientForCluster(t, s, clusterA)
	csB := kubeClientForCluster(t, s, clusterB)

	ns := "ns-" + randSuffix(4)
	if _, err := csA.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("cluster=%s create namespace: %v", clusterA, err)
	}
	if _, err := csB.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("cluster=%s create namespace: %v", clusterB, err)
	}

	wA, err := csA.CoreV1().ConfigMaps(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: "0"})
	if err != nil {
		t.Fatalf("cluster=%s watch configmaps: %v", clusterA, err)
	}
	defer wA.Stop()
	wB, err := csB.CoreV1().ConfigMaps(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: "0"})
	if err != nil {
		t.Fatalf("cluster=%s watch configmaps: %v", clusterB, err)
	}
	defer wB.Stop()

	cmShared := "cm-shared-" + randSuffix(3)
	cmAOnly := "cm-a-" + randSuffix(3)
	cmBOnly := "cm-b-" + randSuffix(3)

	_, err = csA.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmShared},
		Data:       map[string]string{"owner": "a"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create shared cm: %v", clusterA, err)
	}
	_, err = csB.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmShared},
		Data:       map[string]string{"owner": "b"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create shared cm: %v", clusterB, err)
	}
	_, err = csA.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmAOnly},
		Data:       map[string]string{"owner": "a"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create a-only cm: %v", clusterA, err)
	}
	_, err = csB.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmBOnly},
		Data:       map[string]string{"owner": "b"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create b-only cm: %v", clusterB, err)
	}

	waitForList := func(client string, listFn func(context.Context, metav1.ListOptions) (*corev1.ConfigMapList, error), want map[string]string) {
		err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
			list, err := listFn(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			seen := map[string]string{}
			for i := range list.Items {
				seen[list.Items[i].Name] = list.Items[i].Data["owner"]
			}
			for name, owner := range want {
				if got, ok := seen[name]; !ok || got != owner {
					return false, nil
				}
			}
			if _, ok := seen[cmAOnly]; ok && want[cmAOnly] == "" {
				return false, nil
			}
			if _, ok := seen[cmBOnly]; ok && want[cmBOnly] == "" {
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatalf("%s list isolation failed: %v", client, err)
		}
	}

	waitForList("clusterA", csA.CoreV1().ConfigMaps(ns).List, map[string]string{
		cmShared: "a",
		cmAOnly:  "a",
	})
	waitForList("clusterB", csB.CoreV1().ConfigMaps(ns).List, map[string]string{
		cmShared: "b",
		cmBOnly:  "b",
	})

	expectWatch := func(client string, w watch.Interface, want map[string]string) {
		seen := map[string]bool{}
		deadline := time.After(5 * time.Second)
		for len(seen) < len(want) {
			select {
			case <-deadline:
				t.Fatalf("%s watch did not observe all expected configmaps: %v", client, want)
			case evt := <-w.ResultChan():
				if evt.Type == watch.Bookmark || evt.Object == nil {
					continue
				}
				cm, ok := evt.Object.(*corev1.ConfigMap)
				if !ok {
					continue
				}
				if owner, ok := want[cm.Name]; ok {
					if got := cm.Data["owner"]; got != owner {
						t.Fatalf("%s watch got wrong owner for %s: %s", client, cm.Name, got)
					}
					seen[cm.Name] = true
					continue
				}
				t.Fatalf("%s watch observed unexpected configmap %s", client, cm.Name)
			}
		}
	}

	expectWatch("clusterA", wA, map[string]string{
		cmShared: "a",
		cmAOnly:  "a",
	})
	expectWatch("clusterB", wB, map[string]string{
		cmShared: "b",
		cmBOnly:  "b",
	})
}

func TestListWatchReturnsObjectsInCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterID := "c-" + randSuffix(3)
	ns := "ns-" + randSuffix(4)

	cs := kubeClientForCluster(t, s, clusterID)

	_, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create namespace: %v", clusterID, err)
	}

	cmName := "cm-" + randSuffix(4)
	_, err = cs.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmName},
		Data:       map[string]string{"ok": "true"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create configmap: %v", clusterID, err)
	}

	list, err := cs.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("cluster=%s list configmaps: %v", clusterID, err)
	}
	found := false
	for _, cm := range list.Items {
		if cm.Name == cmName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("cluster=%s list missing configmap %s\nlogs:\n%s", clusterID, cmName, s.logs())
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watcher, err := cs.CoreV1().ConfigMaps(ns).Watch(watchCtx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("cluster=%s watch configmaps: %v", clusterID, err)
	}
	defer watcher.Stop()

	cmName2 := "cm-" + randSuffix(4)
	_, err = cs.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmName2},
		Data:       map[string]string{"ok": "true"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create configmap 2: %v", clusterID, err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Object == nil {
				continue
			}
			if event.Type != watch.Added {
				continue
			}
			cm := event.Object.(*corev1.ConfigMap)
			if cm.Name == cmName2 {
				return
			}
			// Ignore initial events for existing objects.
		case <-deadline:
			t.Fatalf("cluster=%s timed out waiting for watch event\nlogs:\n%s", clusterID, s.logs())
		}
	}
}
