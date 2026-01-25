package smoke

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func randSuffix(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func TestCRUD_RandomizedAcrossClusters(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clusters := []string{
		"root",
		"c-" + randSuffix(3),
		"c-" + randSuffix(3),
	}

	for _, cid := range clusters {
		cs := kubeClientForCluster(t, s, cid)

		ns := "default"
		cmName := "cm-" + randSuffix(4)
		_, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: cmName,
			},
			Data: map[string]string{"hello": "world"},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("cluster=%s create configmap: %v", cid, err)
		}

		got, err := cs.CoreV1().ConfigMaps(ns).Get(ctx, cmName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("cluster=%s get configmap: %v", cid, err)
		}
		if got.Data["hello"] != "world" {
			t.Fatalf("cluster=%s got unexpected data: %#v", cid, got.Data)
		}

		got.Data["hello"] = "there"
		_, err = cs.CoreV1().ConfigMaps(ns).Update(ctx, got, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("cluster=%s update configmap: %v", cid, err)
		}

		got2, err := cs.CoreV1().ConfigMaps(ns).Get(ctx, cmName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("cluster=%s get configmap (after update): %v", cid, err)
		}
		if got2.Data["hello"] != "there" {
			t.Fatalf("cluster=%s got unexpected data after update: %#v", cid, got2.Data)
		}

		// Delete the configmap
		err = cs.CoreV1().ConfigMaps(ns).Delete(ctx, cmName, metav1.DeleteOptions{})
		if err != nil {
			t.Fatalf("cluster=%s delete configmap: %v", cid, err)
		}
	}

	// Isolation check: create in one cluster, ensure not visible in another.
	if len(clusters) < 2 {
		return
	}
	cA, cB := clusters[1], clusters[2]
	csA := kubeClientForCluster(t, s, cA)
	csB := kubeClientForCluster(t, s, cB)

	ns := "default"
	cmName := "cm-" + randSuffix(4)
	_, err := csA.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmName},
		Data:       map[string]string{"x": "y"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create configmap: %v", cA, err)
	}

	_, err = csB.CoreV1().ConfigMaps(ns).Get(ctx, cmName, metav1.GetOptions{})
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("expected notfound in other cluster (%s), got: %v", cB, err)
	}

	t.Logf("clusters tested: %s", fmt.Sprintf("%v", clusters))
}
