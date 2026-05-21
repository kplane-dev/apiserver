package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	kplanev1 "github.com/kplane-dev/apiserver/pkg/apis/kplane/v1"
)

var fleetGVR = schema.GroupVersionResource{
	Group:    kplanev1.GroupName,
	Version:  kplanev1.Version,
	Resource: "fleets",
}

// TestFleetCreatesMemberVCPs creates a Fleet object in the root control plane
// and verifies that the FleetController primes the member VCPs and reports
// them ready in status.
func TestFleetCreatesMemberVCPs(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dyn := dynamicClientForCluster(t, s, s.root)

	// Wait for the Fleet CRD to be registered by the controller. It runs
	// asynchronously after apiserver startup.
	if err := waitForFleetCRD(ctx, dyn); err != nil {
		t.Fatalf("Fleet CRD never registered: %v\nlogs:\n%s", err, s.logs())
	}

	fleetName := "smoke-" + randSuffix(3)
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": kplanev1.GroupName + "/" + kplanev1.Version,
		"kind":       "Fleet",
		"metadata":   map[string]interface{}{"name": fleetName},
		"spec": map[string]interface{}{
			"replicas":   int64(2),
			"namePrefix": fleetName + "-",
		},
	}}

	if _, err := dyn.Resource(fleetGVR).Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create Fleet: %v", err)
	}
	t.Cleanup(func() {
		_ = dyn.Resource(fleetGVR).Delete(context.Background(), fleetName, metav1.DeleteOptions{})
	})

	deadline := time.Now().Add(90 * time.Second)
	var lastReady int64
	for time.Now().Before(deadline) {
		got, err := dyn.Resource(fleetGVR).Get(ctx, fleetName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			t.Fatalf("get Fleet: %v", err)
		}
		ready, _, _ := unstructured.NestedInt64(got.Object, "status", "readyReplicas")
		lastReady = ready
		if ready >= 2 {
			members, _, _ := unstructured.NestedSlice(got.Object, "status", "members")
			if len(members) == 2 {
				return
			}
		}
		time.Sleep(750 * time.Millisecond)
	}

	t.Fatalf("Fleet %s did not reach readyReplicas=2 in 90s (last=%d)\nlogs:\n%s",
		fleetName, lastReady, s.logs())
}

func waitForFleetCRD(ctx context.Context, dyn dynamic.Interface) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		_, err := dyn.Resource(fleetGVR).List(ctx, metav1.ListOptions{Limit: 1})
		if err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

