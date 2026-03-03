package storage

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/watch"

	extstorage "github.com/kplane-dev/storage"
)

// TestInternalEntry_IsObjectWithClusterIdentity confirms the type alias works:
// InternalEntry and ObjectWithClusterIdentity are the same type.
func TestInternalEntry_IsObjectWithClusterIdentity(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx",
			Namespace: "default",
		},
	}

	// Create as external ObjectWithClusterIdentity
	ext := &extstorage.ObjectWithClusterIdentity{
		Object:    pod,
		ClusterID: "c1",
	}

	// Assign to InternalEntry (alias — same type, zero-cost)
	var ie *InternalEntry = ext

	if ie.ClusterID != "c1" {
		t.Errorf("ClusterID mismatch: got %q, want c1", ie.ClusterID)
	}
	if ie.Object != pod {
		t.Error("Object mismatch after alias assignment")
	}
}

// TestObjectWithClusterIdentity_ImplementsMetaAccessor verifies that the
// external type (used via alias) properly delegates metav1.Object methods.
func TestObjectWithClusterIdentity_ImplementsMetaAccessor(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "nginx",
			Namespace:       "default",
			ResourceVersion: "42",
			Labels:          map[string]string{"app": "web"},
		},
	}

	entry := &InternalEntry{
		Object:    pod,
		ClusterID: "c1",
	}

	// meta.Accessor should work on the entry because identity_meta.go
	// delegates to the inner object
	acc, err := meta.Accessor(entry)
	if err != nil {
		t.Fatalf("meta.Accessor failed: %v", err)
	}
	if acc.GetName() != "nginx" {
		t.Errorf("GetName() = %q, want nginx", acc.GetName())
	}
	if acc.GetNamespace() != "default" {
		t.Errorf("GetNamespace() = %q, want default", acc.GetNamespace())
	}
	if acc.GetResourceVersion() != "42" {
		t.Errorf("GetResourceVersion() = %q, want 42", acc.GetResourceVersion())
	}
	if acc.GetLabels()["app"] != "web" {
		t.Errorf("GetLabels() missing app=web")
	}
}

// TestEntryWatch_UnwrapsObjectWithClusterIdentity verifies that EntryWatch
// properly unwraps the ObjectWithClusterIdentity envelope (via alias).
func TestEntryWatch_UnwrapsObjectWithClusterIdentity(t *testing.T) {
	fakeWatcher := watch.NewFake()

	ew := NewEntryWatch(fakeWatcher)
	defer ew.Stop()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx",
			Namespace: "default",
		},
	}

	// Send an event with the ObjectWithClusterIdentity envelope
	go func() {
		fakeWatcher.Add(&InternalEntry{
			Object:    pod,
			ClusterID: "c1",
		})
	}()

	event := <-ew.ResultChan()
	if event.Type != watch.Added {
		t.Fatalf("expected Added, got %v", event.Type)
	}

	// The event object should be the inner pod, not the envelope
	unwrapped, ok := event.Object.(*corev1.Pod)
	if !ok {
		t.Fatalf("expected *corev1.Pod, got %T", event.Object)
	}
	if unwrapped.Name != "nginx" {
		t.Errorf("name = %q, want nginx", unwrapped.Name)
	}
}

// TestNoLabelsOnUnwrappedObjects confirms that the ObjectWithClusterIdentity
// envelope does not inject any labels or annotations onto the inner object.
func TestNoLabelsOnUnwrappedObjects(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx",
			Namespace: "default",
		},
	}

	entry := &InternalEntry{
		Object:     pod,
		ClusterID:  "c1",
		StorageKey: "/pods/clusters/c1/default/nginx",
	}

	// Verify no labels or annotations were added to the inner object
	if len(pod.Labels) != 0 {
		t.Errorf("inner object has labels: %v", pod.Labels)
	}
	if len(pod.Annotations) != 0 {
		t.Errorf("inner object has annotations: %v", pod.Annotations)
	}

	// Access through entry — still no labels/annotations
	acc, err := meta.Accessor(entry)
	if err != nil {
		t.Fatalf("meta.Accessor: %v", err)
	}
	if len(acc.GetLabels()) != 0 {
		t.Errorf("envelope accessor returned labels: %v", acc.GetLabels())
	}
	if len(acc.GetAnnotations()) != 0 {
		t.Errorf("envelope accessor returned annotations: %v", acc.GetAnnotations())
	}

	// Deep copy — still clean
	copied := entry.DeepCopyObject().(*InternalEntry)
	innerPod := copied.Object.(*corev1.Pod)
	if len(innerPod.Labels) != 0 {
		t.Errorf("deep-copied inner object has labels: %v", innerPod.Labels)
	}
	if len(innerPod.Annotations) != 0 {
		t.Errorf("deep-copied inner object has annotations: %v", innerPod.Annotations)
	}
}
