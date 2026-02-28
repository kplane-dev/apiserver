package multicluster

import "k8s.io/apimachinery/pkg/runtime"

// SetObjectClusterIdentity intentionally does not persist identity on objects.
func SetObjectClusterIdentity(_ runtime.Object, _ string) {}

// ClearObjectClusterIdentity intentionally no-ops.
func ClearObjectClusterIdentity(_ runtime.Object) {}

// ObjectClusterIdentity intentionally returns empty to avoid metadata coupling.
func ObjectClusterIdentity(_ interface{}) string {
	return ""
}

