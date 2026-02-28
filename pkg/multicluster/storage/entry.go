package storage

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// InternalEntry carries key-aware multicluster metadata for storage/cache paths.
// It is never serialized to API clients.
type InternalEntry struct {
	Object        runtime.Object
	ClusterID     string
	StorageKey    string
	LayoutVersion string
}

func (e *InternalEntry) DeepCopyObject() runtime.Object {
	if e == nil {
		return nil
	}
	out := *e
	if e.Object != nil {
		out.Object = e.Object.DeepCopyObject()
	}
	return &out
}

func (e *InternalEntry) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}

