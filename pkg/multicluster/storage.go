package multicluster

import (
	"context"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
)

// RESTOptionsDecorator wraps the underlying getter to inject a decorator that
// wraps the storage.Interface with our key-rewriting adapter so keys include cluster.

type RESTOptionsDecorator struct {
	Delegate generic.RESTOptionsGetter
	Options  Options
}

func (w RESTOptionsDecorator) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	opts, err := w.Delegate.GetRESTOptions(resource, example)
	if err != nil {
		return opts, err
	}
	base := opts.Decorator
	if base == nil {
		base = generic.UndecoratedStorage
	}
	layout := KeyLayout{Options: w.Options}
	opts.Decorator = func(
		config *storagebackend.ConfigForResource,
		resourcePrefix string,
		keyFunc func(obj runtime.Object) (string, error),
		newFunc func() runtime.Object,
		newListFunc func() runtime.Object,
		getAttrsFunc storage.AttrFunc,
		trigger storage.IndexerFuncs,
		indexers *cache.Indexers,
	) (storage.Interface, factory.DestroyFunc, error) {
		s, destroy, err := base(config, resourcePrefix, keyFunc, newFunc, newListFunc, getAttrsFunc, trigger, indexers)
		if err != nil {
			return s, destroy, err
		}
		return keyRewritingStorage{Interface: s, Options: w.Options, Layout: layout}, destroy, nil
	}
	return opts, nil
}

// keyRewritingStorage wraps a storage.Interface and injects a cluster segment
// after the resource in keys for CRUD based on request context. For list/watch,
// it keeps kind-root prefixes intact (KindRoot strategy) and filters by annotation.

type keyRewritingStorage struct {
	storage.Interface
	Options Options
	Layout  KeyLayout
}

func (w keyRewritingStorage) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	key = w.rewriteKey(ctx, key, false)
	return w.Interface.Get(ctx, key, opts, objPtr)
}

func (w keyRewritingStorage) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	key = w.rewriteKey(ctx, key, false)
	return w.Interface.Create(ctx, key, obj, out, ttl)
}

func (w keyRewritingStorage) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	key = w.rewriteKey(ctx, key, false)
	return w.Interface.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts)
}

func (w keyRewritingStorage) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	key = w.rewriteKey(ctx, key, true)
	if err := w.Interface.GetList(ctx, key, opts, listObj); err != nil {
		return err
	}
	cid, all, _ := FromContext(ctx)
	if all || cid == "" {
		return nil
	}
	return filterListByCluster(listObj, cid, w.Options.ClusterAnnotationKey)
}

func (w keyRewritingStorage) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	key = w.rewriteKey(ctx, key, true)
	wi, err := w.Interface.Watch(ctx, key, opts)
	if err != nil {
		return nil, err
	}
	cid, all, _ := FromContext(ctx)
	if all || cid == "" {
		return wi, nil
	}
	ann := w.Options.ClusterAnnotationKey
	return watch.Filter(wi, func(in watch.Event) (watch.Event, bool) {
		if in.Object == nil {
			return in, true
		}
		if matchesCluster(in.Object, cid, ann) {
			return in, true
		}
		return in, false
	}), nil
}

func (w keyRewritingStorage) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, precond *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	key = w.rewriteKey(ctx, key, false)
	return w.Interface.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, precond, tryUpdate, cachedExistingObject)
}

func (w keyRewritingStorage) rewriteKey(ctx context.Context, key string, isPrefix bool) string {
	cid, _, _ := FromContext(ctx)
	if cid == "" {
		cid = w.Options.DefaultCluster
	}
	if cid == "" {
		cid = DefaultClusterName
	}
	if isPrefix {
		// KindRoot strategy: keep kind-root prefixes unchanged for shared watches
		return key
	}
	return injectClusterSegment(key, cid, w.Layout)
}

func injectClusterSegment(key, cluster string, layout KeyLayout) string {
	if cluster == "" {
		cluster = DefaultClusterName
	}
	leadingSlash := strings.HasPrefix(key, "/")
	trimmed := strings.TrimPrefix(key, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		if leadingSlash {
			return "/"
		}
		return key
	}
	// Compute position after kind-root: /<prefix>/<group?>/<resource>/... => insert after resource
	resIdx := 0
	p := layout.Options.EtcdPrefix
	if p == "" {
		p = DefaultEtcdPrefix
	}
	if parts[0] == strings.TrimPrefix(p, "/") { // starts with etcd prefix sans leading '/'
		if len(parts) >= 3 && strings.Contains(parts[1], ".") {
			resIdx = 2
		} else {
			resIdx = 1
		}
	} else if strings.Contains(parts[0], ".") {
		resIdx = 1
	} else {
		resIdx = 0
	}
	if resIdx >= len(parts) {
		resIdx = len(parts) - 1
	}
	insertPos := resIdx + 1
	if insertPos < len(parts) && parts[insertPos] == cluster {
		if leadingSlash {
			return "/" + strings.Join(parts, "/")
		}
		return strings.Join(parts, "/")
	}
	newParts := append(append([]string{}, parts[:insertPos]...), append([]string{cluster}, parts[insertPos:]...)...)
	if leadingSlash {
		return "/" + strings.Join(newParts, "/")
	}
	return strings.Join(newParts, "/")
}

func matchesCluster(obj runtime.Object, cid, annotationKey string) bool {
	if annotationKey == "" {
		annotationKey = DefaultClusterAnnotation
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return false
	}
	anns := acc.GetAnnotations()
	if anns == nil {
		return false
	}
	return anns[annotationKey] == cid
}

func filterListByCluster(listObj runtime.Object, cid, annotationKey string) error {
	items, err := meta.ExtractList(listObj)
	if err != nil {
		return err
	}
	filtered := make([]runtime.Object, 0, len(items))
	for _, it := range items {
		if matchesCluster(it, cid, annotationKey) {
			filtered = append(filtered, it)
		}
	}
	return meta.SetList(listObj, filtered)
}
