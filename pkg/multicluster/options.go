package multicluster

import (
	"context"
	"net/http"

	"github.com/kplane-dev/apiserver/pkg/multicluster/internalcap"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// Options holds multicluster configuration.
// EtcdPrefix must match the apiserver's configured etcd prefix (e.g. "/registry").
// DefaultCluster is used when no cluster can be extracted.
// PathPrefix and ControlPlaneSegment define the URL form for path-based extraction.
// ClusterFieldKey is a synthetic field name exposed via AttrFunc for selectors.
// WatchStrategy selects shared kind-root or per-cluster watch behavior.
// ClusterSource optionally provides cluster IDs for per-cluster watch.
//
// These defaults are safe and aim for lowest watch cardinality.

// SpannerConfig holds connection details for a Spanner backend.
// When set on Options, storage uses Spanner instead of etcd.
type SpannerConfig struct {
	Project      string
	Instance     string
	Database     string
	EmulatorHost string // for development/testing
}

type Options struct {
	EtcdPrefix           string
	DefaultCluster       string
	PathPrefix           string
	ControlPlaneSegment  string
	ClusterFieldKey      string
	WatchStrategy        WatchStrategy
	ClusterSource        ClusterSource
	// OnClusterSelected is invoked by routing middleware after cluster extraction.
	// It can be used to lazily initialize per-cluster bootstrap state.
	OnClusterSelected func(clusterID string)
	// ServerName identifies the apiserver instance using this options set (for metrics/logging).
	ServerName string
	// InformerRegistry provides MultiClusterInformer instances for resource types.
	// Set by config.go and used by storage.go to register clusteredStorage instances.
	InformerRegistry *InformerRegistry
	// Spanner, when non-nil, selects Spanner as the storage backend instead of etcd.
	Spanner *SpannerConfig
}

// ResourceScope defines which keyspace view a request should use.
type ResourceScope string

const (
	// ResourceScopeCluster scopes reads/writes to one cluster keyspace.
	ResourceScopeCluster ResourceScope = "cluster"
	// ResourceScopeCrossClusterRead scopes reads to the shared all-clusters keyspace.
	// This scope is intended for internal readers only.
	ResourceScopeCrossClusterRead ResourceScope = "cross-cluster-read"
)

const (
	DefaultEtcdPrefix                     = "/registry"
	DefaultClusterName                    = "root"
	DefaultPathPrefix                     = "/clusters/"
	DefaultControlPlaneSegment            = "control-plane"
	DefaultInternalCrossClusterUser       = "system:apiserver"
	DefaultInternalCrossClusterCapability = "kplane.internal/cross-cluster-read"
	DefaultInternalCrossClusterUserAgent  = "kplane-internal-cross-cluster"
	DefaultClusterField                   = "metadata.cluster"
)

var DefaultOptions = Options{
	EtcdPrefix:           DefaultEtcdPrefix,
	DefaultCluster:       DefaultClusterName,
	PathPrefix:           DefaultPathPrefix,
	ControlPlaneSegment:  DefaultControlPlaneSegment,
	ClusterFieldKey:      DefaultClusterField,
	WatchStrategy:        KindRootWatch{},
}

// Extractor extracts the cluster selection from an HTTP request/context.
// all=false indicates per-cluster scope.
// all=true maps to all-clusters scope and should only be set by trusted internal code.

type Extractor interface {
	Extract(ctx context.Context, r *http.Request) (clusterID string, all bool, err error)
}

// PathExtractor extracts /clusters/{cluster}/control-plane/... and normalizes the path.

type PathExtractor struct {
	PathPrefix          string
	ControlPlaneSegment string
}

func (p PathExtractor) Extract(ctx context.Context, r *http.Request) (string, bool, error) {
	prefix := p.PathPrefix
	if prefix == "" {
		prefix = DefaultPathPrefix
	}
	segment := p.ControlPlaneSegment
	if segment == "" {
		segment = DefaultControlPlaneSegment
	}
	path := r.URL.Path
	if len(path) >= len(prefix) && path[:len(prefix)] == prefix {
		rest := path[len(prefix):]
		// rest = {cluster}/...
		for i := 0; i < len(rest); i++ {
			if rest[i] == '/' {
				cluster := rest[:i]
				// optionally strip control-plane segment
				after := rest[i+1:]
				if len(after) >= len(segment) && after[:len(segment)] == segment {
					trim := after[len(segment):]
					if len(trim) > 0 && trim[0] == '/' {
						trim = trim[1:]
					}
					newPath := "/" + trim
					if newPath == "/" {
						newPath = "/"
					}
					r.URL.Path = newPath
					r.URL.RawPath = newPath
				}
				return cluster, false, nil
			}
		}
		// no slash after cluster, treat rest as cluster id
		if rest != "" {
			r.URL.Path = "/"
			r.URL.RawPath = "/"
			return rest, false, nil
		}
	}
	return "", false, nil
}

// HeaderExtractor reads a cluster id from a header, e.g. X-Cluster.

type HeaderExtractor struct{ Header string }

func (h HeaderExtractor) Extract(ctx context.Context, r *http.Request) (string, bool, error) {
	name := h.Header
	if name == "" {
		name = "X-Cluster"
	}
	val := r.Header.Get(name)
	if val != "" {
		return val, false, nil
	}
	return "", false, nil
}

// ComposeExtractor tries multiple extractors; first non-empty wins.

type ComposeExtractor []Extractor

func (c ComposeExtractor) Extract(ctx context.Context, r *http.Request) (string, bool, error) {
	for _, ex := range c {
		if ex == nil {
			continue
		}
		cid, all, err := ex.Extract(ctx, r)
		if err != nil {
			return "", false, err
		}
		if cid != "" || all {
			return cid, all, nil
		}
	}
	return "", false, nil
}

// Context helpers

type clusterContextKey struct{}

type clusterSelection struct {
	ID    string
	Scope ResourceScope
}

func WithCluster(ctx context.Context, id string, all bool) context.Context {
	scope := ResourceScopeCluster
	if all {
		scope = ResourceScopeCrossClusterRead
	}
	return WithClusterScope(ctx, id, scope)
}

func WithClusterScope(ctx context.Context, id string, scope ResourceScope) context.Context {
	if scope == "" {
		scope = ResourceScopeCluster
	}
	return context.WithValue(ctx, clusterContextKey{}, clusterSelection{ID: id, Scope: scope})
}

// WithInternalCrossClusterRead marks context for trusted internal cross-cluster reads.
func WithInternalCrossClusterRead(ctx context.Context, id string) context.Context {
	ctx = WithClusterScope(ctx, id, ResourceScopeCrossClusterRead)
	return internalcap.WithAllClustersCapability(ctx)
}

func FromContext(ctx context.Context) (id string, all bool, ok bool) {
	id, scope, ok := FromContextScope(ctx)
	return id, scope == ResourceScopeCrossClusterRead, ok
}

func FromContextScope(ctx context.Context) (id string, scope ResourceScope, ok bool) {
	v := ctx.Value(clusterContextKey{})
	if v == nil {
		return "", ResourceScopeCluster, false
	}
	cs := v.(clusterSelection)
	if cs.Scope == "" {
		cs.Scope = ResourceScopeCluster
	}
	return cs.ID, cs.Scope, true
}

// HasInternalCrossClusterCapability reports whether the authenticated request user
// is trusted for internal cross-cluster reads.
func HasInternalCrossClusterCapability(ctx context.Context) bool {
	u, ok := apirequest.UserFrom(ctx)
	if !ok || u == nil || u.GetName() != DefaultInternalCrossClusterUser {
		return false
	}
	extras := u.GetExtra()
	if len(extras) == 0 {
		return false
	}
	for _, v := range extras[DefaultInternalCrossClusterCapability] {
		if v == "true" {
			return true
		}
	}
	return false
}

// Watch strategy plumbing

type WatchStrategy interface {
	// WatchPrefixes returns list/watch prefixes to open per group/resource based on layout.
	WatchPrefixes(ctx context.Context, layout KeyLayout, gr GroupResource) ([]string, error)
}

type KindRootWatch struct{}

func (KindRootWatch) WatchPrefixes(ctx context.Context, layout KeyLayout, gr GroupResource) ([]string, error) {
	return []string{layout.KindRoot(gr)}, nil
}

type PerClusterWatch struct{}

func (PerClusterWatch) WatchPrefixes(ctx context.Context, layout KeyLayout, gr GroupResource) ([]string, error) {
	cid, _, _ := FromContext(ctx)
	if cid == "" {
		cid = layout.Options.DefaultCluster
	}
	if cid == "" {
		cid = DefaultClusterName
	}
	return []string{layout.PerClusterRoot(cid, gr)}, nil
}

// ClusterSource allows discovering available clusters (used for fanout watches)

type ClusterSource interface {
	ListClusters(ctx context.Context) ([]string, error)
}

// Key layout helpers

type GroupResource struct{ Group, Resource string }

type KeyLayout struct{ Options Options }

func (l KeyLayout) KindRoot(gr GroupResource) string {
	p := l.Options.EtcdPrefix
	if p == "" {
		p = DefaultEtcdPrefix
	}
	if gr.Group == "" { // core
		return p + "/" + gr.Resource + "/clusters"
	}
	return p + "/" + gr.Group + "/" + gr.Resource + "/clusters"
}

// PerClusterRoot returns the per-cluster layout root for a resource kind.
func (l KeyLayout) PerClusterRoot(cluster string, gr GroupResource) string {
	p := l.Options.EtcdPrefix
	if p == "" {
		p = DefaultEtcdPrefix
	}
	if gr.Group == "" {
		return p + "/" + gr.Resource + "/clusters/" + cluster
	}
	return p + "/" + gr.Group + "/" + gr.Resource + "/clusters/" + cluster
}
