package multicluster

import (
	"net/http"

	"github.com/kplane-dev/apiserver/pkg/multicluster/internalcap"
)

// WithClusterRouting wraps an http.Handler to extract cluster from the request using the provided Extractor,
// defaulting to Options.DefaultCluster if empty, and normalizes the path when using PathExtractor semantics.
func WithClusterRouting(next http.Handler, ex Extractor, o Options) http.Handler {
	if ex == nil {
		// default: path extractor
		ex = PathExtractor{PathPrefix: o.PathPrefix, ControlPlaneSegment: o.ControlPlaneSegment}
	}
	if o.DefaultCluster == "" {
		o.DefaultCluster = DefaultClusterName
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid, all, err := ex.Extract(r.Context(), r)
		if err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if cid == "" {
			if existingCID, existingAll, ok := FromContext(r.Context()); ok && existingCID != "" {
				cid = existingCID
				all = existingAll
			} else {
				cid = o.DefaultCluster
			}
		}
		if o.OnClusterSelected != nil && cid != "" {
			o.OnClusterSelected(cid)
		}
		ctx := WithCluster(r.Context(), cid, all)
		if all {
			ctx = internalcap.WithAllClustersCapability(ctx)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
