package multicluster

import (
	"net/http"
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
		cid, all, _ := ex.Extract(r.Context(), r)
		if cid == "" {
			cid = o.DefaultCluster
		}
		next.ServeHTTP(w, r.WithContext(WithCluster(r.Context(), cid, all)))
	})
}
