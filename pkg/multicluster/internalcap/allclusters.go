package internalcap

import "context"

type allClustersCapabilityKey struct{}

// WithAllClustersCapability marks context as trusted to access all-clusters reads.
func WithAllClustersCapability(ctx context.Context) context.Context {
	return context.WithValue(ctx, allClustersCapabilityKey{}, true)
}

// HasAllClustersCapability reports whether the context has trusted all-clusters capability.
func HasAllClustersCapability(ctx context.Context) bool {
	v, _ := ctx.Value(allClustersCapabilityKey{}).(bool)
	return v
}
