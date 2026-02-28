package storage

import "strings"

// PlacementResolver resolves cluster identity from storage key metadata.
type PlacementResolver interface {
	ClusterFromStorageKey(storageKey string) (clusterID string, ok bool)
}

// KeyLayoutPlacementResolver resolves cluster identity for keys shaped like:
// <kindRootPrefix>/<clusterID>/...
// where kindRootPrefix is ".../clusters".
type KeyLayoutPlacementResolver struct {
	KindRootPrefix string
}

func (r KeyLayoutPlacementResolver) ClusterFromStorageKey(storageKey string) (string, bool) {
	if strings.TrimSpace(storageKey) == "" {
		return "", false
	}
	root := strings.TrimSuffix(r.KindRootPrefix, "/")
	key := strings.TrimSuffix(storageKey, "/")
	if root == "" || !strings.HasPrefix(key, root+"/") {
		return "", false
	}
	rest := strings.TrimPrefix(key, root+"/")
	if rest == "" {
		return "", false
	}
	i := strings.IndexByte(rest, '/')
	if i <= 0 {
		return rest, true
	}
	return rest[:i], true
}

