package storage

import "strings"

// ObjectPathFromStorageKey extracts namespace and name from a storage key
// of the form <rootPrefix><clusterID>/[<namespace>/]<name>.
// The rootPrefix should include the trailing slash (e.g. ".../clusters/").
func ObjectPathFromStorageKey(storageKey, rootPrefix string) (namespace, name string, ok bool) {
	if !strings.HasPrefix(storageKey, rootPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(storageKey, rootPrefix)
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return "", "", false
	}
	// parts[0] is cluster ID.
	switch len(parts) {
	case 2:
		return "", parts[1], true
	default:
		return parts[1], parts[2], true
	}
}

// ClusterFromStorageKeyAuto extracts cluster ID from a storage key by
// auto-discovering the root prefix from the "/clusters/" segment.
// This is useful when the caller does not know the kind root prefix.
func ClusterFromStorageKeyAuto(storageKey string) string {
	i := strings.Index(storageKey, "/clusters/")
	if i <= 0 {
		return ""
	}
	root := strings.TrimSuffix(storageKey[:i+len("/clusters")], "/")
	resolver := KeyLayoutPlacementResolver{KindRootPrefix: root}
	cid, ok := resolver.ClusterFromStorageKey(storageKey)
	if !ok {
		return ""
	}
	return cid
}
