package storage

import "fmt"

// ClusterScopedObjectKey builds a stable composite key for internal indexes.
func ClusterScopedObjectKey(clusterID, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", clusterID, namespace, name)
}

