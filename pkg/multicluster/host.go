package multicluster

import (
	"net/url"
	"strings"
)

// ClusterHost returns a copy of host with the cluster path prefix appended.
func ClusterHost(host string, opts Options, clusterID string) (string, error) {
	return withClusterPrefix(host, opts.PathPrefix, clusterID, opts.ControlPlaneSegment)
}

func withClusterPrefix(host, pathPrefix, clusterID, controlPlaneSegment string) (string, error) {
	u, err := url.Parse(host)
	if err != nil {
		return "", err
	}
	pp := pathPrefix
	if pp == "" {
		pp = DefaultPathPrefix
	}
	seg := controlPlaneSegment
	if seg == "" {
		seg = DefaultControlPlaneSegment
	}
	// ensure pp ends with "/"
	if !strings.HasSuffix(pp, "/") {
		pp = pp + "/"
	}
	// join onto existing path
	basePath := strings.TrimRight(u.Path, "/")
	u.Path = basePath + pp + clusterID + "/" + seg
	return u.String(), nil
}
