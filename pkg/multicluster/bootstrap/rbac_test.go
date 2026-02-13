package bootstrap

import (
	"testing"
	"time"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"k8s.io/client-go/rest"
)

func TestRBACBootstrapperSkipsNilBaseConfig(t *testing.T) {
	b := NewRBACBootstrapper(RBACOptions{
		BaseLoopbackClientConfig: nil,
		Timeout:                  time.Second,
	})
	// Should no-op and not panic.
	b.Ensure("vc-1")
}

func TestRBACBootstrapperBuildsClusterHost(t *testing.T) {
	b := NewRBACBootstrapper(RBACOptions{
		BaseLoopbackClientConfig: &rest.Config{Host: "https://127.0.0.1:6443"},
		PathPrefix:               mc.DefaultPathPrefix,
		ControlPlaneSegment:      mc.DefaultControlPlaneSegment,
		Timeout:                  time.Second,
	})

	cfg := rest.CopyConfig(b.base)
	host, err := mc.ClusterHost(cfg.Host, mc.Options{
		PathPrefix:          b.pathPrefix,
		ControlPlaneSegment: b.controlPlaneSegment,
	}, "vc-1")
	if err != nil {
		t.Fatalf("ClusterHost() error = %v", err)
	}
	want := "https://127.0.0.1:6443/clusters/vc-1/control-plane"
	if host != want {
		t.Fatalf("unexpected host: got %q want %q", host, want)
	}
}
