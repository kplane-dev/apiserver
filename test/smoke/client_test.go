package smoke

import (
	"crypto/tls"
	"net/http"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func kubeConfigForCluster(base string) *rest.Config {
	// rest.Config.Host can include a path prefix; client-go will append /api, /apis, etc.
	return &rest.Config{
		Host:        base,
		BearerToken: "smoketoken",
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true, // test-only, apiserver uses self-signed certs by default
		},
		// Make sure the transport doesn't try to use the system proxy in CI/dev boxes.
		Proxy: http.ProxyFromEnvironment,
		WrapTransport: func(rt http.RoundTripper) http.RoundTripper {
			if rt == nil {
				rt = http.DefaultTransport
			}
			if t, ok := rt.(*http.Transport); ok {
				cp := t.Clone()
				if cp.TLSClientConfig == nil {
					cp.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test-only
				} else {
					cp.TLSClientConfig = cp.TLSClientConfig.Clone()
					cp.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // test-only
				}
				return cp
			}
			return rt
		},
	}
}

func kubeClientForCluster(t interface {
	Fatalf(string, ...any)
}, s *testAPIServer, clusterID string) *kubernetes.Clientset {
	cfg := kubeConfigForCluster(s.clusterURL(clusterID))
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("failed to build clientset: %v", err)
	}
	return cs
}
