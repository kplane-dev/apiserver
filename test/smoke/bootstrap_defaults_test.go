package smoke

import (
	"crypto/tls"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestUpstreamControlPlanePostStartHooksStillPresent(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // test only
		},
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, s.clusterURL(s.root)+"/readyz?verbose=1", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer smoketoken")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read readyz body: %v", err)
	}
	body := string(bodyBytes)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz non-200 status=%d body=%s", resp.StatusCode, body)
	}

	// These hook names are upstream defaults we rely on.
	required := []string{
		"poststarthook/start-system-namespaces-controller",
		"poststarthook/bootstrap-controller",
		"poststarthook/start-cluster-authentication-info-controller",
		"poststarthook/start-legacy-token-tracking-controller",
		"poststarthook/rbac/bootstrap-roles",
	}
	for _, needle := range required {
		if !strings.Contains(body, needle) {
			t.Fatalf("expected readyz to include %q\nbody:\n%s", needle, body)
		}
	}
}
