package smoke

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type testAPIServer struct {
	t       *testing.T
	binPath string
	port    int
	tmpDir  string

	cmd    *exec.Cmd
	cancel context.CancelFunc

	logMu sync.Mutex
	log   strings.Builder
}

func mustFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func buildAPIServerBinary(t *testing.T) string {
	t.Helper()

	tmp := t.TempDir()
	out := filepath.Join(tmp, "apiserver.testbin")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/apiserver")
	cmd.Env = os.Environ()
	cmd.Dir = repoRoot(t)
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build apiserver: %v\n%s", err, string(b))
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find go.mod from %s", wd)
		}
		dir = parent
	}
}

func mustWriteRSAKey(t *testing.T, path string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return path
}

func mustWriteTokenFile(t *testing.T, path string) string {
	t.Helper()
	const token = "smoketoken"
	line := fmt.Sprintf("%s,admin,admin,system:masters\n", token)
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	return path
}

func startAPIServer(t *testing.T, etcdEndpoints string) *testAPIServer {
	t.Helper()
	if strings.TrimSpace(etcdEndpoints) == "" {
		t.Skip("ETCD_ENDPOINTS is not set; skipping integration smoke tests")
	}

	bin := buildAPIServerBinary(t)
	port := mustFreePort(t)
	tmp := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	s := &testAPIServer{
		t:       t,
		binPath: bin,
		port:    port,
		tmpDir:  tmp,
		cancel:  cancel,
	}

	args := []string{
		"--etcd-servers=" + etcdEndpoints,
		"--cert-dir=" + filepath.Join(tmp, "certs"),
		"--secure-port=" + fmt.Sprintf("%d", port),
		"--enable-aggregator-routing=true",
		"--authorization-mode=RBAC",
		"--anonymous-auth=true",
		"--token-auth-file=" + mustWriteTokenFile(t, filepath.Join(tmp, "tokens.csv")),
		"--allow-privileged=true",
		"--service-cluster-ip-range=10.0.0.0/24",
		"--service-account-issuer=test",
		"--service-account-signing-key-file=" + mustWriteRSAKey(t, filepath.Join(tmp, "sa.key")),
		"--service-account-key-file=" + filepath.Join(tmp, "sa.key"),
		// reduce noise + make startup faster for tests
		"--v=2",
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = os.Environ()
	// keep stdout/stderr for debugging; buffer in-memory too
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start apiserver: %v", err)
	}
	s.cmd = cmd

	// Best-effort log capture
	go s.capture("stdout", stdout)
	go s.capture("stderr", stderr)

	t.Cleanup(func() {
		s.Stop()
	})

	// Wait for readiness on root cluster
	s.waitReady(t, "root")

	return s
}

func (s *testAPIServer) capture(stream string, r io.ReadCloser) {
	defer r.Close()
	sc := bufio.NewScanner(r)
	// allow long log lines
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 2*1024*1024)

	s.logMu.Lock()
	s.log.WriteString("\n--- " + stream + " ---\n")
	s.logMu.Unlock()

	for sc.Scan() {
		s.logMu.Lock()
		s.log.WriteString(sc.Text())
		s.log.WriteString("\n")
		s.logMu.Unlock()
	}
}

func (s *testAPIServer) baseURL() string {
	return fmt.Sprintf("https://127.0.0.1:%d", s.port)
}

func (s *testAPIServer) clusterURL(clusterID string) string {
	return fmt.Sprintf("%s/clusters/%s/control-plane", s.baseURL(), clusterID)
}

func (s *testAPIServer) waitReady(t *testing.T, clusterID string) {
	t.Helper()

	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // test only
		},
	}

	url := s.clusterURL(clusterID) + "/readyz"
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer smoketoken")
		resp, err := client.Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			err = fmt.Errorf("readyz status=%d", resp.StatusCode)
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	s.t.Fatalf("apiserver never became ready: %v\nlogs:\n%s", lastErr, s.logs())
}

func (s *testAPIServer) logs() string {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	return s.log.String()
}

func (s *testAPIServer) Stop() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}

	// Try graceful first.
	_ = s.cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()

	select {
	case <-time.After(5 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	case <-done:
	}
}

func isConnRefused(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	return strings.Contains(err.Error(), "connection refused")
}
