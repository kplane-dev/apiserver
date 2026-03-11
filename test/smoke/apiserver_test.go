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

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	spannerstore "github.com/kplane-dev/spanner"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type testAPIServer struct {
	t       *testing.T
	binPath string
	port    int
	tmpDir  string
	root    string

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

type apiserverOptions struct {
	rootCluster string
	etcdPrefix  string
	port        int
	extraArgs   []string
	// spannerDB, when non-empty, adds --spanner-* flags to use Spanner storage.
	spannerDB string
}

func storageBackend() string {
	return os.Getenv("KPLANE_STORAGE_BACKEND") // "" or "spanner"
}

func spannerEmulatorHost() string {
	if h := os.Getenv("SPANNER_EMULATOR_HOST"); h != "" {
		return h
	}
	return "localhost:9010"
}

var spannerInstanceOnce sync.Once

func ensureSpannerInstance(t *testing.T) {
	t.Helper()
	spannerInstanceOnce.Do(func() {
		cfg := spannerstore.SpannerConfig{
			Project:      "test-project",
			Instance:     "test-instance",
			EmulatorHost: spannerEmulatorHost(),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := spannerstore.EnsureInstance(ctx, cfg); err != nil {
			t.Fatalf("EnsureInstance: %v", err)
		}
	})
}

func setupSpannerDB(t *testing.T) string {
	t.Helper()
	ensureSpannerInstance(t)
	dbName := fmt.Sprintf("smoke_%d", time.Now().UnixNano())
	cfg := spannerstore.SpannerConfig{
		Project:      "test-project",
		Instance:     "test-instance",
		Database:     dbName,
		EmulatorHost: spannerEmulatorHost(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := spannerstore.EnsureSchema(ctx, cfg); err != nil {
		t.Fatalf("EnsureSchema for smoke test: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		_ = spannerstore.DropDatabase(dropCtx, cfg)
	})
	return dbName
}

func startAPIServer(t *testing.T, etcdEndpoints string) *testAPIServer {
	return startAPIServerWithOptions(t, etcdEndpoints, apiserverOptions{})
}

func startAPIServerWithOptions(t *testing.T, etcdEndpoints string, opts apiserverOptions) *testAPIServer {
	t.Helper()
	if storageBackend() == "spanner" {
		if opts.spannerDB == "" {
			opts.spannerDB = setupSpannerDB(t)
		}
	}
	if strings.TrimSpace(etcdEndpoints) == "" {
		t.Skip("ETCD_ENDPOINTS is not set; skipping integration smoke tests")
	}

	bin := buildAPIServerBinary(t)
	port := opts.port
	if port == 0 {
		port = mustFreePort(t)
	}
	tmp := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	s := &testAPIServer{
		t:       t,
		binPath: bin,
		port:    port,
		tmpDir:  tmp,
		cancel:  cancel,
	}
	if opts.rootCluster == "" {
		opts.rootCluster = mc.DefaultClusterName
	}
	if opts.etcdPrefix == "" {
		name := strings.ToLower(t.Name())
		name = strings.NewReplacer("/", "-", " ", "-", "_", "-", ":", "-").Replace(name)
		opts.etcdPrefix = fmt.Sprintf("/registry-smoke-%s-%d", name, time.Now().UnixNano())
	}
	s.root = opts.rootCluster

	args := []string{
		"--etcd-servers=" + etcdEndpoints,
		"--etcd-prefix=" + opts.etcdPrefix,
		"--cert-dir=" + filepath.Join(tmp, "certs"),
		"--secure-port=" + fmt.Sprintf("%d", port),
		"--enable-aggregator-routing=true",
		"--authorization-mode=RBAC",
		"--anonymous-auth=true",
		"--token-auth-file=" + mustWriteTokenFile(t, filepath.Join(tmp, "tokens.csv")),
		"--allow-privileged=true",
		"--service-cluster-ip-range=10.0.0.0/24",
		"--service-cidr-sharing-mode=shared",
		"--kubernetes-service-mode=shared",
		"--service-account-issuer=test",
		"--api-audiences=test,https://kubernetes.default.svc",
		"--service-account-signing-key-file=" + mustWriteRSAKey(t, filepath.Join(tmp, "sa.key")),
		"--service-account-key-file=" + filepath.Join(tmp, "sa.key"),
		// reduce noise + make startup faster for tests
		"--v=2",
	}
	if opts.rootCluster != mc.DefaultClusterName {
		args = append(args, "--root-control-plane-name="+opts.rootCluster)
	}
	if opts.spannerDB != "" {
		args = append(args,
			"--spanner-project=test-project",
			"--spanner-instance=test-instance",
			"--spanner-database="+opts.spannerDB,
			"--spanner-emulator-host="+spannerEmulatorHost(),
		)
	}
	if len(opts.extraArgs) > 0 {
		args = append(args, opts.extraArgs...)
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
	s.waitReady(t, s.root)

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

func TestRootControlPlaneNameOverride(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServerWithOptions(t, etcd, apiserverOptions{rootCluster: "default"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cs := kubeClientForCluster(t, s, s.root)
	cmName := "cm-" + randSuffix(4)
	_, err := cs.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cmName},
		Data:       map[string]string{"root": "ok"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create configmap in root cluster %q: %v", s.root, err)
	}
}
