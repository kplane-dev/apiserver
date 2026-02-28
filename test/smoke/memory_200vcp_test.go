package smoke

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type memorySnapshot struct {
	heapInuse  float64
	rss        float64
	goRoutines float64
}

const (
	postBootstrapSettle = 15 * time.Second
	steadyStateWait     = 90 * time.Second
)

const currentCRDServingMode = "current-default"

func TestMemoryAfter200VCPCurrentSystem(t *testing.T) {
	runMemoryAfter200VCP(t)
}

func runMemoryAfter200VCP(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	if strings.TrimSpace(etcd) == "" {
		t.Skip("ETCD_ENDPOINTS is not set; skipping memory smoke")
	}

	s := startAPIServerWithOptions(t, etcd, apiserverOptions{
		extraArgs: nil,
	})
	// Capture process baseline before creating additional VCP activity.
	time.Sleep(5 * time.Second)
	baseline, err := sampleMemorySnapshot(s)
	if err != nil {
		t.Fatalf("sample baseline metrics: %v", err)
	}

	const totalClusters = 200
	clusterIDs := make([]string, 0, totalClusters)
	for i := 0; i < totalClusters; i++ {
		clusterIDs = append(clusterIDs, fmt.Sprintf("c-%03d", i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	workerN := 12
	workCh := make(chan string)
	errCh := make(chan error, totalClusters)
	var wg sync.WaitGroup
	for i := 0; i < workerN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cid := range workCh {
				if err := exerciseClusterForMemory(ctx, t, s, cid); err != nil {
					errCh <- fmt.Errorf("cluster=%s: %w", cid, err)
					return
				}
			}
		}()
	}

	for _, cid := range clusterIDs {
		workCh <- cid
	}
	close(workCh)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Phase B: bootstrap burst stabilized.
	time.Sleep(postBootstrapSettle)
	postBootstrap, err := sampleMemorySnapshot(s)
	if err != nil {
		t.Fatalf("sample post-bootstrap metrics: %v", err)
	}
	// Phase C: steady state after no new cluster creation.
	time.Sleep(steadyStateWait)
	steady, err := sampleMemorySnapshot(s)
	if err != nil {
		t.Fatalf("sample steady-state metrics: %v", err)
	}

	bootstrapDeltaHeap := postBootstrap.heapInuse - baseline.heapInuse
	bootstrapDeltaRSS := postBootstrap.rss - baseline.rss
	bootstrapDeltaGo := postBootstrap.goRoutines - baseline.goRoutines
	steadyTailHeap := steady.heapInuse - postBootstrap.heapInuse
	steadyTailRSS := steady.rss - postBootstrap.rss
	steadyTailGo := steady.goRoutines - postBootstrap.goRoutines

	t.Logf(
		"phase metrics mode=%s clusters=%d baseline(heap=%.2fMiB rss=%.2fMiB goroutines=%.0f) post_bootstrap(heap=%.2fMiB rss=%.2fMiB goroutines=%.0f) steady(heap=%.2fMiB rss=%.2fMiB goroutines=%.0f) bootstrap_delta(heap=%.2fMiB rss=%.2fMiB goroutines=%.0f) bootstrap_per_vcp(heap=%.2fMiB rss=%.2fMiB goroutines=%.2f) steady_tail_delta(heap=%.2fMiB rss=%.2fMiB goroutines=%.0f)",
		currentCRDServingMode,
		totalClusters,
		baseline.heapInuse/(1024*1024), baseline.rss/(1024*1024), baseline.goRoutines,
		postBootstrap.heapInuse/(1024*1024), postBootstrap.rss/(1024*1024), postBootstrap.goRoutines,
		steady.heapInuse/(1024*1024), steady.rss/(1024*1024), steady.goRoutines,
		bootstrapDeltaHeap/(1024*1024), bootstrapDeltaRSS/(1024*1024), bootstrapDeltaGo,
		bootstrapDeltaHeap/(1024*1024)/totalClusters, bootstrapDeltaRSS/(1024*1024)/totalClusters, bootstrapDeltaGo/totalClusters,
		steadyTailHeap/(1024*1024), steadyTailRSS/(1024*1024), steadyTailGo,
	)

	outDir := filepath.Join(repoRoot(t), "dev", "profiles")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir profiles dir: %v", err)
	}
	saveProfilesForPhase(t, s, outDir, currentCRDServingMode, "post-bootstrap")
	saveProfilesForPhase(t, s, outDir, currentCRDServingMode, "steady")
}

func exerciseClusterForMemory(ctx context.Context, t *testing.T, s *testAPIServer, clusterID string) error {
	s.waitReady(t, clusterID)

	cs := kubeClientForCluster(t, s, clusterID)
	if err := waitForNamespace(ctx, cs, "default"); err != nil {
		return err
	}

	crdClient := apixClientForCluster(t, s, clusterID)
	group := fmt.Sprintf("mem-%s.kplane.dev", strings.ReplaceAll(clusterID, "_", "-"))
	plural := "memwidgets"
	crdName := plural + "." + group
	createTestCRD(ctx, t, crdClient, crdName, group, plural)
			waitForCRDEstablished(ctx, t, crdClient, clusterID, crdName)
	waitForResourcePresence(t, cs, clusterID, group+"/v1", plural, true)
	return nil
}

func fetchFromRoot(s *testAPIServer, path string) (string, error) {
	client := &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, s.baseURL()+path, nil)
	req.Header.Set("Authorization", "Bearer smoketoken")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	return string(b), nil
}

func sampleMemorySnapshot(s *testAPIServer) (memorySnapshot, error) {
	metricsText, err := fetchFromRoot(s, "/metrics")
	if err != nil {
		return memorySnapshot{}, err
	}
	heapInuse, err := parsePromGauge(metricsText, "go_memstats_heap_inuse_bytes")
	if err != nil {
		return memorySnapshot{}, err
	}
	rss, err := parsePromGauge(metricsText, "process_resident_memory_bytes")
	if err != nil {
		return memorySnapshot{}, err
	}
	goRoutines, err := parsePromGauge(metricsText, "go_goroutines")
	if err != nil {
		return memorySnapshot{}, err
	}
	return memorySnapshot{
		heapInuse:  heapInuse,
		rss:        rss,
		goRoutines: goRoutines,
	}, nil
}

func saveProfilesForPhase(t *testing.T, s *testAPIServer, outDir, mode, phase string) {
	t.Helper()
	heapProfile, err := fetchFromRoot(s, "/debug/pprof/heap")
	if err != nil {
		t.Fatalf("fetch heap profile (%s): %v", phase, err)
	}
	heapPath := filepath.Join(outDir, "heap-200vcp-"+mode+"-"+phase+".pb.gz")
	if err := os.WriteFile(heapPath, []byte(heapProfile), 0o644); err != nil {
		t.Fatalf("write heap profile (%s): %v", phase, err)
	}
	t.Logf("saved heap profile (%s): %s", phase, heapPath)

	goroutineProfile, err := fetchFromRoot(s, "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatalf("fetch goroutine profile (%s): %v", phase, err)
	}
	gPath := filepath.Join(outDir, "goroutine-200vcp-"+mode+"-"+phase+".txt")
	if err := os.WriteFile(gPath, []byte(goroutineProfile), 0o644); err != nil {
		t.Fatalf("write goroutine profile (%s): %v", phase, err)
	}
	t.Logf("saved goroutine profile (%s): %s", phase, gPath)
}

func parsePromGauge(metricsText, metric string) (float64, error) {
	for _, line := range strings.Split(metricsText, "\n") {
		if strings.HasPrefix(line, metric+" ") {
			v := strings.TrimSpace(strings.TrimPrefix(line, metric))
			return strconv.ParseFloat(v, 64)
		}
	}
	return 0, fmt.Errorf("metric %s not found", metric)
}
