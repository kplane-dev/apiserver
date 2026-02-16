package multicluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kplane-dev/apiserver/pkg/multicluster/internalcap"
	"k8s.io/apiserver/pkg/authentication/user"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

func TestWithClusterRoutingExtractorErrorForbidden(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := WithClusterRouting(next, errorExtractor{}, DefaultOptions)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example.test/apis", nil)

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", rec.Code)
	}
}

func TestWithClusterRoutingDoesNotTrustUserForScope(t *testing.T) {
	var (
		gotScope ResourceScope
		gotCap   bool
	)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, scope, ok := FromContextScope(r.Context())
		if !ok {
			t.Fatalf("expected cluster selection in context")
		}
		gotScope = scope
		gotCap = internalcap.HasAllClustersCapability(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := WithClusterRouting(next, PathExtractor{}, DefaultOptions)

	req := httptest.NewRequest(http.MethodGet, "http://example.test/apis", nil)
	req = req.WithContext(apirequest.WithUser(req.Context(), &user.DefaultInfo{
		Name: DefaultInternalCrossClusterUser,
		Extra: map[string][]string{
			DefaultInternalCrossClusterCapability: {"true"},
		},
	}))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if gotScope != ResourceScopeCluster {
		t.Fatalf("expected cluster scope, got %q", gotScope)
	}
	if gotCap {
		t.Fatalf("expected no all-clusters capability on context")
	}
}

func TestWithClusterRoutingNonInternalRequestClusterScope(t *testing.T) {
	var gotScope ResourceScope
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, scope, ok := FromContextScope(r.Context())
		if !ok {
			t.Fatalf("expected cluster selection in context")
		}
		gotScope = scope
		w.WriteHeader(http.StatusOK)
	})
	h := WithClusterRouting(next, PathExtractor{}, DefaultOptions)

	req := httptest.NewRequest(http.MethodGet, "http://example.test/apis", nil)
	req = req.WithContext(apirequest.WithUser(req.Context(), &user.DefaultInfo{
		Name: DefaultInternalCrossClusterUser,
	}))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if gotScope != ResourceScopeCluster {
		t.Fatalf("expected cluster scope, got %q", gotScope)
	}
}

func TestHasInternalCrossClusterCapability(t *testing.T) {
	ctx := apirequest.WithUser(context.Background(), &user.DefaultInfo{
		Name: DefaultInternalCrossClusterUser,
		Extra: map[string][]string{
			DefaultInternalCrossClusterCapability: {"true"},
		},
	})
	if !HasInternalCrossClusterCapability(ctx) {
		t.Fatalf("expected internal cross-cluster capability to be detected")
	}
}

func TestHasInternalCrossClusterCapabilityFalseForRegularUser(t *testing.T) {
	ctx := apirequest.WithUser(context.Background(), &user.DefaultInfo{
		Name: "alice",
		Extra: map[string][]string{
			DefaultInternalCrossClusterCapability: {"true"},
		},
	})
	if HasInternalCrossClusterCapability(ctx) {
		t.Fatalf("expected capability check to reject non-internal user")
	}
}

type errorExtractor struct{}

func (errorExtractor) Extract(context.Context, *http.Request) (string, bool, error) {
	return "", false, errors.New("boom")
}
