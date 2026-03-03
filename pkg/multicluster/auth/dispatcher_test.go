package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

type fakeAuthenticator struct {
	name   string
	called *string
}

func (f *fakeAuthenticator) AuthenticateRequest(*http.Request) (*authenticator.Response, bool, error) {
	if f.called != nil {
		*f.called = f.name
	}
	return &authenticator.Response{User: &user.DefaultInfo{Name: f.name}}, true, nil
}

type badUserAuthenticator struct{}

func (b *badUserAuthenticator) AuthenticateRequest(*http.Request) (*authenticator.Response, bool, error) {
	return &authenticator.Response{}, true, nil
}

type fakeAuthorizer struct {
	name   string
	called *string
}

func (f *fakeAuthorizer) Authorize(ctx context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
	if f.called != nil {
		*f.called = f.name
	}
	return authorizer.DecisionAllow, f.name, nil
}

func (f *fakeAuthorizer) RulesFor(ctx context.Context, _ user.Info, _ string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	if f.called != nil {
		*f.called = f.name
	}
	return nil, nil, false, nil
}

type panicAuthorizer struct{}

func (p *panicAuthorizer) Authorize(context.Context, authorizer.Attributes) (authorizer.Decision, string, error) {
	panic("boom")
}

func (p *panicAuthorizer) RulesFor(context.Context, user.Info, string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	return nil, nil, false, nil
}

type fakeResolver struct {
	authn        authenticator.Request
	authz        authorizer.Authorizer
	ruleResolver authorizer.RuleResolver
	lastCluster  *string
}

func (f *fakeResolver) AuthenticatorForCluster(clusterID string) (authenticator.Request, error) {
	if f.lastCluster != nil {
		*f.lastCluster = clusterID
	}
	return f.authn, nil
}

func (f *fakeResolver) AuthorizerForCluster(clusterID string) (authorizer.Authorizer, authorizer.RuleResolver, error) {
	if f.lastCluster != nil {
		*f.lastCluster = clusterID
	}
	return f.authz, f.ruleResolver, nil
}

func TestClusterAuthenticatorDispatch(t *testing.T) {
	var called string
	var lastCluster string

	root := &fakeAuthenticator{name: "root", called: &called}
	cluster := &fakeAuthenticator{name: "cluster", called: &called}
	resolver := &fakeResolver{authn: cluster, lastCluster: &lastCluster}

	dispatch := NewClusterAuthenticator("root", root, resolver)

	req := httptest.NewRequest("GET", "http://example", nil)
	req = req.WithContext(mc.WithCluster(req.Context(), "root", false))
	_, _, _ = dispatch.AuthenticateRequest(req)
	if called != "root" {
		t.Fatalf("expected root authenticator, got %q", called)
	}

	called = ""
	lastCluster = ""
	req = req.WithContext(mc.WithCluster(req.Context(), "c-1", false))
	_, _, _ = dispatch.AuthenticateRequest(req)
	if called != "cluster" {
		t.Fatalf("expected cluster authenticator, got %q", called)
	}
	if lastCluster != "c-1" {
		t.Fatalf("expected resolver to see cluster c-1, got %q", lastCluster)
	}
}

func TestClusterAuthorizerDispatch(t *testing.T) {
	var called string
	var lastCluster string

	root := &fakeAuthorizer{name: "root", called: &called}
	cluster := &fakeAuthorizer{name: "cluster", called: &called}
	resolver := &fakeResolver{authz: cluster, ruleResolver: cluster, lastCluster: &lastCluster}

	dispatch := NewClusterAuthorizer("root", root, root, resolver)
	attrs := authorizer.AttributesRecord{User: &user.DefaultInfo{Name: "test-user"}}

	ctx := mc.WithCluster(context.Background(), "root", false)
	_, _, _ = dispatch.Authorize(ctx, attrs)
	if called != "root" {
		t.Fatalf("expected root authorizer, got %q", called)
	}

	called = ""
	lastCluster = ""
	ctx = mc.WithCluster(context.Background(), "c-2", false)
	_, _, _ = dispatch.Authorize(ctx, attrs)
	if called != "cluster" {
		t.Fatalf("expected cluster authorizer, got %q", called)
	}
	if lastCluster != "c-2" {
		t.Fatalf("expected resolver to see cluster c-2, got %q", lastCluster)
	}

	called = ""
	lastCluster = ""
	ctx = mc.WithCluster(context.Background(), "c-3", false)
	_, _, _, _ = dispatch.RulesFor(ctx, &user.DefaultInfo{Name: "test"}, "")
	if called != "cluster" {
		t.Fatalf("expected cluster rule resolver, got %q", called)
	}
	if lastCluster != "c-3" {
		t.Fatalf("expected resolver to see cluster c-3, got %q", lastCluster)
	}
}

func TestClusterAuthenticatorUsesTokenHintWithoutClusterContext(t *testing.T) {
	var called string
	var lastCluster string

	root := &fakeAuthenticator{name: "root", called: &called}
	cluster := &fakeAuthenticator{name: "cluster", called: &called}
	resolver := &fakeResolver{authn: cluster, lastCluster: &lastCluster}
	dispatch := NewClusterAuthenticator("root", root, resolver)

	token := "tok-" + t.Name()
	rememberTokenReviewHint(token, "c-42")

	req := httptest.NewRequest("GET", "http://example", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	_, _, _ = dispatch.AuthenticateRequest(req)

	if called != "cluster" {
		t.Fatalf("expected cluster authenticator via token hint, got %q", called)
	}
	if lastCluster != "c-42" {
		t.Fatalf("expected resolver cluster c-42, got %q", lastCluster)
	}
}

func TestClusterAuthenticatorRejectsNilUserResponse(t *testing.T) {
	dispatch := NewClusterAuthenticator("root", nil, &fakeResolver{
		authn: &badUserAuthenticator{},
	})
	req := httptest.NewRequest("GET", "http://example", nil)
	req = req.WithContext(mc.WithCluster(req.Context(), "c-bad-user", false))

	resp, ok, err := dispatch.AuthenticateRequest(req)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if ok {
		t.Fatalf("expected ok=false for invalid auth response")
	}
	if resp != nil {
		t.Fatalf("expected nil response on invalid auth response")
	}
}

func TestClusterAuthorizerTypedNilDoesNotPanic(t *testing.T) {
	var typedNilCluster *fakeAuthorizer
	dispatch := NewClusterAuthorizer("root", &fakeAuthorizer{name: "root"}, nil, &fakeResolver{
		authz:        typedNilCluster,
		ruleResolver: typedNilCluster,
	})
	attrs := authorizer.AttributesRecord{User: &user.DefaultInfo{Name: "test-user"}}

	ctx := mc.WithCluster(context.Background(), "c-typed-nil", false)
	decision, reason, err := dispatch.Authorize(ctx, attrs)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if decision != authorizer.DecisionNoOpinion {
		t.Fatalf("expected DecisionNoOpinion, got %v", decision)
	}
	if reason != "no cluster authorizer" {
		t.Fatalf("expected no cluster authorizer reason, got %q", reason)
	}

	_, _, incomplete, err := dispatch.RulesFor(ctx, &user.DefaultInfo{Name: "test"}, "")
	if err != nil {
		t.Fatalf("expected nil error from RulesFor, got %v", err)
	}
	if incomplete {
		t.Fatalf("expected incomplete=false for missing resolver")
	}
}

func TestClusterAuthorizerRootTypedNilDoesNotPanic(t *testing.T) {
	var typedNilRoot *fakeAuthorizer
	dispatch := NewClusterAuthorizer("root", typedNilRoot, nil, nil)

	decision, reason, err := dispatch.Authorize(context.Background(), authorizer.AttributesRecord{User: &user.DefaultInfo{Name: "test-user"}})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if decision != authorizer.DecisionNoOpinion {
		t.Fatalf("expected DecisionNoOpinion, got %v", decision)
	}
	if reason != "no root authorizer" {
		t.Fatalf("expected no root authorizer reason, got %q", reason)
	}
}

func TestClusterAuthorizerPanicIsRecovered(t *testing.T) {
	dispatch := NewClusterAuthorizer("root", &fakeAuthorizer{name: "root"}, nil, &fakeResolver{
		authz: &panicAuthorizer{},
	})
	attrs := authorizer.AttributesRecord{User: &user.DefaultInfo{Name: "test-user"}}

	ctx := mc.WithCluster(context.Background(), "c-panic", false)
	decision, _, err := dispatch.Authorize(ctx, attrs)
	if err == nil {
		t.Fatalf("expected recovered panic error, got nil")
	}
	if decision != authorizer.DecisionDeny {
		t.Fatalf("expected DecisionDeny on panic, got %v", decision)
	}
}

func TestClusterAuthorizerNilUserDenied(t *testing.T) {
	dispatch := NewClusterAuthorizer("root", &fakeAuthorizer{name: "root"}, nil, nil)
	decision, _, err := dispatch.Authorize(context.Background(), authorizer.AttributesRecord{})
	if err == nil {
		t.Fatalf("expected error for nil user, got nil")
	}
	if decision != authorizer.DecisionDeny {
		t.Fatalf("expected DecisionDeny for nil user, got %v", decision)
	}
}
