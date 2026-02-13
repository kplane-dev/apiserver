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

	ctx := mc.WithCluster(context.Background(), "root", false)
	_, _, _ = dispatch.Authorize(ctx, authorizer.AttributesRecord{})
	if called != "root" {
		t.Fatalf("expected root authorizer, got %q", called)
	}

	called = ""
	lastCluster = ""
	ctx = mc.WithCluster(context.Background(), "c-2", false)
	_, _, _ = dispatch.Authorize(ctx, authorizer.AttributesRecord{})
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
