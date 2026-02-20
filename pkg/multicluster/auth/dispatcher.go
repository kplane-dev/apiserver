package auth

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// ClusterAuthResolver provides per-cluster auth components.
type ClusterAuthResolver interface {
	AuthenticatorForCluster(clusterID string) (authenticator.Request, error)
	AuthorizerForCluster(clusterID string) (authorizer.Authorizer, authorizer.RuleResolver, error)
}

// ClusterAuthenticator dispatches authentication per cluster.
type ClusterAuthenticator struct {
	rootCluster string
	root        authenticator.Request
	resolver    ClusterAuthResolver
}

// NewClusterAuthenticator creates a cluster-aware authenticator.
func NewClusterAuthenticator(rootCluster string, root authenticator.Request, resolver ClusterAuthResolver) *ClusterAuthenticator {
	if rootCluster == "" {
		rootCluster = mc.DefaultClusterName
	}
	return &ClusterAuthenticator{
		rootCluster: rootCluster,
		root:        root,
		resolver:    resolver,
	}
}

// AuthenticateRequest dispatches by cluster context.
func (c *ClusterAuthenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	if req == nil {
		return nil, false, nil
	}
	cid := clusterFromContext(req.Context())
	if cid == "" {
		if token := bearerTokenFromAuthHeader(req.Header.Get("Authorization")); token != "" {
			cid = clusterHintForToken(token)
		}
	}
	useRoot := cid == "" || cid == c.rootCluster
	if useRoot && !isNil(c.root) {
		return authenticateSafely(c.root, req, "root")
	}
	if c.resolver == nil {
		return nil, false, nil
	}
	authn, err := c.resolver.AuthenticatorForCluster(cid)
	if err != nil {
		return nil, false, err
	}
	if isNil(authn) {
		return nil, false, nil
	}
	return authenticateSafely(authn, req, cid)
}

// ClusterAuthorizer dispatches authorization per cluster.
type ClusterAuthorizer struct {
	rootCluster  string
	root         authorizer.Authorizer
	rootResolver authorizer.RuleResolver
	resolver     ClusterAuthResolver
}

// NewClusterAuthorizer creates a cluster-aware authorizer + rule resolver.
func NewClusterAuthorizer(rootCluster string, root authorizer.Authorizer, rootResolver authorizer.RuleResolver, resolver ClusterAuthResolver) *ClusterAuthorizer {
	if rootCluster == "" {
		rootCluster = mc.DefaultClusterName
	}
	if rootResolver == nil {
		if rr, ok := root.(authorizer.RuleResolver); ok {
			rootResolver = rr
		}
	}
	return &ClusterAuthorizer{
		rootCluster:  rootCluster,
		root:         root,
		rootResolver: rootResolver,
		resolver:     resolver,
	}
}

// Authorize dispatches by cluster context.
func (c *ClusterAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	cid := clusterFromContext(ctx)
	if cid == "" || (cid == c.rootCluster && !isNil(c.root)) || c.resolver == nil {
		if isNil(c.root) {
			return authorizer.DecisionNoOpinion, "no root authorizer", nil
		}
		if err := validateAttributesForAuthorize(a, "root", authorizerType(c.root)); err != nil {
			return authorizer.DecisionDeny, "", err
		}
		return authorizeSafely(c.root, ctx, a, "root")
	}
	authz, _, err := c.resolver.AuthorizerForCluster(cid)
	if err != nil {
		return authorizer.DecisionDeny, "", err
	}
	if isNil(authz) {
		return authorizer.DecisionNoOpinion, "no cluster authorizer", nil
	}
	if err := validateAttributesForAuthorize(a, cid, authorizerType(authz)); err != nil {
		return authorizer.DecisionDeny, "", err
	}
	return authorizeSafely(authz, ctx, a, cid)
}

// RulesFor dispatches rule resolution per cluster.
func (c *ClusterAuthorizer) RulesFor(ctx context.Context, u user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	cid := clusterFromContext(ctx)
	if cid == "" || (cid == c.rootCluster && !isNil(c.rootResolver)) || c.resolver == nil {
		if isNil(c.rootResolver) {
			return nil, nil, false, nil
		}
		return c.rootResolver.RulesFor(ctx, u, namespace)
	}
	_, resolver, err := c.resolver.AuthorizerForCluster(cid)
	if err != nil {
		return nil, nil, false, err
	}
	if isNil(resolver) {
		return nil, nil, false, nil
	}
	return resolver.RulesFor(ctx, u, namespace)
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func authorizeSafely(authz authorizer.Authorizer, ctx context.Context, a authorizer.Attributes, target string) (decision authorizer.Decision, reason string, err error) {
	defer func() {
		if r := recover(); r != nil {
			decision = authorizer.DecisionDeny
			reason = ""
			err = fmt.Errorf("authorizer panic for cluster %q (type=%s): %v", target, authorizerType(authz), r)
		}
	}()
	return authz.Authorize(ctx, a)
}

func validateAttributesForAuthorize(a authorizer.Attributes, clusterID, authzType string) error {
	if isNil(a) {
		return fmt.Errorf("invalid authorization attributes for cluster %q (authorizer=%s): attributes is nil", clusterID, authzType)
	}
	u, err := userFromAttributes(a)
	if err != nil {
		return fmt.Errorf("invalid authorization attributes for cluster %q (authorizer=%s): %w", clusterID, authzType, err)
	}
	if isNil(u) {
		return fmt.Errorf("invalid authorization attributes for cluster %q (authorizer=%s): user is nil", clusterID, authzType)
	}
	return nil
}

func userFromAttributes(a authorizer.Attributes) (u user.Info, err error) {
	defer func() {
		if r := recover(); r != nil {
			u = nil
			err = fmt.Errorf("GetUser panic: %v", r)
		}
	}()
	return a.GetUser(), nil
}

func authorizerType(authz authorizer.Authorizer) string {
	if isNil(authz) {
		return "<nil>"
	}
	return fmt.Sprintf("%T", authz)
}

func authenticateSafely(authn authenticator.Request, req *http.Request, target string) (resp *authenticator.Response, ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			resp = nil
			ok = false
			err = fmt.Errorf("authenticator panic for cluster %q (type=%T): %v", target, authn, r)
		}
	}()
	resp, ok, err = authn.AuthenticateRequest(req)
	if err != nil || !ok {
		return resp, ok, err
	}
	if resp == nil {
		return nil, false, fmt.Errorf("invalid authenticator response for cluster %q (type=%T): response is nil", target, authn)
	}
	if isNil(resp.User) {
		return nil, false, fmt.Errorf("invalid authenticator response for cluster %q (type=%T): user is nil", target, authn)
	}
	return resp, ok, nil
}

func clusterFromContext(ctx context.Context) string {
	cid, _, _ := mc.FromContext(ctx)
	return cid
}

func bearerTokenFromAuthHeader(authz string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return ""
	}
	token := strings.TrimSpace(strings.TrimPrefix(authz, prefix))
	return token
}
