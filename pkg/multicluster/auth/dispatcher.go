package auth

import (
	"context"
	"net/http"
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
	if useRoot && c.root != nil {
		return c.root.AuthenticateRequest(req)
	}
	if c.resolver == nil {
		return nil, false, nil
	}
	authn, err := c.resolver.AuthenticatorForCluster(cid)
	if err != nil {
		return nil, false, err
	}
	if authn == nil {
		return nil, false, nil
	}
	return authn.AuthenticateRequest(req)
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
	if cid == "" || (cid == c.rootCluster && c.root != nil) || c.resolver == nil {
		if c.root == nil {
			return authorizer.DecisionNoOpinion, "no root authorizer", nil
		}
		return c.root.Authorize(ctx, a)
	}
	authz, _, err := c.resolver.AuthorizerForCluster(cid)
	if err != nil {
		return authorizer.DecisionNoOpinion, "", err
	}
	if authz == nil {
		return authorizer.DecisionNoOpinion, "no cluster authorizer", nil
	}
	return authz.Authorize(ctx, a)
}

// RulesFor dispatches rule resolution per cluster.
func (c *ClusterAuthorizer) RulesFor(ctx context.Context, u user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	cid := clusterFromContext(ctx)
	if cid == "" || (cid == c.rootCluster && c.rootResolver != nil) || c.resolver == nil {
		if c.rootResolver == nil {
			return nil, nil, false, nil
		}
		return c.rootResolver.RulesFor(ctx, u, namespace)
	}
	_, resolver, err := c.resolver.AuthorizerForCluster(cid)
	if err != nil {
		return nil, nil, false, err
	}
	if resolver == nil {
		return nil, nil, false, nil
	}
	return resolver.RulesFor(ctx, u, namespace)
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
