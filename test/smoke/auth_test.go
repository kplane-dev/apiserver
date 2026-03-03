package smoke

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func TestRBACIsolationAcrossClusters(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	root := s.root

	csRoot := kubeClientForCluster(t, s, root)
	csA := kubeClientForCluster(t, s, clusterA)

	roleName := "role-" + randSuffix(4)
	bindingName := "rb-" + randSuffix(4)
	subjectUser := "user-" + randSuffix(4)

	_, err := csA.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "list"},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create clusterrole: %v\nlogs:\n%s", clusterA, err, s.logs())
	}
	if _, getErr := csA.RbacV1().ClusterRoles().Get(ctx, roleName, metav1.GetOptions{}); getErr != nil {
		t.Fatalf("cluster=%s get clusterrole: %v", clusterA, getErr)
	}
	t.Cleanup(func() {
		_ = csA.RbacV1().ClusterRoles().Delete(context.Background(), roleName, metav1.DeleteOptions{})
	})

	_, err = csA.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: bindingName},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.UserKind, Name: subjectUser},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     roleName,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create clusterrolebinding: %v", clusterA, err)
	}
	if _, getErr := csA.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{}); getErr != nil {
		t.Fatalf("cluster=%s get clusterrolebinding: %v", clusterA, getErr)
	}
	t.Cleanup(func() {
		_ = csA.RbacV1().ClusterRoleBindings().Delete(context.Background(), bindingName, metav1.DeleteOptions{})
	})

	sar := &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User: subjectUser,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: "default",
				Verb:      "get",
				Group:     "",
				Resource:  "configmaps",
			},
		},
	}

	waitForSubjectAccessReviewWithLogs(ctx, t, csA, sar, true, s.logs)
	waitForSubjectAccessReviewWithLogs(ctx, t, csRoot, sar, false, s.logs)
}

func TestServiceAccountTokenIsolationAcrossClusters(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	root := s.root

	csRoot := kubeClientForCluster(t, s, root)
	csA := kubeClientForCluster(t, s, clusterA)

	ns := "default"
	if err := waitForNamespace(ctx, csRoot, ns); err != nil {
		t.Fatalf("cluster=%s wait for namespace %q: %v", root, ns, err)
	}
	if err := waitForNamespace(ctx, csA, ns); err != nil {
		t.Fatalf("cluster=%s wait for namespace %q: %v", clusterA, ns, err)
	}
	saRoot := "sa-" + randSuffix(3)
	saA := "sa-" + randSuffix(3)

	_, err := csRoot.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: saRoot},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create serviceaccount: %v", root, err)
	}

	_, err = csA.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: saA},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create serviceaccount: %v", clusterA, err)
	}

	tokenRoot := requestServiceAccountToken(ctx, t, csRoot, ns, saRoot)
	tokenA := requestServiceAccountToken(ctx, t, csA, ns, saA)

	waitForTokenReview(ctx, t, csRoot, tokenRoot, true)
	waitForTokenReview(ctx, t, csA, tokenA, true)
	waitForTokenReview(ctx, t, csRoot, tokenA, false)
	waitForTokenReview(ctx, t, csA, tokenRoot, false)
}

func TestRBACCreateClusterRole(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	csA := kubeClientForCluster(t, s, clusterA)
	roleName := "role-" + randSuffix(4)

	obj, err := csA.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get"},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create clusterrole: %v", clusterA, err)
	}
	if obj == nil {
		t.Fatalf("expected created clusterrole object")
	}
}

func waitForSubjectAccessReview(ctx context.Context, t *testing.T, cs kubernetes.Interface, sar *authorizationv1.SubjectAccessReview, wantAllowed bool) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := cs.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
		if err == nil {
			if resp.Status.Allowed == wantAllowed {
				return
			}
			lastErr = fmt.Errorf("allowed=%v reason=%s", resp.Status.Allowed, resp.Status.Reason)
		} else {
			if errors.IsForbidden(err) || errors.IsUnauthorized(err) {
				lastErr = err
			} else {
				lastErr = err
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("expected SAR allowed=%v, last error: %v", wantAllowed, lastErr)
}

func waitForSubjectAccessReviewWithLogs(ctx context.Context, t *testing.T, cs kubernetes.Interface, sar *authorizationv1.SubjectAccessReview, wantAllowed bool, logsFn func() string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := cs.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
		if err == nil {
			if resp.Status.Allowed == wantAllowed {
				return
			}
			lastErr = fmt.Errorf("allowed=%v reason=%s", resp.Status.Allowed, resp.Status.Reason)
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	if logsFn != nil {
		t.Fatalf("expected SAR allowed=%v, last error: %v\nlogs:\n%s", wantAllowed, lastErr, logsFn())
	}
	t.Fatalf("expected SAR allowed=%v, last error: %v", wantAllowed, lastErr)
}

func requestServiceAccountToken(ctx context.Context, t *testing.T, cs kubernetes.Interface, namespace, name string) string {
	t.Helper()
	resp, err := cs.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, name, &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{"https://kubernetes.default.svc"},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create token for serviceaccount %s/%s: %v", namespace, name, err)
	}
	if resp == nil || resp.Status.Token == "" {
		t.Fatalf("empty token for serviceaccount %s/%s", namespace, name)
	}
	return resp.Status.Token
}

func waitForTokenReview(ctx context.Context, t *testing.T, cs kubernetes.Interface, token string, wantAuthenticated bool) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := cs.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
			Spec: authenticationv1.TokenReviewSpec{
				Token: token,
			},
		}, metav1.CreateOptions{})
		if err == nil {
			if resp.Status.Authenticated == wantAuthenticated {
				return
			}
			lastErr = fmt.Errorf("authenticated=%v error=%q", resp.Status.Authenticated, resp.Status.Error)
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("expected TokenReview authenticated=%v, last error: %v", wantAuthenticated, lastErr)
}
