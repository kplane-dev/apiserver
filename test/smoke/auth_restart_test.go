package smoke

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRBACIsolationAfterAPIServerRestart(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	if etcd == "" {
		t.Skip("ETCD_ENDPOINTS is not set; skipping integration smoke tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	port := mustFreePort(t)
	prefix := fmt.Sprintf("/registry-smoke-restart-rbac-%d", time.Now().UnixNano())
	opts := apiserverOptions{etcdPrefix: prefix, port: port}

	s1 := startAPIServerWithOptions(t, etcd, opts)
	clusterA := "c-" + randSuffix(3)
	subjectUser := "user-" + randSuffix(4)
	roleName := "role-" + randSuffix(4)
	bindingName := "rb-" + randSuffix(4)

	csA1 := kubeClientForCluster(t, s1, clusterA)
	csRoot1 := kubeClientForCluster(t, s1, s1.root)

	_, err := csA1.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
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
	_, err = csA1.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
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
	waitForSubjectAccessReviewWithLogs(ctx, t, csA1, sar, true, s1.logs)
	waitForSubjectAccessReviewWithLogs(ctx, t, csRoot1, sar, false, s1.logs)

	// Simulate a full process restart while keeping persisted etcd state.
	s1.Stop()

	s2 := startAPIServerWithOptions(t, etcd, opts)
	csA2 := kubeClientForCluster(t, s2, clusterA)
	csRoot2 := kubeClientForCluster(t, s2, s2.root)

	waitForRBACObject := func(name string, getFn func(context.Context, string, metav1.GetOptions) (interface{}, error)) {
		deadline := time.Now().Add(20 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			if _, err := getFn(ctx, name, metav1.GetOptions{}); err == nil {
				return
			} else {
				lastErr = err
			}
			time.Sleep(300 * time.Millisecond)
		}
		t.Fatalf("cluster=%s object %q not visible after restart: %v", clusterA, name, lastErr)
	}
	waitForRBACObject(roleName, func(ctx context.Context, name string, opts metav1.GetOptions) (interface{}, error) {
		return csA2.RbacV1().ClusterRoles().Get(ctx, name, opts)
	})
	waitForRBACObject(bindingName, func(ctx context.Context, name string, opts metav1.GetOptions) (interface{}, error) {
		return csA2.RbacV1().ClusterRoleBindings().Get(ctx, name, opts)
	})
	if _, err := csRoot2.RbacV1().ClusterRoles().Get(ctx, roleName, metav1.GetOptions{}); err == nil {
		t.Fatalf("unexpectedly found clusterrole %q in root cluster after restart", roleName)
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("root clusterrole get unexpected error: %v", err)
	}
	if _, err := csRoot2.RbacV1().ClusterRoleBindings().Get(ctx, bindingName, metav1.GetOptions{}); err == nil {
		t.Fatalf("unexpectedly found clusterrolebinding %q in root cluster after restart", bindingName)
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("root clusterrolebinding get unexpected error: %v", err)
	}

	// Shared RBAC projection must rebuild from relist/watch after restart.
	waitForSubjectAccessReviewWithLogs(ctx, t, csA2, sar, true, s2.logs)
	waitForSubjectAccessReviewWithLogs(ctx, t, csRoot2, sar, false, s2.logs)
}

