package smoke

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
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
		t.Fatalf("cluster=%s create clusterrole: %v", clusterA, err)
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

	waitForSubjectAccessReview(ctx, t, csA, sar, true)
	waitForSubjectAccessReview(ctx, t, csRoot, sar, false)
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
