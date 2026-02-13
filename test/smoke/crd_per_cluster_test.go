package smoke

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestCRDEstablishesInVirtualCluster(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clusterID := "c-" + randSuffix(3)
	group := fmt.Sprintf("testing-%s.kplane.dev", randSuffix(3))
	plural := "testwidgets"
	crdName := plural + "." + group

	crdClient := apixClientForCluster(t, s, clusterID)
	kubeClient := kubeClientForCluster(t, s, clusterID)
	createTestCRD(ctx, t, crdClient, crdName, group, plural)
	waitForCRDEstablished(ctx, t, crdClient, clusterID, crdName)
	waitForResourcePresence(t, kubeClient, clusterID, group+"/v1", plural, true)
}

func TestCRDDefinitionsIsolatedAcrossClusters(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	clusterB := "c-" + randSuffix(3)
	group := fmt.Sprintf("isolated-%s.kplane.dev", randSuffix(3))
	plural := "testwidgets"
	crdName := plural + "." + group

	crdClientA := apixClientForCluster(t, s, clusterA)
	crdClientB := apixClientForCluster(t, s, clusterB)
	kubeClientA := kubeClientForCluster(t, s, clusterA)
	kubeClientB := kubeClientForCluster(t, s, clusterB)

	createTestCRD(ctx, t, crdClientA, crdName, group, plural)
	waitForCRDEstablished(ctx, t, crdClientA, clusterA, crdName)
	waitForResourcePresence(t, kubeClientA, clusterA, group+"/v1", plural, true)
	waitForResourcePresence(t, kubeClientB, clusterB, group+"/v1", plural, false)

	createTestCRD(ctx, t, crdClientB, crdName, group, plural)
	waitForCRDEstablished(ctx, t, crdClientB, clusterB, crdName)
	waitForResourcePresence(t, kubeClientB, clusterB, group+"/v1", plural, true)

	crdA, err := crdClientA.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cluster=%s get CRD %s: %v", clusterA, crdName, err)
	}
	crdB, err := crdClientB.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cluster=%s get CRD %s: %v", clusterB, crdName, err)
	}
	if crdA.UID == crdB.UID {
		t.Fatalf("expected per-cluster CRD objects; got same UID %q", crdA.UID)
	}
}

func TestCRDDiscoveryUpdateInADoesNotAffectB(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	clusterB := "c-" + randSuffix(3)
	group := fmt.Sprintf("update-%s.kplane.dev", randSuffix(3))
	plural := "testwidgets"
	crdName := plural + "." + group

	crdClientA := apixClientForCluster(t, s, clusterA)
	crdClientB := apixClientForCluster(t, s, clusterB)
	kubeClientA := kubeClientForCluster(t, s, clusterA)
	kubeClientB := kubeClientForCluster(t, s, clusterB)

	createTestCRD(ctx, t, crdClientA, crdName, group, plural)
	createTestCRD(ctx, t, crdClientB, crdName, group, plural)
	waitForCRDEstablished(ctx, t, crdClientA, clusterA, crdName)
	waitForCRDEstablished(ctx, t, crdClientB, clusterB, crdName)
	waitForResourcePresence(t, kubeClientA, clusterA, group+"/v1", plural, true)
	waitForResourcePresence(t, kubeClientB, clusterB, group+"/v1", plural, true)

	updateCRDAddVersion(ctx, t, crdClientA, crdName)
	waitForResourcePresence(t, kubeClientA, clusterA, group+"/v2", plural, true)
	waitForResourcePresence(t, kubeClientB, clusterB, group+"/v2", plural, false)
}

func TestCRDResourceCRUDPerClusterIsolation(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clusterA := "c-" + randSuffix(3)
	clusterB := "c-" + randSuffix(3)
	group := fmt.Sprintf("crud-%s.kplane.dev", randSuffix(3))
	plural := "testwidgets"
	crdName := plural + "." + group
	gvr := schema.GroupVersionResource{Group: group, Version: "v1", Resource: plural}
	namespace := "default"

	crdClientA := apixClientForCluster(t, s, clusterA)
	crdClientB := apixClientForCluster(t, s, clusterB)
	kubeClientA := kubeClientForCluster(t, s, clusterA)
	kubeClientB := kubeClientForCluster(t, s, clusterB)
	dynA := dynamicClientForCluster(t, s, clusterA)
	dynB := dynamicClientForCluster(t, s, clusterB)

	if err := waitForNamespace(ctx, kubeClientA, namespace); err != nil {
		t.Fatalf("cluster=%s wait namespace %s: %v", clusterA, namespace, err)
	}
	if err := waitForNamespace(ctx, kubeClientB, namespace); err != nil {
		t.Fatalf("cluster=%s wait namespace %s: %v", clusterB, namespace, err)
	}

	createTestCRD(ctx, t, crdClientA, crdName, group, plural)
	waitForCRDEstablished(ctx, t, crdClientA, clusterA, crdName)
	waitForResourcePresence(t, kubeClientA, clusterA, group+"/v1", plural, true)

	_, err := dynA.Resource(gvr).Namespace(namespace).Create(ctx, testWidget(group, "w-a"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create CR instance: %v", clusterA, err)
	}
	if _, err := dynA.Resource(gvr).Namespace(namespace).Get(ctx, "w-a", metav1.GetOptions{}); err != nil {
		t.Fatalf("cluster=%s get CR instance: %v", clusterA, err)
	}

	_, err = dynB.Resource(gvr).Namespace(namespace).Create(ctx, testWidget(group, "w-b"), metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("cluster=%s expected create CR instance to fail before CRD install", clusterB)
	}
	if !apierrors.IsNotFound(err) && !strings.Contains(strings.ToLower(err.Error()), "not found") {
		t.Fatalf("cluster=%s expected not found before CRD install, got: %v", clusterB, err)
	}

	createTestCRD(ctx, t, crdClientB, crdName, group, plural)
	waitForCRDEstablished(ctx, t, crdClientB, clusterB, crdName)
	waitForResourcePresence(t, kubeClientB, clusterB, group+"/v1", plural, true)

	if _, err := dynB.Resource(gvr).Namespace(namespace).Create(ctx, testWidget(group, "w-b"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("cluster=%s create CR instance after CRD install: %v", clusterB, err)
	}
}

func apixClientForCluster(t *testing.T, s *testAPIServer, clusterID string) *apiextensionsclient.Clientset {
	t.Helper()
	cfg := kubeConfigForCluster(s.clusterURL(clusterID))
	cs, err := apiextensionsclient.NewForConfig(rest.CopyConfig(cfg))
	if err != nil {
		t.Fatalf("cluster=%s build apiextensions client: %v", clusterID, err)
	}
	return cs
}

func dynamicClientForCluster(t *testing.T, s *testAPIServer, clusterID string) dynamic.Interface {
	t.Helper()
	cfg := kubeConfigForCluster(s.clusterURL(clusterID))
	c, err := dynamic.NewForConfig(rest.CopyConfig(cfg))
	if err != nil {
		t.Fatalf("cluster=%s build dynamic client: %v", clusterID, err)
	}
	return c
}

func testWidget(group, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": group + "/v1",
			"kind":       "TestWidget",
			"metadata": map[string]any{
				"name": name,
			},
			"spec": map[string]any{
				"foo": "bar",
			},
		},
	}
}

func createTestCRD(ctx context.Context, t *testing.T, cs *apiextensionsclient.Clientset, crdName, group, plural string) {
	t.Helper()
	_, err := cs.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: crdName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   plural,
				Singular: "testwidget",
				Kind:     "TestWidget",
				ListKind: "TestWidgetList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1",
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create CRD %s: %v", crdName, err)
	}
}

func waitForCRDEstablished(ctx context.Context, t *testing.T, cs *apiextensionsclient.Clientset, clusterID, crdName string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		crd, err := cs.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
		if err == nil {
			for _, c := range crd.Status.Conditions {
				if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
					return
				}
			}
			lastErr = fmt.Errorf("conditions=%v", crd.Status.Conditions)
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("cluster=%s CRD %s not Established: %v", clusterID, crdName, lastErr)
}

func updateCRDAddVersion(ctx context.Context, t *testing.T, cs *apiextensionsclient.Clientset, crdName string) {
	t.Helper()
	crd, err := cs.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CRD %s before update: %v", crdName, err)
	}
	crd.Spec.Versions = append(crd.Spec.Versions, apiextensionsv1.CustomResourceDefinitionVersion{
		Name:    "v2",
		Served:  true,
		Storage: false,
		Schema: &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
		},
	})
	if _, err := cs.ApiextensionsV1().CustomResourceDefinitions().Update(ctx, crd, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update CRD %s add v2: %v", crdName, err)
	}
}

func waitForResourcePresence(t *testing.T, cs kubernetes.Interface, clusterID, gv, resource string, want bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		rl, err := cs.Discovery().ServerResourcesForGroupVersion(gv)
		if err != nil {
			if !want && apierrors.IsNotFound(err) {
				return
			}
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		found := false
		for _, r := range rl.APIResources {
			if r.Name == resource {
				found = true
				break
			}
		}
		if found == want {
			return
		}
		lastErr = fmt.Errorf("resource %q in %s presence=%t want=%t", resource, gv, found, want)
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("cluster=%s resource discovery mismatch for %s %s: %v", clusterID, gv, resource, lastErr)
}

