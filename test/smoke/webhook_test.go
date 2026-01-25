package smoke

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func newCA(t *testing.T) (caCert *x509.Certificate, caKey *rsa.PrivateKey, caPEM []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen ca key: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "smoke-ca",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	var b []byte
	b = append(b, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	return cert, key, b
}

func newServerCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey, host string) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen server key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: host,
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip := net.ParseIP(host); ip != nil {
		tpl.IPAddresses = []net.IP{ip}
	} else {
		tpl.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("keypair: %v", err)
	}
	return cert
}

func startValidatingWebhookServer(t *testing.T, listenHost, certDNS string) (addr string, caBundle []byte, stop func()) {
	t.Helper()

	caCert, caKey, caPEM := newCA(t)
	_ = caCert
	srvCert := newServerCert(t, caCert, caKey, certDNS)

	mux := http.NewServeMux()
	mux.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var review admissionv1.AdmissionReview
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := admissionv1.AdmissionResponse{
			UID:     review.Request.UID,
			Allowed: false,
			Result: &metav1.Status{
				Message: "denied by smoke webhook",
				Reason:  metav1.StatusReasonForbidden,
				Code:    403,
			},
		}
		out := admissionv1.AdmissionReview{
			TypeMeta: review.TypeMeta,
			Response: &resp,
		}
		_ = json.NewEncoder(w).Encode(out)
	})

	ln, err := net.Listen("tcp", net.JoinHostPort(listenHost, "0"))
	if err != nil {
		t.Fatalf("listen webhook: %v", err)
	}
	srv := &http.Server{
		Handler: mux,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{srvCert},
		},
	}
	go func() {
		_ = srv.ServeTLS(ln, "", "")
	}()

	stopFn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
	return ln.Addr().String(), caPEM, stopFn
}

func TestWebhook_ServiceBacked_MulticlusterScoping(t *testing.T) {
	etcd := os.Getenv("ETCD_ENDPOINTS")
	s := startAPIServer(t, etcd)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	hostIP := firstNonLoopbackIPv4(t)

	// Pick a non-root cluster to validate the suspected bug.
	clusterA := "c-" + randSuffix(3)
	clusterB := "c-" + randSuffix(3)
	root := "root"

	csA := kubeClientForCluster(t, s, clusterA)
	csB := kubeClientForCluster(t, s, clusterB)
	csRoot := kubeClientForCluster(t, s, root)

	ns := "default"

	// Service + EndpointSlice that resolves to our local webhook server.
	svcName := "svc-wh-" + randSuffix(3)
	serviceDNS := svcName + "." + ns + ".svc"

	whAddr, caBundle, stopWh := startValidatingWebhookServer(t, hostIP, serviceDNS)
	defer stopWh()

	_, portStr, err := net.SplitHostPort(whAddr)
	if err != nil {
		t.Fatalf("split webhook addr: %v", err)
	}
	_, err = csA.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcName},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "https", Port: 443, TargetPort: intstrFromString("https")}},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("cluster=%s create svc: %v", clusterA, err)
	}

	esName := "eps-" + randSuffix(3)
	proto := corev1.ProtocolTCP
	_, err = csA.DiscoveryV1().EndpointSlices(ns).Create(ctx, &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: esName,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: svcName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{hostIP}},
		},
		Ports: []discoveryv1.EndpointPort{
			{Name: ptr("https"), Protocol: &proto, Port: ptrInt32(parsePort(t, portStr))},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create endpointslice: %v", clusterA, err)
	}

	// ValidatingWebhookConfiguration in clusterA that should deny ConfigMap creates.
	whName := "vwh-" + randSuffix(3)
	sideEffects := admissionregv1.SideEffectClassNone
	failurePolicy := admissionregv1.Fail
	matchPolicy := admissionregv1.Equivalent
	timeout := int32(5)
	scope := admissionregv1.NamespacedScope
	path := "/validate"
	port := int32(443)

	_, err = csA.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx, &admissionregv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: whName},
		Webhooks: []admissionregv1.ValidatingWebhook{
			{
				Name: "deny-configmaps.smoke.kplane.dev",
				ClientConfig: admissionregv1.WebhookClientConfig{
					Service: &admissionregv1.ServiceReference{
						Namespace: ns,
						Name:      svcName,
						Path:      &path,
						Port:      &port,
					},
					CABundle: caBundle,
				},
				Rules: []admissionregv1.RuleWithOperations{
					{
						Operations: []admissionregv1.OperationType{admissionregv1.Create},
						Rule: admissionregv1.Rule{
							APIGroups:   []string{""},
							APIVersions: []string{"v1"},
							Resources:   []string{"configmaps"},
							Scope:       &scope,
						},
					},
				},
				FailurePolicy:           &failurePolicy,
				MatchPolicy:             &matchPolicy,
				SideEffects:             &sideEffects,
				AdmissionReviewVersions: []string{"v1"},
				TimeoutSeconds:          &timeout,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("cluster=%s create validatingwebhookconfiguration: %v", clusterA, err)
	}
	if list, err := csRoot.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, metav1.ListOptions{}); err == nil {
		t.Logf("root sees %d validatingwebhookconfigurations", len(list.Items))
	}
	t.Cleanup(func() {
		_ = csA.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(context.Background(), whName, metav1.DeleteOptions{})
	})

	// Allow informers to observe the service/endpoints before admission calls.
	time.Sleep(2 * time.Second)

	// Isolation assertion: the webhook Service/EndpointSlice should not exist in root.
	{
		_, err := csRoot.CoreV1().Services(ns).Get(ctx, svcName, metav1.GetOptions{})
		if err == nil || !apierrors.IsNotFound(err) {
			t.Fatalf("expected root to NOT have webhook service %s/%s, got: %v", ns, svcName, err)
		}
		slices, err := csRoot.DiscoveryV1().EndpointSlices(ns).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set{discoveryv1.LabelServiceName: svcName}.AsSelector().String(),
		})
		if err != nil {
			t.Fatalf("list root endpointslices: %v", err)
		}
		if len(slices.Items) != 0 {
			t.Fatalf("expected root to have 0 endpointslices for service %s/%s, got: %d", ns, svcName, len(slices.Items))
		}
	}

	// Try to create a ConfigMap in clusterA; we expect the webhook to deny.
	_, err = csA.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-deny-" + randSuffix(3)},
		Data:       map[string]string{"a": "b"},
	}, metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("expected webhook to deny configmap create in cluster=%s, but it succeeded (this indicates the webhook registration/resolution is scoped wrong)", clusterA)
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden from webhook in cluster=%s, got: %v", clusterA, err)
	}

	// Cross-cluster assertion: clusterB should NOT be affected by clusterA's webhook.
	_, err = csB.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm-allow-" + randSuffix(3)},
		Data:       map[string]string{"a": "b"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("expected configmap create to succeed in other cluster (%s), got: %v", clusterB, err)
	}
}

func ptr[T any](v T) *T { return &v }

func ptrInt32(v int32) *int32 { return &v }

func parsePort(t *testing.T, s string) int32 {
	t.Helper()
	var p int
	_, err := fmt.Sscanf(s, "%d", &p)
	if err != nil || p <= 0 || p > 65535 {
		t.Fatalf("bad port %q: %v", s, err)
	}
	return int32(p)
}

func intstrFromString(s string) intstr.IntOrString {
	return intstr.IntOrString{Type: intstr.String, StrVal: s}
}

func firstNonLoopbackIPv4(t *testing.T) string {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("list interfaces: %v", err)
	}
	for _, iface := range ifaces {
		if (iface.Flags&net.FlagUp) == 0 || (iface.Flags&net.FlagLoopback) != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}
			return ip.String()
		}
	}
	t.Fatalf("no non-loopback IPv4 address found")
	return ""
}
