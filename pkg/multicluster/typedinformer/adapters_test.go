/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package typedinformer

import (
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apistorage "k8s.io/apiserver/pkg/storage"

	"github.com/kplane-dev/informer"
)

// TestListersReturnTypedNotFound asserts that every Lister.Get returns an
// apierrors.IsNotFound-compatible error when the requested object is
// absent from the cache. Upstream's NamespaceLifecycle admission plugin
// distinguishes "not in cache, fall back to live lookup" from "real
// error, return 500" via apierrors.IsNotFound(err) — if the lister
// returns plain fmt.Errorf("namespace %q not found", name) instead of a
// typed NotFound, IsNotFound returns false, admission wraps the error in
// errors.NewInternalError and returns 500 to the client without ever
// attempting the recovery path.
//
// This silently breaks bootstrap on any storage backend slower than etcd:
// etcd is fast enough that the informer cache holds kube-system by the
// time the first ConfigMap/Lease create lands, but Spanner's higher
// per-write latency means dependent writes arrive BEFORE the namespace
// reaches the lister cache. Without typed NotFound, admission returns
// 500 forever instead of falling through to the live lookup that would
// succeed.
//
// Fixing only namespaceLister would leave the others (Secret, Pod, Node,
// Service, EndpointSlice, MutatingWebhookConfiguration,
// ValidatingWebhookConfiguration, ServiceAccount) with the same bug for
// every controller that retries on IsNotFound. This test pins every
// adapter in adapters.go.
func TestListersReturnTypedNotFound(t *testing.T) {
	mci := informer.New(informer.Config{
		Storage:        nopStorage{},
		ResourcePrefix: "/anything/clusters/",
		GroupResource:  schema.GroupResource{Resource: "anything"},
	})

	tests := []struct {
		name string
		get  func() error
	}{
		{"namespace", func() error {
			_, err := NewNamespaceLister(mci, "c1").Get("kube-system")
			return err
		}},
		{"secret", func() error {
			_, err := NewSecretLister(mci, "c1").Secrets("ns").Get("missing")
			return err
		}},
		{"serviceaccount", func() error {
			_, err := NewServiceAccountLister(mci, "c1").ServiceAccounts("ns").Get("missing")
			return err
		}},
		{"pod", func() error {
			_, err := NewPodLister(mci, "c1").Pods("ns").Get("missing")
			return err
		}},
		{"node", func() error {
			_, err := NewNodeLister(mci, "c1").Get("missing")
			return err
		}},
		{"service", func() error {
			_, err := NewServiceLister(mci, "c1").Services("ns").Get("missing")
			return err
		}},
		{"endpointslice", func() error {
			_, err := NewEndpointSliceLister(mci, "c1").EndpointSlices("ns").Get("missing")
			return err
		}},
		{"mutatingwebhookconfiguration", func() error {
			_, err := NewMutatingWebhookConfigurationLister(mci, "c1").Get("missing")
			return err
		}},
		{"validatingwebhookconfiguration", func() error {
			_, err := NewValidatingWebhookConfigurationLister(mci, "c1").Get("missing")
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.get()
			if err == nil {
				t.Fatalf("expected error on missing object")
			}
			if !apierrors.IsNotFound(err) {
				t.Fatalf("expected apierrors.IsNotFound(err)==true; got err=%v (%T) — admission plugins that recover via live lookup require typed NotFound", err, err)
			}
		})
	}
}

// nopStorage satisfies apistorage.Interface enough for informer.New —
// the informer's Run loop never starts in this test.
type nopStorage struct{ apistorage.Interface }
