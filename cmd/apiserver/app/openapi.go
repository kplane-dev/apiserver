/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package app

import (
	"net/http"

	"github.com/kplane-dev/apiserver/api/openapi"
)

// matchKplaneOpenAPI reports whether the request targets the kplane OpenAPI
// document. These endpoints live at the *server* root and are not scoped to
// any virtual control plane:
//
//	/openapi/kplane.json
//	/openapi/kplane.yaml
//
// PathExtractor only rewrites paths under /clusters/{cid}/control-plane/, so
// the unstripped form is what hits this matcher.
func matchKplaneOpenAPI(r *http.Request) (format string, ok bool) {
	if r == nil {
		return "", false
	}
	switch r.URL.Path {
	case "/openapi/kplane.json":
		return "json", true
	case "/openapi/kplane.yaml":
		return "yaml", true
	}
	return "", false
}

// withKplaneOpenAPI wraps next so requests to /openapi/kplane.{json,yaml}
// short-circuit and serve the embedded spec. Anything else falls through.
//
// This wrapper is mounted on every BuildHandlerChainFunc in the apiserver
// (kube, apiextensions, aggregator) so the spec is reachable no matter which
// server ultimately handles the request.
func withKplaneOpenAPI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if format, ok := matchKplaneOpenAPI(r); ok {
			serveKplaneOpenAPI(w, format)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// serveKplaneOpenAPI writes the embedded kplane OpenAPI document to w in the
// requested format. The document is the contract every SDK generates from.
func serveKplaneOpenAPI(w http.ResponseWriter, format string) {
	switch format {
	case "yaml":
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openapi.YAML())
		return
	case "json":
		b, err := openapi.JSON()
		if err != nil {
			http.Error(w, "openapi: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
		return
	}
	http.NotFound(w, nil)
}
