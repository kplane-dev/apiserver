/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package openapi

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestSpecParses ensures the embedded YAML can be turned into JSON without
// errors. Catches any drift / syntax breakage at build time.
func TestSpecParses(t *testing.T) {
	t.Parallel()
	b, err := JSON()
	if err != nil {
		t.Fatalf("YAML→JSON conversion failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("converted JSON is empty")
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("JSON not parseable: %v", err)
	}
	if v, ok := doc["openapi"].(string); !ok || !strings.HasPrefix(v, "3.") {
		t.Fatalf("missing/invalid openapi version field: %v", doc["openapi"])
	}
}

// TestSpecContainsCoreSurface guards against accidental removal of the two
// V0 endpoints the SDK depends on.
func TestSpecContainsCoreSurface(t *testing.T) {
	t.Parallel()
	b, err := JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	var doc struct {
		Paths map[string]any `json:"paths"`
		Components struct {
			Schemas map[string]any `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	wantPaths := []string{
		"/clusters/{cluster}/control-plane/snapshot",
		"/apis/kplane.dev/v1/fleets",
		"/apis/kplane.dev/v1/fleets/{name}",
		"/apis/kplane.dev/v1/fleets/{name}/status",
	}
	for _, p := range wantPaths {
		if _, ok := doc.Paths[p]; !ok {
			t.Errorf("missing path %q in spec", p)
		}
	}
	wantSchemas := []string{"Snapshot", "SnapshotResource", "Fleet", "FleetList", "FleetSpec", "FleetStatus", "FleetMember"}
	for _, s := range wantSchemas {
		if _, ok := doc.Components.Schemas[s]; !ok {
			t.Errorf("missing component schema %q in spec", s)
		}
	}
}
