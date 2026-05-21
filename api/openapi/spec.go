/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package openapi embeds and serves the canonical OpenAPI 3 document for
// kplane-native endpoints (snapshot + Fleet). SDKs and tooling generate
// against this single source of truth at api/openapi/kplane.v1.yaml.
package openapi

import (
	_ "embed"
	"sync"

	"sigs.k8s.io/yaml"
)

//go:embed kplane.v1.yaml
var specYAML []byte

var (
	specJSONOnce sync.Once
	specJSON     []byte
	specJSONErr  error
)

// YAML returns the OpenAPI document as YAML.
func YAML() []byte { return specYAML }

// JSON returns the OpenAPI document as JSON (lazily converted, cached).
func JSON() ([]byte, error) {
	specJSONOnce.Do(func() {
		specJSON, specJSONErr = yaml.YAMLToJSON(specYAML)
	})
	return specJSON, specJSONErr
}
