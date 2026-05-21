/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package v1 contains kplane-native API types served by the kplane apiserver.
//
// These types are installed as CRDs in the root control plane on apiserver
// startup, and provide management-plane primitives — like Fleet — for
// orchestrating virtual control planes (VCPs).
package v1
