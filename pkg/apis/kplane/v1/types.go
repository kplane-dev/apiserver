/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the kplane-native API group. Resources in this group are
// management-plane primitives; they live in the root control plane only.
const GroupName = "kplane.dev"

// Version is the served version of the kplane API.
const Version = "v1"

// SchemeGroupVersion is the group/version used to register kplane types.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// FleetGroupResource identifies the Fleet resource.
var FleetGroupResource = schema.GroupResource{Group: GroupName, Resource: "fleets"}

// Fleet declares a desired number of virtual control planes derived from a
// template. The Fleet controller running inside the apiserver picks cluster
// IDs, primes them with the same bootstrap that organic traffic would
// trigger, and reports per-member readiness in Status.
type Fleet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FleetSpec   `json:"spec,omitempty"`
	Status FleetStatus `json:"status,omitempty"`
}

// FleetSpec is the desired state of a Fleet.
//
// V0 semantics: a Fleet provisions empty VCPs (system namespaces, RBAC,
// default service). Scenario seeding, manifest application, and TTL-based
// destruction are not part of V0.
type FleetSpec struct {
	// Replicas is the desired number of VCPs in this Fleet.
	// Required. Must be >= 0.
	Replicas int32 `json:"replicas"`

	// NamePrefix is prepended to the synthesized cluster IDs. If unset,
	// "<fleet-name>-" is used. Synthesized IDs are "<prefix><index>" where
	// index is a 4-digit zero-padded counter starting at 0000.
	// Ignored when Names is non-empty.
	NamePrefix string `json:"namePrefix,omitempty"`

	// Names overrides synthesized cluster IDs. When set, len(Names) must
	// equal Replicas (validated by the controller). Each name must be a
	// valid DNS label.
	Names []string `json:"names,omitempty"`

	// TTLSeconds is reserved for future use. V0 does not garbage-collect
	// VCPs when a Fleet is deleted; this field is parsed and stored but
	// not yet enforced.
	TTLSeconds *int64 `json:"ttlSeconds,omitempty"`
}

// FleetMemberPhase is a coarse state for a single Fleet member.
type FleetMemberPhase string

const (
	// FleetMemberPending means the bootstrap workers have not finished for
	// this member yet.
	FleetMemberPending FleetMemberPhase = "Pending"
	// FleetMemberReady means the apiserver has finished priming the VCP and
	// the cluster's /readyz responds 200.
	FleetMemberReady FleetMemberPhase = "Ready"
	// FleetMemberFailed indicates a non-recoverable bootstrap error for this
	// member (current V0 surfaces this via the Message field; the controller
	// will retry on the next resync).
	FleetMemberFailed FleetMemberPhase = "Failed"
)

// FleetMember is the observed state of a single VCP in a Fleet.
type FleetMember struct {
	// ClusterID is the path segment under /clusters/{id}/control-plane/...
	ClusterID string `json:"clusterID"`
	// Phase is a coarse readiness state.
	Phase FleetMemberPhase `json:"phase"`
	// Message is a free-form description of the last reconcile outcome for
	// this member. Useful for debugging V0; will be replaced by structured
	// conditions in a future version.
	Message string `json:"message,omitempty"`
	// LastTransitionTime is when Phase last changed.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// FleetStatus is the observed state of a Fleet.
type FleetStatus struct {
	// ObservedGeneration is the Fleet metadata.generation that the
	// controller last reconciled.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ReadyReplicas is the number of members currently in the Ready phase.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Members is the per-VCP detail for every desired member.
	Members []FleetMember `json:"members,omitempty"`
}

// FleetList is the list type for Fleet.
type FleetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Fleet `json:"items"`
}
