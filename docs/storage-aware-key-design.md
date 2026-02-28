# Key-Aware Shared Storage Design

## Overview
This document proposes a storage architecture that keeps the shared informer/watch
fanout model while removing the need to persist a user-visible cluster label on
objects for list/watch correctness.

The core idea is to carry cluster identity as internal storage metadata derived
from keyspace semantics, not from object metadata.

## Problem Statement
Current multicluster storage uses one shared store/watchcache per resource kind,
which is the right scalability direction. However, cache keying currently depends
on a server-owned label (for example `multicluster.k8s.io/cluster`) persisted on
objects so cluster identity can be recovered from object-only key functions.

That creates two issues:
- **API shape drift**: clients may observe server-owned placement labels.
- **Coupling**: cluster identity is coupled to object metadata rather than storage
  metadata, making future layout changes harder.

## Goals
- Preserve one shared watcher/cache per resource kind.
- Preserve strict per-cluster list/watch/read/write isolation.
- Eliminate dependency on object labels for watchcache/index keying.
- Keep cluster identity internal-only and non-serialized to clients.
- Allow future etcd key layout evolution without touching API object schema.

## Non-Goals
- Replacing Kubernetes generic registry behavior wholesale.
- Introducing per-cluster watchcaches/stores again.
- Changing external Kubernetes API semantics for selectors/versioning/watch.

## Design Summary
Introduce a key-aware internal envelope used inside storage/cache fanout:
- `Object runtime.Object` (decoded Kubernetes object)
- `ClusterID string` (internal placement identity)
- `StorageKey string` (canonical key)
- optional `LayoutVersion string`

All fanout/index decisions use `ClusterID` from the envelope. Only `Object` is
serialized in API responses.

### Placement Resolver
Add a small abstraction that resolves cluster identity from keyspace metadata:
- `ClusterFromStorageKey(key string) (clusterID string, ok bool)`
- versioned implementations for key layout evolution.

This centralizes parsing and decouples routing logic from hard-coded path parsing
scattered across caches.

## Architecture
1. Request routing still establishes cluster scope in context.
2. Storage key rewrite still places data under cluster-specific subpaths.
3. Shared low-level watch/list reads from kind-root prefix.
4. Internal cache transforms events into envelope entries.
5. Envelope `ClusterID` drives per-request filter/fanout.
6. API response encoding emits only wrapped object payload.

## New and Modified Files / Methods

### New Files (proposed)
- `pkg/multicluster/storage/placement_resolver.go`
  - `type PlacementResolver interface`
  - `type KeyLayoutPlacementResolver struct`
  - `func (r *KeyLayoutPlacementResolver) ClusterFromStorageKey(...)`

- `pkg/multicluster/storage/entry.go`
  - `type InternalEntry struct { Object, ClusterID, StorageKey, LayoutVersion }`
  - helper constructors and cloning utilities.

- `pkg/multicluster/storage/keyaware_cache.go`
  - shared cache wrapper that consumes watch events and stores `InternalEntry`.
  - per-request filtering by cluster.

- `pkg/multicluster/storage/keyaware_watch.go`
  - wraps watch streams and emits only runtime objects while using entry metadata
    for internal dispatch.

- `pkg/multicluster/storage/keyaware_index.go`
  - index helpers keyed by `(clusterID, namespace, name)` from entry metadata.

- `pkg/multicluster/storage/keyaware_cache_test.go`
- `pkg/multicluster/storage/placement_resolver_test.go`

### Existing Files to Modify
- `pkg/multicluster/storage.go`
  - keep context-based key rewrite and shared kind-root behavior.
  - replace object-label-based key derivation path with key-aware cache adapter.
  - remove cluster-label enforcement paths once migration completes.

- `pkg/multicluster/options.go`
  - add options for placement resolver selection and layout version.

- `cmd/apiserver/app/config.go`
  - wire new decorator/options into server config path.

- `pkg/multicluster/storage_test.go`
  - adapt tests to assert isolation without relying on persisted cluster label.

- `test/smoke/*.go` (targeted)
  - add/adjust smoke tests for list/watch isolation with no visible cluster label.

## Wiring Plan

### 1) Decorator Wiring
Continue using `RESTOptionsDecorator` entrypoint, but swap internals:
- current: wraps storage and relies on object metadata for cache keying.
- proposed: wraps storage with key-aware cache/entry adapter.

Wiring location:
- `cmd/apiserver/app/config.go`
  - existing `decorateRESTOptionsGetter(...)` integration remains the anchor.

### 2) Resolver Injection
Inject `PlacementResolver` via multicluster options:
- default resolver handles current `/.../clusters/<cid>/...` layout.
- optional future resolver supports new key layouts.

## Upstream Reuse vs Custom Implementation

### Reuse from Upstream
- Generic registry strategy flow (`Create/Get/List/Update/Delete/Watch` contract).
- Etcd storage backend and encoding pipeline.
- Selection predicates and API machinery object conversion.
- Existing apiserver handler chain, authn/authz/admission plumbing.

### Wrap / Extend Locally
- Multicluster RESTOptions decoration and key rewrite.
- Placement resolver and internal entry envelope.
- Shared cache dispatch keyed by cluster metadata.

### Likely Not Reusable As-Is
- Upstream cacher assumptions where keying/indexing is object-only.
  - We should avoid deep invasive upstream patching in-tree.
  - Prefer local key-aware adapter/fork boundary with minimal surface area.

## Migration Plan

### Phase 0: Design and Scaffolding
- Add resolver and entry types.
- No behavior change.

### Phase 1: Immediate Cutover
- Implement key-aware cache path and switch read/watch dispatch directly during implementation.
- Keep label writes temporarily for rollback safety.

### Phase 2: Remove Label Dependency
- Stop requiring label for keying/indexing.
- Remove cluster-label mutating/validation requirements where safe.
- Add response-shape tests ensuring no server-owned cluster label leakage.

### Phase 3: Cleanup
- Delete legacy code paths.
- Update docs and operational runbooks.

## Validation and Test Plan
- Unit:
  - resolver correctness for current and alternate layouts.
  - key-aware cache fanout correctness and concurrency behavior.
  - watch event ordering and bookmark behavior.
- Integration:
  - existing smoke suite must pass.
  - dedicated tests for list/watch isolation with absent cluster label.
  - CRD/list/watch behavior parity tests.
- Performance:
  - compare goroutine count, memory, and watch fanout metrics vs current model.

## Risks and Mitigations
- **Risk**: subtle watch semantics regressions.
  - Mitigation: broaden integration/smoke coverage and validate watch semantics in targeted tests before merge.
- **Risk**: cache key collisions across clusters.
  - Mitigation: explicit `(clusterID, namespace, name)` composite indexes.
- **Risk**: rollout complexity.
  - Mitigation: feature gate + phased migration + smoke regression gates.

## Open Questions
- Should we expose layout version in metrics for easier migrations?
- Where should fork boundary live to minimize divergence from upstream changes?

## Decision
Proceed with key-aware internal entry and placement resolver design, preserving
shared informer/watch fanout while removing object-label dependency for cluster
identity.
