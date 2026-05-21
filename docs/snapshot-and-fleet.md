# Snapshot + Fleet (V0)

This document describes two kplane-native primitives added to the apiserver:

1. `GET /clusters/{cluster}/control-plane/snapshot` — an aggregate, in-memory
   read of all live informer state for a virtual control plane.
2. `kind: Fleet` (group `kplane.dev/v1`) — a declarative way to provision N
   virtual control planes from a template.

Together they let an external client (a Python SDK, an RL training loop, an
agent platform) treat a VCP as a cheap, scoreable, forkable unit of work.

## Why these primitives

Existing AI "sandboxes" are Linux-shaped: an agent gets a shell, a filesystem,
and maybe a Jupyter kernel. That works for code execution, but it falls down
the moment an agent needs to deploy a service, wire pods together, share
state between sub-agents, or be granted scoped infrastructure capabilities by
a human.

Kubernetes is the universal infrastructure API. kplane is the only system
that can hand a *real* control plane to every agent or RL rollout at single-
digit-millisecond latency and single-digit-megabyte memory cost. Snapshot +
Fleet turn that capability into a usable substrate:

- **Snapshot** is the read primitive. It returns a structured view of the
  current state of a plane — every resource the apiserver knows about — in a
  single round-trip, served from the shared informer cache (no etcd hit).
  This is what makes scoring an agent rollout cheap.
- **Fleet** is the write primitive. It lets a caller say "I want 10,000
  planes derived from this template" and have the apiserver pick IDs, prime
  them with the standard bootstrap pipeline (system namespaces, RBAC,
  default service, CRD runtime), and report readiness.

V0 is deliberately small. We expose *capabilities*, not opinions: no
scenario DSL, no scoring DSL, no opinionated trajectory format. Callers
build those on top in whatever shape suits their training stack.

## Architecture

```
                   ┌────────────────────────────────────────┐
client (kubectl,   │  kplane apiserver (single Go process)  │
 SDK, controller)  │                                        │
                   │  ┌──────────────────────────────────┐  │
   /clusters/X/    │  │ multicluster routing             │  │
   control-plane/  ├──┤  PathExtractor → cid in ctx      │  │
   {api|snapshot}  │  └──────────────┬───────────────────┘  │
                   │                 │                      │
                   │  ┌──────────────▼───────────────────┐  │
                   │  │ dispatch                         │  │
                   │  │   matchSnapshot?  → snapshot     │  │
                   │  │   CRD path?       → CRD runtime  │  │
                   │  │   else            → kube REST    │  │
                   │  └──────────────┬───────────────────┘  │
                   │                 │                      │
                   │  ┌──────────────▼───────────────────┐  │
                   │  │ shared InformerRegistry          │  │
                   │  │   one MCI per resource           │  │
                   │  │   List(cid) is in-memory         │  │
                   │  └──────────────────────────────────┘  │
                   │                                        │
                   │  ┌──────────────────────────────────┐  │
                   │  │ FleetController (in-process)     │  │
                   │  │   installs CRD into root         │  │
                   │  │   watches Fleet objects          │  │
                   │  │   calls OnClusterSelected(cid)   │  │
                   │  └──────────────────────────────────┘  │
                   └────────────────────────────────────────┘
```

### Snapshot

- Mounted in the same dispatch layer as CRD routing, so it sees the cluster
  context that `WithClusterRouting` populates and runs through the standard
  auth/audit/panic-recovery chain via `wrapClusterCRDHandler`.
- Iterates either `InformerRegistry.ListLive()` (default) or
  `InformerRegistry.ListRegistered()` (`?warm=true`) and calls
  `mci.List(cid)` for each resource. Reads never hit etcd.
- Strips `managedFields` from each object to keep payloads tractable.
- Reports a `synced: bool` per resource so callers can tell apart "no
  objects" from "informer hasn't synced yet."

### Fleet

- `Fleet` is a cluster-scoped CRD in the `kplane.dev` API group. The CRD is
  installed into the root control plane on startup by `FleetController.Start`.
- The controller uses a `dynamic` client + `dynamicinformer` against the
  root cluster loopback. Reconciles are driven by a `workqueue` plus a
  periodic resync to refresh per-member readiness.
- `EnsureCluster` is wired by `cmd/apiserver/app/config.go` to invoke the
  composed `mcOpts.OnClusterSelected` — exactly the same pipeline that
  organic traffic to a new cluster ID would trigger. This means a Fleet
  member is bootstrapped identically to a member created by a `kubectl`
  request landing on that path.
- Readiness probing uses each cluster's loopback kube client to call
  `/readyz` with a short timeout. The `phase` transitions are coarse for
  V0; structured conditions are a follow-up.

## Out of scope for V0

These are deliberate omissions, not oversights:

| Concern | V0 behavior | Follow-up |
|---|---|---|
| Scenario seeding (apply manifests into each Fleet member on creation) | Members boot empty | `Fleet.spec.template.objects` or a sidecar Bootstrapper |
| TTL-based destruction of Fleet members | `ttlSeconds` is parsed but not enforced; members linger | `FleetGCController` + per-plane delete primitive |
| Snapshot of CRD-defined types | Only core types appear in `/snapshot` | Merge `CRDRuntimeManager` projection into snapshot |
| Scoring DSL | Callers write asserts against `snapshotResponse` in their language of choice | Optional `ScoringPolicy` CRD if a pattern emerges |
| Trajectory format | Callers diff snapshots themselves | Optional sdec-framed delta stream |

The intent is to ship the primitive, get it in front of real RL/agent
workloads, and let observed needs (not design committee opinions) drive the
shape of V1.

## Files touched

| File | Purpose |
|---|---|
| `pkg/multicluster/informer_registry.go` | `ListRegistered`, `ListLive`, `Peek` |
| `cmd/apiserver/app/snapshot.go` | `/snapshot` handler |
| `cmd/apiserver/app/config.go` | Mount snapshot + start FleetController |
| `pkg/apis/kplane/v1/{doc,types,crd}.go` | Fleet API types and CRD manifest |
| `pkg/multicluster/bootstrap/fleet_controller.go` | Fleet reconciler |
| `pkg/multicluster/bootstrap/fleet_controller_test.go` | Unit tests for member-ID derivation |
| `test/smoke/snapshot_test.go` | End-to-end snapshot smoke |
| `test/smoke/fleet_test.go` | End-to-end Fleet smoke |
