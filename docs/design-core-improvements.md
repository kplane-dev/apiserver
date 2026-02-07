# Core Components Improvements Design

## Overview
This document captures the core multicluster apiserver improvements implemented to
strengthen correctness, isolation, and scalability, while reducing per-cluster
overhead. The scope includes storage, admission, webhook routing, auth/env
management, CEL runtime, and observability.

## Goals
- Correct multicluster isolation for CRUD, LIST, and WATCH operations.
- Avoid per-cluster storage stacks and watchcaches where possible.
- Ensure admission and webhook logic is cluster-aware without relying on
  root/global control plane views.
- Reduce per-cluster client/informer duplication.
- Improve diagnostics for store creation and decorator usage.
- Keep behavior aligned with Kubernetes semantics and watchcache usage.

## Non‑Goals
- Cross-cluster federated queries beyond the explicit "all clusters" path.
- Direct etcd access or bypassing apiserver semantics.
- Long-term sharding strategies for extremely high churn environments.

## Architecture Summary
The system remains a single apiserver process with path-based routing:
`/clusters/<cid>/control-plane/...` determines the cluster context. The cluster
context is injected into the request context and used to:
- Rewrite storage keys into a cluster-scoped etcd keyspace.
- Scope admission and webhook behavior to the active cluster.
- Build or reuse per-cluster client/informer environments where required.

### High‑Level Request Flow (Diagram)
```
Client
  |
  |  HTTPS /clusters/<cid>/control-plane/...
  v
Multicluster API Server
  |-- Path extractor -> context{clusterID}
  |-- Admission (mutating/validating)
  |-- REST storage (clusteredStorage)
  v
Etcd
  /registry/<resource>/clusters/<cid>/...
```

### Storage + Watchcache (Diagram)
```
                    (one per resource)
               +-----------------------+
               |   watchcache/cacher   |
               |  (in-memory cache)    |
               +----------+------------+
                          |
                          | LIST+WATCH
                          v
                     Etcd /registry/<resource>/clusters

Client watches -> apiserver -> cache fanout (no per-watch etcd stream)
```

### Per‑Cluster Dependencies (Diagram)
```
             +-----------------+
             | ClientPool      |
             | (per cluster)   |
             +--------+--------+
                      |
                      v
             +-----------------+
             | InformerPool    |
             | (per cluster)   |
             +--------+--------+
                      |
            +---------+----------+
            |                    |
         Webhook               Auth/NS
        managers              managers
```

## Storage: Single Store per Resource
### Problem
Previously, one storage stack (and watchcache) was created per cluster, leading
to O(resources × clusters) watch pressure and memory usage.

### Change
Build exactly one store per resource and encode the cluster into the etcd key
layout:

```
/registry/<group?>/<resource>/clusters/<cid>/...
```

Key rewriting injects `clusters/<cid>` immediately after the resource prefix.
The store is created once at the "kind root" prefix:

```
/registry/<group?>/<resource>/clusters
```

All CRUD/LIST/WATCH keys are rewritten per request using the cluster derived
from the request context.

### Watchcache Keying
The watchcache indexes objects using a key function that **only sees objects**.
To ensure the cache can produce correct keys, we persist the cluster ID on the
object itself via a server‑owned label. The cache key function uses that label
to compute the correct cluster-scoped key.

This preserves watchcache usage and avoids per‑cluster caches.

### Code: Key Rewriting and Single Store
```go
// clusteredStorage.storeAndKey
rewritten := c.rewriteKey(cid, key)
return store, rewritten, nil
```

```go
// clusteredStorage.ensureStore
kindRootPrefix := c.kindRootPrefix() // "/<resource>/clusters"
store, destroy, err := c.base(&cfg, kindRootPrefix, keyFunc, ...)
```

```go
// clusteredStorage.rewriteKey
rp := strings.TrimSuffix(c.resourcePrefix, "/")
return rp + "/clusters/" + cluster + strings.TrimPrefix(key, rp)
```

### Impact
- One watchcache per resource.
- LIST/WATCH isolation preserved by key prefix.
- Lower etcd watch pressure and memory.

## Admission: Cluster Label Enforcement
### Problem
Cluster identity must be available on stored objects to support cache keying
and prevent cross‑cluster writes.

### Change
Introduce a server‑owned **cluster label**:
`multicluster.k8s.io/cluster`

Mutating admission sets the label on create/update; validating admission
rejects mismatches and cross‑cluster updates.

### Code: Server‑Owned Cluster Label
```go
// Mutating admission: enforce server-owned label
lbls[key] = cid
accessor.SetLabels(lbls)
```

```go
// Validating admission: reject mismatches
if cid := acc.GetLabels()[key]; cid != reqCID {
  return fmt.Errorf("cluster label %q=%q must match request cluster %q", key, cid, reqCID)
}
```

### Impact
- Cluster is enforced server‑side.
- Clients cannot override or spoof cluster placement.
- Watchcache keying remains correct.

## Webhooks: Per‑Cluster Environments
### Problem
Upstream webhook plugins and service resolution were scoped to root control
plane data, causing cluster mismatches for virtual control planes.

### Change
Fork upstream webhook admission plugins and wrap them with a multicluster
dispatcher that creates a per‑cluster environment:
- Cluster‑scoped clientset
- Cluster‑scoped informer factory (from shared pool)
- Direct service resolver against the cluster

### Impact
Webhook resolution and admission behavior is isolated per control plane.

### Code: Per‑Cluster Env (simplified)
```go
cs, inf, _, err := m.opts.InformerPool.Get(clusterID)
resolver := newDirectServiceResolver(cs)
plugin := newUpstreamWebhookPlugin(cs, inf, resolver)
```

## Namespace Lifecycle: Per‑Cluster View
### Problem
Namespace lifecycle admission consulted root namespace registry, breaking virtual
cluster isolation.

### Change
Wrap the lifecycle plugin with a per‑cluster environment analogous to the
webhook approach.

### Code: Per‑Cluster Informer Usage (simplified)
```go
cs, inf, _, err := m.opts.InformerPool.Get(clusterID)
plugin := mclifecycle.NewLifecycle(cs, inf)
```

### Impact
Namespace existence and lifecycle checks are cluster‑scoped.

## Client Pooling
### Problem
Per‑cluster clients and transports created excessive goroutines and memory.

### Change
Introduce a `ClientPool` that caches `kubernetes.Interface` per cluster and
reuses underlying REST configuration and transports.

### Code: Client Pool (simplified)
```go
clientset, err := clientPool.KubeClientForCluster(clusterID)
```

### Impact
Reduced per‑cluster REST client creation and HTTP/2 overhead.

## Informer Pooling
### Problem
Auth/webhook/namespace managers each created their own per‑cluster informers,
duplicating watches.

### Change
Introduce a shared `InformerPool` to reuse a single `SharedInformerFactory`
per cluster across managers.

### Code: Informer Pool (simplified)
```go
cs, factory, stopCh, err := informerPool.Get(clusterID)
factory.Start(stopCh)
```

### Impact
Fewer duplicate watches and lower goroutine count.

## CEL Runtime Caching
### Problem
CEL environments were built too frequently, increasing CPU and memory.

### Change
Introduce a `CelRuntime` that caches base envs and compilers, with metrics to
track cache hits, builds, and size.

### Code: CEL Runtime (simplified)
```go
compiler := celRuntime.CompilerForKey(clusterID, gvk, crdRV)
```

### Impact
Lower memory churn and reduced per‑request overhead.

## Observability & Safety
### Storage Instrumentation
Add counters and logs to confirm store creation behavior:
- `mc_storage_ensure_store_total`
- `mc_storage_base_decorator_total`

Optional debug logging can be enabled with:
`MC_STOREANDKEY_DEBUG=1`

### REST Options Guard
Prevent double decoration by detecting if the RESTOptionsGetter is already
wrapped by the multicluster decorator.

### Diagram: Decorator Stack
```
GenericRESTOptionsGetter
  -> mc.RESTOptionsDecorator
      -> base storage decorator (watchcache)
```

## Tests & Validation
- Smoke tests for LIST/WATCH isolation and webhook scoping.
- Unit tests for storage key rewriting and single-store behavior.
- CEL runtime tests validating caching and compiler reuse.

### Example Smoke Test Coverage
```go
// Create in cluster A, ensure cluster B does not see it.
listA := csA.CoreV1().ConfigMaps(ns).List(...)
listB := csB.CoreV1().ConfigMaps(ns).List(...)
```

## Open Considerations
- Cross‑cluster queries remain an explicit, opt‑in capability.
- If fully hiding cluster metadata is required, it would require upstream
  watchcache changes to carry etcd keys, which is out of scope.

## Summary
These improvements make the multicluster apiserver:
- Correct for LIST/WATCH under shared watchcache.
- Efficient under high cluster counts.
- Properly isolated for admission and webhook routing.
- Easier to debug via targeted metrics and logs.
