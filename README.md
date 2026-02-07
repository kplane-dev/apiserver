## apiserver

Multicluster Kubernetes API server with path-based cluster routing.

### Requirements
- Go 1.24.6
- etcd (3.5+ for local testing)

### Build locally
```bash
go build ./cmd/apiserver
```

### Run locally (against a local etcd)
```bash
# start etcd (example)
docker run --rm -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes -e ETCD_ADVERTISE_CLIENT_URLS=http://127.0.0.1:2379 -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 bitnami/etcd:3.5

# in another shell
./apiserver \
  --etcd-servers=http://127.0.0.1:2379 \
  --service-cluster-ip-range=10.0.0.0/24 \
  --allow-privileged=true \
  --authorization-mode=AlwaysAllow \
  --anonymous-auth=true \
  --secure-port=6443
```

Probe readiness (self-signed TLS; skip verification in curl):
```bash
curl -k https://127.0.0.1:6443/clusters/root/control-plane/readyz
```

### Multicluster control planes
Requests are scoped by path:
`/clusters/{cluster}/control-plane/...`

The **root control plane** is the default cluster that receives CLI flag
configuration and is used for default routing when no cluster is specified.
You can change its name with:
```bash
--root-control-plane-name=default
```
When changed, the readiness path becomes:
```
https://127.0.0.1:6443/clusters/default/control-plane/readyz
```

#### Webhooks
Admission webhooks are scoped per control plane. For each cluster, the server
creates a per-cluster webhook client/informer stack and resolves Services and
EndpointSlices within that cluster only. This prevents webhook config and
service discovery from leaking across clusters.

#### Auth (authentication + authorization)
Authentication and authorization are also per control plane. For each cluster,
the server constructs authenticators/authorizers using per-cluster loopback
clients and informers. This isolates RBAC data, service-account tokens, and
TokenReview/SubjectAccessReview evaluations so clusters can operate alongside
each other without sharing auth state.

### Core improvements
This section summarizes the core multicluster improvements and shows code
examples from this repo.

#### Storage + watchcache correctness
We keep **one store per resource** and encode the cluster into the etcd key
layout. Each request rewrites the key with the cluster ID from the request
context. The watchcache uses a key function that derives the cluster from the
stored object label so LIST/WATCH stay correct without per-cluster caches.

We place `clusters/<cid>` **after the resource prefix** (not `/clusters/<cid>/...`)
because keys are produced by per-resource `keyFunc`s. Those functions only know
the resource-local prefix (like `/registry/<group?>/<resource>/...`), so the
cluster segment must be inserted within that prefix to keep key rewriting and
watchcache keying consistent. A top-level `/clusters/<cid>/...` layout would
push us toward per-cluster stores and per-cluster watchcaches, which multiply
LIST/WATCH streams (`resources × clusters`) and goroutines. By keeping one
per-resource store and list/watch and filtering by key prefix server-side, we
reduce watch fanout and avoid unnecessary per-cluster overhead.

Key layout:
`/registry/<group?>/<resource>/clusters/<cid>/...`

```go
func (c *clusteredStorage) storeAndKey(ctx context.Context, key string) (storage.Interface, string, error) {
	cid, all, _ := FromContext(ctx)
	if cid == "" {
		cid = c.defaultCluster()
	}
	store, err := c.ensureStore()
	if err != nil {
		return nil, key, err
	}
	if all {
		return store, c.kindRootPrefix(), nil
	}
	rewritten := c.rewriteKey(cid, key)
	return store, rewritten, nil
}
```

```go
keyFunc := func(obj runtime.Object) (string, error) {
	key, err := c.keyFunc(obj)
	if err != nil {
		return "", err
	}
	// Watchcache keys must include the object cluster label.
	return c.rewriteKey(c.clusterFromObject(obj), key), nil
}
```

#### Admission: server-owned cluster label
We stamp a server-owned cluster label on persisted objects and validate it on
create/update to prevent cross-cluster writes and support watchcache keying.

```go
lbls := accessor.GetLabels()
if lbls == nil {
	lbls = map[string]string{}
}
key := m.Options.ClusterAnnotationKey
if key == "" {
	key = mcv1.DefaultClusterAnnotation
}
lbls[key] = cid
accessor.SetLabels(lbls)
```
Note: the `ClusterAnnotationKey` fallback to `DefaultClusterAnnotation` is
temporary and not ideal. It exists to preserve compatibility while we migrate
callers to an explicit label key; avoid relying on this fallback long term.

```go
if cid := acc.GetLabels()[key]; cid != reqCID {
	return fmt.Errorf("cluster label %q=%q must match request cluster %q", key, cid, reqCID)
}
```

#### Webhooks: per-cluster envs
Webhook admission is isolated by building a per-cluster environment with a
cluster-scoped clientset, shared informer factory, and service resolver.

```go
cs, inf, stopCh, err := m.opts.InformerPool.Get(clusterID)
if err != nil {
	return nil, err
}
sr := newDirectServiceResolver(cs, m.opts.EnableAggregatorRouting, m.opts.Hostname)
e := &clusterEnv{
	cid:             clusterID,
	stopCh:          stopCh,
	synced:          make(chan struct{}),
	clientset:       cs,
	informers:       inf,
	serviceResolver: sr,
}
_ = inf.Core().V1().Namespaces().Informer()
_ = inf.Core().V1().Services().Informer()
_ = inf.Discovery().V1().EndpointSlices().Informer()
_ = inf.Admissionregistration().V1().MutatingWebhookConfigurations().Informer()
_ = inf.Admissionregistration().V1().ValidatingWebhookConfigurations().Informer()
inf.Start(stopCh)
go func() {
	ok := inf.WaitForCacheSync(e.stopCh)
	e.okMu.Lock()
	e.ok = allSynced(ok)
	e.okMu.Unlock()
	close(e.synced)
}()
```

#### Client + informer pooling with eviction
Clients and informers are shared per cluster and can be evicted after idle
periods to reduce long-lived memory/goroutine growth.

```go
cfg := rest.CopyConfig(p.base)
host, err := ClusterHost(cfg.Host, Options{
	PathPrefix:          p.pathPrefix,
	ControlPlaneSegment: p.controlPlaneSegment,
}, clusterID)
if err != nil {
	return nil, err
}
cfg.Host = host
```

```go
if p.opts.StartOnGet {
	entry.start()
}
return entry.clientset, entry.factory, entry.stopCh, nil
```

#### CEL runtime caching
CEL environments and compilers for webhook match conditions are cached by
cluster/GVK/schema and tracked with metrics.

```go
r.mu.RLock()
if compiler, ok := r.compilers[key]; ok {
	r.mu.RUnlock()
	r.cacheHitTotal.WithLabelValues(reason).Inc()
	return compiler, nil
}
r.mu.RUnlock()
```

#### Observability
We added counters/gauges to validate store creation patterns and CEL caching:
- `mc_storage_ensure_store_total`
- `mc_storage_base_decorator_total`
- `mc_cel_env_build_total`
- `mc_cel_env_cache_hit_total`
- `mc_cel_env_cache_size`

### Docker image
Build locally:
```bash
docker build -t yourrepo/apiserver:dev .
```

Run container (etcd assumed on host):
```bash
docker run --rm -p 6443:6443 \
  yourrepo/apiserver:dev \
  --etcd-servers=http://host.docker.internal:2379 \
  --service-cluster-ip-range=10.0.0.0/24 \
  --allow-privileged=true \
  --authorization-mode=AlwaysAllow \
  --anonymous-auth=true
```

### Tests
Smoke test brings up the server against etcd and probes `/readyz` and discovery:
```bash
ETCD_ENDPOINTS=http://127.0.0.1:2379 go test -v ./test/smoke -timeout 10m
```

### CI/CD
- `ci` workflow runs on pushes and PRs: builds and runs smoke tests (with an etcd service).
- `release` workflow triggers on GitHub Release creation: runs smoke tests, then builds and pushes a multi-arch Docker image to Docker Hub.