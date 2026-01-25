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

Configure repository variables and secrets:
- Variable `DOCKERHUB_REPO`: e.g. `youruser/apiserver`
- Secret `DOCKERHUB_USERNAME`
- Secret `DOCKERHUB_TOKEN` (Docker Hub access token)
