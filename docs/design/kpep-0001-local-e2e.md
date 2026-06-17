# KPEP-0001: local e2e test loop (no merges required)

Goal: run `kubectl create ... ; kubectl get ...` against a local kplane
apiserver backed by Spanner, using the three open PRs as-is — no merges,
no published releases.

## Prereqs

- Go 1.25.8 toolchain (or let Go auto-download via `GOTOOLCHAIN=auto`)
- `gcloud` (for the Spanner emulator) **or** Docker
- `kubectl`
- Local checkouts of: `kplane-dev/apiserver` (this repo), `kplane-dev/storage`

## Setup once

```bash
# 1. Storage PR branch checked out
cd ~/repos/kplane-dev/storage
git checkout kpep-0001/add-registry

# 2. Apiserver PR branch checked out
cd ~/repos/kplane-dev/apiserver
git checkout kpep-0001/dispatch-swap

# 3. Tell the apiserver's go.mod to use the local storage checkout
#    (this stays local — do NOT commit the replace)
go mod edit -replace=github.com/kplane-dev/storage=../storage
```

## Start the Spanner emulator

```bash
# Option A: gcloud emulator
gcloud emulators spanner start --host-port=localhost:9010 &

# Option B: docker
docker run -p 9010:9010 -p 9020:9020 \
  gcr.io/cloud-spanner-emulator/emulator
```

## Provision a Spanner instance + database

The Spanner backend's `register.go` calls `cfg.NewClient(...)` which
expects the instance and database to already exist. Use the emulator REST
or the `gcloud spanner` CLI:

```bash
export SPANNER_EMULATOR_HOST=localhost:9010
gcloud config configurations create emulator || true
gcloud config set auth/disable_credentials true
gcloud config set project test-project
gcloud config set api_endpoint_overrides/spanner http://${SPANNER_EMULATOR_HOST}/

gcloud spanner instances create test-instance --config=emulator-config --description=test --nodes=1
gcloud spanner databases create kplane --instance=test-instance
```

Apply the kv schema from `kplane-dev/storage/backends/spanner/config.go`'s
`EnsureSchema` (it runs the DDL automatically when the apiserver starts).

## Build the apiserver

```bash
cd ~/repos/kplane-dev/apiserver
GOTOOLCHAIN=auto go build -mod=mod -o bin/kplane-apiserver ./cmd/apiserver
```

## Run the apiserver against Spanner

```bash
./bin/kplane-apiserver \
  --storage-backend=spanner \
  --spanner-project=test-project \
  --spanner-instance=test-instance \
  --spanner-database=kplane \
  --spanner-emulator-host=localhost:9010 \
  --etcd-servers=http://localhost:2379 `# still required by upstream EtcdOptions; not used` \
  --secure-port=6443 \
  --service-cluster-ip-range=10.0.0.0/16 \
  --service-account-key-file=/tmp/sa.pub \
  --service-account-signing-key-file=/tmp/sa.key \
  --service-account-issuer=https://kplane.local
```

(Generate `/tmp/sa.{key,pub}` with `openssl genrsa` + `openssl rsa -pubout`
if you don't have them.)

## kubectl against the running apiserver

```bash
kubectl --server=https://localhost:6443 \
        --insecure-skip-tls-verify=true \
        --token=<admin-token> \
        create namespace kpep-test

kubectl --server=https://localhost:6443 \
        --insecure-skip-tls-verify=true \
        --token=<admin-token> \
        -n kpep-test \
        run nginx --image=nginx

kubectl --server=https://localhost:6443 \
        --insecure-skip-tls-verify=true \
        --token=<admin-token> \
        -n kpep-test \
        get pods
```

## What proves the storage backend is Spanner

Inspect the Spanner emulator state directly:

```bash
gcloud spanner databases execute-sql kplane \
  --instance=test-instance \
  --sql='SELECT key, LENGTH(value) FROM kv WHERE key LIKE "/registry/pods%" LIMIT 10'
```

A row with `/registry/pods/kpep-test/nginx` confirms the apiserver served
the CR through `--storage-backend=spanner` and the data landed in
Spanner's `kv` table.

## Flip to etcd (control test)

Stop the apiserver, drop `--storage-backend=spanner` (and the four
`--spanner-*` flags), point `--etcd-servers` at a real etcd:

```bash
etcd --advertise-client-urls=http://localhost:2379 --listen-client-urls=http://localhost:2379 &

./bin/kplane-apiserver \
  --etcd-servers=http://localhost:2379 \
  --secure-port=6443 \
  ...
```

The same `kubectl create` flow lands in etcd. No code changes — only the
flag changes.

## Cleanup

```bash
pkill kplane-apiserver
pkill gcloud  # or stop the emulator container
git checkout go.mod  # drop the local replace directive
```

## Branches in play

- `kplane-dev/kubernetes` `feat/per-cluster-allocators` — fork branch
  with the `storage.WithDecodeCallback` symbol exposure. Pinned via
  pseudo-version `v0.0.0-20260311054814-32f5e9075db5` in storage and
  apiserver `go.mod` `replace` blocks.
- `kplane-dev/storage` `kpep-0001/add-registry` — PR #2. Adds the
  registry, `BackendFactory` hook on `DecoratorConfig`, migrated
  `backends/spanner/`, and the `backends/register.go` aggregator.
- `kplane-dev/apiserver` `kpep-0001/dispatch-swap` — PR (this branch).
  Adds the `--storage-backend` dispatch in `cmd/apiserver/app/options/
  options.go` + `cmd/apiserver/app/config.go`. Threads `BackendFactory`
  through `pkg/multicluster/options.go` and `pkg/multicluster/storage.go`.

Merge order: kubernetes → storage → apiserver. Each consumer bumps its
pseudo-version pin to the resulting `main` SHA as we go.
