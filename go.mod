module github.com/kplane-dev/apiserver

go 1.25.0

require (
	github.com/kplane-dev/informer v0.0.0-00010101000000-000000000000
	github.com/kplane-dev/storage v0.0.0
	github.com/spf13/cobra v1.10.1
	go.etcd.io/etcd/client/v3 v3.6.7
	go.opentelemetry.io/otel v1.40.0
	golang.org/x/sync v0.19.0
	gopkg.in/evanphx/json-patch.v4 v4.13.0
	k8s.io/api v0.34.1
	k8s.io/apiextensions-apiserver v0.34.1
	k8s.io/apimachinery v0.34.1
	k8s.io/apiserver v0.34.1
	k8s.io/client-go v1.5.2
	k8s.io/component-base v0.34.1
	k8s.io/klog/v2 v2.130.1
	k8s.io/kube-aggregator v0.34.1
	k8s.io/kubernetes v1.34.1
	k8s.io/utils v0.0.0-20260210185600-b8788abfbbc2
)

replace (
	github.com/kplane-dev/informer => github.com/kplane-dev/informer v0.0.0-20260303050920-e9c86850386e
	github.com/kplane-dev/storage => github.com/kplane-dev/storage v0.0.0-20260303050750-8ad94e8ce404
	k8s.io/api => github.com/kplane-dev/kubernetes/staging/src/k8s.io/api v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/apiextensions-apiserver => github.com/kplane-dev/kubernetes/staging/src/k8s.io/apiextensions-apiserver v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/apimachinery => github.com/kplane-dev/kubernetes/staging/src/k8s.io/apimachinery v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/apiserver => github.com/kplane-dev/kubernetes/staging/src/k8s.io/apiserver v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/cli-runtime => github.com/kplane-dev/kubernetes/staging/src/k8s.io/cli-runtime v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/client-go => github.com/kplane-dev/kubernetes/staging/src/k8s.io/client-go v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/cloud-provider => github.com/kplane-dev/kubernetes/staging/src/k8s.io/cloud-provider v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/cluster-bootstrap => github.com/kplane-dev/kubernetes/staging/src/k8s.io/cluster-bootstrap v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/code-generator => github.com/kplane-dev/kubernetes/staging/src/k8s.io/code-generator v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/component-base => github.com/kplane-dev/kubernetes/staging/src/k8s.io/component-base v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/component-helpers => github.com/kplane-dev/kubernetes/staging/src/k8s.io/component-helpers v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/controller-manager => github.com/kplane-dev/kubernetes/staging/src/k8s.io/controller-manager v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/cri-api => github.com/kplane-dev/kubernetes/staging/src/k8s.io/cri-api v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/cri-client => github.com/kplane-dev/kubernetes/staging/src/k8s.io/cri-client v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/csi-translation-lib => github.com/kplane-dev/kubernetes/staging/src/k8s.io/csi-translation-lib v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/dynamic-resource-allocation => github.com/kplane-dev/kubernetes/staging/src/k8s.io/dynamic-resource-allocation v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/endpointslice => github.com/kplane-dev/kubernetes/staging/src/k8s.io/endpointslice v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/externaljwt => github.com/kplane-dev/kubernetes/staging/src/k8s.io/externaljwt v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kms => github.com/kplane-dev/kubernetes/staging/src/k8s.io/kms v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kube-aggregator => github.com/kplane-dev/kubernetes/staging/src/k8s.io/kube-aggregator v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kube-controller-manager => github.com/kplane-dev/kubernetes/staging/src/k8s.io/kube-controller-manager v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kube-proxy => github.com/kplane-dev/kubernetes/staging/src/k8s.io/kube-proxy v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kube-scheduler => github.com/kplane-dev/kubernetes/staging/src/k8s.io/kube-scheduler v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kubectl => github.com/kplane-dev/kubernetes/staging/src/k8s.io/kubectl v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kubelet => github.com/kplane-dev/kubernetes/staging/src/k8s.io/kubelet v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/kubernetes => github.com/kplane-dev/kubernetes v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/metrics => github.com/kplane-dev/kubernetes/staging/src/k8s.io/metrics v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/mount-utils => github.com/kplane-dev/kubernetes/staging/src/k8s.io/mount-utils v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/pod-security-admission => github.com/kplane-dev/kubernetes/staging/src/k8s.io/pod-security-admission v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/sample-apiserver => github.com/kplane-dev/kubernetes/staging/src/k8s.io/sample-apiserver v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/sample-cli-plugin => github.com/kplane-dev/kubernetes/staging/src/k8s.io/sample-cli-plugin v0.0.0-20260303044756-e9e2a52adaf0
	k8s.io/sample-controller => github.com/kplane-dev/kubernetes/staging/src/k8s.io/sample-controller v0.0.0-20260303044756-e9e2a52adaf0
)

require (
	cel.dev/expr v0.24.0 // indirect
	cyphar.com/go-pathrs v0.2.2 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/go-oidc v2.5.0+incompatible // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.7.0 // indirect
	github.com/cyphar/filepath-securejoin v0.6.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-openapi/jsonpointer v0.22.0 // indirect
	github.com/go-openapi/jsonreference v0.21.1 // indirect
	github.com/go-openapi/swag v0.24.1 // indirect
	github.com/go-openapi/swag/cmdutils v0.24.0 // indirect
	github.com/go-openapi/swag/conv v0.24.0 // indirect
	github.com/go-openapi/swag/fileutils v0.24.0 // indirect
	github.com/go-openapi/swag/jsonname v0.24.0 // indirect
	github.com/go-openapi/swag/jsonutils v0.24.0 // indirect
	github.com/go-openapi/swag/loading v0.24.0 // indirect
	github.com/go-openapi/swag/mangling v0.24.0 // indirect
	github.com/go-openapi/swag/netutils v0.24.0 // indirect
	github.com/go-openapi/swag/stringutils v0.24.0 // indirect
	github.com/go-openapi/swag/typeutils v0.24.0 // indirect
	github.com/go-openapi/swag/yamlutils v0.24.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/cel-go v0.26.1 // indirect
	github.com/google/gnostic-models v0.7.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.3 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.7 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mailru/easyjson v0.9.1 // indirect
	github.com/moby/spdystream v0.5.0 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mxk/go-flowrate v0.0.0-20140419014527-cca7078d478f // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/selinux v1.13.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/pquerna/cachecontrol v0.2.0 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.19.2 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/stoewer/go-strcase v1.3.1 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.etcd.io/etcd/api/v3 v3.6.7 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.7 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.65.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.65.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.40.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.40.0 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/sdk v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.47.0 // indirect
	golang.org/x/exp v0.0.0-20251219203646-944ab1f22d93 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/term v0.39.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	golang.org/x/time v0.14.0 // indirect
	golang.org/x/tools v0.40.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260128011058-8636f8732409 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260128011058-8636f8732409 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/go-jose/go-jose.v2 v2.6.3 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/cloud-provider v0.34.1 // indirect
	k8s.io/cluster-bootstrap v0.0.0 // indirect
	k8s.io/component-helpers v0.34.1 // indirect
	k8s.io/controller-manager v0.34.1 // indirect
	k8s.io/cri-client v0.34.1 // indirect
	k8s.io/csi-translation-lib v0.0.0 // indirect
	k8s.io/dynamic-resource-allocation v0.34.1 // indirect
	k8s.io/endpointslice v0.34.1 // indirect
	k8s.io/externaljwt v0.34.1 // indirect
	k8s.io/kms v0.34.1 // indirect
	k8s.io/kube-controller-manager v0.0.0 // indirect
	k8s.io/kube-openapi v0.0.0-20260127142750-a19766b6e2d4 // indirect
	k8s.io/kube-proxy v0.0.0 // indirect
	k8s.io/kube-scheduler v0.0.0 // indirect
	k8s.io/kubectl v0.0.0 // indirect
	k8s.io/kubelet v0.34.1 // indirect
	k8s.io/metrics v0.0.0 // indirect
	k8s.io/mount-utils v0.34.1 // indirect
	k8s.io/pod-security-admission v0.0.0 // indirect
	sigs.k8s.io/apiserver-network-proxy/konnectivity-client v0.34.0 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.2 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)
