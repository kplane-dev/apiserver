package admission

import (
	"context"
	"strings"

	mcv1 "github.com/kplane-dev/apiserver/pkg/multicluster"
	mcauth "github.com/kplane-dev/apiserver/pkg/multicluster/auth"
	"k8s.io/apimachinery/pkg/api/meta"
	apiserveradmission "k8s.io/apiserver/pkg/admission"
	authenticationapi "k8s.io/kubernetes/pkg/apis/authentication"
)

const MutatingPluginName = "MulticlusterMutating"

type Mutating struct {
	*apiserveradmission.Handler
	Options mcv1.Options
}

func NewMutating(opts mcv1.Options) *Mutating {
	return &Mutating{
		Handler: apiserveradmission.NewHandler(apiserveradmission.Create, apiserveradmission.Update),
		Options: opts,
	}
}

func (m *Mutating) Handles(op apiserveradmission.Operation) bool { return m.Handler.Handles(op) }

func (m *Mutating) Admit(ctx context.Context, a apiserveradmission.Attributes, _ apiserveradmission.ObjectInterfaces) error {
	// TokenReview is non-persisted and expects empty metadata. Record token->cluster
	// hint so synthetic auth requests in TokenReview storage can route correctly.
	gvk := a.GetKind()
	if gvk.Group == "authentication.k8s.io" && gvk.Kind == "TokenReview" {
		if tr, ok := a.GetObject().(*authenticationapi.TokenReview); ok {
			cid, _, _ := mcv1.FromContext(ctx)
			if cid == "" {
				cid = m.Options.DefaultCluster
			}
			mcauth.RememberTokenReviewHint(tr.Spec.Token, cid)
		}
		return nil
	}
	// Skip other non-persisted review objects (e.g. SAR), which require empty metadata.
	if gvk.Group == "authorization.k8s.io" || gvk.Group == "authentication.k8s.io" || strings.HasSuffix(gvk.Kind, "Review") {
		return nil
	}
	obj := a.GetObject()
	if obj == nil {
		return nil
	}
	cid, _, _ := mcv1.FromContext(ctx)
	if cid == "" {
		cid = m.Options.DefaultCluster
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil
	}
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
	return nil
}
