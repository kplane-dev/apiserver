package admission

import (
	"context"
	"strings"

	mcv1 "github.com/kplane-dev/apiserver/pkg/multicluster"
	"k8s.io/apimachinery/pkg/api/meta"
	apiserveradmission "k8s.io/apiserver/pkg/admission"
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
	// Skip non-persisted review objects (SAR/TokenReview), which require empty metadata
	gvk := a.GetKind()
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
	anns := accessor.GetAnnotations()
	if anns == nil {
		anns = map[string]string{}
	}
	key := m.Options.ClusterAnnotationKey
	if key == "" {
		key = mcv1.DefaultClusterAnnotation
	}
	anns[key] = cid
	accessor.SetAnnotations(anns)
	return nil
}
