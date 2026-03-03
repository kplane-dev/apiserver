package admission

import (
	"context"
	"strings"

	mcv1 "github.com/kplane-dev/apiserver/pkg/multicluster"
	apiserveradmission "k8s.io/apiserver/pkg/admission"
)

const ValidatingPluginName = "MulticlusterValidating"

type Validating struct {
	*apiserveradmission.Handler
	Options mcv1.Options
}

func NewValidating(opts mcv1.Options) *Validating {
	return &Validating{
		Handler: apiserveradmission.NewHandler(apiserveradmission.Create, apiserveradmission.Update),
		Options: opts,
	}
}

func (v *Validating) Handles(op apiserveradmission.Operation) bool { return v.Handler.Handles(op) }

func (v *Validating) Validate(ctx context.Context, a apiserveradmission.Attributes, _ apiserveradmission.ObjectInterfaces) error {
	// Skip non-persisted review objects (SAR/TokenReview), which require empty metadata
	gvk := a.GetKind()
	if gvk.Group == "authorization.k8s.io" || gvk.Group == "authentication.k8s.io" || strings.HasSuffix(gvk.Kind, "Review") {
		return nil
	}
	return nil
}
