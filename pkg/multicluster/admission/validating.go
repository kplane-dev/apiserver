package admission

import (
	"context"
	"fmt"
	"strings"

	mcv1 "github.com/kplane-dev/apiserver/pkg/multicluster"
	"k8s.io/apimachinery/pkg/api/meta"
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
	key := v.Options.ClusterAnnotationKey
	if key == "" {
		key = mcv1.DefaultClusterAnnotation
	}
	reqCID, _, _ := mcv1.FromContext(ctx)
	if reqCID == "" {
		reqCID = v.Options.DefaultCluster
	}

	if a.GetOperation() == apiserveradmission.Create {
		obj := a.GetObject()
		if obj == nil {
			return nil
		}
		acc, err := meta.Accessor(obj)
		if err != nil {
			return nil
		}
		if cid := acc.GetAnnotations()[key]; cid != reqCID {
			return fmt.Errorf("cluster annotation %q=%q must match request cluster %q", key, cid, reqCID)
		}
		return nil
	}

	if a.GetOperation() == apiserveradmission.Update {
		newObj := a.GetObject()
		oldObj := a.GetOldObject()
		if newObj == nil || oldObj == nil {
			return nil
		}
		newAcc, err1 := meta.Accessor(newObj)
		oldAcc, err2 := meta.Accessor(oldObj)
		if err1 != nil || err2 != nil {
			return nil
		}
		oldCID := oldAcc.GetAnnotations()[key]
		newCID := newAcc.GetAnnotations()[key]
		if (oldCID != "" && oldCID != reqCID) || (newCID != "" && newCID != oldCID) {
			return fmt.Errorf("cross-cluster updates are forbidden (old=%q new=%q request=%q)", oldCID, newCID, reqCID)
		}
	}
	return nil
}
