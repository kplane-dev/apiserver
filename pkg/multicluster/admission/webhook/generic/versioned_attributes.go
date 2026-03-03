package generic

import (
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
)

// EnsureVersionedAttributesUserInfo guarantees that AdmissionReview construction
// always sees a non-nil user.Info, preventing nil dereferences in upstream
// request builders when attributes are missing user context.
func EnsureVersionedAttributesUserInfo(attr *admission.VersionedAttributes) *admission.VersionedAttributes {
	if attr == nil || attr.Attributes == nil || attr.Attributes.GetUserInfo() != nil {
		return attr
	}

	cloned := *attr
	cloned.Attributes = userInfoFallbackAttributes{Attributes: attr.Attributes}
	return &cloned
}

type userInfoFallbackAttributes struct {
	admission.Attributes
}

func (a userInfoFallbackAttributes) GetUserInfo() user.Info {
	if a.Attributes == nil {
		return &user.DefaultInfo{}
	}
	if info := a.Attributes.GetUserInfo(); info != nil {
		return info
	}
	return &user.DefaultInfo{}
}
