/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FleetCRDName is the metadata.name of the Fleet CRD.
const FleetCRDName = "fleets." + GroupName

// FleetCRD returns the CustomResourceDefinition object that registers the
// Fleet resource in the root control plane.
//
// The CRD is intentionally permissive on schema fields (no maximums) so
// callers can experiment with large fleets in V0; the controller validates
// shape at reconcile time.
func FleetCRD() *apiextensionsv1.CustomResourceDefinition {
	preserveUnknownFields := false
	return &apiextensionsv1.CustomResourceDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apiextensions.k8s.io/v1",
			Kind:       "CustomResourceDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: FleetCRDName,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "kplane-apiserver",
				"kplane.dev/native":            "true",
			},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: GroupName,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "fleets",
				Singular: "fleet",
				Kind:     "Fleet",
				ListKind: "FleetList",
				ShortNames: []string{"flt"},
				Categories: []string{"kplane"},
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    Version,
					Served:  true,
					Storage: true,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					AdditionalPrinterColumns: []apiextensionsv1.CustomResourceColumnDefinition{
						{Name: "Desired", Type: "integer", JSONPath: ".spec.replicas"},
						{Name: "Ready", Type: "integer", JSONPath: ".status.readyReplicas"},
						{Name: "Age", Type: "date", JSONPath: ".metadata.creationTimestamp"},
					},
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:                   "object",
							XPreserveUnknownFields: &preserveUnknownFields,
							Required:               []string{"spec"},
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"spec": {
									Type:     "object",
									Required: []string{"replicas"},
									Properties: map[string]apiextensionsv1.JSONSchemaProps{
										"replicas": {
											Type:    "integer",
											Minimum: ptrFloat64(0),
											Format:  "int32",
										},
										"namePrefix": {Type: "string"},
										"names": {
											Type: "array",
											Items: &apiextensionsv1.JSONSchemaPropsOrArray{
												Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
											},
										},
										"ttlSeconds": {Type: "integer", Format: "int64", Minimum: ptrFloat64(0)},
									},
								},
								"status": {
									Type: "object",
									Properties: map[string]apiextensionsv1.JSONSchemaProps{
										"observedGeneration": {Type: "integer", Format: "int64"},
										"readyReplicas":      {Type: "integer", Format: "int32"},
										"members": {
											Type: "array",
											Items: &apiextensionsv1.JSONSchemaPropsOrArray{
												Schema: &apiextensionsv1.JSONSchemaProps{
													Type:     "object",
													Required: []string{"clusterID", "phase"},
													Properties: map[string]apiextensionsv1.JSONSchemaProps{
														"clusterID":          {Type: "string"},
														"phase":              {Type: "string"},
														"message":            {Type: "string"},
														"lastTransitionTime": {Type: "string", Format: "date-time"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func ptrFloat64(v float64) *float64 { return &v }
