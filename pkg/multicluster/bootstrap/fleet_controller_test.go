/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package bootstrap

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kplanev1 "github.com/kplane-dev/apiserver/pkg/apis/kplane/v1"
)

func TestDesiredMemberIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		fleet   *kplanev1.Fleet
		want    []string
		wantErr bool
	}{
		{
			name: "synthesized names with default prefix",
			fleet: &kplanev1.Fleet{
				ObjectMeta: metav1.ObjectMeta{Name: "rl"},
				Spec:       kplanev1.FleetSpec{Replicas: 3},
			},
			want: []string{"rl-0000", "rl-0001", "rl-0002"},
		},
		{
			name: "synthesized names with custom prefix",
			fleet: &kplanev1.Fleet{
				ObjectMeta: metav1.ObjectMeta{Name: "rl"},
				Spec:       kplanev1.FleetSpec{Replicas: 2, NamePrefix: "tenant-"},
			},
			want: []string{"tenant-0000", "tenant-0001"},
		},
		{
			name: "explicit names override prefix",
			fleet: &kplanev1.Fleet{
				ObjectMeta: metav1.ObjectMeta{Name: "rl"},
				Spec: kplanev1.FleetSpec{
					Replicas: 2,
					Names:    []string{"alpha", "bravo"},
				},
			},
			want: []string{"alpha", "bravo"},
		},
		{
			name: "explicit names with length mismatch is rejected",
			fleet: &kplanev1.Fleet{
				ObjectMeta: metav1.ObjectMeta{Name: "rl"},
				Spec: kplanev1.FleetSpec{
					Replicas: 3,
					Names:    []string{"alpha", "bravo"},
				},
			},
			wantErr: true,
		},
		{
			name: "negative replicas is rejected",
			fleet: &kplanev1.Fleet{
				ObjectMeta: metav1.ObjectMeta{Name: "rl"},
				Spec:       kplanev1.FleetSpec{Replicas: -1},
			},
			wantErr: true,
		},
		{
			name: "zero replicas yields empty slice",
			fleet: &kplanev1.Fleet{
				ObjectMeta: metav1.ObjectMeta{Name: "rl"},
				Spec:       kplanev1.FleetSpec{Replicas: 0},
			},
			want: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := desiredMemberIDs(tc.fleet)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (out=%v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
