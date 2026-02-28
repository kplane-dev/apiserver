package storage

import "testing"

func TestKeyLayoutPlacementResolver_ClusterFromStorageKey(t *testing.T) {
	r := KeyLayoutPlacementResolver{KindRootPrefix: "/registry/apps/deployments/clusters"}

	tests := []struct {
		name      string
		key       string
		wantCID   string
		wantFound bool
	}{
		{name: "normal", key: "/registry/apps/deployments/clusters/c1/default/d1", wantCID: "c1", wantFound: true},
		{name: "cluster root only", key: "/registry/apps/deployments/clusters/c2", wantCID: "c2", wantFound: true},
		{name: "wrong prefix", key: "/registry/apps/deployments/c1/default/d1", wantFound: false},
		{name: "empty", key: "", wantFound: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCID, found := r.ClusterFromStorageKey(tt.key)
			if found != tt.wantFound {
				t.Fatalf("found=%v want=%v", found, tt.wantFound)
			}
			if gotCID != tt.wantCID {
				t.Fatalf("cid=%q want=%q", gotCID, tt.wantCID)
			}
		})
	}
}

