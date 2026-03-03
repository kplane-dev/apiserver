package storage

import "testing"

func TestObjectPathFromStorageKey(t *testing.T) {
	tests := []struct {
		name       string
		storageKey string
		rootPrefix string
		wantNS     string
		wantName   string
		wantOK     bool
	}{
		{
			name:       "namespaced resource",
			storageKey: "/registry/pods/clusters/c1/default/nginx",
			rootPrefix: "/registry/pods/clusters/",
			wantNS:     "default",
			wantName:   "nginx",
			wantOK:     true,
		},
		{
			name:       "cluster-scoped resource",
			storageKey: "/registry/nodes/clusters/c2/node-1",
			rootPrefix: "/registry/nodes/clusters/",
			wantNS:     "",
			wantName:   "node-1",
			wantOK:     true,
		},
		{
			name:       "wrong prefix",
			storageKey: "/registry/pods/clusters/c1/default/nginx",
			rootPrefix: "/registry/nodes/clusters/",
			wantOK:     false,
		},
		{
			name:       "too few parts after prefix (only clusterID)",
			storageKey: "/registry/pods/clusters/c1",
			rootPrefix: "/registry/pods/clusters/",
			wantOK:     false,
		},
		{
			name:       "empty storage key",
			storageKey: "",
			rootPrefix: "/registry/pods/clusters/",
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, name, ok := ObjectPathFromStorageKey(tt.storageKey, tt.rootPrefix)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if ns != tt.wantNS {
				t.Errorf("namespace = %q, want %q", ns, tt.wantNS)
			}
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
		})
	}
}

func TestClusterFromStorageKeyAuto(t *testing.T) {
	tests := []struct {
		name       string
		storageKey string
		want       string
	}{
		{
			name:       "namespaced key",
			storageKey: "/registry/pods/clusters/c1/default/nginx",
			want:       "c1",
		},
		{
			name:       "cluster-scoped key",
			storageKey: "/registry/nodes/clusters/west/node-1",
			want:       "west",
		},
		{
			name:       "no clusters segment",
			storageKey: "/registry/pods/default/nginx",
			want:       "",
		},
		{
			name:       "empty key",
			storageKey: "",
			want:       "",
		},
		{
			name:       "clusters at start (no preceding path)",
			storageKey: "/clusters/c1/default/nginx",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClusterFromStorageKeyAuto(tt.storageKey)
			if got != tt.want {
				t.Errorf("ClusterFromStorageKeyAuto(%q) = %q, want %q", tt.storageKey, got, tt.want)
			}
		})
	}
}
