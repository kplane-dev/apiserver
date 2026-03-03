package app

import "testing"

func TestIsVersionPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/version", want: true},
		{path: "/version/", want: true},
		{path: "/clusters/kpt-1/version", want: true},
		{path: "/clusters/kpt-1/version/", want: true},
		{path: "/apis", want: false},
	}

	for _, tt := range tests {
		if got := isVersionPath(tt.path); got != tt.want {
			t.Fatalf("isVersionPath(%q)=%v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestNormalizedServerVersion(t *testing.T) {
	info := normalizedServerVersion()
	if info.GitVersion == gitVersionArchivePlaceholder {
		t.Fatalf("expected non-placeholder git version, got %q", info.GitVersion)
	}
}

