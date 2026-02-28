package app

import (
	"encoding/json"
	"net/http"
	"strings"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
	utilversion "k8s.io/component-base/version"
)

const gitVersionArchivePlaceholder = "v0.0.0-master+$Format:%H$"

func withVersionOverride(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isVersionPath(r.URL.Path) {
			info := normalizedServerVersion()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(info)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isVersionPath(path string) bool {
	if path == "/version" || path == "/version/" {
		return true
	}
	trimmed := strings.Trim(path, "/")
	parts := strings.Split(trimmed, "/")
	return len(parts) == 3 && parts[0] == "clusters" && parts[2] == "version"
}

func normalizedServerVersion() apimachineryversion.Info {
	info := utilversion.Get()
	if info.GitVersion != gitVersionArchivePlaceholder {
		return info
	}

	parts := strings.SplitN(utilversion.DefaultKubeBinaryVersion, ".", 2)
	if len(parts) != 2 {
		return info
	}

	info.Major = parts[0]
	info.Minor = parts[1]
	info.GitVersion = "v" + utilversion.DefaultKubeBinaryVersion + ".0"
	return info
}

