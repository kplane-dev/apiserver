/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package app

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
)

// snapshotPath is the request URL after PathExtractor has stripped
// /clusters/{cid}/control-plane/. Hitting that URL returns an aggregate
// read of the live informer cache for that cluster.
const snapshotPath = "/snapshot"

// matchSnapshot reports whether the request targets the snapshot endpoint
// (post-PathExtractor URL is exactly /snapshot).
func matchSnapshot(r *http.Request) bool {
	return r != nil && r.URL.Path == snapshotPath
}

// newSnapshotHandler returns an http.Handler that serves /snapshot using the
// given InformerRegistry. Callers are expected to wrap this handler in the
// apiserver's standard auth/audit/panic-recovery filter chain (see
// wrapClusterCRDHandler) before mounting it on a route.
func newSnapshotHandler(registry *mc.InformerRegistry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveSnapshot(w, r, registry)
	})
}

// snapshotResource is the per-resource section of a snapshot response.
type snapshotResource struct {
	Group     string          `json:"group"`
	Resource  string          `json:"resource"`
	Synced    bool            `json:"synced"`
	ItemCount int             `json:"itemCount"`
	Items     []runtime.Object `json:"items"`
}

// snapshotResponse is the wire shape of GET .../snapshot.
type snapshotResponse struct {
	Cluster       string             `json:"cluster"`
	SnapshotTime  metav1.Time        `json:"snapshotTime"`
	LiveResources int                `json:"liveResources"`
	Resources     []snapshotResource `json:"resources"`
}

// serveSnapshot writes a JSON aggregate snapshot of every resource type for
// which the apiserver currently holds a live MultiClusterInformer for the
// cluster in the request context. Returns true if the request was handled.
//
// Query params:
//   - resource=<plural>[,<plural>...]  limit to a subset of resources
//   - includeEmpty=true                include resources with zero items
//   - warm=true                        force creation of MCIs for all
//                                      registered storages (off by default)
func serveSnapshot(w http.ResponseWriter, r *http.Request, registry *mc.InformerRegistry) bool {
	if registry == nil || r.URL.Path != snapshotPath {
		return false
	}
	cid, _, _ := mc.FromContext(r.Context())
	if cid == "" {
		http.Error(w, "snapshot requires a cluster: use /clusters/{cluster}/control-plane/snapshot", http.StatusBadRequest)
		return true
	}

	q := r.URL.Query()
	includeEmpty, _ := strconv.ParseBool(q.Get("includeEmpty"))
	warm, _ := strconv.ParseBool(q.Get("warm"))
	filter := map[string]struct{}{}
	if v := q.Get("resource"); v != "" {
		for _, s := range strings.Split(v, ",") {
			if s = strings.TrimSpace(s); s != "" {
				filter[s] = struct{}{}
			}
		}
	}

	var grs []schema.GroupResource
	if warm {
		grs = registry.ListRegistered()
	} else {
		grs = registry.ListLive()
	}
	sort.Slice(grs, func(i, j int) bool {
		if grs[i].Group == grs[j].Group {
			return grs[i].Resource < grs[j].Resource
		}
		return grs[i].Group < grs[j].Group
	})

	resp := snapshotResponse{
		Cluster:       cid,
		SnapshotTime:  metav1.Now(),
		LiveResources: len(grs),
	}

	for _, gr := range grs {
		if len(filter) > 0 {
			if _, ok := filter[gr.Resource]; !ok {
				continue
			}
		}

		var objs []runtime.Object
		var synced bool
		if warm {
			got, err := registry.Get(gr)
			if err != nil {
				klog.Warningf("mc.snapshot warm get failed cluster=%s gr=%s err=%v", cid, gr, err)
				continue
			}
			objs = got.List(cid)
			synced = got.HasSynced()
		} else {
			peek, ok := registry.Peek(gr)
			if !ok || peek == nil {
				continue
			}
			objs = peek.List(cid)
			synced = peek.HasSynced()
		}

		if res := buildSnapshotResource(gr, objs, synced, includeEmpty); res != nil {
			resp.Resources = append(resp.Resources, *res)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Snapshot-Cluster", cid)
	w.Header().Set("X-Snapshot-Time", time.Now().UTC().Format(time.RFC3339Nano))
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		klog.Errorf("mc.snapshot encode failed cluster=%s err=%v", cid, err)
	}
	return true
}

// buildSnapshotResource turns a list of runtime.Object into a snapshotResource
// suitable for JSON encoding. It strips managedFields to keep payloads small
// and skips empty resources unless includeEmpty is true.
func buildSnapshotResource(gr schema.GroupResource, objs []runtime.Object, synced, includeEmpty bool) *snapshotResource {
	if len(objs) == 0 && !includeEmpty {
		return nil
	}
	items := make([]runtime.Object, 0, len(objs))
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		if accessor, err := meta.Accessor(obj); err == nil && accessor != nil {
			accessor.SetManagedFields(nil)
		}
		items = append(items, obj)
	}
	return &snapshotResource{
		Group:     gr.Group,
		Resource:  gr.Resource,
		Synced:    synced,
		ItemCount: len(items),
		Items:     items,
	}
}
