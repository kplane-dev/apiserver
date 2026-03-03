package bootstrap

import (
	"sync"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

type CRDServesIndex struct {
	mu sync.Mutex

	clusterSynced map[string]bool
	serves        map[string]bool
	clusterKeys   map[string]map[string]struct{}
	crdKeys       map[string]map[string][]string
}

func NewCRDServesIndex() *CRDServesIndex {
	return &CRDServesIndex{
		clusterSynced: map[string]bool{},
		serves:        map[string]bool{},
		clusterKeys:   map[string]map[string]struct{}{},
		crdKeys:       map[string]map[string][]string{},
	}
}

func MakeServesKey(clusterID, group, version string) string {
	return clusterID + "\x00" + group + "\x00" + version
}

func (i *CRDServesIndex) Lookup(clusterID, group, version string) (bool, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.clusterSynced[clusterID] {
		return false, false
	}
	_, served := i.serves[MakeServesKey(clusterID, group, version)]
	return served, true
}

func (i *CRDServesIndex) RebuildCluster(clusterID string, objs []interface{}) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for k := range i.clusterKeys[clusterID] {
		delete(i.serves, k)
	}
	i.clusterKeys[clusterID] = map[string]struct{}{}
	i.crdKeys[clusterID] = map[string][]string{}

	for _, obj := range objs {
		crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
		if !ok || crd == nil {
			continue
		}
		keys := ServedKeysForCRD(clusterID, crd)
		i.setCRDKeysLocked(clusterID, crd.Name, keys)
	}
	i.clusterSynced[clusterID] = true
}

func (i *CRDServesIndex) UpsertCRD(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) {
	if crd == nil {
		return
	}
	keys := ServedKeysForCRD(clusterID, crd)
	i.mu.Lock()
	defer i.mu.Unlock()
	i.setCRDKeysLocked(clusterID, crd.Name, keys)
	// New clusters often appear after the initial informer sync/rebuild.
	// Mark as synced on first observed event to avoid false "unknown" lookups.
	i.clusterSynced[clusterID] = true
}

func (i *CRDServesIndex) DeleteCRD(clusterID string, crd *apiextensionsv1.CustomResourceDefinition) {
	if crd == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.setCRDKeysLocked(clusterID, crd.Name, nil)
	// Delete events are also a valid synchronization signal for the cluster.
	i.clusterSynced[clusterID] = true
}

func (i *CRDServesIndex) InvalidateCluster(clusterID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for k := range i.clusterKeys[clusterID] {
		delete(i.serves, k)
	}
	delete(i.clusterKeys, clusterID)
	delete(i.crdKeys, clusterID)
	delete(i.clusterSynced, clusterID)
}

func (i *CRDServesIndex) setCRDKeysLocked(clusterID, crdName string, keys []string) {
	if i.clusterKeys[clusterID] == nil {
		i.clusterKeys[clusterID] = map[string]struct{}{}
	}
	if i.crdKeys[clusterID] == nil {
		i.crdKeys[clusterID] = map[string][]string{}
	}
	for _, old := range i.crdKeys[clusterID][crdName] {
		delete(i.serves, old)
		delete(i.clusterKeys[clusterID], old)
	}
	if len(keys) == 0 {
		delete(i.crdKeys[clusterID], crdName)
		return
	}
	i.crdKeys[clusterID][crdName] = keys
	for _, k := range keys {
		i.serves[k] = true
		i.clusterKeys[clusterID][k] = struct{}{}
	}
}
