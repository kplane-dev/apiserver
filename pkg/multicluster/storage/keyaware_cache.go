package storage

import "sync"

// EntryCache is a small in-memory helper for key-aware placement lookups.
// It is intentionally minimal and can be replaced by tighter cache integration.
type EntryCache struct {
	mu      sync.RWMutex
	byKey   map[string]*InternalEntry
	byScope map[string][]*InternalEntry
}

func NewEntryCache() *EntryCache {
	return &EntryCache{
		byKey:   map[string]*InternalEntry{},
		byScope: map[string][]*InternalEntry{},
	}
}

func (c *EntryCache) Upsert(e *InternalEntry) {
	if c == nil || e == nil || e.StorageKey == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byKey[e.StorageKey] = e
	scope := e.ClusterID
	c.byScope[scope] = append(c.byScope[scope], e)
}

func (c *EntryCache) Get(storageKey string) (*InternalEntry, bool) {
	if c == nil || storageKey == "" {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.byKey[storageKey]
	return e, ok
}

