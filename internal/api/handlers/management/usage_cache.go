package management

import (
	"sync"
	"time"
)

// usageQueryCache is a tiny TTL cache for the cluster-aggregated /usage
// payloads. The PG queries that build these payloads run multiple
// SELECT...GROUP BY statements; without a cache, opening 4 admin tabs at
// the same time turns into 4× round-trips per refresh. 5s is enough to
// catch the burst without making numbers feel stale.
//
// Not used in memory/dual modes.
type usageQueryCache struct {
	mu      sync.Mutex
	entries map[string]usageCacheEntry
}

type usageCacheEntry struct {
	val any
	exp time.Time
}

func newUsageQueryCache() *usageQueryCache {
	return &usageQueryCache{entries: make(map[string]usageCacheEntry)}
}

// Get returns the cached value if still fresh.
func (c *usageQueryCache) Get(k string) (any, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[k]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.exp) {
		delete(c.entries, k)
		return nil, false
	}
	return e.val, true
}

// Put stores v under k for ttl. Performs opportunistic GC if the map
// grows large (>256 entries with multiple ranges × admin tabs).
func (c *usageQueryCache) Put(k string, v any, ttl time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[k] = usageCacheEntry{val: v, exp: time.Now().Add(ttl)}
	if len(c.entries) > 256 {
		now := time.Now()
		for kk, ee := range c.entries {
			if now.After(ee.exp) {
				delete(c.entries, kk)
			}
		}
	}
}
