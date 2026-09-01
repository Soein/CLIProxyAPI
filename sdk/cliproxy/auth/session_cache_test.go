package auth

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSessionCacheCapacityEvictsOldestBatch(t *testing.T) {
	cache := newSessionCache(time.Hour, 4, 2, 128)
	defer cache.Stop()

	for i := 1; i <= 4; i++ {
		cache.Set(fmt.Sprintf("session-%d", i), fmt.Sprintf("auth-%d", i))
	}
	cache.Set("session-5", "auth-5")

	cache.mu.RLock()
	entryCount := len(cache.entries)
	cache.mu.RUnlock()
	if entryCount != 3 {
		t.Fatalf("entry count = %d, want 3 after batch eviction", entryCount)
	}
	for _, sessionID := range []string{"session-1", "session-2"} {
		if _, ok := cache.Get(sessionID); ok {
			t.Fatalf("old session %q was not evicted", sessionID)
		}
	}
	for _, sessionID := range []string{"session-3", "session-4", "session-5"} {
		if _, ok := cache.Get(sessionID); !ok {
			t.Fatalf("recent session %q was unexpectedly evicted", sessionID)
		}
	}
}

func TestSessionCache_CapacityEvictionOrder(t *testing.T) {
	t.Parallel()

	maxEntries := 5
	cache := NewSessionCacheWithCapacity(time.Hour, maxEntries)
	defer cache.Stop()

	// Insert 5 entries (fill cache to capacity)
	for i := 1; i <= 5; i++ {
		cache.Set(fmt.Sprintf("sess-%d", i), fmt.Sprintf("auth-%d", i))
	}

	if cache.Len() != 5 {
		t.Fatalf("cache.Len() = %d, want 5", cache.Len())
	}

	// Insert 6th entry: oldest (sess-1) should be evicted
	cache.Set("sess-6", "auth-6")

	if cache.Len() > maxEntries {
		t.Fatalf("cache.Len() = %d, want <= %d", cache.Len(), maxEntries)
	}

	if _, ok := cache.Get("sess-1"); ok {
		t.Fatal("expected oldest entry sess-1 to be evicted")
	}

	for i := 2; i <= 6; i++ {
		if got, ok := cache.Get(fmt.Sprintf("sess-%d", i)); !ok || got != fmt.Sprintf("auth-%d", i) {
			t.Fatalf("Get(sess-%d) = %q, %v; want auth-%d, true", i, got, ok, i)
		}
	}
}

func TestSessionCacheCapacityRemovesExpiredBeforeOldest(t *testing.T) {
	cache := newSessionCache(time.Hour, 3, 2, 128)
	defer cache.Stop()

	cache.Set("expired", "auth-expired")
	cache.Set("live-1", "auth-1")
	cache.Set("live-2", "auth-2")
	cache.mu.Lock()
	entry := cache.entries["expired"]
	entry.expiresAt = time.Now().Add(-time.Second)
	cache.entries["expired"] = entry
	cache.groups[entry.aliases[0]] = entry
	cache.mu.Unlock()

	cache.Set("live-3", "auth-3")

	if _, ok := cache.Get("expired"); ok {
		t.Fatal("expired session was not removed")
	}
	for _, sessionID := range []string{"live-1", "live-2", "live-3"} {
		if _, ok := cache.Get(sessionID); !ok {
			t.Fatalf("live session %q was evicted before an expired entry", sessionID)
		}
	}
}

func TestSessionCache_MultiAliasGroupEviction(t *testing.T) {
	t.Parallel()

	maxEntries := 4
	cache := NewSessionCacheWithCapacity(time.Hour, maxEntries)
	defer cache.Stop()

	// Group 1: 2 aliases (s1-a, s1-b)
	cache.SetAliases("auth-1", "s1-a", "s1-b")
	// Group 2: 2 aliases (s2-a, s2-b)
	cache.SetAliases("auth-2", "s2-a", "s2-b")

	if cache.Len() != 4 {
		t.Fatalf("cache.Len() = %d, want 4", cache.Len())
	}

	// Group 3: 1 alias (s3)
	// Inserting s3 should evict Group 1 completely (both s1-a and s1-b)
	cache.Set("s3", "auth-3")

	if cache.Len() > maxEntries {
		t.Fatalf("cache.Len() = %d, want <= %d", cache.Len(), maxEntries)
	}

	if _, ok := cache.Get("s1-a"); ok {
		t.Fatal("expected s1-a to be evicted")
	}
	if _, ok := cache.Get("s1-b"); ok {
		t.Fatal("expected s1-b to be evicted")
	}

	if got, ok := cache.Get("s2-a"); !ok || got != "auth-2" {
		t.Fatalf("Get(s2-a) = %q, %v", got, ok)
	}
	if got, ok := cache.Get("s3"); !ok || got != "auth-3" {
		t.Fatalf("Get(s3) = %q, %v", got, ok)
	}
}

func TestSessionCache_HighThroughputCapacitySaturated(t *testing.T) {
	t.Parallel()

	maxEntries := 1000
	cache := NewSessionCacheWithCapacity(time.Hour, maxEntries)
	defer cache.Stop()

	// Rapidly write 10,000 entries through a small capacity cache to ensure O(1) eviction
	for i := 0; i < 10000; i++ {
		cache.Set(fmt.Sprintf("sess-%d", i), fmt.Sprintf("auth-%d", i%10))
	}

	if cache.Len() > maxEntries {
		t.Fatalf("cache.Len() = %d, want <= %d", cache.Len(), maxEntries)
	}

	// Latest entries must be present
	for i := 9900; i < 10000; i++ {
		if got, ok := cache.Get(fmt.Sprintf("sess-%d", i)); !ok || got != fmt.Sprintf("auth-%d", i%10) {
			t.Fatalf("Get(sess-%d) = %q, %v", i, got, ok)
		}
	}
}

func TestSessionCacheGetAndRefreshUsesSlidingTTL(t *testing.T) {
	cache := newSessionCache(time.Hour, 4, 2, 128)
	defer cache.Stop()

	cache.Set("session", "auth")
	cache.mu.Lock()
	entry := cache.entries["session"]
	entry.expiresAt = time.Now().Add(time.Minute)
	cache.entries["session"] = entry
	cache.groups[entry.aliases[0]] = entry
	firstExpiration := entry.expiresAt
	cache.mu.Unlock()

	if authID, ok := cache.GetAndRefresh("session"); !ok || authID != "auth" {
		t.Fatalf("GetAndRefresh() = %q, %v; want auth, true", authID, ok)
	}
	cache.mu.RLock()
	refreshedExpiration := cache.entries["session"].expiresAt
	cache.mu.RUnlock()
	if !refreshedExpiration.After(firstExpiration) {
		t.Fatalf("refreshed expiration %v is not after original expiration %v", refreshedExpiration, firstExpiration)
	}

	cache.mu.Lock()
	entry = cache.entries["session"]
	entry.expiresAt = time.Now().Add(-time.Second)
	cache.entries["session"] = entry
	cache.groups[entry.aliases[0]] = entry
	cache.mu.Unlock()
	if _, ok := cache.GetAndRefresh("session"); ok {
		t.Fatal("expired session was refreshed")
	}
}

func TestSessionCacheRejectsOversizedKey(t *testing.T) {
	cache := newSessionCache(time.Hour, 4, 2, 8)
	defer cache.Stop()

	cache.Set(strings.Repeat("x", 9), "auth-large")
	cache.Set(strings.Repeat("y", 8), "auth-normal")

	cache.mu.RLock()
	entryCount := len(cache.entries)
	cache.mu.RUnlock()
	if entryCount != 1 {
		t.Fatalf("entry count = %d, want 1", entryCount)
	}
	if _, ok := cache.Get(strings.Repeat("x", 9)); ok {
		t.Fatal("oversized key was cached")
	}
	if authID, ok := cache.Get(strings.Repeat("y", 8)); !ok || authID != "auth-normal" {
		t.Fatalf("regular key lookup = %q, %v; want auth-normal, true", authID, ok)
	}
}

func TestSessionCacheConcurrentAccessStaysBounded(t *testing.T) {
	const maxEntries = 64
	cache := newSessionCache(time.Hour, maxEntries, 8, 128)

	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				sessionID := fmt.Sprintf("session-%d-%d", worker, i)
				authID := fmt.Sprintf("auth-%d", i%8)
				cache.Set(sessionID, authID)
				cache.Get(sessionID)
				cache.GetAndRefresh(sessionID)
				if i%11 == 0 {
					cache.Invalidate(sessionID)
				}
			}
		}()
	}
	wg.Wait()

	cache.mu.RLock()
	entryCount := len(cache.entries)
	cache.mu.RUnlock()
	if entryCount > maxEntries {
		t.Fatalf("entry count = %d, exceeds hard limit %d", entryCount, maxEntries)
	}

	var stopWG sync.WaitGroup
	for i := 0; i < 8; i++ {
		stopWG.Add(1)
		go func() {
			defer stopWG.Done()
			cache.Stop()
		}()
	}
	stopWG.Wait()
}

func TestSessionCache_ConcurrentSaturatedAccess(t *testing.T) {
	t.Parallel()

	maxEntries := 50
	cache := NewSessionCacheWithCapacity(time.Hour, maxEntries)
	defer cache.Stop()

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("worker-%d-sess-%d", w, i)
				cache.Set(key, fmt.Sprintf("auth-%d", w))
				cache.Get(key)
				if i%5 == 0 {
					cache.Touch(key, fmt.Sprintf("auth-%d", w))
				}
				if i%7 == 0 {
					cache.CompareAndDelete(key, fmt.Sprintf("auth-%d", w))
				}
			}
		}(worker)
	}
	wg.Wait()

	if cache.Len() > maxEntries {
		t.Fatalf("cache.Len() = %d, want <= %d", cache.Len(), maxEntries)
	}
}

func TestSessionCache_NilReceiverSafety(t *testing.T) {
	t.Parallel()

	var cache *SessionCache
	if got, ok := cache.Get("session-1"); ok || got != "" {
		t.Fatalf("nil.Get() = %q, %v; want '', false", got, ok)
	}
	if got, ok := cache.GetAndRefresh("session-1"); ok || got != "" {
		t.Fatalf("nil.GetAndRefresh() = %q, %v; want '', false", got, ok)
	}
	cache.Set("session-1", "auth-1")
	cache.SetAliases("auth-1", "s1", "s2")
	if ok := cache.Touch("session-1", "auth-1"); ok {
		t.Fatal("nil.Touch() unexpectedly succeeded")
	}
	if ok := cache.CompareAndDelete("session-1", "auth-1"); ok {
		t.Fatal("nil.CompareAndDelete() unexpectedly succeeded")
	}
	cache.Invalidate("session-1")
	cache.InvalidateAuth("auth-1")
	if n := cache.Len(); n != 0 {
		t.Fatalf("nil.Len() = %d, want 0", n)
	}
	cache.Stop()
}

func TestSessionCache_StopNilChannelNoPanic(t *testing.T) {
	t.Parallel()

	zeroCache := &SessionCache{}
	zeroCache.Stop()
}
