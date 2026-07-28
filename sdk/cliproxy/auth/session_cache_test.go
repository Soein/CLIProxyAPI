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
		// Expiration order is the activity order used for oldest eviction.
		time.Sleep(time.Millisecond)
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

func TestSessionCacheGetAndRefreshUsesSlidingTTL(t *testing.T) {
	cache := newSessionCache(time.Hour, 4, 2, 128)
	defer cache.Stop()

	cache.Set("session", "auth")
	cache.mu.RLock()
	firstExpiration := cache.entries["session"].expiresAt
	cache.mu.RUnlock()
	time.Sleep(time.Millisecond)

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
	entry := cache.entries["session"]
	entry.expiresAt = time.Now().Add(-time.Second)
	cache.entries["session"] = entry
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
