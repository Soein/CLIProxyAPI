package cache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	homekv "github.com/router-for-me/CLIProxyAPI/v7/internal/home"
)

type fakeXAIResponseContinuityKVClient struct {
	mu      sync.Mutex
	values  map[string][]byte
	setTTL  time.Duration
	getErr  error
	setErr  error
	lastKey string
}

func newFakeXAIResponseContinuityKVClient() *fakeXAIResponseContinuityKVClient {
	return &fakeXAIResponseContinuityKVClient{values: make(map[string][]byte)}
}

func (c *fakeXAIResponseContinuityKVClient) KVGet(_ context.Context, key string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	value, found := c.values[key]
	return append([]byte(nil), value...), found, nil
}

func (c *fakeXAIResponseContinuityKVClient) KVSet(_ context.Context, key string, value []byte, opts homekv.KVSetOptions) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.setErr != nil {
		return false, c.setErr
	}
	c.lastKey = key
	c.setTTL = opts.EX
	c.values[key] = append([]byte(nil), value...)
	return true, nil
}

func useFakeXAIResponseContinuityKVClient(t *testing.T, client xaiResponseContinuityKVClient, homeMode bool, errClient error) {
	t.Helper()
	previous := currentXAIResponseContinuityKVClient
	currentXAIResponseContinuityKVClient = func() (xaiResponseContinuityKVClient, bool, error) {
		return client, homeMode, errClient
	}
	t.Cleanup(func() {
		currentXAIResponseContinuityKVClient = previous
	})
}

func TestXAIResponseContinuityMemoryCacheUsesFixedTTL(t *testing.T) {
	now := time.Unix(100, 0)
	cache := newXAIResponseContinuityMemoryCache(time.Minute, 8, 2, func() time.Time { return now })
	continuity := XAIResponseContinuity{AuthID: "auth-a", PromptCacheKey: "pck", UpstreamKind: "http:official", OpaqueReusable: true}
	cache.store("key", continuity)

	now = now.Add(50 * time.Second)
	if got, found := cache.get("key"); !found || got != continuity {
		t.Fatalf("get before expiry = %#v, %v, want %#v, true", got, found, continuity)
	}
	now = now.Add(11 * time.Second)
	if _, found := cache.get("key"); found {
		t.Fatal("cache hit refreshed a fixed continuity expiry")
	}
}

func TestXAIResponseContinuityCallerScopeIsolatedAndHashed(t *testing.T) {
	apiScope := XAIResponseContinuityCallerScope("private-api-key", "session-a")
	if apiScope == "" || strings.Contains(apiScope, "private-api-key") {
		t.Fatalf("API caller scope = %q, want non-empty hash", apiScope)
	}
	if got := XAIResponseContinuityCallerScope("private-api-key", "session-b"); got != apiScope {
		t.Fatalf("API key must remain the primary boundary: got %q want %q", got, apiScope)
	}
	executionScope := XAIResponseContinuityCallerScope("", "session-a")
	if executionScope == "" || executionScope == apiScope || strings.Contains(executionScope, "session-a") {
		t.Fatalf("execution caller scope = %q, want isolated non-empty hash", executionScope)
	}
	if got := XAIResponseContinuityCallerScope("", ""); got != "" {
		t.Fatalf("empty caller scope = %q, want empty", got)
	}
}

func TestXAIResponseContinuitySessionBindingID(t *testing.T) {
	promptBinding := XAIResponseContinuitySessionBindingID("shared-pck", "trusted-session")
	if promptBinding == "" || strings.Contains(promptBinding, "shared-pck") || strings.Contains(promptBinding, "trusted-session") {
		t.Fatalf("prompt binding is empty or leaks identity: %q", promptBinding)
	}
	if got := XAIResponseContinuitySessionBindingID("shared-pck", "other-session"); got != promptBinding {
		t.Fatalf("prompt cache key must take precedence: got %q want %q", got, promptBinding)
	}
	if executionBinding := XAIResponseContinuitySessionBindingID("", "trusted-session"); executionBinding == "" || executionBinding == promptBinding {
		t.Fatalf("execution binding = %q, want non-empty and distinct", executionBinding)
	}
	if got := XAIResponseContinuitySessionBindingID("", ""); got != "" {
		t.Fatalf("empty binding = %q, want empty", got)
	}
}

func TestXAIResponseContinuityMemoryCacheEvictsOldestAtCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	cache := newXAIResponseContinuityMemoryCache(time.Hour, 2, 1, func() time.Time { return now })
	continuity := XAIResponseContinuity{AuthID: "auth-a", UpstreamKind: "http:official"}
	cache.store("oldest", continuity)
	now = now.Add(time.Second)
	cache.store("newer", continuity)
	now = now.Add(time.Second)
	cache.store("newest", continuity)

	if _, found := cache.get("oldest"); found {
		t.Fatal("oldest cache entry was not evicted")
	}
	if _, found := cache.get("newer"); !found {
		t.Fatal("newer cache entry was unexpectedly evicted")
	}
	if _, found := cache.get("newest"); !found {
		t.Fatal("newest cache entry was unexpectedly evicted")
	}
}

func TestXAIResponseContinuityMemoryCacheConcurrentCapacity(t *testing.T) {
	cache := newXAIResponseContinuityMemoryCache(time.Hour, 16, 4, time.Now)
	continuity := XAIResponseContinuity{AuthID: "auth-a", UpstreamKind: "http:official"}
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := xaiResponseContinuityCacheKey("caller", "xai", "grok", strconv.Itoa(index))
			cache.store(key, continuity)
			_, _ = cache.get(key)
		}(i)
	}
	wg.Wait()

	cache.mu.Lock()
	entryCount := len(cache.entries)
	cache.mu.Unlock()
	if entryCount > 16 {
		t.Fatalf("cache entries = %d, want <= 16", entryCount)
	}
}

func TestXAIResponseContinuityCacheDimensionsAreIsolated(t *testing.T) {
	ClearXAIResponseContinuityCache()
	t.Cleanup(ClearXAIResponseContinuityCache)
	useFakeXAIResponseContinuityKVClient(t, nil, false, nil)

	want := XAIResponseContinuity{AuthID: "auth-a", PromptCacheKey: "pck-a", UpstreamKind: "http:official", OpaqueReusable: true}
	if !StoreXAIResponseContinuity(context.Background(), "caller-a", "xai", "grok-a", "resp-a", want) {
		t.Fatal("StoreXAIResponseContinuity() = false")
	}
	got, found, errGet := GetXAIResponseContinuityRequired(context.Background(), "caller-a", "xai", "grok-a", "resp-a")
	if errGet != nil || !found || got != want {
		t.Fatalf("GetXAIResponseContinuityRequired() = %#v, %v, %v", got, found, errGet)
	}

	for _, query := range []struct {
		caller, provider, model, response string
	}{
		{"caller-b", "xai", "grok-a", "resp-a"},
		{"caller-a", "codex", "grok-a", "resp-a"},
		{"caller-a", "xai", "grok-b", "resp-a"},
		{"caller-a", "xai", "grok-a", "resp-b"},
	} {
		if _, isolated := GetXAIResponseContinuity(query.caller, query.provider, query.model, query.response); isolated {
			t.Fatalf("continuity leaked across dimensions: %#v", query)
		}
	}
}

func TestXAIResponseContinuityHomeKVUsesTTLAndHashedKey(t *testing.T) {
	client := newFakeXAIResponseContinuityKVClient()
	useFakeXAIResponseContinuityKVClient(t, client, true, nil)
	continuity := XAIResponseContinuity{AuthID: "auth-a", PromptCacheKey: "private-pck", UpstreamKind: "http:official", OpaqueReusable: true}

	if !StoreXAIResponseContinuity(context.Background(), "private-caller", "xai", "private-model", "private-response", continuity) {
		t.Fatal("StoreXAIResponseContinuity() = false")
	}
	if client.setTTL != XAIResponseContinuityCacheTTL {
		t.Fatalf("KV TTL = %v, want %v", client.setTTL, XAIResponseContinuityCacheTTL)
	}
	for _, secret := range []string{"private-caller", "private-model", "private-response"} {
		if strings.Contains(client.lastKey, secret) {
			t.Fatalf("KV key leaked %q: %s", secret, client.lastKey)
		}
	}
	got, found, errGet := GetXAIResponseContinuityRequired(context.Background(), "private-caller", "xai", "private-model", "private-response")
	if errGet != nil || !found || got != continuity {
		t.Fatalf("GetXAIResponseContinuityRequired() = %#v, %v, %v", got, found, errGet)
	}
}

func TestXAIResponseContinuityHomeKVErrorDoesNotFallBackLocally(t *testing.T) {
	ClearXAIResponseContinuityCache()
	t.Cleanup(ClearXAIResponseContinuityCache)
	client := newFakeXAIResponseContinuityKVClient()
	client.getErr = errors.New("kv unavailable")
	useFakeXAIResponseContinuityKVClient(t, client, true, nil)

	_, found, errGet := GetXAIResponseContinuityRequired(context.Background(), "caller", "xai", "grok", "resp")
	if errGet == nil || found {
		t.Fatalf("GetXAIResponseContinuityRequired() = found %v err %v, want false error", found, errGet)
	}
}

func TestXAIResponseContinuityRejectsIncompleteProvenance(t *testing.T) {
	useFakeXAIResponseContinuityKVClient(t, nil, false, nil)
	for _, continuity := range []XAIResponseContinuity{
		{},
		{AuthID: "auth-a"},
		{UpstreamKind: "http:official"},
	} {
		if StoreXAIResponseContinuity(context.Background(), "caller", "xai", "grok", "resp", continuity) {
			t.Fatalf("StoreXAIResponseContinuity(%#v) = true", continuity)
		}
	}
}
