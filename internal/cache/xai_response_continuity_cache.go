package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	homekv "github.com/router-for-me/CLIProxyAPI/v7/internal/home"
	log "github.com/sirupsen/logrus"
)

const (
	// XAIResponseContinuityCacheTTL bounds the lifetime of opaque upstream
	// response identifiers. Unlike session affinity, this is intentionally a
	// fixed expiration so reads cannot extend an upstream response forever.
	XAIResponseContinuityCacheTTL = time.Hour

	// XAIResponseContinuityCacheMaxEntries bounds the process-local continuity
	// map when Home KV is not enabled.
	XAIResponseContinuityCacheMaxEntries = 10240

	// XAIResponseContinuityCacheEvictBatchSize leaves headroom after the cache
	// reaches capacity so high write volume does not rescan the map every turn.
	XAIResponseContinuityCacheEvictBatchSize = 128
)

// XAIResponseContinuity records the upstream provenance required to safely
// reuse an opaque xAI Responses identifier on the next turn.
type XAIResponseContinuity struct {
	AuthID         string `json:"auth_id"`
	PromptCacheKey string `json:"prompt_cache_key,omitempty"`
	UpstreamKind   string `json:"upstream_kind"`
	// OpaqueReusable reports whether the completed response was created with
	// upstream storage enabled on a transport that accepts previous_response_id.
	OpaqueReusable bool `json:"opaque_reusable,omitempty"`
}

// XAIResponseContinuityCallerScope derives the isolated caller boundary used
// by both auth selection and the xAI executor. A downstream API key is the
// primary tenant boundary. Trusted execution sessions are used only when an
// API key is unavailable (for example direct SDK execution).
func XAIResponseContinuityCallerScope(callerAPIKey, executionSessionID string) string {
	callerAPIKey = strings.TrimSpace(callerAPIKey)
	executionSessionID = strings.TrimSpace(executionSessionID)
	var source string
	switch {
	case callerAPIKey != "":
		source = "api-key\x00" + callerAPIKey
	case executionSessionID != "":
		source = "execution-session\x00" + executionSessionID
	default:
		return ""
	}
	sum := sha256.Sum256([]byte(source))
	return hex.EncodeToString(sum[:])
}

// XAIResponseContinuitySessionBindingID derives the opaque cache lookup key
// used to bind credential-scoped xAI input to a proven upstream auth. A prompt
// cache key takes precedence; trusted execution sessions are the direct-SDK
// fallback when no prompt cache key is present.
func XAIResponseContinuitySessionBindingID(promptCacheKey, executionSessionID string) string {
	promptCacheKey = strings.TrimSpace(promptCacheKey)
	executionSessionID = strings.TrimSpace(executionSessionID)
	kind := ""
	identity := ""
	if promptCacheKey != "" {
		kind = "prompt-cache"
		identity = promptCacheKey
	} else if executionSessionID != "" {
		kind = "execution-session"
		identity = executionSessionID
	} else {
		return ""
	}
	sum := sha256.Sum256([]byte(kind + "\x00" + identity))
	return "session-binding:" + hex.EncodeToString(sum[:])
}

type xaiResponseContinuityEntry struct {
	Continuity XAIResponseContinuity
	StoredAt   time.Time
}

type xaiResponseContinuityMemoryCache struct {
	mu         sync.Mutex
	entries    map[string]xaiResponseContinuityEntry
	ttl        time.Duration
	maxEntries int
	evictBatch int
	now        func() time.Time
}

func newXAIResponseContinuityMemoryCache(ttl time.Duration, maxEntries, evictBatch int, now func() time.Time) *xaiResponseContinuityMemoryCache {
	if ttl <= 0 {
		ttl = XAIResponseContinuityCacheTTL
	}
	if maxEntries <= 0 {
		maxEntries = XAIResponseContinuityCacheMaxEntries
	}
	if evictBatch <= 0 {
		evictBatch = XAIResponseContinuityCacheEvictBatchSize
	}
	if now == nil {
		now = time.Now
	}
	return &xaiResponseContinuityMemoryCache{
		entries:    make(map[string]xaiResponseContinuityEntry),
		ttl:        ttl,
		maxEntries: maxEntries,
		evictBatch: evictBatch,
		now:        now,
	}
}

var xaiResponseContinuityMemory = newXAIResponseContinuityMemoryCache(
	XAIResponseContinuityCacheTTL,
	XAIResponseContinuityCacheMaxEntries,
	XAIResponseContinuityCacheEvictBatchSize,
	time.Now,
)

type xaiResponseContinuityKVClient interface {
	KVGet(ctx context.Context, key string) ([]byte, bool, error)
	KVSet(ctx context.Context, key string, value []byte, opts homekv.KVSetOptions) (bool, error)
}

var currentXAIResponseContinuityKVClient = func() (xaiResponseContinuityKVClient, bool, error) {
	return homekv.CurrentKVClient()
}

// StoreXAIResponseContinuity stores a completed xAI response provenance entry.
// callerScope must already be an isolated, non-secret caller identifier.
func StoreXAIResponseContinuity(ctx context.Context, callerScope, provider, model, responseID string, continuity XAIResponseContinuity) bool {
	key := xaiResponseContinuityCacheKey(callerScope, provider, model, responseID)
	continuity = normalizeXAIResponseContinuity(continuity)
	if key == "" || !validXAIResponseContinuity(continuity) {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if client, homeMode, errClient := currentXAIResponseContinuityKVClient(); homeMode {
		if errClient != nil {
			log.Errorf("home kv best-effort xai response continuity set failed prefix=cpa:xai:response-continuity:*: %v", errClient)
			return false
		}
		raw, errMarshal := json.Marshal(continuity)
		if errMarshal != nil {
			log.Errorf("home kv best-effort xai response continuity marshal failed: %v", errMarshal)
			return false
		}
		written, errSet := client.KVSet(ctx, xaiResponseContinuityKVKey(callerScope, provider, model, responseID), raw, homekv.KVSetOptions{EX: XAIResponseContinuityCacheTTL})
		if errSet != nil {
			log.Errorf("home kv best-effort xai response continuity set failed prefix=cpa:xai:response-continuity:*: %v", errSet)
			return false
		}
		return written
	}

	xaiResponseContinuityMemory.store(key, continuity)
	return true
}

// GetXAIResponseContinuity retrieves a continuity entry on a best-effort basis.
func GetXAIResponseContinuity(callerScope, provider, model, responseID string) (XAIResponseContinuity, bool) {
	continuity, found, errGet := GetXAIResponseContinuityRequired(context.Background(), callerScope, provider, model, responseID)
	if errGet != nil {
		return XAIResponseContinuity{}, false
	}
	return continuity, found
}

// GetXAIResponseContinuityRequired retrieves continuity state for a request-time
// path. Home KV errors are returned so callers can fail closed and scrub opaque
// state rather than accidentally sending it through another credential.
func GetXAIResponseContinuityRequired(ctx context.Context, callerScope, provider, model, responseID string) (XAIResponseContinuity, bool, error) {
	key := xaiResponseContinuityCacheKey(callerScope, provider, model, responseID)
	if key == "" {
		return XAIResponseContinuity{}, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	client, homeMode, errClient := currentXAIResponseContinuityKVClient()
	if homeMode {
		if errClient != nil {
			return XAIResponseContinuity{}, false, errClient
		}
		raw, found, errGet := client.KVGet(ctx, xaiResponseContinuityKVKey(callerScope, provider, model, responseID))
		if errGet != nil || !found {
			return XAIResponseContinuity{}, false, errGet
		}
		var continuity XAIResponseContinuity
		if errUnmarshal := json.Unmarshal(raw, &continuity); errUnmarshal != nil {
			return XAIResponseContinuity{}, false, errUnmarshal
		}
		continuity = normalizeXAIResponseContinuity(continuity)
		if !validXAIResponseContinuity(continuity) {
			return XAIResponseContinuity{}, false, nil
		}
		return continuity, true, nil
	}

	continuity, found := xaiResponseContinuityMemory.get(key)
	return continuity, found, nil
}

// ClearXAIResponseContinuityCache clears process-local continuity state.
func ClearXAIResponseContinuityCache() {
	xaiResponseContinuityMemory.clear()
}

func normalizeXAIResponseContinuity(continuity XAIResponseContinuity) XAIResponseContinuity {
	continuity.AuthID = strings.TrimSpace(continuity.AuthID)
	continuity.PromptCacheKey = strings.TrimSpace(continuity.PromptCacheKey)
	continuity.UpstreamKind = strings.TrimSpace(continuity.UpstreamKind)
	return continuity
}

func validXAIResponseContinuity(continuity XAIResponseContinuity) bool {
	return continuity.AuthID != "" && continuity.UpstreamKind != ""
}

func xaiResponseContinuityCacheKey(callerScope, provider, model, responseID string) string {
	callerScope = strings.TrimSpace(callerScope)
	provider = strings.ToLower(strings.TrimSpace(provider))
	model = strings.TrimSpace(model)
	responseID = strings.TrimSpace(responseID)
	if callerScope == "" || provider == "" || model == "" || responseID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{callerScope, provider, model, responseID}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func xaiResponseContinuityKVKey(callerScope, provider, model, responseID string) string {
	return "cpa:xai:response-continuity:" +
		homekv.HashKeyPart(strings.TrimSpace(callerScope)) + ":" +
		homekv.HashKeyPart(strings.ToLower(strings.TrimSpace(provider))) + ":" +
		homekv.HashKeyPart(strings.TrimSpace(model)) + ":" +
		homekv.HashKeyPart(strings.TrimSpace(responseID))
}

func (c *xaiResponseContinuityMemoryCache) store(key string, continuity XAIResponseContinuity) {
	if c == nil || key == "" {
		return
	}
	now := c.now()
	c.mu.Lock()
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxEntries {
		c.purgeExpiredLocked(now)
		if len(c.entries) >= c.maxEntries {
			c.evictOldestLocked(c.evictBatch)
		}
	}
	c.entries[key] = xaiResponseContinuityEntry{Continuity: continuity, StoredAt: now}
	c.mu.Unlock()
}

func (c *xaiResponseContinuityMemoryCache) get(key string) (XAIResponseContinuity, bool) {
	if c == nil || key == "" {
		return XAIResponseContinuity{}, false
	}
	now := c.now()
	c.mu.Lock()
	entry, found := c.entries[key]
	if found && now.Sub(entry.StoredAt) > c.ttl {
		delete(c.entries, key)
		found = false
	}
	c.mu.Unlock()
	if !found {
		return XAIResponseContinuity{}, false
	}
	return entry.Continuity, true
}

func (c *xaiResponseContinuityMemoryCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = make(map[string]xaiResponseContinuityEntry)
	c.mu.Unlock()
}

func (c *xaiResponseContinuityMemoryCache) purgeExpiredLocked(now time.Time) {
	for key, entry := range c.entries {
		if now.Sub(entry.StoredAt) > c.ttl {
			delete(c.entries, key)
		}
	}
}

func (c *xaiResponseContinuityMemoryCache) evictOldestLocked(count int) {
	if count <= 0 || len(c.entries) == 0 {
		return
	}
	type candidate struct {
		key      string
		storedAt time.Time
	}
	candidates := make([]candidate, 0, len(c.entries))
	for key, entry := range c.entries {
		candidates = append(candidates, candidate{key: key, storedAt: entry.StoredAt})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].storedAt.Equal(candidates[j].storedAt) {
			return candidates[i].key < candidates[j].key
		}
		return candidates[i].storedAt.Before(candidates[j].storedAt)
	})
	if count > len(candidates) {
		count = len(candidates)
	}
	for i := 0; i < count; i++ {
		delete(c.entries, candidates[i].key)
	}
}
