package openai

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
)

func TestResponsesWebsocketRejectsOversizedInboundMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/v1/responses/ws", (&OpenAIResponsesAPIHandler{BaseAPIHandler: &handlers.BaseAPIHandler{}}).ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses/ws", nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()

	payload := bytes.Repeat([]byte("x"), responsesWebsocketMaxInboundMessageBytes+1)
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_ = conn.WriteMessage(websocket.TextMessage, payload)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, _, errRead := conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !websocket.IsCloseError(errRead, websocket.CloseMessageTooBig) {
		if !errors.As(errRead, &closeErr) {
			t.Fatalf("read error = %v, want close 1009", errRead)
		}
		t.Fatalf("close code = %d, want %d", closeErr.Code, websocket.CloseMessageTooBig)
	}
}

func TestWebsocketToolOutputCacheRejectsOversizedItem(t *testing.T) {
	cache := newWebsocketToolOutputCacheWithLimits(time.Minute, 10, 4, 16, 32, 4)
	if cache.record("session", "call", []byte("12345")) {
		t.Fatal("oversized cache item was accepted")
	}
	if _, ok := cache.get("session", "call"); ok {
		t.Fatal("oversized cache item was stored")
	}
	if cache.totalBytes != 0 || len(cache.sessions) != 0 {
		t.Fatalf("cache retained rejected item: bytes=%d sessions=%d", cache.totalBytes, len(cache.sessions))
	}
}

func TestWebsocketToolOutputCacheMaintainsByteBudgets(t *testing.T) {
	cache := newWebsocketToolOutputCacheWithLimits(time.Minute, 10, 16, 5, 7, 4)
	if !cache.record("session-a", "call-1", []byte("1234")) {
		t.Fatal("first item was rejected")
	}
	if !cache.record("session-a", "call-1", []byte("12")) {
		t.Fatal("replacement item was rejected")
	}
	if got := cache.sessions["session-a"].bytes; got != 2 {
		t.Fatalf("session bytes after replacement = %d, want 2", got)
	}
	if got := cache.totalBytes; got != 2 {
		t.Fatalf("total bytes after replacement = %d, want 2", got)
	}

	if !cache.record("session-a", "call-2", []byte("3456")) {
		t.Fatal("second item was rejected")
	}
	if _, ok := cache.get("session-a", "call-1"); ok {
		t.Fatal("oldest item was not evicted by the per-session byte budget")
	}
	if got := cache.totalBytes; got != 4 {
		t.Fatalf("total bytes after per-session eviction = %d, want 4", got)
	}

	if !cache.record("session-b", "call-1", []byte("5678")) {
		t.Fatal("global-budget item was rejected")
	}
	if _, ok := cache.get("session-a", "call-2"); ok {
		t.Fatal("least-recent session was not evicted by the global byte budget")
	}
	if got := cache.totalBytes; got != 4 {
		t.Fatalf("total bytes after global eviction = %d, want 4", got)
	}

	cache.deleteSession("session-b")
	if cache.totalBytes != 0 || len(cache.sessions) != 0 {
		t.Fatalf("delete session left accounting behind: bytes=%d sessions=%d", cache.totalBytes, len(cache.sessions))
	}
}

func TestWebsocketToolOutputCacheEvictsLeastRecentSessionDeterministically(t *testing.T) {
	cache := newWebsocketToolOutputCacheWithLimits(time.Minute, 10, 16, 16, 64, 2)
	cache.record("session-a", "call", []byte("a"))
	cache.record("session-b", "call", []byte("b"))
	if _, ok := cache.get("session-a", "call"); !ok {
		t.Fatal("failed to refresh session-a")
	}
	cache.record("session-c", "call", []byte("c"))

	if _, ok := cache.get("session-b", "call"); ok {
		t.Fatal("least-recent session-b was not evicted")
	}
	for _, sessionKey := range []string{"session-a", "session-c"} {
		if _, ok := cache.get(sessionKey, "call"); !ok {
			t.Fatalf("session %s was unexpectedly evicted", sessionKey)
		}
	}
}

func TestWebsocketToolOutputCacheCleanupReleasesBytes(t *testing.T) {
	cache := newWebsocketToolOutputCacheWithLimits(time.Second, 10, 16, 16, 64, 4)
	cache.record("expired", "call", []byte("data"))
	cache.mu.Lock()
	cache.sessions["expired"].lastSeen = time.Now().Add(-2 * time.Second)
	cache.mu.Unlock()

	if _, ok := cache.get("expired", "call"); ok {
		t.Fatal("expired session remained readable")
	}
	if cache.totalBytes != 0 || len(cache.sessions) != 0 {
		t.Fatalf("cleanup left accounting behind: bytes=%d sessions=%d", cache.totalBytes, len(cache.sessions))
	}
}

func TestWebsocketToolOutputCacheConcurrentAccounting(t *testing.T) {
	cache := newWebsocketToolOutputCacheWithLimits(time.Minute, 32, 256, 1024, 4096, 16)
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		worker := worker
		workers.Add(1)
		go func() {
			defer workers.Done()
			sessionKey := "session-" + string(rune('a'+worker%8))
			for iteration := 0; iteration < 200; iteration++ {
				callID := "call-" + string(rune('a'+iteration%16))
				cache.record(sessionKey, callID, []byte(strings.Repeat("x", 1+iteration%64)))
				cache.get(sessionKey, callID)
			}
		}()
	}
	workers.Wait()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	computed := 0
	for _, session := range cache.sessions {
		computed += session.bytes
		if session.bytes > cache.maxBytesPerSession {
			t.Fatalf("session bytes = %d, max %d", session.bytes, cache.maxBytesPerSession)
		}
	}
	if computed != cache.totalBytes {
		t.Fatalf("computed bytes = %d, accounted total = %d", computed, cache.totalBytes)
	}
	if cache.totalBytes > cache.maxTotalBytes {
		t.Fatalf("total bytes = %d, max %d", cache.totalBytes, cache.maxTotalBytes)
	}
	if len(cache.sessions) > cache.maxSessions {
		t.Fatalf("sessions = %d, max %d", len(cache.sessions), cache.maxSessions)
	}
}

func TestResponsesWebsocketToolCacheKeyIsCallerScoped(t *testing.T) {
	newCallerContext := func(apiKey string) *gin.Context {
		ginContext, _ := gin.CreateTestContext(httptest.NewRecorder())
		ginContext.Request = httptest.NewRequest(http.MethodGet, "/v1/responses/ws", nil)
		ginContext.Request.Header.Set("Session-Id", "shared-session")
		ginContext.Set("userApiKey", apiKey)
		return ginContext
	}

	keyA := responsesWebsocketToolCacheSessionKey(newCallerContext("api-key-a"), "connection-a")
	keyB := responsesWebsocketToolCacheSessionKey(newCallerContext("api-key-b"), "connection-b")
	if keyA == keyB {
		t.Fatal("different callers received the same websocket tool-cache key")
	}
	for _, secret := range []string{"api-key-a", "api-key-b", "shared-session"} {
		if strings.Contains(keyA, secret) || strings.Contains(keyB, secret) {
			t.Fatalf("cache key leaked raw scope/session value %q", secret)
		}
	}

	cache := newWebsocketToolOutputCache(time.Minute, 10)
	itemA := []byte(`{"type":"function_call_output","call_id":"shared-call","output":"caller-a"}`)
	itemB := []byte(`{"type":"function_call_output","call_id":"shared-call","output":"caller-b"}`)
	cache.record(keyA, "shared-call", itemA)
	cache.record(keyB, "shared-call", itemB)
	if got, ok := cache.get(keyA, "shared-call"); !ok || !bytes.Equal(got, itemA) {
		t.Fatalf("caller-a cache entry = %s, ok=%v", got, ok)
	}
	if got, ok := cache.get(keyB, "shared-call"); !ok || !bytes.Equal(got, itemB) {
		t.Fatalf("caller-b cache entry = %s, ok=%v", got, ok)
	}
}

func TestResponsesWebsocketToolCacheKeyFallsBackToConnectionScope(t *testing.T) {
	newAnonymousContext := func(setEmptyPrincipal bool) *gin.Context {
		ginContext, _ := gin.CreateTestContext(httptest.NewRecorder())
		ginContext.Request = httptest.NewRequest(http.MethodGet, "/v1/responses/ws", nil)
		ginContext.Request.Header.Set("Session-Id", "shared-session")
		if setEmptyPrincipal {
			ginContext.Set("userApiKey", "")
		}
		return ginContext
	}

	for _, setEmptyPrincipal := range []bool{false, true} {
		keyA := responsesWebsocketToolCacheSessionKey(newAnonymousContext(setEmptyPrincipal), "connection-a")
		keyB := responsesWebsocketToolCacheSessionKey(newAnonymousContext(setEmptyPrincipal), "connection-b")
		if keyA == keyB || keyA == "shared-session" || keyB == "shared-session" {
			t.Fatalf("anonymous connection keys with empty principal %t = %q and %q", setEmptyPrincipal, keyA, keyB)
		}
	}
}
