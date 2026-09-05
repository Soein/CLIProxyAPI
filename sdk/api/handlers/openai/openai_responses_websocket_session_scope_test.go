package openai

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketNativeToolCacheUsesCallerScopedSessionKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const (
		rawSessionID = "shared-native-session"
		callID       = "shared-call"
	)

	keyA := forwardNativeToolCallForCacheTest(t, "api-key-a", rawSessionID, "connection-a", callID, "tool_a")
	keyB := forwardNativeToolCallForCacheTest(t, "api-key-b", rawSessionID, "connection-b", callID, "tool_b")
	t.Cleanup(func() {
		for _, key := range []string{keyA, keyB, rawSessionID} {
			defaultWebsocketToolOutputCache.deleteSession(key)
			defaultWebsocketToolCallCache.deleteSession(key)
		}
	})

	if keyA == keyB || keyA == rawSessionID || keyB == rawSessionID {
		t.Fatalf("caller-scoped keys = %q and %q, raw session = %q", keyA, keyB, rawSessionID)
	}
	assertResponsesWebsocketFallbackRestoresNativeToolCall(t, keyA, callID, "tool_a")
	assertResponsesWebsocketFallbackRestoresNativeToolCall(t, keyB, callID, "tool_b")
	assertWebsocketToolCacheSessionAbsent(t, defaultWebsocketToolCallCache, rawSessionID)

	releaseResponsesWebsocketToolCaches(keyA)
	releaseResponsesWebsocketToolCaches(keyB)
	for _, key := range []string{keyA, keyB, rawSessionID} {
		assertWebsocketToolCacheSessionAbsent(t, defaultWebsocketToolOutputCache, key)
		assertWebsocketToolCacheSessionAbsent(t, defaultWebsocketToolCallCache, key)
	}
}

func TestResponsesWebsocketNativeToolCacheDoesNotFallBackToRawSessionKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, sessionKey := range []string{"", " \t "} {
		t.Run(fmt.Sprintf("explicit-key-%q", sessionKey), func(t *testing.T) {
			const rawSessionID = "native-session-without-explicit-scope"
			key := forwardNativeToolCallForCacheTest(t, "api-key-a", rawSessionID, "connection-a", "call-1", "tool_a",
				responsesWebsocketForwardOptions{downstreamSessionKey: sessionKey})
			t.Cleanup(func() {
				releaseResponsesWebsocketToolCaches(key)
				defaultWebsocketToolOutputCache.deleteSession(rawSessionID)
				defaultWebsocketToolCallCache.deleteSession(rawSessionID)
			})
			assertWebsocketToolCacheSessionAbsent(t, defaultWebsocketToolCallCache, rawSessionID)
			if _, ok := defaultWebsocketToolCallCache.get(key, "call-1"); ok {
				t.Fatal("tool call was cached without an explicit session key")
			}
		})
	}
}

func forwardNativeToolCallForCacheTest(
	t *testing.T,
	apiKey string,
	rawSessionID string,
	connectionID string,
	callID string,
	toolName string,
	options ...responsesWebsocketForwardOptions,
) string {
	t.Helper()

	serverErrCh := make(chan error, 1)
	sessionKeyCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrCh <- errUpgrade
			return
		}
		defer func() { _ = conn.Close() }()

		ginContext, _ := gin.CreateTestContext(httptest.NewRecorder())
		ginContext.Request = r
		ginContext.Set("userApiKey", apiKey)
		sessionKey := responsesWebsocketToolCacheSessionKey(ginContext, connectionID)
		retainResponsesWebsocketToolCaches(sessionKey)
		sessionKeyCh <- sessionKey

		data := make(chan []byte, 1)
		errs := make(chan *interfaces.ErrorMessage)
		data <- []byte(fmt.Sprintf(
			`{"type":"response.completed","response":{"id":"response-1","output":[{"type":"function_call","id":"item-1","call_id":%q,"name":%q,"arguments":"{}"}]}}`,
			callID,
			toolName,
		))
		close(data)
		close(errs)

		forwardOptions := responsesWebsocketForwardOptions{downstreamSessionKey: sessionKey}
		if len(options) > 0 {
			forwardOptions = options[0]
		}
		_, _, _, errMsg, errForward := (*OpenAIResponsesAPIHandler)(nil).forwardResponsesWebsocket(
			ginContext,
			newResponsesWebsocketWriter(conn),
			func(...interface{}) {},
			data,
			errs,
			newInMemoryWebsocketTimelineLog(),
			connectionID,
			forwardOptions,
		)
		if errMsg != nil {
			serverErrCh <- fmt.Errorf("forward websocket error message: %v", errMsg.Error)
			return
		}
		serverErrCh <- errForward
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, http.Header{"Session-Id": []string{rawSessionID}})
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()

	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatalf("read websocket completion: %v", errRead)
	}
	if errServer := <-serverErrCh; errServer != nil {
		t.Fatalf("forward websocket: %v", errServer)
	}
	return <-sessionKeyCh
}

func assertResponsesWebsocketFallbackRestoresNativeToolCall(t *testing.T, sessionKey string, callID string, wantName string) {
	t.Helper()

	payload := []byte(fmt.Sprintf(
		`{"input":[{"type":"function_call_output","id":"output-1","call_id":%q,"output":"result"}]}`,
		callID,
	))
	repaired, _ := prepareResponsesWebsocketFallbackTurn(sessionKey, payload)
	input := gjson.GetBytes(repaired, "input").Array()
	if len(input) != 2 {
		t.Fatalf("fallback input len = %d, want 2: %s", len(input), repaired)
	}
	if got := input[0].Get("name").String(); got != wantName {
		t.Fatalf("restored native tool name = %q, want %q: %s", got, wantName, repaired)
	}
	if got := input[1].Get("type").String(); got != "function_call_output" {
		t.Fatalf("fallback output type = %q, want function_call_output: %s", got, repaired)
	}
}

func assertWebsocketToolCacheSessionAbsent(t *testing.T, cache *websocketToolOutputCache, sessionKey string) {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.sessions[sessionKey]; ok {
		t.Fatalf("tool cache retained session key %q", sessionKey)
	}
}
