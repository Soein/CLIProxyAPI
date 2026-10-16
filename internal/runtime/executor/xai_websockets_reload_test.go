package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestXAIWebsocketReloadPreservesConnectionContinuityAndFinalPayloadRules(t *testing.T) {
	var connections atomic.Int32
	captured := make(chan []byte, 2)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()
		for i := 1; i <= 2; i++ {
			_, body, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			captured <- body
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","output":[]}}`, i))
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				t.Error(errWrite)
				return
			}
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()

	initial := NewXAIWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{ProxyURL: "direct"}})
	initial.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	initial.idStore = &xaiWebsocketIDStateStore{sessions: make(map[string]*xaiWebsocketIDState)}
	defer initial.CloseExecutionSession(t.Name())
	auth := &cliproxyauth.Auth{ID: t.Name(), Provider: "xai", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Metadata:     map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: t.Name()},
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(t.Context())
	run := func(executor *XAIWebsocketsExecutor, required bool, body []byte) {
		t.Helper()
		requestCtx := ctx
		if required {
			requestCtx = cliproxyexecutor.WithRequiredUpstreamWebsocket(requestCtx)
		}
		result, errExecute := executor.ExecuteStream(requestCtx, auth, cliproxyexecutor.Request{Model: "grok-4.5", Payload: body}, opts)
		if errExecute != nil {
			t.Fatal(errExecute)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
	}
	run(initial, false, []byte(`{"model":"grok-4.5","input":[{"role":"user","content":"first"}]}`))
	<-captured

	reloaded := NewXAIWebsocketsExecutor(&config.Config{
		SDKConfig: config.SDKConfig{ProxyURL: "http://unused-proxy.invalid:8080"},
		Payload: config.PayloadConfig{Override: []config.PayloadRule{{
			Models: []config.PayloadModelRule{{Name: "grok-4.5"}},
			Params: map[string]any{"store": false, "instructions": "reloaded payload rule"},
		}}},
	})
	reloaded.store = initial.store
	reloaded.idStore = initial.idStore
	encrypted := testValidGrokEncryptedContentForSeed(82)
	followup := []byte(`{"model":"grok-4.5","previous_response_id":"resp-1","input":[{"type":"reasoning","summary":[],"encrypted_content":""},{"role":"user","content":"next"}]}`)
	followup, _ = sjson.SetBytes(followup, "input.0.encrypted_content", encrypted)
	run(reloaded, true, followup)
	body := <-captured
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1", got)
	}
	if got := gjson.GetBytes(body, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %q, want resp-1; payload=%s", got, body)
	}
	if !xaiInputHasEncryptedValue(body, encrypted) {
		t.Fatalf("configuration reload removed same-target opaque continuity: %s", body)
	}
	if gjson.GetBytes(body, "store").Bool() || gjson.GetBytes(body, "instructions").String() != "reloaded payload rule" {
		t.Fatalf("reloaded payload rules were overwritten: %s", body)
	}
}
