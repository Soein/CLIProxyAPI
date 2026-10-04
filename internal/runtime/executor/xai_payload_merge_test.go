package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	internalcache "github.com/router-for-me/CLIProxyAPI/v8/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIHTTPPayloadBarrierAfterResponseContinuity(t *testing.T) {
	internalcache.ClearXAIResponseContinuityCache()
	t.Cleanup(internalcache.ClearXAIResponseContinuityCache)
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "execute", true: "stream"}[stream], func(t *testing.T) {
			captured := make(chan []byte, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, errRead := io.ReadAll(r.Body)
				if errRead != nil {
					t.Error(errRead)
					return
				}
				captured <- body
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_next\",\"output\":[]}}\n\n")
			}))
			defer server.Close()
			ctx := testContextWithAPIKey(t.Name())
			auth := &cliproxyauth.Auth{ID: t.Name(), Provider: "xai", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
			scope := internalcache.XAIResponseContinuityCallerScope(t.Name(), "")
			if !internalcache.StoreXAIResponseContinuity(ctx, scope, "xai", "grok-4.5", "resp_previous", internalcache.XAIResponseContinuity{
				AuthID: auth.ID, PromptCacheKey: "session", UpstreamKind: xaiHTTPUpstreamKind(server.URL), OpaqueReusable: true,
			}) {
				t.Fatal("failed to seed continuity")
			}
			cfg := &config.Config{Payload: config.PayloadConfig{
				Override: []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "grok-4.5", Match: []map[string]any{{"previous_response_id": "resp_previous"}}}}, Params: map[string]any{"instructions": "configured"}}},
				Filter:   []config.PayloadFilterRule{{Models: []config.PayloadModelRule{{Name: "grok-4.5"}}, Params: []string{"previous_response_id", "prompt_cache_key", "input.0"}}},
			}}
			executor := NewXAIExecutor(cfg)
			req := cliproxyexecutor.Request{Model: "grok-4.5", Payload: []byte(`{"previous_response_id":"resp_previous","instructions":"client","input":[{"role":"user","content":"first"},{"role":"user","content":"second"},{"role":"user","content":"third"}]}`)}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}
			if stream {
				result, errStream := executor.ExecuteStream(ctx, auth, req, opts)
				if errStream != nil {
					t.Fatal(errStream)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, errExecute := executor.Execute(ctx, auth, req, opts); errExecute != nil {
				t.Fatal(errExecute)
			}
			body := <-captured
			if gjson.GetBytes(body, "instructions").String() != "configured" || gjson.GetBytes(body, "previous_response_id").Exists() || gjson.GetBytes(body, "prompt_cache_key").Exists() || gjson.GetBytes(body, "input.#").Int() != 2 {
				t.Fatalf("continuity changed final payload rules: %s", body)
			}
		})
	}
}

func TestXAIWebsocketsPayloadBarrierOnReconnect(t *testing.T) {
	captured := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		_, body, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		captured <- body
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"resp_retry","output":[]}}`)); errWrite != nil {
			t.Error(errWrite)
		}
	}))
	defer server.Close()
	cfg := &config.Config{Payload: config.PayloadConfig{
		Override: []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "grok-4.5"}}, Params: map[string]any{"store": false, "instructions": "configured"}}},
		Filter:   []config.PayloadFilterRule{{Models: []config.PayloadModelRule{{Name: "grok-4.5"}}, Params: []string{"previous_response_id", "prompt_cache_key", "input.0"}}},
	}}
	executor := NewXAIWebsocketsExecutor(cfg)
	executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	executor.idStore = &xaiWebsocketIDStateStore{sessions: make(map[string]*xaiWebsocketIDState)}
	defer executor.CloseExecutionSession(t.Name())
	auth := &cliproxyauth.Auth{ID: t.Name(), Provider: "xai", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: t.Name()}}
	req := cliproxyexecutor.Request{Model: "grok-4.5", Payload: []byte(`{"previous_response_id":"resp_previous","input":[{"role":"user","content":"first"},{"role":"user","content":"second"},{"role":"user","content":"third"}]}`)}
	originalWriter := writeXAIWebsocketMessage
	t.Cleanup(func() { writeXAIWebsocketMessage = originalWriter })
	var sendCalls atomic.Int32
	var firstPayload []byte
	writeXAIWebsocketMessage = func(sess *codexWebsocketSession, conn *websocket.Conn, body []byte) error {
		if sendCalls.Add(1) == 1 {
			firstPayload = bytes.Clone(body)
			return websocket.ErrCloseSent
		}
		return originalWriter(sess, conn, body)
	}
	result, errStream := executor.ExecuteStream(cliproxyexecutor.WithDownstreamWebsocket(t.Context()), auth, req, opts)
	if errStream != nil {
		t.Fatal(errStream)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
	}
	if got := sendCalls.Load(); got != 2 {
		t.Fatalf("send attempts = %d, want 2", got)
	}
	body := <-captured
	if !bytes.Equal(body, firstPayload) || gjson.GetBytes(body, "store").Bool() || gjson.GetBytes(body, "instructions").String() != "configured" || gjson.GetBytes(body, "previous_response_id").Exists() || gjson.GetBytes(body, "prompt_cache_key").Exists() || gjson.GetBytes(body, "input.#").Int() != 2 {
		t.Fatalf("reconnect changed final payload rules: first=%s retry=%s", firstPayload, body)
	}
}

func TestXAIApplyPatchStreamCancellationReleasesUpstream(t *testing.T) {
	upstreamClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cancel\"}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamClosed)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	executor := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: t.Name(), Provider: "xai", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "grok-4.5", Payload: []byte(`{"input":"hello","tools":[{"type":"custom","name":"apply_patch"}]}`)}
	result, errStream := executor.ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if errStream != nil {
		t.Fatal(errStream)
	}
	if _, ok := <-result.Chunks; !ok {
		t.Fatal("stream closed before first event")
	}
	cancel()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-result.Chunks:
			if !ok {
				select {
				case <-upstreamClosed:
				case <-deadline:
					t.Fatal("cancel did not release upstream request")
				}
				return
			}
		case <-deadline:
			t.Fatal("cancel did not close patch stream")
		}
	}
}

func TestXAICompactUsageReflectsFinalPayloadReasoning(t *testing.T) {
	const model = "grok-payload-compact-effort-test"
	captured := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, errRead := io.ReadAll(r.Body)
		if errRead != nil {
			t.Error(errRead)
			return
		}
		captured <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp_compact","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`)
	}))
	defer server.Close()
	plugin := &captureXAIUsagePlugin{model: model, records: make(chan usage.Record, 1)}
	usage.RegisterPlugin(plugin)
	executor := NewXAIExecutor(&config.Config{Payload: config.PayloadConfig{Override: []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: model}}, Params: map[string]any{"reasoning.effort": "xhigh"}}}}})
	auth := &cliproxyauth.Auth{ID: t.Name(), Provider: "xai", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: model, Payload: []byte(`{"input":"hello","reasoning":{"effort":"low"}}`)}
	if _, errExecute := executor.Execute(t.Context(), auth, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Alt: "responses/compact"}); errExecute != nil {
		t.Fatal(errExecute)
	}
	body := <-captured
	if got := gjson.GetBytes(body, "reasoning.effort").String(); got != "xhigh" {
		t.Fatalf("upstream effort = %q, want xhigh", got)
	}
	record := waitForXAIUsageRecord(t, plugin.records)
	if record.ReasoningEffort != "xhigh" {
		t.Fatalf("usage effort = %q, want final upstream xhigh", record.ReasoningEffort)
	}
}
