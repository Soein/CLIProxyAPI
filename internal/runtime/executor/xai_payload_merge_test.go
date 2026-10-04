package executor

import (
	"bytes"
	"context"
	"fmt"
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
	const model = "grok-patch-cancellation"
	plugin := &captureXAIUsagePlugin{model: model, records: make(chan usage.Record, 1)}
	usage.RegisterPlugin(plugin)
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
	req := cliproxyexecutor.Request{Model: model, Payload: []byte(`{"input":"hello","tools":[{"type":"custom","name":"apply_patch"}]}`)}
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
		case chunk, ok := <-result.Chunks:
			if !ok {
				select {
				case <-upstreamClosed:
				case <-deadline:
					t.Fatal("cancel did not release upstream request")
				}
				record := waitForXAIUsageRecord(t, plugin.records)
				if !record.Failed || record.Phases == nil || record.Phases.TerminalKind != "canceled" || record.Fail.Body != context.Canceled.Error() {
					t.Fatalf("cancellation reported as another failure: %+v", record)
				}
				return
			}
			if chunk.Err != nil || bytes.Contains(chunk.Payload, []byte("response.failed")) || bytes.Contains(chunk.Payload, []byte("[DONE]")) {
				t.Fatalf("cancellation emitted a source error or success marker: %+v", chunk)
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

func TestXAIApplyPatchTerminalClosesKeepOpenUpstream(t *testing.T) {
	for _, tt := range []struct {
		name         string
		called       bool
		upstreamDone bool
	}{
		{name: "declared_only", upstreamDone: true},
		{name: "patch_call", called: true, upstreamDone: true},
		{name: "patch_call_without_done", called: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			model := "grok-terminal-" + tt.name
			plugin := &captureXAIUsagePlugin{model: model, records: make(chan usage.Record, 1)}
			usage.RegisterPlugin(plugin)
			upstreamClosed := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				output := "[]"
				if tt.called {
					item := `{"type":"function_call","id":"a","call_id":"c","name":"apply_patch","arguments":"{\"input\":\"valid patch\"}"}`
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":%s}\n\n", item)
					output = "[" + item + "]"
				}
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_completed\",\"status\":\"completed\",\"output\":%s,\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n", output)
				if tt.upstreamDone {
					_, _ = io.WriteString(w, "data: [DONE]\n\ndata: [DONE]\n\n")
				}
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(upstreamClosed)
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			executor := NewXAIExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{ID: t.Name(), Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
			req := cliproxyexecutor.Request{Model: model, Payload: []byte(`{"input":"hello","stream":true,"tools":[{"type":"custom","name":"apply_patch"}]}`)}
			result, errStream := executor.ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
			if errStream != nil {
				t.Fatal(errStream)
			}
			var output []byte
			completed := false
			deadline := time.After(2 * time.Second)
			for {
				select {
				case chunk, ok := <-result.Chunks:
					if !ok {
						if !completed || bytes.Count(output, []byte("[DONE]")) != 1 || !bytes.Contains(output, []byte("\n\ndata: [DONE]\n\n")) {
							t.Fatalf("terminal SSE output lost: %s", output)
						}
						if tt.called && (!bytes.Contains(output, []byte(`"type":"custom_tool_call"`)) || !bytes.Contains(output, []byte(`"input":"valid patch"`))) {
							t.Fatalf("converted patch output lost: %s", output)
						}
						select {
						case <-upstreamClosed:
						case <-deadline:
							t.Fatal("closed stream did not release upstream request")
						}
						select {
						case record := <-plugin.records:
							if record.Failed || record.Detail.InputTokens != 1 || record.Detail.OutputTokens != 2 || record.Detail.TotalTokens != 3 || record.Phases == nil || record.Phases.TerminalKind != "completed" {
								t.Fatalf("terminal usage = %+v", record)
							}
						case <-deadline:
							t.Fatal("terminal usage was not published")
						}
						return
					}
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					output = append(output, chunk.Payload...)
					for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
						data := bytes.TrimSpace(bytes.TrimPrefix(line, xaiDataTag))
						completed = completed || gjson.GetBytes(data, "type").String() == "response.completed"
					}
				case <-deadline:
					t.Fatalf("stream still open after flushed terminal: completed=%t done=%d", completed, bytes.Count(output, []byte("[DONE]")))
				}
			}
		})
	}
}
