package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

func TestCodexDuplexCompactionDispatchAdmission(t *testing.T) {
	for _, outcome := range []string{"rejected", "completed", "upstream_error", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			var admissions, releases, active, httpCalls atomic.Int32
			httpStarted := make(chan struct{})
			httpCanceled := make(chan struct{})
			stopWS := make(chan struct{})
			var stopOnce sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
					if errUpgrade != nil {
						t.Errorf("upgrade websocket: %v", errUpgrade)
						return
					}
					defer func() { _ = conn.Close() }()
					_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
					if _, _, errRead := conn.ReadMessage(); errRead != nil {
						t.Errorf("read initial request: %v", errRead)
						return
					}
					for _, event := range []string{
						`{"type":"response.created","response":{"id":"r1","output":[]}}`,
						`{"type":"response.completed","response":{"id":"r1","output":[]}}`,
					} {
						if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(event)); errWrite != nil {
							t.Errorf("write initial response: %v", errWrite)
							return
						}
					}
					<-stopWS
					return
				}
				httpCalls.Add(1)
				if got := active.Load(); got != 1 {
					t.Errorf("active admissions at HTTP start = %d, want 1", got)
				}
				_, _ = io.Copy(io.Discard, r.Body)
				close(httpStarted)
				if outcome == "canceled" {
					<-r.Context().Done()
					close(httpCanceled)
					return
				}
				if outcome == "upstream_error" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = io.WriteString(w, `{"error":{"message":"summary credential rejected"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: "+`{"type":"response.completed","response":{"id":"summary","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Conversation summary."}]}]}}`+"\n\n")
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer func() {
				cancel()
				stopOnce.Do(func() { close(stopWS) })
			}()
			input := make(chan core.WebsocketInput, 1)
			ctx = core.WithWebsocketInput(core.WithDownstreamWebsocket(ctx), input)
			ctx = core.WithDispatchAdmission(ctx, func(authID string) (func(), bool) {
				if authID != "duplex-compaction-admission" {
					t.Errorf("admitted auth = %q", authID)
				}
				admissions.Add(1)
				if outcome == "rejected" {
					return nil, false
				}
				active.Add(1)
				return func() {
					active.Add(-1)
					releases.Add(1)
				}, true
			})
			const model = "duplex-compaction-model"
			cfg := &config.Config{CodexKey: []config.CodexKey{{APIKey: "test-key", BaseURL: server.URL, Models: []config.CodexModel{{Name: model, IsCompat: true}}}}}
			cfg.Codex.ResponseSteering = true
			cfg.CodexResponseSteering = true
			exec := NewCodexWebsocketsExecutor(cfg)
			exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
			defer exec.CloseExecutionSession(t.Name())
			auth := &cliproxyauth.Auth{ID: "duplex-compaction-admission", Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL, "websockets": "true"}}
			stream, errStream := exec.ExecuteStream(ctx, auth, core.Request{Model: model, Payload: []byte(`{"model":"duplex-compaction-model","input":[]}`)}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}})
			if errStream != nil {
				t.Fatalf("ExecuteStream: %v", errStream)
			}
			read := func() core.StreamChunk {
				t.Helper()
				select {
				case chunk, ok := <-stream.Chunks:
					if !ok {
						t.Fatal("stream ended before expected event")
					}
					return chunk
				case <-ctx.Done():
					t.Fatal(ctx.Err())
					return core.StreamChunk{}
				}
			}
			eventType := func(payload []byte) string {
				for _, line := range bytes.Split(payload, []byte("\n")) {
					line = bytes.TrimSpace(bytes.TrimPrefix(bytes.TrimSpace(line), dataTag))
					if typ := ojson.GetBytesNoCopy(line, "type").String(); typ != "" {
						return typ
					}
				}
				return ""
			}
			for {
				chunk := read()
				if chunk.Err != nil {
					t.Fatalf("initial response: %v", chunk.Err)
				}
				if ojson.GetBytesNoCopy(chunk.Payload, "type").String() == "response.completed" {
					break
				}
			}
			input <- core.WebsocketInput{Payload: []byte(`{"type":"response.create","event_id":"compact","input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"}]}`)}
			if outcome == "canceled" {
				select {
				case <-httpStarted:
				case <-ctx.Done():
					t.Fatal("compaction HTTP request did not start")
				}
				stopOnce.Do(func() { close(stopWS) })
				select {
				case <-httpCanceled:
				case <-ctx.Done():
					t.Fatal("upstream websocket loss did not cancel compaction HTTP")
				}
			} else {
				chunk := read()
				typ := eventType(chunk.Payload)
				if outcome == "completed" {
					for chunk.Err == nil && typ != "response.completed" {
						chunk = read()
						typ = eventType(chunk.Payload)
					}
					if chunk.Err != nil {
						t.Fatalf("compaction response: %v", chunk.Err)
					}
				} else if outcome == "rejected" {
					if typ != "error" || !strings.Contains(string(chunk.Payload), "dispatch authority rejected admission") {
						t.Fatalf("compaction admission was not rejected: payload=%s err=%v", chunk.Payload, chunk.Err)
					}
				} else if chunk.Err == nil && typ != "error" {
					t.Fatalf("missing compaction upstream error: %s", chunk.Payload)
				}
			}
			cancel()
			for range stream.Chunks {
			}
			wantHTTP, wantRelease := int32(1), int32(1)
			if outcome == "rejected" {
				wantHTTP, wantRelease = 0, 0
			}
			if admissions.Load() != 1 || releases.Load() != wantRelease || active.Load() != 0 || httpCalls.Load() != wantHTTP {
				t.Fatalf("admissions=%d releases=%d active=%d HTTP calls=%d; want 1/%d/0/%d", admissions.Load(), releases.Load(), active.Load(), httpCalls.Load(), wantRelease, wantHTTP)
			}
		})
	}
}
