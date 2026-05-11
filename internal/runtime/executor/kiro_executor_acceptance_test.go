package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// TestAcceptance_KiroEndToEnd dispatches a Claude request through the Kiro
// executor against a mock event-stream upstream and verifies the streamed
// SSE output round-trips correctly back to the caller.
func TestAcceptance_KiroEndToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify Kiro headers are present.
		if r.Header.Get("X-Amzn-Kiro-Agent-Mode") != "vibe" {
			t.Errorf("missing kiro mode header: %v", r.Header)
		}
		if r.Header.Get("Authorization") != "Bearer at" {
			t.Errorf("missing auth header")
		}
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		w.WriteHeader(200)
		w.Write(makeKiroFrame("content", `{"text":"Hello"}`))
		w.Write(makeKiroFrame("content", `{"text":" world"}`))
		w.Write(makeKiroFrame("contextUsage", `{"contextUsagePercentage":1.0}`))
	}))
	t.Cleanup(srv.Close)

	auth := &cliproxyauth.Auth{
		ID: "kiro-1", Provider: "kiro",
		Storage: &kiroAuthStorage{accessToken: "at", profileArn: "arn:profile"},
	}
	e := NewKiroExecutor(&config.Config{})
	e.endpointOverride = srv.URL

	req := cliproxyexecutor.Request{
		Model:   "claude-sonnet-4.5",
		Payload: []byte(`{"conversationState":{"agentTaskType":"vibe","chatTriggerType":"MANUAL","conversationId":"c","currentMessage":{"userInputMessage":{"content":"hi","modelId":"claude-sonnet-4.5","origin":"AI_EDITOR"}}}}`),
	}

	// Streaming path
	stream, err := e.ExecuteStream(context.Background(), auth, req, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream: %v", err)
	}
	var streamMerged []byte
	for ch := range stream.Chunks {
		if ch.Err != nil {
			t.Errorf("chunk err: %v", ch.Err)
		}
		streamMerged = append(streamMerged, ch.Payload...)
		streamMerged = append(streamMerged, '\n')
	}
	if !strings.Contains(string(streamMerged), `"text":"Hello"`) {
		t.Errorf("stream missing 'Hello': %s", streamMerged)
	}
	if !strings.Contains(string(streamMerged), `"text":" world"`) {
		t.Errorf("stream missing ' world': %s", streamMerged)
	}
	if !strings.Contains(string(streamMerged), "output_tokens") {
		t.Errorf("stream missing usage delta: %s", streamMerged)
	}
}
