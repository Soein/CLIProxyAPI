package executor

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/eventstream/awsstream"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestKiroIdentifier(t *testing.T) {
	e := NewKiroExecutor(&config.Config{})
	if got := e.Identifier(); got != "kiro" {
		t.Errorf("Identifier = %q; want kiro", got)
	}
}

func TestKiroHttpRequestInjectsHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test_at" {
			t.Errorf("Authorization = %q; want Bearer test_at", r.Header.Get("Authorization"))
		}
		if !strings.Contains(r.Header.Get("X-Amz-User-Agent"), "KiroIDE-") {
			t.Errorf("X-Amz-User-Agent missing KiroIDE marker: %q", r.Header.Get("X-Amz-User-Agent"))
		}
		if r.Header.Get("X-Amzn-Kiro-Agent-Mode") != "vibe" {
			t.Errorf("X-Amzn-Kiro-Agent-Mode = %q; want vibe", r.Header.Get("X-Amzn-Kiro-Agent-Mode"))
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)

	auth := &cliproxyauth.Auth{
		ID:       "kiro-1",
		Provider: "kiro",
		Storage:  &kiroAuthStorage{accessToken: "test_at", profileArn: "arn:profile"},
	}

	e := NewKiroExecutor(&config.Config{})
	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader("{}"))
	resp, err := e.HttpRequest(context.Background(), auth, req)
	if err != nil {
		t.Fatalf("HttpRequest: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

// kiroAuthStorage is a minimal Auth.Storage implementation used in tests.
// It implements baseauth.TokenStorage (SaveTokenToFile) plus the kiroAccessor
// interface that loadKiroCredentials checks via type assertion.
type kiroAuthStorage struct {
	accessToken string
	profileArn  string
}

// SaveTokenToFile implements baseauth.TokenStorage. No-op for tests.
func (s *kiroAuthStorage) SaveTokenToFile(_ string) error { return nil }

// GetAccessToken and GetProfileArn implement the kiroAccessor interface
// detected by loadKiroCredentials Strategy 1.
func (s *kiroAuthStorage) GetAccessToken() string { return s.accessToken }
func (s *kiroAuthStorage) GetProfileArn() string  { return s.profileArn }

// makeKiroFrame creates a fake event-stream frame containing one JSON payload.
// Mirrors awsstream/decoder_test.go's makeFrame helper.
func makeKiroFrame(eventType, jsonPayload string) []byte {
	headers := []byte{}
	headers = append(headers, byte(len(":event-type")))
	headers = append(headers, []byte(":event-type")...)
	headers = append(headers, byte(awsstream.HeaderValueTypeString))
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(eventType)))
	headers = append(headers, lenBuf[:]...)
	headers = append(headers, []byte(eventType)...)

	totalLen := uint32(awsstream.PreludeSize + len(headers) + len(jsonPayload) + awsstream.MessageCRCSize)
	headersLen := uint32(len(headers))

	frame := make([]byte, 0, totalLen)
	var preludeBuf [12]byte
	binary.BigEndian.PutUint32(preludeBuf[0:4], totalLen)
	binary.BigEndian.PutUint32(preludeBuf[4:8], headersLen)
	binary.BigEndian.PutUint32(preludeBuf[8:12], crc32.ChecksumIEEE(preludeBuf[0:8]))
	frame = append(frame, preludeBuf[:]...)
	frame = append(frame, headers...)
	frame = append(frame, []byte(jsonPayload)...)
	msgCRC := crc32.ChecksumIEEE(frame[:totalLen-awsstream.MessageCRCSize])
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], msgCRC)
	frame = append(frame, crcBuf[:]...)
	return frame
}

func TestKiroExecuteStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		flusher, _ := w.(http.Flusher)
		w.WriteHeader(200)
		w.Write(makeKiroFrame("content", `{"text":"Hi"}`))
		if flusher != nil {
			flusher.Flush()
		}
		w.Write(makeKiroFrame("content", `{"text":"!"}`))
	}))
	t.Cleanup(srv.Close)

	auth := &cliproxyauth.Auth{
		ID: "kiro-1", Provider: "kiro",
		Storage: &kiroAuthStorage{accessToken: "test_at", profileArn: "arn"},
	}
	e := NewKiroExecutor(&config.Config{})
	e.endpointOverride = srv.URL

	req := cliproxyexecutor.Request{
		Model:   "claude-sonnet-4.5",
		Payload: []byte(`{}`),
	}
	result, err := e.ExecuteStream(context.Background(), auth, req, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream: %v", err)
	}
	if result == nil {
		t.Fatal("nil StreamResult")
	}

	got := []string{}
	for ch := range result.Chunks {
		if ch.Err != nil {
			t.Errorf("chunk err: %v", ch.Err)
		}
		got = append(got, string(ch.Payload))
	}
	merged := strings.Join(got, "\n")
	if !strings.Contains(merged, `"text":"Hi"`) || !strings.Contains(merged, `"text":"!"`) {
		t.Errorf("missing chunks in:\n%s", merged)
	}
}

func TestKiroExecuteNonStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		w.WriteHeader(200)
		w.Write(makeKiroFrame("content", `{"text":"Hello "}`))
		w.Write(makeKiroFrame("content", `{"text":"world"}`))
		w.Write(makeKiroFrame("contextUsage", `{"contextUsagePercentage":1.5}`))
	}))
	t.Cleanup(srv.Close)

	auth := &cliproxyauth.Auth{
		ID:       "kiro-1",
		Provider: "kiro",
		Storage:  &kiroAuthStorage{accessToken: "test_at", profileArn: "arn:profile"},
	}

	e := NewKiroExecutor(&config.Config{})
	e.endpointOverride = srv.URL

	req := cliproxyexecutor.Request{
		Model:   "claude-sonnet-4.5",
		Payload: []byte(`{"profileArn":"arn","conversationState":{"agentTaskType":"vibe","chatTriggerType":"MANUAL","conversationId":"c","currentMessage":{"userInputMessage":{"content":"hi","modelId":"claude-sonnet-4.5","origin":"AI_EDITOR"}}}}`),
	}
	resp, err := e.Execute(context.Background(), auth, req, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(string(resp.Payload), "Hello") || !strings.Contains(string(resp.Payload), "world") {
		t.Errorf("payload missing text: %s", resp.Payload)
	}
}
