package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
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
