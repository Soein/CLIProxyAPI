package kiro

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuilderIDRegisterClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/client/register") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"clientId":              "cid-x",
			"clientSecret":          "csec-x",
			"clientSecretExpiresAt": time.Now().Add(90 * 24 * time.Hour).Unix(),
		})
	}))
	t.Cleanup(srv.Close)

	bd := NewBuilderIDClient(srv.Client())
	bd.OIDCEndpointOverride = srv.URL

	out, err := bd.RegisterClient(context.Background())
	if err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	if out.ClientID != "cid-x" || out.ClientSecret != "csec-x" {
		t.Errorf("missing fields: %+v", out)
	}
	if out.ExpiresAt.IsZero() {
		t.Errorf("ExpiresAt zero")
	}
}

func TestBuilderIDStartDeviceAuthorization(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/device_authorization") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"deviceCode":              "dc-1",
			"userCode":                "ABCD-1234",
			"verificationUri":         "https://device.sso.aws",
			"verificationUriComplete": "https://device.sso.aws?code=ABCD-1234",
			"expiresIn":               300,
			"interval":                5,
		})
	}))
	t.Cleanup(srv.Close)

	bd := NewBuilderIDClient(srv.Client())
	bd.OIDCEndpointOverride = srv.URL

	out, err := bd.StartDeviceAuthorization(context.Background(), "cid", "csec")
	if err != nil {
		t.Fatalf("StartDeviceAuthorization: %v", err)
	}
	if out.DeviceCode != "dc-1" || out.UserCode != "ABCD-1234" || out.Interval != 5 {
		t.Errorf("missing fields: %+v", out)
	}
}

func TestBuilderIDPollSuccess(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 2 {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "authorization_pending"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accessToken":  "at",
			"refreshToken": "rt",
			"expiresIn":    3600,
		})
	}))
	t.Cleanup(srv.Close)

	bd := NewBuilderIDClient(srv.Client())
	bd.OIDCEndpointOverride = srv.URL
	bd.PollInterval = 10 * time.Millisecond

	creds, err := bd.PollToken(context.Background(), "cid", "csec", "dc", 100, time.Second)
	if err != nil {
		t.Fatalf("PollToken: %v", err)
	}
	if creds.AccessToken != "at" || creds.RefreshToken != "rt" {
		t.Errorf("missing fields: %+v", creds)
	}
	if creds.AuthMethod != AuthMethodBuilderID {
		t.Errorf("AuthMethod = %s; want %s", creds.AuthMethod, AuthMethodBuilderID)
	}
	if calls.Load() != 2 {
		t.Errorf("expected 2 polls, got %d", calls.Load())
	}
}

func TestBuilderIDPollTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "authorization_pending"})
	}))
	t.Cleanup(srv.Close)

	bd := NewBuilderIDClient(srv.Client())
	bd.OIDCEndpointOverride = srv.URL
	bd.PollInterval = 10 * time.Millisecond

	_, err := bd.PollToken(context.Background(), "cid", "csec", "dc", 100, 50*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestBuilderIDPollExplicitError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "access_denied"})
	}))
	t.Cleanup(srv.Close)

	bd := NewBuilderIDClient(srv.Client())
	bd.OIDCEndpointOverride = srv.URL
	bd.PollInterval = 1 * time.Millisecond

	_, err := bd.PollToken(context.Background(), "cid", "csec", "dc", 100, time.Second)
	if err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Fatalf("expected access_denied error, got %v", err)
	}
}
