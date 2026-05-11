//go:build smoke

package kiro

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

// TestM1Acceptance_ImportThenRefresh exercises the full M1 happy path:
// load an existing credential file → detect near-expiry → refresh via mock
// social endpoint → save back. Run with `go test -tags smoke ./internal/auth/kiro/...`.
func TestM1Acceptance_ImportThenRefresh(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accessToken":  "fresh_at",
			"refreshToken": "fresh_rt",
			"profileArn":   "arn:profile",
			"expiresIn":    3600,
		})
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	path := filepath.Join(dir, "kiro.json")

	original := &Credentials{
		AuthMethod:   AuthMethodImport,
		AccessToken:  "stale_at",
		RefreshToken: "stale_rt",
		ExpiresAt:    time.Now().Add(10 * time.Second), // near expiry
		Region:       "us-east-1",
	}
	if err := SaveCredentials(path, original); err != nil {
		t.Fatalf("save: %v", err)
	}

	a := NewKiroAuth(srv.Client())
	a.Refresher.SocialRefreshURLOverride = srv.URL
	a.RefreshLeeway = time.Minute

	loaded, err := a.Load(context.Background(), path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	fresh, err := a.EnsureFresh(context.Background(), loaded)
	if err != nil {
		t.Fatalf("ensure fresh: %v", err)
	}
	if fresh.AccessToken != "fresh_at" {
		t.Errorf("AccessToken not refreshed: %q", fresh.AccessToken)
	}
	if err := a.Save(context.Background(), path, fresh); err != nil {
		t.Fatalf("save back: %v", err)
	}

	// Verify the on-disk version reflects the refresh.
	reloaded, _ := LoadCredentials(path)
	if reloaded.AccessToken != "fresh_at" {
		t.Errorf("on-disk AccessToken not updated: %q", reloaded.AccessToken)
	}
}
