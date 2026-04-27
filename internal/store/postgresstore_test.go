package store

import (
	"path/filepath"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// TestBuildAuthFromRow_RestoresDisabledTrue verifies the read path turns
// metadata.disabled=true back into auth.Disabled=true and Status=StatusDisabled.
// Regression: prior to the fix, Disabled was never persisted/read, so codex
// auto-disabled auths reactivated themselves on every restart.
func TestBuildAuthFromRow_RestoresDisabledTrue(t *testing.T) {
	authDir := t.TempDir()
	store := &PostgresStore{authDir: authDir}

	payload := `{"type":"codex","email":"u@x.com","disabled":true,"codex_weekly_auto_disabled_at":"2026-04-27T05:20:57Z"}`
	now := time.Date(2026, 4, 27, 5, 21, 0, 0, time.UTC)

	auth, ok := store.buildAuthFromRow("acct-1.json", payload, now, now)
	if !ok {
		t.Fatalf("buildAuthFromRow returned ok=false")
	}
	if !auth.Disabled {
		t.Fatalf("auth.Disabled = false, want true")
	}
	if auth.Status != cliproxyauth.StatusDisabled {
		t.Fatalf("auth.Status = %s, want %s", auth.Status, cliproxyauth.StatusDisabled)
	}
	if got := auth.Metadata["codex_weekly_auto_disabled_at"]; got != "2026-04-27T05:20:57Z" {
		t.Fatalf("metadata.codex_weekly_auto_disabled_at = %v, want timestamp", got)
	}
}

// TestBuildAuthFromRow_DefaultsToActive verifies a missing or false `disabled`
// flag yields auth.Disabled=false and Status=StatusActive — preserving the
// pre-fix behavior for unaffected auths.
func TestBuildAuthFromRow_DefaultsToActive(t *testing.T) {
	authDir := t.TempDir()
	store := &PostgresStore{authDir: authDir}
	now := time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		payload string
	}{
		{"missing-disabled-key", `{"type":"codex","email":"u@x.com"}`},
		{"explicit-false", `{"type":"codex","email":"u@x.com","disabled":false}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth, ok := store.buildAuthFromRow("acct.json", tc.payload, now, now)
			if !ok {
				t.Fatalf("buildAuthFromRow returned ok=false")
			}
			if auth.Disabled {
				t.Fatalf("auth.Disabled = true, want false")
			}
			if auth.Status != cliproxyauth.StatusActive {
				t.Fatalf("auth.Status = %s, want %s", auth.Status, cliproxyauth.StatusActive)
			}
		})
	}
}

// TestBuildAuthFromRow_AuthOutsideSpoolReturnsFalse confirms paths escaping
// the spool dir are rejected — invariant preserved from before the fix.
func TestBuildAuthFromRow_AuthOutsideSpoolReturnsFalse(t *testing.T) {
	store := &PostgresStore{authDir: filepath.Join(t.TempDir(), "auths")}
	now := time.Now()
	if _, ok := store.buildAuthFromRow("../escape.json", `{"type":"codex"}`, now, now); ok {
		t.Fatalf("expected ok=false for path outside spool")
	}
}
