package management

import (
	"errors"
	"testing"
	"time"

	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
)

func TestKiroSessionStorePKCE(t *testing.T) {
	store := newKiroSessionStore(5 * time.Minute)
	sid := store.NewPKCESession("verifier-1", "state-1", "redirect")
	if sid == "" {
		t.Fatal("empty session id")
	}
	got, err := store.GetPKCE(sid)
	if err != nil {
		t.Fatal(err)
	}
	if got.CodeVerifier != "verifier-1" || got.State != "state-1" {
		t.Errorf("got: %+v", got)
	}
	store.DeletePKCE(sid)
	if _, err := store.GetPKCE(sid); !errors.Is(err, errKiroSessionNotFound) {
		t.Errorf("expected not-found after delete; got %v", err)
	}
}

func TestKiroSessionStoreDevice(t *testing.T) {
	store := newKiroSessionStore(5 * time.Minute)
	sid := store.NewDeviceSession("cid", "csec", "dc", "ABCD-1234", "https://verify")
	got, err := store.GetDevice(sid)
	if err != nil {
		t.Fatal(err)
	}
	if got.UserCode != "ABCD-1234" {
		t.Errorf("UserCode = %q", got.UserCode)
	}

	creds := &internalkiro.Credentials{AccessToken: "at"}
	store.CompleteDevice(sid, creds, nil)

	got, _ = store.GetDevice(sid)
	if got.Status != "success" || got.Credentials == nil {
		t.Errorf("after complete: %+v", got)
	}
}

func TestKiroSessionStoreExpiry(t *testing.T) {
	store := newKiroSessionStore(10 * time.Millisecond)
	sid := store.NewPKCESession("v", "s", "r")
	time.Sleep(50 * time.Millisecond)
	if _, err := store.GetPKCE(sid); err == nil {
		t.Error("expected expiry error")
	}
}

func TestKiroSessionStorePKCEStatusInitiallyPending(t *testing.T) {
	store := newKiroSessionStore(5 * time.Minute)
	sid := store.NewPKCESession("v", "s", "r")
	got, err := store.GetPKCE(sid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "pending" {
		t.Errorf("Status = %q; want pending", got.Status)
	}
}

func TestKiroSessionStoreCompletePKCE(t *testing.T) {
	store := newKiroSessionStore(5 * time.Minute)
	sid := store.NewPKCESession("v", "s", "r")
	creds := &internalkiro.Credentials{AccessToken: "at"}
	store.CompletePKCE(sid, creds, nil)
	got, err := store.GetPKCE(sid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "success" || got.Credentials == nil {
		t.Errorf("after complete: %+v", got)
	}
}

func TestKiroSessionStoreCompletePKCEError(t *testing.T) {
	store := newKiroSessionStore(5 * time.Minute)
	sid := store.NewPKCESession("v", "s", "r")
	store.CompletePKCE(sid, nil, errors.New("something broke"))
	got, err := store.GetPKCE(sid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "error" || got.Err == "" {
		t.Errorf("after error complete: %+v", got)
	}
}

func TestKiroSessionStorePKCECompletedNotPurgedImmediately(t *testing.T) {
	// Completed PKCE sessions should survive past their TTL for 2 extra minutes
	// so the frontend has time to fetch the result (mirrors device-session behaviour).
	store := newKiroSessionStore(10 * time.Millisecond)
	sid := store.NewPKCESession("v", "s", "r")
	store.CompletePKCE(sid, &internalkiro.Credentials{AccessToken: "tok"}, nil)

	// Sleep past the original TTL.
	time.Sleep(50 * time.Millisecond)

	// Trigger purge by creating a new session.
	store.NewPKCESession("v2", "s2", "r2")

	// The completed session must still be accessible.
	got, err := store.GetPKCE(sid)
	if err != nil {
		t.Fatalf("completed session was purged too early: %v", err)
	}
	if got.Status != "success" {
		t.Errorf("Status = %q; want success", got.Status)
	}
}
