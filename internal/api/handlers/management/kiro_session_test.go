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
