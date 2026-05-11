package kiro

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestKiroAuthLoadAndSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kiro.json")
	c := &Credentials{
		AuthMethod:   AuthMethodImport,
		AccessToken:  "at",
		RefreshToken: "rt",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := SaveCredentials(path, c); err != nil {
		t.Fatal(err)
	}

	a := NewKiroAuth(nil)
	got, err := a.Load(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "at" {
		t.Errorf("AccessToken = %q; want at", got.AccessToken)
	}
}

func TestKiroAuthEnsureFreshSkipsValid(t *testing.T) {
	a := NewKiroAuth(http.DefaultClient)
	c := &Credentials{
		AuthMethod:   AuthMethodImport,
		RefreshToken: "rt",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	out, err := a.EnsureFresh(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if out.AccessToken != c.AccessToken {
		t.Errorf("EnsureFresh on valid creds should not refresh; got %+v", out)
	}
}
