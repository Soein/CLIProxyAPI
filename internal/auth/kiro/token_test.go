package kiro

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCredentialsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kiro.json")

	want := &Credentials{
		AuthMethod:                  AuthMethodBuilderID,
		AccessToken:                 "AKIAEXAMPLE",
		RefreshToken:                "rt_example",
		ProfileArn:                  "",
		ClientID:                    "client-123",
		ClientSecret:                "secret-xyz",
		ClientRegistrationExpiresAt: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
		Region:                      "us-east-1",
		UUID:                        "device-uuid",
		ExpiresAt:                   time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	}

	if err := SaveCredentials(path, want); err != nil {
		t.Fatalf("SaveCredentials: %v", err)
	}

	got, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}

	if got.AuthMethod != want.AuthMethod || got.AccessToken != want.AccessToken ||
		got.RefreshToken != want.RefreshToken || got.ClientID != want.ClientID ||
		got.ClientSecret != want.ClientSecret || got.Region != want.Region ||
		got.UUID != want.UUID || !got.ExpiresAt.Equal(want.ExpiresAt) ||
		!got.ClientRegistrationExpiresAt.Equal(want.ClientRegistrationExpiresAt) {
		t.Errorf("round-trip mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestLoadCredentialsMissingFile(t *testing.T) {
	_, err := LoadCredentials("/nonexistent/path/kiro.json")
	if !os.IsNotExist(err) {
		t.Fatalf("expected os.IsNotExist, got %v", err)
	}
}

func TestIsExpiredLeeway(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name      string
		expiresAt time.Time
		leeway    time.Duration
		want      bool
	}{
		{"already expired", now.Add(-time.Minute), time.Second, true},
		{"about to expire within leeway", now.Add(30 * time.Second), time.Minute, true},
		{"safely valid", now.Add(time.Hour), time.Minute, false},
		{"zero expiresAt is treated as expired", time.Time{}, time.Minute, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Credentials{ExpiresAt: tc.expiresAt}
			if got := IsExpired(c, tc.leeway); got != tc.want {
				t.Errorf("IsExpired(%v, %v) = %v; want %v", tc.expiresAt, tc.leeway, got, tc.want)
			}
		})
	}
}

func TestSaveCredentialsAlwaysSetsType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kiro.json")
	c := &Credentials{
		AccessToken:  "at",
		RefreshToken: "rt",
		AuthMethod:   AuthMethodImport,
		ExpiresAt:    time.Now().Add(time.Hour),
		// Note: Type is intentionally NOT set
	}
	if err := SaveCredentials(path, c); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"type": "kiro"`) {
		t.Errorf("on-disk JSON missing type=kiro: %s", raw)
	}
}
