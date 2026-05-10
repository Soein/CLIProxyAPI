package kiro

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Credentials represents a Kiro account's persisted OAuth state.
// All three auth methods share this struct; method-specific fields are
// populated as appropriate.
type Credentials struct {
	AuthMethod                  string    `json:"auth_method"`
	AccessToken                 string    `json:"access_token"`
	RefreshToken                string    `json:"refresh_token"`
	ProfileArn                  string    `json:"profile_arn,omitempty"`
	ClientID                    string    `json:"client_id,omitempty"`
	ClientSecret                string    `json:"client_secret,omitempty"`
	ClientRegistrationExpiresAt time.Time `json:"client_registration_expires_at,omitempty"`
	Region                      string    `json:"region,omitempty"`
	UUID                        string    `json:"uuid,omitempty"`
	ExpiresAt                   time.Time `json:"expires_at"`
}

// LoadCredentials reads and parses a Kiro credential JSON file.
func LoadCredentials(path string) (*Credentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Credentials
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("kiro: parse %s: %w", path, err)
	}
	if c.Region == "" {
		c.Region = DefaultRegion
	}
	return &c, nil
}

// SaveCredentials writes credentials atomically to path. Parent directory is
// created if missing. The file is written 0600 since it contains tokens.
func SaveCredentials(path string, c *Credentials) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("kiro: mkdir: %w", err)
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("kiro: marshal: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("kiro: write tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("kiro: rename: %w", err)
	}
	return nil
}

// IsExpired returns true when the credential is past its expiry minus leeway.
// A zero ExpiresAt is treated as expired (forces a fresh fetch).
func IsExpired(c *Credentials, leeway time.Duration) bool {
	if c == nil || c.ExpiresAt.IsZero() {
		return true
	}
	return time.Now().Add(leeway).After(c.ExpiresAt)
}
