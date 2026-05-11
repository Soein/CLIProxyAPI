package kiro

import (
	"context"
	"net/http"
	"time"
)

// KiroAuth aggregates credential loading, persistence, and refresh into a
// single service consumed by the SDK and management handlers.
type KiroAuth struct {
	HTTPClient *http.Client
	Refresher  *Refresher
	BuilderID  *BuilderIDClient

	// RefreshLeeway controls how far ahead of expiry EnsureFresh refreshes.
	RefreshLeeway time.Duration
}

// NewKiroAuth constructs a KiroAuth using the supplied HTTP client (or
// http.DefaultClient when nil).
func NewKiroAuth(client *http.Client) *KiroAuth {
	if client == nil {
		client = http.DefaultClient
	}
	return &KiroAuth{
		HTTPClient:    client,
		Refresher:     NewRefresher(client),
		BuilderID:     NewBuilderIDClient(client),
		RefreshLeeway: time.Minute,
	}
}

// Load reads a credential file from disk.
func (a *KiroAuth) Load(_ context.Context, path string) (*Credentials, error) {
	return LoadCredentials(path)
}

// Save writes credentials atomically.
func (a *KiroAuth) Save(_ context.Context, path string, c *Credentials) error {
	return SaveCredentials(path, c)
}

// EnsureFresh refreshes credentials if they are within RefreshLeeway of
// expiry. Returns the (possibly updated) credentials.
func (a *KiroAuth) EnsureFresh(ctx context.Context, c *Credentials) (*Credentials, error) {
	if !IsExpired(c, a.RefreshLeeway) {
		return c, nil
	}
	return a.Refresher.Refresh(ctx, c)
}
