package kiro

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Refresher executes credential refresh against the appropriate Kiro endpoint
// based on auth method. URL overrides allow tests to redirect HTTP calls.
type Refresher struct {
	HTTPClient                *http.Client
	SocialRefreshURLOverride  string // for tests
	BuilderIDTokenURLOverride string // for tests
}

// NewRefresher constructs a Refresher with the given HTTP client.
// If client is nil, http.DefaultClient is used.
func NewRefresher(client *http.Client) *Refresher {
	if client == nil {
		client = http.DefaultClient
	}
	return &Refresher{HTTPClient: client}
}

// Refresh dispatches to the correct backend based on c.AuthMethod and returns
// a new Credentials with rotated access/refresh tokens.
func (r *Refresher) Refresh(ctx context.Context, c *Credentials) (*Credentials, error) {
	switch c.AuthMethod {
	case AuthMethodSocial, AuthMethodImport:
		return r.refreshSocial(ctx, c)
	case AuthMethodBuilderID:
		return r.refreshBuilderID(ctx, c)
	default:
		return nil, fmt.Errorf("kiro: unknown auth_method: %q", c.AuthMethod)
	}
}

type socialRefreshResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ProfileArn   string `json:"profileArn"`
	ExpiresIn    int64  `json:"expiresIn"`
}

func (r *Refresher) refreshSocial(ctx context.Context, c *Credentials) (*Credentials, error) {
	url := r.SocialRefreshURLOverride
	if url == "" {
		url = strings.ReplaceAll(SocialRefreshEndpoint, "{region}", regionOrDefault(c))
	}
	body, _ := json.Marshal(map[string]string{"refreshToken": c.RefreshToken})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("kiro: build refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kiro: refresh request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("kiro: social refresh status %d: %s", resp.StatusCode, raw)
	}
	var out socialRefreshResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("kiro: decode refresh response: %w", err)
	}
	if out.AccessToken == "" {
		return nil, fmt.Errorf("kiro: refresh response missing accessToken")
	}
	updated := *c
	updated.AccessToken = out.AccessToken
	if out.RefreshToken != "" {
		updated.RefreshToken = out.RefreshToken
	}
	if out.ProfileArn != "" {
		updated.ProfileArn = out.ProfileArn
	}
	expiresIn := out.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	updated.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)
	return &updated, nil
}

type builderIDTokenResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int64  `json:"expiresIn"`
}

func (r *Refresher) refreshBuilderID(ctx context.Context, c *Credentials) (*Credentials, error) {
	url := r.BuilderIDTokenURLOverride
	if url == "" {
		url = strings.ReplaceAll(BuilderIDOIDCEndpoint, "{region}", regionOrDefault(c)) + "/token"
	}
	body, _ := json.Marshal(map[string]string{
		"refreshToken": c.RefreshToken,
		"clientId":     c.ClientID,
		"clientSecret": c.ClientSecret,
		"grantType":    "refresh_token",
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("kiro: build builder-id refresh: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kiro: builder-id refresh: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("kiro: builder-id refresh status %d: %s", resp.StatusCode, raw)
	}
	var out builderIDTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("kiro: decode builder-id refresh response: %w", err)
	}
	if out.AccessToken == "" {
		return nil, fmt.Errorf("kiro: builder-id refresh missing accessToken")
	}
	updated := *c
	updated.AccessToken = out.AccessToken
	if out.RefreshToken != "" {
		updated.RefreshToken = out.RefreshToken
	}
	expiresIn := out.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	updated.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)
	return &updated, nil
}

func regionOrDefault(c *Credentials) string {
	if c == nil || c.Region == "" {
		return DefaultRegion
	}
	return c.Region
}
