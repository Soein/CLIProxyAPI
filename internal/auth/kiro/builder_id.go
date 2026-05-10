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

// BuilderIDClient implements the AWS Builder ID device-code OAuth flow used by
// Kiro for users without social accounts. The flow is:
//
//  1. RegisterClient — anonymously register a new OIDC client; receive
//     clientId/clientSecret valid for ~90 days.
//  2. StartDeviceAuthorization — receive a userCode and verification URL the
//     end user must open in a browser.
//  3. PollToken — poll /token until the user completes consent.
type BuilderIDClient struct {
	HTTPClient           *http.Client
	OIDCEndpointOverride string        // for tests
	PollInterval         time.Duration // overrides default for tests
	Region               string
	StartURL             string
}

// NewBuilderIDClient constructs a BuilderIDClient with sensible defaults.
func NewBuilderIDClient(client *http.Client) *BuilderIDClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &BuilderIDClient{
		HTTPClient:   client,
		Region:       DefaultRegion,
		StartURL:     BuilderIDStartURL,
		PollInterval: time.Duration(DeviceCodePollingIntervalSec) * time.Second,
	}
}

func (b *BuilderIDClient) endpoint() string {
	if b.OIDCEndpointOverride != "" {
		return b.OIDCEndpointOverride
	}
	region := b.Region
	if region == "" {
		region = DefaultRegion
	}
	return strings.ReplaceAll(BuilderIDOIDCEndpoint, "{region}", region)
}

// RegisterClientResult is the parsed response of POST /client/register.
type RegisterClientResult struct {
	ClientID     string
	ClientSecret string
	ExpiresAt    time.Time
}

// RegisterClient anonymously creates a new OIDC client.
func (b *BuilderIDClient) RegisterClient(ctx context.Context) (*RegisterClientResult, error) {
	body, _ := json.Marshal(map[string]any{
		"clientName": "Kiro IDE",
		"clientType": "public",
		"scopes":     BuilderIDScopes,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint()+"/client/register", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "KiroIDE")

	resp, err := b.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kiro: register client: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("kiro: register status %d: %s", resp.StatusCode, raw)
	}
	var out struct {
		ClientID              string `json:"clientId"`
		ClientSecret          string `json:"clientSecret"`
		ClientSecretExpiresAt int64  `json:"clientSecretExpiresAt"` // Unix seconds
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("kiro: decode register: %w", err)
	}
	if out.ClientID == "" || out.ClientSecret == "" {
		return nil, fmt.Errorf("kiro: register missing client_id/secret")
	}
	expires := time.Now().Add(90 * 24 * time.Hour)
	if out.ClientSecretExpiresAt > 0 {
		expires = time.Unix(out.ClientSecretExpiresAt, 0)
	}
	return &RegisterClientResult{
		ClientID:     out.ClientID,
		ClientSecret: out.ClientSecret,
		ExpiresAt:    expires,
	}, nil
}

// DeviceAuthorization is the result of starting the device flow.
type DeviceAuthorization struct {
	DeviceCode              string
	UserCode                string
	VerificationURI         string
	VerificationURIComplete string
	ExpiresIn               int
	Interval                int
}

// StartDeviceAuthorization initiates the device-code flow.
func (b *BuilderIDClient) StartDeviceAuthorization(ctx context.Context, clientID, clientSecret string) (*DeviceAuthorization, error) {
	startURL := b.StartURL
	if startURL == "" {
		startURL = BuilderIDStartURL
	}
	body, _ := json.Marshal(map[string]string{
		"clientId":     clientID,
		"clientSecret": clientSecret,
		"startUrl":     startURL,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint()+"/device_authorization", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kiro: device auth: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("kiro: device auth status %d: %s", resp.StatusCode, raw)
	}
	var out struct {
		DeviceCode              string `json:"deviceCode"`
		UserCode                string `json:"userCode"`
		VerificationURI         string `json:"verificationUri"`
		VerificationURIComplete string `json:"verificationUriComplete"`
		ExpiresIn               int    `json:"expiresIn"`
		Interval                int    `json:"interval"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("kiro: decode device auth: %w", err)
	}
	if out.DeviceCode == "" {
		return nil, fmt.Errorf("kiro: device auth missing deviceCode")
	}
	return &DeviceAuthorization{
		DeviceCode:              out.DeviceCode,
		UserCode:                out.UserCode,
		VerificationURI:         out.VerificationURI,
		VerificationURIComplete: out.VerificationURIComplete,
		ExpiresIn:               out.ExpiresIn,
		Interval:                out.Interval,
	}, nil
}

// PollToken polls the /token endpoint until the user completes consent or
// the timeout elapses. interval is overridden by b.PollInterval if set.
func (b *BuilderIDClient) PollToken(ctx context.Context, clientID, clientSecret, deviceCode string, _ int, timeout time.Duration) (*Credentials, error) {
	deadline := time.Now().Add(timeout)
	interval := b.PollInterval
	if interval <= 0 {
		interval = time.Duration(DeviceCodePollingIntervalSec) * time.Second
	}

	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("kiro: device code timeout")
		}

		body, _ := json.Marshal(map[string]string{
			"clientId":     clientID,
			"clientSecret": clientSecret,
			"deviceCode":   deviceCode,
			"grantType":    "urn:ietf:params:oauth:grant-type:device_code",
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint()+"/token", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "KiroIDE")

		resp, err := b.HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("kiro: poll: %w", err)
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		_ = resp.Body.Close()

		var out struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
			ExpiresIn    int64  `json:"expiresIn"`
			Error        string `json:"error"`
		}
		_ = json.Unmarshal(raw, &out)

		if out.AccessToken != "" {
			expiresIn := out.ExpiresIn
			if expiresIn <= 0 {
				expiresIn = 3600
			}
			return &Credentials{
				AuthMethod:   AuthMethodBuilderID,
				AccessToken:  out.AccessToken,
				RefreshToken: out.RefreshToken,
				ClientID:     clientID,
				ClientSecret: clientSecret,
				Region:       b.Region,
				ExpiresAt:    time.Now().Add(time.Duration(expiresIn) * time.Second),
			}, nil
		}

		switch out.Error {
		case "authorization_pending":
			// keep polling
		case "slow_down":
			interval += 5 * time.Second
		case "":
			return nil, fmt.Errorf("kiro: poll status %d: %s", resp.StatusCode, raw)
		default:
			return nil, fmt.Errorf("kiro: %s", out.Error)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}
