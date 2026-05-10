package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/eventstream/awsstream"
	kiroclaude "github.com/router-for-me/CLIProxyAPI/v7/internal/translator/kiro/claude"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

const (
	kiroEndpointTemplate = "https://q.{region}.amazonaws.com/generateAssistantResponse"
	kiroDefaultRegion    = "us-east-1"
)

// KiroExecutor implements cliproxyauth.ProviderExecutor for AWS Kiro (Amazon Q Developer).
type KiroExecutor struct {
	cfg        *config.Config
	httpClient *http.Client
	// endpointOverride lets tests redirect HTTP calls. Empty in production.
	endpointOverride string
}

// NewKiroExecutor constructs a KiroExecutor with config-based proxy.
func NewKiroExecutor(cfg *config.Config) *KiroExecutor {
	if cfg == nil {
		cfg = &config.Config{}
	}
	return &KiroExecutor{
		cfg:        cfg,
		httpClient: util.SetProxy(&cfg.SDKConfig, &http.Client{}),
	}
}

// Identifier returns the provider key.
func (e *KiroExecutor) Identifier() string { return "kiro" }

// HttpRequest injects Kiro auth headers into req and executes it.
// The caller is responsible for closing the response body.
func (e *KiroExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if auth == nil {
		return nil, fmt.Errorf("kiro executor: auth is nil")
	}
	creds, err := loadKiroCredentials(auth)
	if err != nil {
		return nil, err
	}
	machineID := internalkiro.MachineID(creds)
	injectKiroHeaders(req, creds.AccessToken, machineID)
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	return e.httpClient.Do(req)
}

// Refresh delegates to internal/auth/kiro.Refresher and persists the rotated
// tokens back to disk + the in-memory Auth metadata so subsequent requests
// pick up the fresh access token without a server restart.
//
// Persistence strategy:
//  1. Load credentials from auth.Storage (existing path).
//  2. Call Refresher.Refresh to rotate tokens.
//  3. Write the updated credentials atomically to the file path stored in
//     auth.Attributes["path"] (set by management.buildAuthFromFileData).
//  4. Mirror the new fields into auth.Metadata so the conductor and any UI
//     consumers see the rotated values.
//  5. Stamp auth.LastRefreshedAt so the auto-refresh loop respects the
//     freshly-set expiry instead of immediately scheduling another refresh.
//
// In-memory Storage update is best-effort: if Storage implements an updater
// interface (kiroWriter), we mutate it in place; otherwise the next request
// will read fresh tokens via the Metadata path inside loadKiroCredentials —
// see Strategy 3 there.
func (e *KiroExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil {
		return nil, fmt.Errorf("kiro executor: refresh: auth is nil")
	}
	creds, err := loadKiroCredentials(auth)
	if err != nil {
		return nil, err
	}
	r := internalkiro.NewRefresher(e.httpClient)
	updated, err := r.Refresh(ctx, creds)
	if err != nil {
		return nil, err
	}
	if err := persistKiroRefresh(auth, updated); err != nil {
		// Disk write failed but tokens are still valid in memory; surface the
		// error so the auto-refresh loop can apply backoff / alerting.
		return nil, fmt.Errorf("kiro executor: persist refreshed credentials: %w", err)
	}
	return auth, nil
}

// Execute performs a non-streaming request: reads all event-stream frames,
// passes each through the response translator, and returns the aggregated
// SSE-flavored payload as the single response.
func (e *KiroExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	frames, err := e.fetchFrames(ctx, auth, req.Payload)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}

	var allBytes []byte
	param := new(any)
	for _, f := range frames {
		lines := kiroclaude.ConvertKiroResponseToClaude(ctx, req.Model, req.Payload, req.Payload, f.Payload, param)
		for _, ln := range lines {
			allBytes = append(allBytes, ln...)
			allBytes = append(allBytes, '\n')
		}
	}
	return cliproxyexecutor.Response{Payload: allBytes}, nil
}

// fetchFrames POSTs the body to Kiro and returns all decoded frames. Used by
// Execute (sync). ExecuteStream uses postKiro directly + a goroutine.
func (e *KiroExecutor) fetchFrames(ctx context.Context, auth *cliproxyauth.Auth, body []byte) ([]*awsstream.Frame, error) {
	resp, err := e.postKiro(ctx, auth, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("kiro: upstream status %d", resp.StatusCode)
	}
	dec := awsstream.NewDecoder(resp.Body)
	var frames []*awsstream.Frame
	for {
		f, err := dec.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return frames, fmt.Errorf("kiro executor: decode frame: %w", err)
		}
		frames = append(frames, f)
	}
	return frames, nil
}

// postKiro builds the HTTP request and dispatches via HttpRequest.
func (e *KiroExecutor) postKiro(ctx context.Context, auth *cliproxyauth.Auth, body []byte) (*http.Response, error) {
	endpoint := e.endpointOverride
	if endpoint == "" {
		creds, _ := loadKiroCredentials(auth)
		region := kiroDefaultRegion
		if creds != nil && creds.Region != "" {
			region = creds.Region
		}
		endpoint = strings.ReplaceAll(kiroEndpointTemplate, "{region}", region)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("kiro executor: build request: %w", err)
	}
	return e.HttpRequest(ctx, auth, httpReq)
}

// ExecuteStream performs a streaming request: starts a goroutine that decodes
// event-stream frames and sends translated SSE chunks via the result channel.
// The goroutine handles cleanup via deferred close + body close.
func (e *KiroExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	resp, err := e.postKiro(ctx, auth, req.Payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("kiro: upstream status %d", resp.StatusCode)
	}

	chunks := make(chan cliproxyexecutor.StreamChunk, 16)
	go func() {
		defer close(chunks)
		defer resp.Body.Close()
		dec := awsstream.NewDecoder(resp.Body)
		param := new(any)
		for {
			f, err := dec.ReadFrame()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				select {
				case chunks <- cliproxyexecutor.StreamChunk{Err: fmt.Errorf("kiro: decode: %w", err)}:
				case <-ctx.Done():
				}
				return
			}
			lines := kiroclaude.ConvertKiroResponseToClaude(ctx, req.Model, req.Payload, req.Payload, f.Payload, param)
			for _, ln := range lines {
				select {
				case chunks <- cliproxyexecutor.StreamChunk{Payload: ln}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return &cliproxyexecutor.StreamResult{
		Headers: resp.Header,
		Chunks:  chunks,
	}, nil
}

// CountTokens returns 0 for now. Real counting is M5.
func (e *KiroExecutor) CountTokens(_ context.Context, _ *cliproxyauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	body := []byte(`{"input_tokens":0,"output_tokens":0}`)
	return cliproxyexecutor.Response{Payload: body}, nil
}

// ---------- helpers ----------

func injectKiroHeaders(req *http.Request, accessToken, machineID string) {
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Amz-Sdk-Invocation-Id", uuid.NewString())
	req.Header.Set("X-Amz-User-Agent", fmt.Sprintf("aws-sdk-js/%s KiroIDE-%s-%s",
		internalkiro.AwsSdkJsVersion, internalkiro.KiroVersion, machineID))
	req.Header.Set("User-Agent", fmt.Sprintf(
		"aws-sdk-js/%s ua/%s os/%s lang/js md/nodejs#%s api/codewhispererstreaming#%s m/E KiroIDE-%s-%s",
		internalkiro.AwsSdkJsVersion, internalkiro.AwsSdkUaVersion, runtime.GOOS,
		internalkiro.NodeFakeVersion, internalkiro.AwsSdkJsVersion,
		internalkiro.KiroVersion, machineID))
	req.Header.Set("X-Amzn-Codewhisperer-Optout", "true")
	req.Header.Set("X-Amzn-Kiro-Agent-Mode", "vibe")
}

// kiroAccessor is an interface satisfied by KiroTokenStorage (production) and
// the test stub kiroAuthStorage. loadKiroCredentials checks this first.
type kiroAccessor interface {
	GetAccessToken() string
	GetProfileArn() string
}

// kiroWriter is the optional companion to kiroAccessor. When auth.Storage
// implements both, persistKiroRefresh updates the in-memory token state so
// concurrent in-flight requests don't race against a stale Storage.
type kiroWriter interface {
	SetAccessToken(string)
	SetRefreshToken(string)
	SetExpiresAt(time.Time)
}

// loadKiroCredentials extracts Kiro credentials from the auth Storage.
// Strategy 1: type-assert to kiroAccessor (production KiroTokenStorage + test stub).
// Strategy 2: read from auth.Metadata when previously refreshed in-place.
// Strategy 3: json.Marshaler fallback (generic storage that can serialize itself).
func loadKiroCredentials(auth *cliproxyauth.Auth) (*internalkiro.Credentials, error) {
	if auth == nil {
		return nil, fmt.Errorf("kiro executor: nil auth")
	}

	// Strategy 1: direct accessor interface (KiroTokenStorage and test stub).
	if auth.Storage != nil {
		if a, ok := auth.Storage.(kiroAccessor); ok {
			c := &internalkiro.Credentials{
				AccessToken: a.GetAccessToken(),
				ProfileArn:  a.GetProfileArn(),
				AuthMethod:  internalkiro.AuthMethodImport,
			}
			// Overlay the latest values from Metadata (set by persistKiroRefresh)
			// so a refreshed token isn't shadowed by stale Storage state.
			overlayKiroMetadata(c, auth.Metadata)
			return c, nil
		}
	}

	// Strategy 2: Metadata-driven (file uploaded via management API).
	if c := credsFromMetadata(auth.Metadata); c != nil {
		return c, nil
	}

	// Strategy 3: JSON marshaler (fallback for generic storages).
	if auth.Storage != nil {
		if marshaler, ok := auth.Storage.(json.Marshaler); ok {
			data, err := marshaler.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("kiro executor: marshal storage: %w", err)
			}
			var c internalkiro.Credentials
			if err := json.Unmarshal(data, &c); err != nil {
				return nil, fmt.Errorf("kiro executor: parse credentials: %w", err)
			}
			return &c, nil
		}
	}

	return nil, fmt.Errorf("kiro executor: no usable storage or metadata")
}

// credsFromMetadata reconstructs Credentials from the Metadata map populated
// by management.buildAuthFromFileData (which json.Unmarshals the upload).
func credsFromMetadata(meta map[string]any) *internalkiro.Credentials {
	if len(meta) == 0 {
		return nil
	}
	access, _ := meta["access_token"].(string)
	if access == "" {
		return nil
	}
	c := &internalkiro.Credentials{AccessToken: access}
	overlayKiroMetadata(c, meta)
	if c.AuthMethod == "" {
		c.AuthMethod = internalkiro.AuthMethodImport
	}
	return c
}

// overlayKiroMetadata copies refresh-relevant fields from Metadata onto c.
// It is the inverse of persistKiroRefresh's metadata writes.
func overlayKiroMetadata(c *internalkiro.Credentials, meta map[string]any) {
	if c == nil || len(meta) == 0 {
		return
	}
	if v, ok := meta["access_token"].(string); ok && v != "" {
		c.AccessToken = v
	}
	if v, ok := meta["refresh_token"].(string); ok && v != "" {
		c.RefreshToken = v
	}
	if v, ok := meta["profile_arn"].(string); ok && v != "" {
		c.ProfileArn = v
	}
	if v, ok := meta["client_id"].(string); ok && v != "" {
		c.ClientID = v
	}
	if v, ok := meta["client_secret"].(string); ok && v != "" {
		c.ClientSecret = v
	}
	if v, ok := meta["region"].(string); ok && v != "" {
		c.Region = v
	}
	if v, ok := meta["uuid"].(string); ok && v != "" {
		c.UUID = v
	}
	if v, ok := meta["auth_method"].(string); ok && v != "" {
		c.AuthMethod = v
	}
	if v, ok := meta["expires_at"].(string); ok && v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			c.ExpiresAt = t
		}
	}
}

// persistKiroRefresh writes refreshed credentials back to disk (using the path
// stored in auth.Attributes["path"]) and mirrors the new fields onto Metadata
// + LastRefreshedAt. If Storage implements kiroWriter, the in-memory token
// state is also updated so concurrent requests don't see stale tokens.
func persistKiroRefresh(auth *cliproxyauth.Auth, updated *internalkiro.Credentials) error {
	if auth == nil || updated == nil {
		return fmt.Errorf("kiro executor: nil auth or credentials")
	}

	// 1. Write to disk if we know the file path.
	if path := authFilePath(auth); path != "" {
		if err := internalkiro.SaveCredentials(path, updated); err != nil {
			return err
		}
	}

	// 2. Mirror onto Metadata so subsequent loadKiroCredentials sees fresh values.
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["access_token"] = updated.AccessToken
	if updated.RefreshToken != "" {
		auth.Metadata["refresh_token"] = updated.RefreshToken
	}
	if updated.ProfileArn != "" {
		auth.Metadata["profile_arn"] = updated.ProfileArn
	}
	if updated.ClientID != "" {
		auth.Metadata["client_id"] = updated.ClientID
	}
	if updated.ClientSecret != "" {
		auth.Metadata["client_secret"] = updated.ClientSecret
	}
	if updated.Region != "" {
		auth.Metadata["region"] = updated.Region
	}
	if updated.AuthMethod != "" {
		auth.Metadata["auth_method"] = updated.AuthMethod
	}
	if !updated.ExpiresAt.IsZero() {
		auth.Metadata["expires_at"] = updated.ExpiresAt.Format(time.RFC3339)
	}

	// 3. Stamp LastRefreshedAt so the auto-refresh loop honors the new expiry.
	auth.LastRefreshedAt = time.Now()

	// 4. Best-effort in-memory Storage update.
	if auth.Storage != nil {
		if w, ok := auth.Storage.(kiroWriter); ok {
			w.SetAccessToken(updated.AccessToken)
			if updated.RefreshToken != "" {
				w.SetRefreshToken(updated.RefreshToken)
			}
			if !updated.ExpiresAt.IsZero() {
				w.SetExpiresAt(updated.ExpiresAt)
			}
		}
	}

	return nil
}

// authFilePath returns the on-disk path stored in Attributes by
// management.buildAuthFromFileData. Empty when the auth wasn't loaded from
// disk (e.g. constructed directly by SDK login flow).
func authFilePath(auth *cliproxyauth.Auth) string {
	if auth == nil || auth.Attributes == nil {
		return ""
	}
	if p, ok := auth.Attributes["path"]; ok {
		return p
	}
	return ""
}
