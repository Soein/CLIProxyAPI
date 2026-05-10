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

// Refresh delegates to internal/auth/kiro.Refresher.
func (e *KiroExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil {
		return nil, fmt.Errorf("kiro executor: refresh: auth is nil")
	}
	creds, err := loadKiroCredentials(auth)
	if err != nil {
		return nil, err
	}
	r := internalkiro.NewRefresher(e.httpClient)
	if _, err := r.Refresh(ctx, creds); err != nil {
		return nil, err
	}
	// Persistence of refreshed tokens is wired in M3 (management API).
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

// ExecuteStream is implemented in Task 4.
func (e *KiroExecutor) ExecuteStream(_ context.Context, _ *cliproxyauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, fmt.Errorf("kiro executor: ExecuteStream not yet implemented (Task 4)")
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

// loadKiroCredentials extracts Kiro credentials from the auth Storage.
// Strategy 1: type-assert to kiroAccessor (production KiroTokenStorage + test stub).
// Strategy 2: json.Marshaler fallback (generic storage that can serialize itself).
func loadKiroCredentials(auth *cliproxyauth.Auth) (*internalkiro.Credentials, error) {
	if auth == nil || auth.Storage == nil {
		return nil, fmt.Errorf("kiro executor: missing storage")
	}

	// Strategy 1: direct accessor interface (KiroTokenStorage and test stub).
	if a, ok := auth.Storage.(kiroAccessor); ok {
		return &internalkiro.Credentials{
			AccessToken: a.GetAccessToken(),
			ProfileArn:  a.GetProfileArn(),
			AuthMethod:  internalkiro.AuthMethodImport,
		}, nil
	}

	// Strategy 2: JSON marshaler (fallback for generic storages).
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

	return nil, fmt.Errorf("kiro executor: unsupported storage type %T", auth.Storage)
}
