package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"

	"github.com/google/uuid"
	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// KiroExecutor implements cliproxyauth.ProviderExecutor for AWS Kiro (Amazon Q Developer).
type KiroExecutor struct {
	cfg        *config.Config
	httpClient *http.Client
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

// Execute is implemented in Task 3.
func (e *KiroExecutor) Execute(_ context.Context, _ *cliproxyauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, fmt.Errorf("kiro executor: Execute not yet implemented (Task 3)")
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
