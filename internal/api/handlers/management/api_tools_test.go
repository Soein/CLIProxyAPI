package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestAPICallUsesRequestProxyURL(t *testing.T) {
	t.Parallel()

	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("proxied"))
	}))
	defer proxyServer.Close()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://127.0.0.1:1"},
		},
	}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"method":"GET","url":"http://upstream.invalid/test","proxy_url":"` + proxyServer.URL + `"}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response apiCallResponse
	if errDecode := json.NewDecoder(recorder.Body).Decode(&response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("upstream status code = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	if response.Body != "proxied" {
		t.Fatalf("upstream body = %q, want %q", response.Body, "proxied")
	}
}

func TestAPICallTransportDirectBypassesGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}

	transport := h.apiCallTransport(&coreauth.Auth{ProxyURL: "direct"}, "")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}
	if httpTransport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

func TestAPICallTransportInvalidAuthFallsBackToGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}

	transport := h.apiCallTransport(&coreauth.Auth{ProxyURL: "bad-value"}, "")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}

	req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequest returned error: %v", errRequest)
	}

	proxyURL, errProxy := httpTransport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://global-proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://global-proxy.example.com:8080", proxyURL)
	}
}

func TestAPICallTransportRequestProxyOverridesCredentialAndGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}
	auth := &coreauth.Auth{ProxyURL: "http://credential-proxy.example.com:8080"}

	transport := h.apiCallTransport(auth, " http://request-proxy.example.com:8080 ")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}

	req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequest returned error: %v", errRequest)
	}

	proxyURL, errProxy := httpTransport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://request-proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://request-proxy.example.com:8080", proxyURL)
	}
}

func TestAPICallTransportInvalidRequestProxyDoesNotFallBack(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}
	auth := &coreauth.Auth{ProxyURL: "http://credential-proxy.example.com:8080"}

	transport := h.apiCallTransport(auth, "bad-value")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}
	if httpTransport.Proxy != nil {
		t.Fatal("expected invalid request proxy to avoid lower-priority proxy settings")
	}
}

func TestAPICallTransportAPIKeyAuthFallsBackToConfigProxyURL(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
			GeminiKey: []config.GeminiKey{{
				APIKey:   "gemini-key",
				ProxyURL: "http://gemini-proxy.example.com:8080",
			}},
			ClaudeKey: []config.ClaudeKey{{
				APIKey:   "claude-key",
				ProxyURL: "http://claude-proxy.example.com:8080",
			}},
			CodexKey: []config.CodexKey{{
				APIKey:   "codex-key",
				ProxyURL: "http://codex-proxy.example.com:8080",
			}},
			XAIKey: []config.XAIKey{{
				APIKey:   "xai-key",
				ProxyURL: "http://xai-proxy.example.com:8080",
			}},
			OpenAICompatibility: []config.OpenAICompatibility{{
				Name:    "bohe",
				BaseURL: "https://bohe.example.com",
				APIKeyEntries: []config.OpenAICompatibilityAPIKey{{
					APIKey:   "compat-key",
					ProxyURL: "http://compat-proxy.example.com:8080",
				}},
			}},
		},
	}

	cases := []struct {
		name      string
		auth      *coreauth.Auth
		wantProxy string
	}{
		{
			name: "gemini",
			auth: &coreauth.Auth{
				Provider:   "gemini",
				Attributes: map[string]string{"api_key": "gemini-key"},
			},
			wantProxy: "http://gemini-proxy.example.com:8080",
		},
		{
			name: "claude",
			auth: &coreauth.Auth{
				Provider:   "claude",
				Attributes: map[string]string{"api_key": "claude-key"},
			},
			wantProxy: "http://claude-proxy.example.com:8080",
		},
		{
			name: "codex",
			auth: &coreauth.Auth{
				Provider:   "codex",
				Attributes: map[string]string{"api_key": "codex-key"},
			},
			wantProxy: "http://codex-proxy.example.com:8080",
		},
		{
			name: "xai",
			auth: &coreauth.Auth{
				Provider:   "xai",
				Attributes: map[string]string{"api_key": "xai-key"},
			},
			wantProxy: "http://xai-proxy.example.com:8080",
		},
		{
			name: "openai-compatibility",
			auth: &coreauth.Auth{
				Provider: "bohe",
				Attributes: map[string]string{
					"api_key":      "compat-key",
					"compat_name":  "bohe",
					"provider_key": "bohe",
				},
			},
			wantProxy: "http://compat-proxy.example.com:8080",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport := h.apiCallTransport(tc.auth, "")
			httpTransport, ok := transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport type = %T, want *http.Transport", transport)
			}

			req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
			if errRequest != nil {
				t.Fatalf("http.NewRequest returned error: %v", errRequest)
			}

			proxyURL, errProxy := httpTransport.Proxy(req)
			if errProxy != nil {
				t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
			}
			if proxyURL == nil || proxyURL.String() != tc.wantProxy {
				t.Fatalf("proxy URL = %v, want %s", proxyURL, tc.wantProxy)
			}
		})
	}
}

func TestAuthByIndexDistinguishesSharedAPIKeysAcrossProviders(t *testing.T) {
	t.Parallel()

	manager := coreauth.NewManager(nil, nil, nil)
	geminiAuth := &coreauth.Auth{
		ID:       "gemini:apikey:123",
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key": "shared-key",
		},
	}
	compatAuth := &coreauth.Auth{
		ID:       "openai-compatibility:bohe:456",
		Provider: "bohe",
		Label:    "bohe",
		Attributes: map[string]string{
			"api_key":      "shared-key",
			"compat_name":  "bohe",
			"provider_key": "bohe",
		},
	}

	if _, errRegister := manager.Register(context.Background(), geminiAuth); errRegister != nil {
		t.Fatalf("register gemini auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), compatAuth); errRegister != nil {
		t.Fatalf("register compat auth: %v", errRegister)
	}

	geminiIndex := geminiAuth.EnsureIndex()
	compatIndex := compatAuth.EnsureIndex()
	if geminiIndex == compatIndex {
		t.Fatalf("shared api key produced duplicate auth_index %q", geminiIndex)
	}

	h := &Handler{authManager: manager}

	gotGemini := h.authByIndex(geminiIndex)
	if gotGemini == nil {
		t.Fatal("expected gemini auth by index")
	}
	if gotGemini.ID != geminiAuth.ID {
		t.Fatalf("authByIndex(gemini) returned %q, want %q", gotGemini.ID, geminiAuth.ID)
	}

	gotCompat := h.authByIndex(compatIndex)
	if gotCompat == nil {
		t.Fatal("expected compat auth by index")
	}
	if gotCompat.ID != compatAuth.ID {
		t.Fatalf("authByIndex(compat) returned %q, want %q", gotCompat.ID, compatAuth.ID)
	}
}

func TestAPICallDispatchAdmissionRejectsAntigravityRefreshWithoutSideEffects(t *testing.T) {
	var refreshRequests atomic.Int64
	refreshServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		refreshRequests.Add(1)
	}))
	defer refreshServer.Close()
	originalTokenURL := antigravityOAuthTokenURL
	antigravityOAuthTokenURL = refreshServer.URL
	t.Cleanup(func() { antigravityOAuthTokenURL = originalTokenURL })

	var targetRequests atomic.Int64
	targetServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetRequests.Add(1)
	}))
	defer targetServer.Close()

	store := &apiCallDispatchStore{}
	manager := coreauth.NewManager(store, nil, nil)
	registered := registerAPICallAuth(t, manager, &coreauth.Auth{
		ID:       "management-antigravity-rejected",
		Provider: "antigravity",
		Metadata: map[string]any{
			"access_token":  "expired-access-token",
			"refresh_token": "unchanged-refresh-token",
			"expired":       time.Now().Add(-time.Minute).Format(time.RFC3339),
		},
	})
	authority := &apiCallDispatchAuthority{}
	manager.SetDispatchAuthority(authority)
	h := &Handler{cfg: &config.Config{}, authManager: manager}

	recorder := performAPICall(t, h, apiCallRequest{
		AuthIndexSnake: stringPointer(registered.Index),
		Method:         http.MethodGet,
		URL:            targetServer.URL,
		Header:         map[string]string{"Authorization": "Bearer $TOKEN$"},
	})

	assertAPICallAuthUnavailable(t, recorder)
	if got := refreshRequests.Load(); got != 0 {
		t.Fatalf("refresh HTTP requests = %d, want 0", got)
	}
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("target HTTP requests = %d, want 0", got)
	}
	if got := store.saves.Load(); got != 0 {
		t.Fatalf("credential persistence calls = %d, want 0", got)
	}
	current, ok := manager.GetByID(registered.ID)
	if !ok {
		t.Fatal("registered auth disappeared")
	}
	if got := stringValue(current.Metadata, "access_token"); got != "expired-access-token" {
		t.Fatalf("access_token = %q, want unchanged expired-access-token", got)
	}
	if got := stringValue(current.Metadata, "refresh_token"); got != "unchanged-refresh-token" {
		t.Fatalf("refresh_token = %q, want unchanged unchanged-refresh-token", got)
	}
	if !current.LastRefreshedAt.IsZero() || !current.UpdatedAt.IsZero() {
		t.Fatalf("credential timestamps mutated after rejection: refreshed=%v updated=%v", current.LastRefreshedAt, current.UpdatedAt)
	}
	if admits, releases, active := authority.counts(); admits != 1 || releases != 0 || active != 0 {
		t.Fatalf("dispatch admits/releases/active = %d/%d/%d, want 1/0/0", admits, releases, active)
	}
}

func TestAPICallDispatchAdmissionRejectsSelectedAuthRequestBeforeNetwork(t *testing.T) {
	var targetRequests atomic.Int64
	targetServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetRequests.Add(1)
	}))
	defer targetServer.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	registered := registerAPICallAuth(t, manager, &coreauth.Auth{
		ID:         "management-auth-request-rejected",
		Provider:   "gemini",
		Attributes: map[string]string{"api_key": "secret"},
	})
	authority := &apiCallDispatchAuthority{}
	manager.SetDispatchAuthority(authority)
	h := &Handler{cfg: &config.Config{}, authManager: manager}

	recorder := performAPICall(t, h, apiCallRequest{
		AuthIndexSnake: stringPointer(registered.Index),
		Method:         http.MethodGet,
		URL:            targetServer.URL,
		Header:         map[string]string{"Authorization": "Bearer $TOKEN$"},
	})

	assertAPICallAuthUnavailable(t, recorder)
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("target HTTP requests = %d, want 0", got)
	}
	if admits, releases, active := authority.counts(); admits != 1 || releases != 0 || active != 0 {
		t.Fatalf("dispatch admits/releases/active = %d/%d/%d, want 1/0/0", admits, releases, active)
	}
}

func TestAPICallDispatchAdmissionReleasesRefreshAndRequest(t *testing.T) {
	var refreshRequests atomic.Int64
	refreshServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		refreshRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"fresh-access-token","refresh_token":"fresh-refresh-token","expires_in":3600}`))
	}))
	defer refreshServer.Close()
	originalTokenURL := antigravityOAuthTokenURL
	antigravityOAuthTokenURL = refreshServer.URL
	t.Cleanup(func() { antigravityOAuthTokenURL = originalTokenURL })

	var targetRequests atomic.Int64
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		targetRequests.Add(1)
		if got := req.Header.Get("Authorization"); got != "Bearer fresh-access-token" {
			t.Errorf("Authorization = %q, want refreshed token", got)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer targetServer.Close()

	store := &apiCallDispatchStore{}
	manager := coreauth.NewManager(store, nil, nil)
	registered := registerAPICallAuth(t, manager, &coreauth.Auth{
		ID:       "management-antigravity-admitted",
		Provider: "antigravity",
		Metadata: map[string]any{
			"refresh_token": "old-refresh-token",
		},
	})
	authority := &apiCallDispatchAuthority{allow: true}
	manager.SetDispatchAuthority(authority)
	h := &Handler{cfg: &config.Config{}, authManager: manager}

	recorder := performAPICall(t, h, apiCallRequest{
		AuthIndexSnake: stringPointer(registered.Index),
		Method:         http.MethodGet,
		URL:            targetServer.URL,
		Header:         map[string]string{"Authorization": "Bearer $TOKEN$"},
	})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if got := refreshRequests.Load(); got != 1 {
		t.Fatalf("refresh HTTP requests = %d, want 1", got)
	}
	if got := targetRequests.Load(); got != 1 {
		t.Fatalf("target HTTP requests = %d, want 1", got)
	}
	if got := store.saves.Load(); got != 1 {
		t.Fatalf("credential persistence calls = %d, want 1", got)
	}
	if admits, releases, active := authority.counts(); admits != 2 || releases != 2 || active != 0 {
		t.Fatalf("dispatch admits/releases/active = %d/%d/%d, want 2/2/0", admits, releases, active)
	}
}

func TestAPICallWithoutAuthIndexBypassesDispatchAdmission(t *testing.T) {
	var targetRequests atomic.Int64
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer targetServer.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	authority := &apiCallDispatchAuthority{}
	manager.SetDispatchAuthority(authority)
	h := &Handler{cfg: &config.Config{}, authManager: manager}

	recorder := performAPICall(t, h, apiCallRequest{Method: http.MethodGet, URL: targetServer.URL})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if got := targetRequests.Load(); got != 1 {
		t.Fatalf("target HTTP requests = %d, want 1", got)
	}
	if admits, releases, active := authority.counts(); admits != 0 || releases != 0 || active != 0 {
		t.Fatalf("dispatch admits/releases/active = %d/%d/%d, want 0/0/0", admits, releases, active)
	}
}

type apiCallDispatchStore struct {
	saves atomic.Int64
}

func (*apiCallDispatchStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }
func (s *apiCallDispatchStore) Save(context.Context, *coreauth.Auth) (string, error) {
	s.saves.Add(1)
	return "", nil
}
func (*apiCallDispatchStore) Delete(context.Context, string) error { return nil }

type apiCallDispatchAuthority struct {
	allow    bool
	admits   atomic.Int64
	releases atomic.Int64
	active   atomic.Int64
}

func (a *apiCallDispatchAuthority) Admit(string) (func(), bool) {
	a.admits.Add(1)
	if !a.allow {
		return nil, false
	}
	a.active.Add(1)
	return func() {
		a.active.Add(-1)
		a.releases.Add(1)
	}, true
}

func (*apiCallDispatchAuthority) Wake()                           {}
func (*apiCallDispatchAuthority) Ready() bool                     { return true }
func (*apiCallDispatchAuthority) WaitReady(context.Context) error { return nil }
func (*apiCallDispatchAuthority) CloseAdmissions()                {}
func (a *apiCallDispatchAuthority) counts() (int64, int64, int64) {
	return a.admits.Load(), a.releases.Load(), a.active.Load()
}

func registerAPICallAuth(t *testing.T, manager *coreauth.Manager, auth *coreauth.Auth) *coreauth.Auth {
	t.Helper()
	registered, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	if registered == nil {
		t.Fatal("register auth returned nil")
	}
	return registered
}

func performAPICall(t *testing.T, h *Handler, body apiCallRequest) *httptest.ResponseRecorder {
	t.Helper()
	payload, errMarshal := json.Marshal(body)
	if errMarshal != nil {
		t.Fatalf("marshal APICall request: %v", errMarshal)
	}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", bytes.NewReader(payload))
	ginContext.Request.Header.Set("Content-Type", "application/json")
	h.APICall(ginContext)
	return recorder
}

func assertAPICallAuthUnavailable(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"error":"auth_unavailable"`) {
		t.Fatalf("body = %s, want auth_unavailable", recorder.Body.String())
	}
}

func stringPointer(value string) *string { return &value }
