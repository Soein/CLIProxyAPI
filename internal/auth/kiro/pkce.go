package kiro

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// GenerateCodeVerifier produces a base64url-encoded random verifier per RFC 7636.
func GenerateCodeVerifier() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("kiro: generate verifier: %w", err)
	}
	return strings.TrimRight(base64.URLEncoding.EncodeToString(buf), "="), nil
}

// CodeChallenge computes the S256 challenge = base64url(sha256(verifier)).
func CodeChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return strings.TrimRight(base64.URLEncoding.EncodeToString(sum[:]), "=")
}

// GenerateState returns a base64url-encoded random state value for CSRF.
func GenerateState() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("kiro: generate state: %w", err)
	}
	return strings.TrimRight(base64.URLEncoding.EncodeToString(buf), "="), nil
}

// BuildPKCEAuthURL constructs the social-login authorization URL for the given
// idp ("Google" or "Github"). The redirectURI must point at the local callback
// server started by StartCallbackServer.
func BuildPKCEAuthURL(idp, redirectURI, challenge, state, region string) string {
	if region == "" {
		region = DefaultRegion
	}
	base := strings.ReplaceAll(SocialAuthEndpoint, "{region}", region)
	params := url.Values{}
	params.Set("idp", idp)
	params.Set("redirect_uri", redirectURI)
	params.Set("code_challenge", challenge)
	params.Set("code_challenge_method", "S256")
	params.Set("state", state)
	params.Set("prompt", "select_account")
	return base + "/login?" + params.Encode()
}

// CallbackResult is delivered to (CallbackServer).Result on either success or
// failure. Code is set on success; Err is set on failure.
type CallbackResult struct {
	Code string
	Err  error
}

// CallbackOptions configures the local PKCE callback server.
type CallbackOptions struct {
	ExpectedState string
	PortRange     []int         // ports tried in order; use {0} to let OS pick
	Timeout       time.Duration // optional; default 10 minutes
}

// CallbackServer represents a running callback HTTP server.
type CallbackServer struct {
	Port        int
	RedirectURI string
	Result      chan CallbackResult
	server      *http.Server
	once        sync.Once
}

// Close shuts down the underlying HTTP server.
func (c *CallbackServer) Close() {
	c.once.Do(func() {
		if c.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = c.server.Shutdown(ctx)
		}
	})
}

// StartCallbackServer binds 127.0.0.1 on the first available port in
// opts.PortRange and listens for /oauth/callback. The caller MUST drain
// Result; the server auto-shuts after first delivery or Timeout.
func StartCallbackServer(opts CallbackOptions) (*CallbackServer, error) {
	if len(opts.PortRange) == 0 {
		opts.PortRange = CallbackPortRange
	}
	if opts.Timeout == 0 {
		opts.Timeout = 10 * time.Minute
	}

	var listener net.Listener
	var port int
	for _, p := range opts.PortRange {
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err == nil {
			listener = l
			port = l.Addr().(*net.TCPAddr).Port
			break
		}
	}
	if listener == nil {
		return nil, fmt.Errorf("kiro: no free port in %v", opts.PortRange)
	}

	cb := &CallbackServer{
		Port:        port,
		RedirectURI: fmt.Sprintf("http://127.0.0.1:%d/oauth/callback", port),
		Result:      make(chan CallbackResult, 1),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/callback", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		code := q.Get("code")
		state := q.Get("state")
		errParam := q.Get("error")

		var res CallbackResult
		switch {
		case errParam != "":
			res = CallbackResult{Err: fmt.Errorf("kiro: oauth error: %s", errParam)}
		case state != opts.ExpectedState:
			res = CallbackResult{Err: fmt.Errorf("kiro: state mismatch")}
		case code == "":
			res = CallbackResult{Err: fmt.Errorf("kiro: missing code")}
		default:
			res = CallbackResult{Code: code}
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if res.Err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "<html><body><h1>授权失败</h1><p>%s</p></body></html>", res.Err)
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `<html><body><h1>授权成功</h1><p>请关闭此窗口</p></body></html>`)
		}
		select {
		case cb.Result <- res:
		default:
		}
	})

	cb.server = &http.Server{Handler: mux}
	go func() { _ = cb.server.Serve(listener) }()

	// Auto-shutdown timer.
	go func() {
		time.Sleep(opts.Timeout)
		cb.Close()
	}()

	return cb, nil
}

// PKCEExchangeOptions configures token exchange after callback.
type PKCEExchangeOptions struct {
	TokenURLOverride string // for tests
	Code             string
	CodeVerifier     string
	RedirectURI      string
	Region           string
}

// ExchangePKCECode swaps the authorization code for tokens. Returns a
// Credentials with AuthMethod=social populated.
func ExchangePKCECode(ctx context.Context, client *http.Client, opts PKCEExchangeOptions) (*Credentials, error) {
	if client == nil {
		client = http.DefaultClient
	}
	tokenURL := opts.TokenURLOverride
	if tokenURL == "" {
		region := opts.Region
		if region == "" {
			region = DefaultRegion
		}
		tokenURL = strings.ReplaceAll(SocialAuthEndpoint, "{region}", region) + "/oauth/token"
	}
	body, _ := json.Marshal(map[string]string{
		"code":          opts.Code,
		"code_verifier": opts.CodeVerifier,
		"redirect_uri":  opts.RedirectURI,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("kiro: build exchange request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "AIClient2API/1.0.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kiro: exchange: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("kiro: exchange status %d: %s", resp.StatusCode, raw)
	}
	var out struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ProfileArn   string `json:"profileArn"`
		ExpiresIn    int64  `json:"expiresIn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("kiro: decode exchange: %w", err)
	}
	if out.AccessToken == "" {
		return nil, fmt.Errorf("kiro: exchange missing accessToken")
	}
	expiresIn := out.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	return &Credentials{
		AuthMethod:   AuthMethodSocial,
		AccessToken:  out.AccessToken,
		RefreshToken: out.RefreshToken,
		ProfileArn:   out.ProfileArn,
		Region:       opts.Region,
		ExpiresAt:    time.Now().Add(time.Duration(expiresIn) * time.Second),
	}, nil
}
