package kiro

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGenerateCodeVerifier(t *testing.T) {
	v1, err := GenerateCodeVerifier()
	if err != nil {
		t.Fatal(err)
	}
	v2, _ := GenerateCodeVerifier()
	if v1 == v2 {
		t.Errorf("GenerateCodeVerifier should be random; got duplicate %q", v1)
	}
	// 32 random bytes → base64url length = 43
	if len(v1) < 40 || len(v1) > 50 {
		t.Errorf("verifier length unexpected: %d (%q)", len(v1), v1)
	}
	if strings.ContainsAny(v1, "+/=") {
		t.Errorf("verifier must be base64url (no +/=): %q", v1)
	}
}

func TestCodeChallenge(t *testing.T) {
	verifier := "test-verifier"
	got := CodeChallenge(verifier)
	hash := sha256.Sum256([]byte(verifier))
	want := strings.TrimRight(base64.URLEncoding.EncodeToString(hash[:]), "=")
	if got != want {
		t.Errorf("CodeChallenge mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildPKCEAuthURL(t *testing.T) {
	got := BuildPKCEAuthURL("Google", "http://127.0.0.1:19876/oauth/callback", "challenge_xyz", "state_abc", "us-east-1")
	if !strings.Contains(got, "idp=Google") {
		t.Errorf("missing idp=Google: %s", got)
	}
	if !strings.Contains(got, "code_challenge=challenge_xyz") {
		t.Errorf("missing code_challenge: %s", got)
	}
	if !strings.Contains(got, "code_challenge_method=S256") {
		t.Errorf("missing code_challenge_method: %s", got)
	}
	if !strings.Contains(got, "state=state_abc") {
		t.Errorf("missing state: %s", got)
	}
	if !strings.Contains(got, "prod.us-east-1.auth.desktop.kiro.dev") {
		t.Errorf("region not substituted: %s", got)
	}
}

func TestExchangePKCECode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/oauth/token") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var body map[string]string
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		if body["code"] != "abc" || body["code_verifier"] != "v" || body["redirect_uri"] != "http://127.0.0.1:1/cb" {
			t.Errorf("unexpected body: %+v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accessToken":  "at",
			"refreshToken": "rt",
			"profileArn":   "arn",
			"expiresIn":    7200,
		})
	}))
	t.Cleanup(srv.Close)

	out, err := ExchangePKCECode(context.Background(), srv.Client(),
		PKCEExchangeOptions{
			TokenURLOverride: srv.URL + "/oauth/token",
			Code:             "abc",
			CodeVerifier:     "v",
			RedirectURI:      "http://127.0.0.1:1/cb",
			Region:           "us-east-1",
		})
	if err != nil {
		t.Fatalf("ExchangePKCECode: %v", err)
	}
	if out.AccessToken != "at" || out.RefreshToken != "rt" || out.ProfileArn != "arn" {
		t.Errorf("missing fields: %+v", out)
	}
	if out.AuthMethod != AuthMethodSocial {
		t.Errorf("AuthMethod = %s; want %s", out.AuthMethod, AuthMethodSocial)
	}
}

func TestStartCallbackServerPickPort(t *testing.T) {
	cb, err := StartCallbackServer(CallbackOptions{
		ExpectedState: "state-x",
		PortRange:     []int{0}, // 0 lets OS pick a free port
	})
	if err != nil {
		t.Fatalf("StartCallbackServer: %v", err)
	}
	defer cb.Close()

	if cb.Port == 0 {
		t.Fatal("Port not set after Start")
	}
	if !strings.HasPrefix(cb.RedirectURI, "http://127.0.0.1:") {
		t.Errorf("RedirectURI looks wrong: %s", cb.RedirectURI)
	}

	// Simulate browser redirect with code & state.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/oauth/callback?code=mycode&state=state-x", cb.Port))
	if err != nil {
		t.Fatalf("GET callback: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("callback status = %d; want 200", resp.StatusCode)
	}

	select {
	case res := <-cb.Result:
		if res.Code != "mycode" {
			t.Errorf("Code = %s; want mycode", res.Code)
		}
		if res.Err != nil {
			t.Errorf("unexpected err: %v", res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback never resolved")
	}
}

func TestStartCallbackServerStateMismatch(t *testing.T) {
	cb, err := StartCallbackServer(CallbackOptions{
		ExpectedState: "good",
		PortRange:     []int{0},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer cb.Close()
	resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/oauth/callback?code=c&state=BAD", cb.Port))
	if resp != nil {
		_ = resp.Body.Close()
	}
	select {
	case res := <-cb.Result:
		if res.Err == nil || !strings.Contains(res.Err.Error(), "state") {
			t.Errorf("expected state mismatch error, got %v", res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected error never delivered")
	}
}
