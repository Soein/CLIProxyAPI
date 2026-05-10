package kiro

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
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
