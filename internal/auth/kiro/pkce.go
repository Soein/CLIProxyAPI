package kiro

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
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
