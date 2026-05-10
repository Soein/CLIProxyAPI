package kiro

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRefreshSocial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/refreshToken") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var body struct{ RefreshToken string `json:"refreshToken"` }
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.RefreshToken != "rt_old" {
			t.Errorf("unexpected refreshToken in body: %s", body.RefreshToken)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accessToken":  "at_new",
			"refreshToken": "rt_new",
			"profileArn":   "arn:aws:codewhisperer:::profile",
			"expiresIn":    3600,
		})
	}))
	t.Cleanup(srv.Close)

	c := &Credentials{
		AuthMethod:   AuthMethodSocial,
		RefreshToken: "rt_old",
		Region:       "us-east-1",
	}
	r := NewRefresher(srv.Client())
	r.SocialRefreshURLOverride = srv.URL + "/refreshToken"

	out, err := r.Refresh(context.Background(), c)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if out.AccessToken != "at_new" || out.RefreshToken != "rt_new" || out.ProfileArn == "" {
		t.Errorf("missing fields after refresh: %+v", out)
	}
	if time.Until(out.ExpiresAt) < 30*time.Minute {
		t.Errorf("ExpiresAt should be ~1h ahead, got %v", out.ExpiresAt)
	}
}

func TestRefreshBuilderID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/token") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["clientId"] != "cid" || body["clientSecret"] != "csec" || body["grantType"] != "refresh_token" {
			t.Errorf("unexpected body: %+v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accessToken":  "at_new",
			"refreshToken": "rt_new",
			"expiresIn":    1800,
		})
	}))
	t.Cleanup(srv.Close)

	c := &Credentials{
		AuthMethod:   AuthMethodBuilderID,
		RefreshToken: "rt_old",
		ClientID:     "cid",
		ClientSecret: "csec",
		Region:       "us-east-1",
	}
	r := NewRefresher(srv.Client())
	r.BuilderIDTokenURLOverride = srv.URL + "/token"

	out, err := r.Refresh(context.Background(), c)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if out.AccessToken != "at_new" || out.RefreshToken != "rt_new" {
		t.Errorf("missing fields: %+v", out)
	}
}

func TestRefreshUnknownMethod(t *testing.T) {
	r := NewRefresher(http.DefaultClient)
	_, err := r.Refresh(context.Background(), &Credentials{AuthMethod: "weird"})
	if err == nil || !strings.Contains(err.Error(), "auth_method") {
		t.Fatalf("expected auth_method error, got %v", err)
	}
}
