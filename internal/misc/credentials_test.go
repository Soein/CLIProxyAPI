package misc

import (
	"testing"
)

func TestMergeMetadata(t *testing.T) {
	source := map[string]any{
		"type":         "codex",
		"access_token": "token-123",
	}
	metadata := map[string]any{
		"disabled":   false,
		"email":      "test@example.com",
		"prefix":     "custom-prefix",
		"websockets": false,
		"note":       "custom note",
	}

	result, err := MergeMetadata(source, metadata)
	if err != nil {
		t.Fatalf("MergeMetadata() error = %v", err)
	}

	if result["type"] != "codex" {
		t.Errorf("type = %v, want codex", result["type"])
	}
	if result["access_token"] != "token-123" {
		t.Errorf("access_token = %v, want token-123", result["access_token"])
	}
	if result["disabled"] != false {
		t.Errorf("disabled = %v, want false", result["disabled"])
	}
	if result["email"] != "test@example.com" {
		t.Errorf("email = %v, want test@example.com", result["email"])
	}
	if result["prefix"] != "custom-prefix" {
		t.Errorf("prefix = %v, want custom-prefix", result["prefix"])
	}
	if result["websockets"] != false {
		t.Errorf("websockets = %v, want false", result["websockets"])
	}
	if result["note"] != "custom note" {
		t.Errorf("note = %v, want custom note", result["note"])
	}
}

func TestMergeMetadataKeepsSourceFieldsAuthoritative(t *testing.T) {
	source := map[string]any{
		"type":         "codex",
		"access_token": "new-token",
		"account_id":   "new-account",
	}
	metadata := map[string]any{
		"access_token": "old-token",
		"account_id":   "old-account",
		"prefix":       "custom-prefix",
	}

	result, err := MergeMetadata(source, metadata)
	if err != nil {
		t.Fatalf("MergeMetadata() error = %v", err)
	}

	if got := result["access_token"]; got != "new-token" {
		t.Errorf("access_token = %v, want new-token", got)
	}
	if got := result["account_id"]; got != "new-account" {
		t.Errorf("account_id = %v, want new-account", got)
	}
	if got := result["prefix"]; got != "custom-prefix" {
		t.Errorf("prefix = %v, want custom-prefix", got)
	}
}

func TestMergeMetadataKeepsStructSourceFieldsAuthoritative(t *testing.T) {
	type credential struct {
		AccessToken string `json:"access_token"`
		AccountID   string `json:"account_id"`
	}

	result, err := MergeMetadata(credential{
		AccessToken: "new-token",
		AccountID:   "new-account",
	}, map[string]any{
		"access_token":     "old-token",
		"account_id":       "old-account",
		"custom_extension": "preserved",
	})
	if err != nil {
		t.Fatalf("MergeMetadata() error = %v", err)
	}

	if got := result["access_token"]; got != "new-token" {
		t.Errorf("access_token = %v, want new-token", got)
	}
	if got := result["account_id"]; got != "new-account" {
		t.Errorf("account_id = %v, want new-account", got)
	}
	if got := result["custom_extension"]; got != "preserved" {
		t.Errorf("custom_extension = %v, want preserved", got)
	}
}
