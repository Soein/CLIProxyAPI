package auth_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	baseauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth"
	claudeauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/claude"
	codexauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/codex"
	kimiauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kimi"
	vertexauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/vertex"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/xai"
)

func TestBuiltInTokenJSONMarshalersMatchFileOutput(t *testing.T) {
	tests := []struct {
		name    string
		storage func() baseauth.TokenStorage
	}{
		{
			name: "codex",
			storage: func() baseauth.TokenStorage {
				return &codexauth.CodexTokenStorage{
					AccessToken: "codex-access",
					AccountID:   "codex-account",
					Metadata:    map[string]any{"custom": "codex"},
				}
			},
		},
		{
			name: "claude",
			storage: func() baseauth.TokenStorage {
				return &claudeauth.ClaudeTokenStorage{
					AccessToken: "claude-access",
					AccountUUID: "claude-account",
					Metadata:    map[string]any{"custom": "claude"},
				}
			},
		},
		{
			name: "kimi",
			storage: func() baseauth.TokenStorage {
				return &kimiauth.KimiTokenStorage{
					AccessToken: "kimi-access",
					DeviceID:    "kimi-device",
					Metadata:    map[string]any{"custom": "kimi"},
				}
			},
		},
		{
			name: "xai",
			storage: func() baseauth.TokenStorage {
				return &xaiauth.TokenStorage{
					AccessToken: "xai-access",
					Subject:     "xai-subject",
					Metadata:    map[string]any{"custom": "xai"},
				}
			},
		},
		{
			name: "vertex",
			storage: func() baseauth.TokenStorage {
				return &vertexauth.VertexCredentialStorage{
					ServiceAccount: map[string]any{"private_key_id": "vertex-key"},
					ProjectID:      "vertex-project",
					Metadata:       map[string]any{"custom": "vertex"},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileStorage := tt.storage()
			path := filepath.Join(t.TempDir(), "auth.json")
			if errSave := fileStorage.SaveTokenToFile(path); errSave != nil {
				t.Fatalf("SaveTokenToFile() error = %v", errSave)
			}
			filePayload, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatalf("read saved token: %v", errRead)
			}

			payloadStorage := tt.storage()
			marshaler, ok := payloadStorage.(baseauth.TokenJSONMarshaler)
			if !ok {
				t.Fatalf("%T does not implement TokenJSONMarshaler", payloadStorage)
			}
			payload, errMarshal := marshaler.MarshalTokenJSON()
			if errMarshal != nil {
				t.Fatalf("MarshalTokenJSON() error = %v", errMarshal)
			}
			if !bytes.Equal(payload, filePayload) {
				t.Fatalf("MarshalTokenJSON() = %q, want exact file output %q", payload, filePayload)
			}
		})
	}
}
