package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func postgresCodexSnapshot() *cliproxyauth.Auth {
	auth := &cliproxyauth.Auth{
		ID: "codex.json", FileName: "codex.json", Provider: "codex",
		Metadata: map[string]any{
			"type": "codex", "access_token": "current-access", "refresh_token": "current-refresh",
			"last_refresh": "2026-10-16T00:00:00Z", "disabled": false, "note": "database-note",
			"siwc": map[string]any{"access_token": "siwc-access", "refresh_token": "siwc-refresh"},
		},
	}
	auth.SetStoreGeneration(3)
	return auth
}

func TestPostgresStoreCodexPreservesAuthoritativeSnapshot(t *testing.T) {
	for _, operation := range []string{"save", "batch", "restore"} {
		for _, intent := range []string{"refresh", "stale-storage", "login"} {
			t.Run(operation+"/"+intent, func(t *testing.T) {
				state := &postgresBatchDriverState{upsertRows: 1, upsertVersion: 4}
				store := newPostgresBatchTestStore(t, state)
				path := filepath.Join(store.authDir, "codex.json")
				if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"untrusted-mirror","refresh_token":"untrusted-refresh","note":"mirror-note"}`), 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				auth := postgresCodexSnapshot()
				ctx := context.Background()
				wantAccess := "current-access"
				switch intent {
				case "refresh":
					before := auth.Clone()
					auth.Metadata["access_token"] = "refreshed-access"
					auth.Metadata["refresh_token"] = "refreshed-refresh"
					auth.CodexRefreshPatch = codex.NewCredentialRefreshPatch(before.Metadata, auth.Metadata, true, false)
					auth.Storage = &codex.CodexTokenStorage{AccessToken: "stale-storage"}
					wantAccess = "refreshed-access"
				case "stale-storage":
					auth.Storage = &codex.CodexTokenStorage{AccessToken: "stale-storage", RefreshToken: "stale-refresh"}
				case "login":
					auth.Storage = &codex.CodexTokenStorage{AccessToken: "login-access", RefreshToken: "login-refresh"}
					ctx = cliproxyauth.WithAuthCreationIntent(ctx)
					wantAccess = "login-access"
				}
				var errSave error
				switch operation {
				case "save":
					_, _, errSave = store.SaveVersioned(ctx, auth, 3)
				case "restore":
					_, _, errSave = store.Restore(ctx, auth, 3)
				case "batch":
					errSave = store.SaveBatch(ctx, []*cliproxyauth.Auth{auth}, func(commit func() error) error {
						beforeCommit, errRead := os.ReadFile(path)
						if errRead != nil || string(beforeCommit) != `{"type":"codex","access_token":"untrusted-mirror","refresh_token":"untrusted-refresh","note":"mirror-note"}` {
							t.Fatalf("mirror changed before commit: %s, %v", beforeCommit, errRead)
						}
						if errCommit := commit(); errCommit != nil {
							return errCommit
						}
						if auth.Metadata["access_token"] != wantAccess {
							t.Fatalf("batch finalizer received stale metadata: %#v", auth.Metadata)
						}
						return nil
					})
				}
				if errSave != nil {
					t.Fatal(errSave)
				}
				state.mu.Lock()
				payloads := append([]string(nil), state.upsertPayloads...)
				state.mu.Unlock()
				if len(payloads) != 1 {
					t.Fatalf("database payload count = %d, want 1", len(payloads))
				}
				var persisted map[string]any
				if errJSON := json.Unmarshal([]byte(payloads[0]), &persisted); errJSON != nil {
					t.Fatal(errJSON)
				}
				if persisted["access_token"] != wantAccess || persisted["note"] != "database-note" || persisted["siwc"].(map[string]any)["refresh_token"] != "siwc-refresh" {
					t.Fatalf("database snapshot lost credentials/configuration: %#v", persisted)
				}
				if auth.Metadata["access_token"] != wantAccess || auth.CodexRefreshPatch != nil || auth.StoreGeneration() != 4 {
					t.Fatalf("committed auth was not synchronized: %#v", auth)
				}
				if _, leaked := auth.Metadata[postgresAuthGenerationPayloadKey]; leaked {
					t.Fatal("mirror generation leaked into auth metadata")
				}
			})
		}
	}
}

func TestPostgresStoreCodexConflictPreservesRefreshAndMirror(t *testing.T) {
	state := &postgresBatchDriverState{upsertRows: 1, upsertConflictID: "codex.json"}
	store := newPostgresBatchTestStore(t, state)
	auth := postgresCodexSnapshot()
	before := auth.Clone()
	auth.Metadata["access_token"] = "refreshed-access"
	patch := codex.NewCredentialRefreshPatch(before.Metadata, auth.Metadata, true, false)
	auth.CodexRefreshPatch = patch
	snapshot := auth.Clone()
	path := filepath.Join(store.authDir, "codex.json")
	original := []byte(`{"type":"codex","access_token":"original-mirror"}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, _, errSave := store.SaveVersioned(context.Background(), auth, 3); !errors.Is(errSave, cliproxyauth.ErrAuthStoreConflict) {
		t.Fatalf("SaveVersioned error = %v, want version conflict", errSave)
	}
	if auth.CodexRefreshPatch != patch || !reflect.DeepEqual(auth.Metadata, snapshot.Metadata) || auth.StoreGeneration() != 3 {
		t.Fatal("failed CAS consumed or changed the refresh result")
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || string(got) != string(original) {
		t.Fatalf("failed CAS changed mirror: %s, %v", got, errRead)
	}
}
