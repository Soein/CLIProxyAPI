package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestPostgresModifyFailedSerializationKeepsTokenStoragePrivate(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			store := newPostgresBatchTestStore(t, &postgresBatchDriverState{upsertRows: 1})
			manager := cliproxyauth.NewManager(store, nil, nil)
			initial := mirroredCredential(provider, true)
			base, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), initial)
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			_, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(current *cliproxyauth.Auth) error {
				current.Metadata["note"] = "failed edit"
				current.Metadata["unencodable"] = make(chan int)
				return nil
			})
			if committed || errModify == nil {
				t.Fatalf("Modify() committed=%v err=%v, want serialization error", committed, errModify)
			}
			live, _ := manager.GetByID(base.ID)
			if live == nil || live.Metadata["note"] != nil {
				t.Fatal("failed edit changed live auth metadata")
			}
			var storageMetadata map[string]any
			switch storage := live.Storage.(type) {
			case *claude.ClaudeTokenStorage:
				storageMetadata = storage.Metadata
			case *codex.CodexTokenStorage:
				storageMetadata = storage.Metadata
			}
			if storageMetadata["note"] != nil || storageMetadata["unencodable"] != nil {
				t.Fatal("failed edit mutated shared token storage")
			}
		})
	}
}

func TestPostgresModifyAllowsPrivateLegacyTokenWriter(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "writer-failure"
		}
		t.Run(name, func(t *testing.T) {
			store := newPostgresBatchTestStore(t, &postgresBatchDriverState{upsertRows: 1, upsertVersion: 4})
			manager := cliproxyauth.NewManager(store, nil, nil)
			writer := &privateLegacyTokenWriter{fail: fail}
			initial := mirroredCredential("legacy", false)
			initial.Storage = writer
			base, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), initial)
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			saved, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(current *cliproxyauth.Auth) error {
				current.Metadata["note"] = "operator"
				return nil
			})
			if fail {
				if committed || errModify == nil {
					t.Fatalf("Modify() committed=%v err=%v, want writer failure", committed, errModify)
				}
			} else if errModify != nil || !committed || saved.StoreGeneration() != 4 || saved.Metadata["note"] != "operator" {
				t.Fatalf("Modify() committed=%v saved=%+v err=%v", committed, saved, errModify)
			}
			if writer.metadata["note"] != nil {
				t.Fatal("staging mutated the previous token writer")
			}
		})
	}
}

type privateLegacyTokenWriter struct {
	metadata map[string]any
	fail     bool
}

func (s *privateLegacyTokenWriter) CloneTokenStorage() cliproxyauth.TokenStorage {
	cloned := &privateLegacyTokenWriter{fail: s.fail, metadata: make(map[string]any, len(s.metadata))}
	for key, value := range s.metadata {
		cloned.metadata[key] = value
	}
	return cloned
}

func (s *privateLegacyTokenWriter) SetMetadata(metadata map[string]any) {
	s.metadata = metadata
}

func (s *privateLegacyTokenWriter) SaveTokenToFile(path string) error {
	if s.fail {
		return errors.New("token writer failed")
	}
	payload, errMarshal := json.Marshal(s.metadata)
	if errMarshal != nil {
		return errMarshal
	}
	return os.WriteFile(path, payload, 0o600)
}
