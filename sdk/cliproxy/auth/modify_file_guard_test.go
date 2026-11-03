package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/meta"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/vertex"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestManagerModifyRejectsUnreplayedFileReplacement(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		for _, storage := range []bool{false, true} {
			for _, replacement := range []string{"rotation", "removed", "recreated"} {
				name := provider + "/metadata/" + replacement
				if storage {
					name = provider + "/storage/" + replacement
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					dir := t.TempDir()
					store := &afterSaveStore{FileTokenStore: sdkauth.NewFileTokenStore()}
					store.SetBaseDir(dir)
					manager := coreauth.NewManager(store, nil, nil)
					path := filepath.Join(dir, "credential.json")
					initial := &coreauth.Auth{ID: "credential.json", FileName: "credential.json", Provider: provider, Status: coreauth.StatusActive,
						Metadata: map[string]any{"type": provider, "access_token": "initial", "refresh_token": "initial"}}
					if storage {
						if provider == "claude" {
							initial.Storage = &claude.ClaudeTokenStorage{AccessToken: "initial", RefreshToken: "initial"}
						} else {
							initial.Storage = &codex.CodexTokenStorage{AccessToken: "initial", RefreshToken: "initial"}
						}
					}
					base, errRegister := manager.Register(ctx, initial)
					if errRegister != nil {
						t.Fatal(errRegister)
					}
					external := []byte(`{"type":"` + provider + `","access_token":"rotated","refresh_token":"rotated"}`)
					if replacement != "rotation" {
						if errRemove := os.Remove(path); errRemove != nil {
							t.Fatal(errRemove)
						}
					}
					if replacement != "removed" {
						if errWrite := os.WriteFile(path, external, 0o600); errWrite != nil {
							t.Fatal(errWrite)
						}
					}
					_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
						current.Metadata["note"] = "operator"
						return nil
					})
					if committed || !errors.Is(errModify, coreauth.ErrAuthStoreConflict) {
						t.Errorf("Modify() committed=%v err=%v, want a file conflict", committed, errModify)
					}
					live, _ := manager.GetByID(base.ID)
					if live != nil && live.Metadata["note"] != nil {
						t.Error("failed edit was published")
					}
					actual, errRead := os.ReadFile(path)
					if replacement == "removed" {
						if !os.IsNotExist(errRead) {
							t.Fatalf("removed file was recreated: %s, %v", actual, errRead)
						}
						return
					}
					if errRead != nil || !bytes.Equal(actual, external) {
						t.Fatalf("external credential was overwritten: %s, %v", actual, errRead)
					}
					live, _ = manager.GetByID(base.ID)
					if live != nil && live.Metadata["refresh_token"] == "rotated" {
						_, committed, errModify = manager.Modify(ctx, base.ID, live.RegistrationEpoch, func(current *coreauth.Auth) error {
							current.Metadata["note"] = "retry after reload"
							return nil
						})
						if errModify != nil || !committed || diskField(t, path, "refresh_token") != "rotated" {
							t.Fatalf("retry used stale credential storage: committed=%v err=%v disk token=%v", committed, errModify, diskField(t, path, "refresh_token"))
						}
					}
					var metadata map[string]any
					if errDecode := json.Unmarshal(external, &metadata); errDecode != nil {
						t.Fatal(errDecode)
					}
					queued := &coreauth.Auth{ID: base.ID, FileName: base.FileName, Provider: provider, Status: coreauth.StatusActive, Metadata: metadata,
						Attributes: map[string]string{coreauth.AttributePath: path}}
					queued.BindFileSnapshot(path, external)
					if _, errUpdate := manager.Update(coreauth.WithSkipPersist(ctx), queued); errUpdate != nil {
						t.Fatal(errUpdate)
					}
					live, _ = manager.GetByID(base.ID)
					if live.Metadata["refresh_token"] != "rotated" {
						t.Fatal("queued rotation was not applied")
					}
					_, committed, errModify = manager.Modify(ctx, base.ID, live.RegistrationEpoch, func(current *coreauth.Auth) error {
						current.Metadata["note"] = "operator"
						return nil
					})
					if errModify != nil || !committed {
						t.Fatalf("retry after replay failed: committed=%v err=%v", committed, errModify)
					}
					live, _ = manager.GetByID(base.ID)
					if diskField(t, path, "refresh_token") != "rotated" || live.Metadata["refresh_token"] != "rotated" || diskField(t, path, "note") != "operator" || live.Metadata["note"] != "operator" {
						t.Fatal("retry lost rotation or note")
					}
				})
			}
		}
	}
}

func TestManagerModifySaveFailureDoesNotPublish(t *testing.T) {
	manager, store, path := newTrafficManager(t)
	base, _ := manager.GetByID(trafficAuthID)
	store.failNext = errors.New("store unavailable")
	_, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
		current.Metadata["note"] = "failed edit"
		return nil
	})
	live, _ := manager.GetByID(base.ID)
	if committed || errModify == nil || live.Metadata["note"] != nil || diskField(t, path, "note") != nil {
		t.Fatalf("failed save published edit: committed=%v err=%v runtime=%v disk=%v", committed, errModify, live.Metadata["note"], diskField(t, path, "note"))
	}
}

func TestManagerModifyLoadedFileNeedsNoPriorSave(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "loaded.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"claude","access_token":"loaded","refresh_token":"loaded"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	loaded, errList := store.List(ctx)
	if errList != nil || len(loaded) != 1 || loaded[0].HasFileSnapshot() {
		t.Fatalf("List() must retain ordinary loaded-record semantics: records=%v err=%v", loaded, errList)
	}
	manager := coreauth.NewManager(store, nil, nil)
	if errLoad := manager.Load(ctx); errLoad != nil {
		t.Fatal(errLoad)
	}
	for _, note := range []string{"first", "second"} {
		current, _ := manager.GetByID("loaded.json")
		_, committed, errModify := manager.Modify(ctx, current.ID, current.RegistrationEpoch, func(candidate *coreauth.Auth) error {
			candidate.Metadata["note"] = note
			return nil
		})
		if errModify != nil || !committed || diskField(t, path, "note") != note || diskField(t, path, "refresh_token") != "loaded" {
			t.Fatalf("loaded-file edit failed: committed=%v err=%v", committed, errModify)
		}
	}
}

func TestManagerModifyMetaStoragePersistsConsecutiveEdits(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "meta.json", FileName: "meta.json", Provider: "meta", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "meta", "access_token": "LLM|initial", "dca_token": "dca:initial"},
		Storage:  &meta.MetaTokenStorage{AccessToken: "LLM|initial", DCAToken: "dca:initial"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	for _, note := range []string{"first", "second"} {
		_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
			candidate.Metadata["note"] = note
			return nil
		})
		if errModify != nil || !committed || diskField(t, filepath.Join(dir, base.ID), "note") != note {
			t.Fatalf("Meta edit failed: committed=%v err=%v", committed, errModify)
		}
	}
}

func TestManagerModifyReloadDoesNotRestoreVertexPrivateKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "vertex.json", FileName: "vertex.json", Provider: "vertex", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "vertex", "service_account": map[string]any{"private_key": "initial", "client_email": "same@example.com"}},
		Storage:  &vertex.VertexCredentialStorage{ServiceAccount: map[string]any{"private_key": "initial", "client_email": "same@example.com"}}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	path := filepath.Join(dir, base.ID)
	external := []byte(`{"type":"vertex","service_account":{"private_key":"rotated","client_email":"same@example.com"}}`)
	if errWrite := os.WriteFile(path, external, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	for attempt := 0; attempt < 2; attempt++ {
		_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
			candidate.Metadata["note"] = "operator"
			return nil
		})
		if attempt == 0 && (committed || !errors.Is(errModify, coreauth.ErrAuthStoreConflict)) {
			t.Fatalf("stale Vertex edit committed=%v err=%v", committed, errModify)
		}
		if attempt == 1 && (!committed || errModify != nil) {
			t.Fatalf("reloaded Vertex edit committed=%v err=%v", committed, errModify)
		}
		if diskField(t, path, "service_account").(map[string]any)["private_key"] != "rotated" {
			t.Fatal("old Vertex private key restored by token storage")
		}
	}
}

func TestManagerModifyEncodingFailureDoesNotMutatePublishedStorage(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "claude.json", FileName: "claude.json", Provider: "claude", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "claude", "access_token": "initial"}, Storage: &claude.ClaudeTokenStorage{AccessToken: "initial"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	path := filepath.Join(dir, base.ID)
	original, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
		candidate.Metadata["note"] = "failed edit"
		candidate.Metadata["unencodable"] = make(chan int)
		return nil
	})
	live, _ := manager.GetByID(base.ID)
	if committed || errModify == nil || live.Metadata["note"] != nil || live.Storage.(*claude.ClaudeTokenStorage).Metadata["note"] != nil {
		t.Fatalf("encoding failure mutated published auth or storage: committed=%v err=%v", committed, errModify)
	}
	actual, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(actual, original) {
		t.Fatalf("encoding failure changed disk: %s err=%v", actual, errRead)
	}
}

type clonableSDKStorage struct {
	metadata map[string]any
}

func (s *clonableSDKStorage) SetMetadata(metadata map[string]any) { s.metadata = metadata }

func (s *clonableSDKStorage) CloneTokenStorage() coreauth.TokenStorage {
	return &clonableSDKStorage{metadata: s.metadata}
}

func (s *clonableSDKStorage) MarshalTokenJSON() ([]byte, error) {
	return json.Marshal(s.metadata)
}

func (s *clonableSDKStorage) SaveTokenToFile(path string) error {
	data, errMarshal := s.MarshalTokenJSON()
	if errMarshal != nil {
		return errMarshal
	}
	return os.WriteFile(path, data, 0o600)
}

func TestManagerModifySupportsSDKClonableStorage(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "sdk.json", FileName: "sdk.json", Provider: "sdk", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "sdk", "access_token": "initial"}, Storage: &clonableSDKStorage{}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	for _, note := range []string{"first", "second"} {
		_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
			candidate.Metadata["note"] = note
			return nil
		})
		live, _ := manager.GetByID(base.ID)
		if errModify != nil || !committed || live.Storage.(*clonableSDKStorage).metadata["note"] != note || diskField(t, filepath.Join(dir, base.ID), "note") != note {
			t.Fatalf("SDK storage edit failed: committed=%v err=%v", committed, errModify)
		}
	}
}

type uncloneableSDKStorage struct{ metadata map[string]any }

func (s *uncloneableSDKStorage) SetMetadata(metadata map[string]any) { s.metadata = metadata }
func (s *uncloneableSDKStorage) MarshalTokenJSON() ([]byte, error)   { return json.Marshal(s.metadata) }
func (s *uncloneableSDKStorage) SaveTokenToFile(path string) error {
	data, errMarshal := s.MarshalTokenJSON()
	if errMarshal != nil {
		return errMarshal
	}
	return os.WriteFile(path, data, 0o600)
}

func TestManagerModifyUnsupportedStorageDoesNotReloadUnchangedCredential(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "sdk.json", FileName: "sdk.json", Provider: "sdk", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "sdk", "access_token": "initial"}, Storage: &uncloneableSDKStorage{}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	for range 2 {
		_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
			candidate.Metadata["note"] = "unsupported edit"
			return nil
		})
		live, _ := manager.GetByID(base.ID)
		if committed || !errors.Is(errModify, coreauth.ErrAuthStorageEditUnsupported) || errors.Is(errModify, coreauth.ErrAuthStoreConflict) || live.Generation != base.Generation || live.Disabled || live.Metadata["note"] != nil || live.Storage.(*uncloneableSDKStorage).metadata["note"] != nil {
			t.Fatalf("unsupported edit was treated as a changed credential: committed=%v err=%v generation=%v", committed, errModify, live.Generation)
		}
	}
}

func TestManagerModifyAfterSetDisabledUsesSavedSource(t *testing.T) {
	manager, _, path := newTrafficManager(t)
	for _, disabled := range []bool{true, false} {
		if _, errDisable := manager.SetDisabled(context.Background(), []string{trafficAuthID}, disabled); errDisable != nil {
			t.Fatal(errDisable)
		}
		base, _ := manager.GetByID(trafficAuthID)
		_, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
			candidate.Metadata["note"] = "operator"
			return nil
		})
		if !committed || errModify != nil || diskField(t, path, "note") != "operator" || diskField(t, path, "disabled") != disabled {
			t.Fatalf("edit immediately after SetDisabled(%v) failed: committed=%v err=%v", disabled, committed, errModify)
		}
	}
}

func TestManagerModifyMetaStorageKeepsAtomicPublication(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "meta.json", FileName: "meta.json", Provider: "meta", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "meta", "access_token": "LLM|initial"}, Storage: &meta.MetaTokenStorage{AccessToken: "LLM|initial"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	path := filepath.Join(dir, base.ID)
	before, errStat := os.Stat(path)
	if errStat != nil {
		t.Fatal(errStat)
	}
	_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
		candidate.Metadata["note"] = "operator"
		return nil
	})
	after, errStat := os.Stat(path)
	if errStat != nil || errModify != nil || !committed || os.SameFile(before, after) {
		t.Fatalf("Meta edit did not retain atomic file replacement: committed=%v err=%v stat=%v", committed, errModify, errStat)
	}
}

func TestManagerModifyMetaRelativePathWithoutBaseDir(t *testing.T) {
	t.Chdir(t.TempDir())
	ctx := context.Background()
	store := sdkauth.NewFileTokenStore()
	manager := coreauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "meta.json", FileName: "meta.json", Provider: "meta", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "meta", "access_token": "LLM|initial"}, Storage: &meta.MetaTokenStorage{AccessToken: "LLM|initial"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	for _, note := range []string{"first", "second"} {
		_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(candidate *coreauth.Auth) error {
			candidate.Metadata["note"] = note
			return nil
		})
		if errModify != nil || !committed || diskField(t, "meta.json", "note") != note || diskField(t, "meta.json", "access_token") != "LLM|initial" {
			t.Fatalf("relative Meta edit failed: committed=%v err=%v", committed, errModify)
		}
	}
}
