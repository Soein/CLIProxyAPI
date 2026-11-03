package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestMirroredCredentialModifyRejectsChangedSource(t *testing.T) {
	for _, backend := range []string{"git", "object"} {
		for _, provider := range []string{"claude", "codex"} {
			for _, typed := range []bool{false, true} {
				for _, mutation := range []string{"replacement", "removed", "empty"} {
					name := backend + "/" + provider
					if typed {
						name += "/typed"
					}
					if mutation != "replacement" {
						name += "/" + mutation
					}
					t.Run(name, func(t *testing.T) {
						store, path, remotePayload := newMirroredCredentialStore(t, backend)
						manager := cliproxyauth.NewManager(store, nil, nil)
						base, errRegister := manager.Register(context.Background(), mirroredCredential(provider, typed))
						if errRegister != nil {
							t.Fatal(errRegister)
						}
						initialRemote := append([]byte(nil), remotePayload()...)
						external := []byte(`{"type":"` + provider + `","access_token":"rotated","refresh_token":"rotated"}`)
						if mutation == "empty" {
							external = nil
						}
						_, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(current *cliproxyauth.Auth) error {
							current.Metadata["note"] = "operator"
							if mutation == "removed" {
								return os.Remove(path)
							}
							return os.WriteFile(path, external, 0o600)
						})
						if committed || !errors.Is(errModify, cliproxyauth.ErrAuthStoreConflict) {
							t.Errorf("Modify() committed=%v err=%v, want source conflict", committed, errModify)
						}
						actual, errRead := os.ReadFile(path)
						if mutation == "removed" {
							if !os.IsNotExist(errRead) {
								t.Errorf("deleted credential was recreated: %s, %v", actual, errRead)
							}
						} else if errRead != nil || !bytes.Equal(actual, external) {
							t.Errorf("rotated credential was overwritten: %s, %v", actual, errRead)
						}
						if actualRemote := remotePayload(); !bytes.Equal(actualRemote, initialRemote) {
							t.Errorf("rejected edit changed remote credential: %s", actualRemote)
						}
						if live, _ := manager.GetByID(base.ID); live != nil && live.Metadata["note"] != nil {
							t.Error("rejected edit was published")
						}
					})
				}
			}
		}
	}
}

func TestMirroredCredentialModifyTracksLoadedAndSavedSources(t *testing.T) {
	for _, backend := range []string{"git", "object"} {
		for _, loaded := range []bool{false, true} {
			for _, typed := range []bool{false, true} {
				name := backend
				if loaded {
					name += "/loaded"
				}
				if typed {
					name += "/typed"
				}
				t.Run(name, func(t *testing.T) {
					store, path, remotePayload := newMirroredCredentialStore(t, backend)
					manager := cliproxyauth.NewManager(store, nil, nil)
					initial := mirroredCredential("claude", typed)
					if loaded {
						if _, errSave := store.Save(context.Background(), initial); errSave != nil {
							t.Fatal(errSave)
						}
						if errLoad := manager.Load(context.Background()); errLoad != nil {
							t.Fatal(errLoad)
						}
					} else if _, errRegister := manager.Register(context.Background(), initial); errRegister != nil {
						t.Fatal(errRegister)
					}
					for _, note := range []string{"first", "second"} {
						live, _ := manager.GetByID(initial.ID)
						_, committed, errModify := manager.Modify(context.Background(), initial.ID, live.RegistrationEpoch, func(current *cliproxyauth.Auth) error {
							current.Metadata["note"] = note
							return nil
						})
						if errModify != nil || !committed {
							t.Fatalf("Modify(%s) committed=%v err=%v", note, committed, errModify)
						}
						payload, errRead := os.ReadFile(path)
						if errRead != nil {
							t.Fatal(errRead)
						}
						var metadata map[string]any
						if errDecode := json.Unmarshal(payload, &metadata); errDecode != nil {
							t.Fatal(errDecode)
						}
						if metadata["note"] != note || metadata["refresh_token"] != "initial" {
							t.Fatalf("edit lost credential or metadata: %s", payload)
						}
						if !bytes.Equal(remotePayload(), payload) {
							t.Fatal("remote credential differs from saved payload")
						}
					}
				})
			}
		}
	}
}

func TestGitCredentialModifyRejectsSourceChangedByPull(t *testing.T) {
	store, path, remotePayload := newMirroredCredentialStore(t, "git")
	manager := cliproxyauth.NewManager(store, nil, nil)
	base, errRegister := manager.Register(context.Background(), mirroredCredential("claude", false))
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	gitStore := store.(*GitTokenStore)
	other := NewGitTokenStore(gitStore.remote, "", "", "")
	other.SetBaseDir(filepath.Join(t.TempDir(), "work", "auths"))
	rotated := mirroredCredential("claude", false)
	rotated.Metadata["access_token"] = "remote-rotation"
	rotated.Metadata["refresh_token"] = "remote-rotation"
	if _, errSave := other.Save(context.Background(), rotated); errSave != nil {
		t.Fatal(errSave)
	}
	wantRemote := append([]byte(nil), remotePayload()...)
	_, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(current *cliproxyauth.Auth) error {
		current.Metadata["note"] = "operator"
		return nil
	})
	if committed || !errors.Is(errModify, cliproxyauth.ErrAuthStoreConflict) {
		t.Errorf("Modify() committed=%v err=%v, want source conflict after pull", committed, errModify)
	}
	if actual, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(actual, wantRemote) {
		t.Errorf("pulled credential was overwritten: %s, %v", actual, errRead)
	}
	if !bytes.Equal(remotePayload(), wantRemote) {
		t.Error("rejected edit overwrote remote rotation")
	}
}

func TestGitCredentialConflictReloadPreservesLocalAndRemoteChanges(t *testing.T) {
	for _, sameCredential := range []bool{false, true} {
		name := "remote-added-credential"
		if sameCredential {
			name = "remote-rotated-credential"
		}
		t.Run(name, func(t *testing.T) {
			store, path, remotePayload := newMirroredCredentialStore(t, "git")
			manager := cliproxyauth.NewManager(store, nil, nil)
			base, errRegister := manager.Register(context.Background(), mirroredCredential("claude", false))
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			gitStore := store.(*GitTokenStore)
			other := NewGitTokenStore(gitStore.remote, "", "", "")
			other.SetBaseDir(filepath.Join(t.TempDir(), "work", "auths"))
			remote := mirroredCredential("claude", false)
			remote.Metadata["access_token"] = "remote-rotation"
			remote.Metadata["refresh_token"] = "remote-rotation"
			if !sameCredential {
				remote.ID, remote.FileName = "remote-only.json", "remote-only.json"
			}
			if _, errSave := other.Save(context.Background(), remote); errSave != nil {
				t.Fatal(errSave)
			}
			wantRemote := append([]byte(nil), remotePayload()...)
			wantRemoteChanged := readMirroredGitCredential(t, gitStore.remote, remote.FileName)
			local := []byte(`{"type":"claude","access_token":"local-rotation","refresh_token":"local-rotation"}`)
			_, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(current *cliproxyauth.Auth) error {
				current.Metadata["note"] = "rejected"
				return os.WriteFile(path, local, 0o600)
			})
			if committed || !errors.Is(errModify, cliproxyauth.ErrAuthStoreConflict) {
				t.Errorf("Modify() committed=%v err=%v, want source conflict", committed, errModify)
			}
			if actual, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(actual, local) {
				t.Errorf("conflict reload reverted local rotation: %s, %v", actual, errRead)
			}
			if !bytes.Equal(remotePayload(), wantRemote) {
				t.Error("conflict reload reverted remote credential")
			}
			if !bytes.Equal(readMirroredGitCredential(t, gitStore.remote, remote.FileName), wantRemoteChanged) {
				t.Error("conflict reload reverted remote change")
			}
			if live, _ := manager.GetByID(base.ID); live != nil && live.Metadata["note"] != nil {
				t.Error("rejected edit was published")
			}
		})
	}
}

func TestMirroredCredentialModifyRejectsOpaqueStorageBeforeMetadataInjection(t *testing.T) {
	for _, backend := range []string{"git", "object"} {
		for _, rotated := range []bool{false, true} {
			name := backend + "/unchanged"
			if rotated {
				name = backend + "/rotated"
			}
			t.Run(name, func(t *testing.T) {
				store, path, remotePayload := newMirroredCredentialStore(t, backend)
				storage := &opaqueMirroredCredentialStorage{}
				initial := mirroredCredential("claude", false)
				initial.Storage = storage
				manager := cliproxyauth.NewManager(store, nil, nil)
				base, errRegister := manager.Register(context.Background(), initial)
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				before, errRead := os.ReadFile(path)
				if errRead != nil {
					t.Fatal(errRead)
				}
				wantFile := before
				if rotated {
					wantFile = []byte(`{"type":"claude","access_token":"rotated","refresh_token":"rotated"}`)
				}
				_, committed, errModify := manager.Modify(context.Background(), base.ID, base.RegistrationEpoch, func(current *cliproxyauth.Auth) error {
					current.Metadata["note"] = "rejected"
					if rotated {
						return os.WriteFile(path, wantFile, 0o600)
					}
					return nil
				})
				if committed || errModify == nil || errors.Is(errModify, cliproxyauth.ErrAuthStoreConflict) != rotated {
					t.Fatalf("Modify() committed=%v err=%v, want source-conflict=%v", committed, errModify, rotated)
				}
				if errors.Is(errModify, cliproxyauth.ErrAuthStorageEditUnsupported) == rotated {
					t.Fatalf("Modify() err=%v, want unsupported-storage=%v", errModify, !rotated)
				}
				if storage.metadata["note"] != nil {
					t.Fatal("rejected edit mutated shared token storage")
				}
				if actual, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(actual, wantFile) || !bytes.Equal(remotePayload(), before) {
					t.Fatalf("rejected storage edit changed credential: %s, %v", actual, errRead)
				}
				if live, _ := manager.GetByID(base.ID); !rotated && (live == nil || live.Disabled) {
					t.Error("unsupported private storage edit changed live credential availability")
				}
			})
		}
	}
}

type opaqueMirroredCredentialStorage struct {
	metadata map[string]any
}

func (s *opaqueMirroredCredentialStorage) SetMetadata(metadata map[string]any) {
	s.metadata = metadata
}

func (s *opaqueMirroredCredentialStorage) MarshalTokenJSON() ([]byte, error) {
	return json.Marshal(s.metadata)
}

func (s *opaqueMirroredCredentialStorage) SaveTokenToFile(path string) error {
	payload, errMarshal := s.MarshalTokenJSON()
	if errMarshal != nil {
		return errMarshal
	}
	return os.WriteFile(path, payload, 0o600)
}

func mirroredCredential(provider string, typed bool) *cliproxyauth.Auth {
	auth := &cliproxyauth.Auth{
		ID: "credential.json", FileName: "credential.json", Provider: provider, Status: cliproxyauth.StatusActive,
		Metadata: map[string]any{"type": provider, "access_token": "initial", "refresh_token": "initial"},
	}
	if typed {
		if provider == "claude" {
			auth.Storage = &claude.ClaudeTokenStorage{AccessToken: "initial", RefreshToken: "initial"}
		} else {
			auth.Storage = &codex.CodexTokenStorage{AccessToken: "initial", RefreshToken: "initial"}
		}
	}
	return auth
}

func newMirroredCredentialStore(t *testing.T, backend string) (cliproxyauth.Store, string, func() []byte) {
	t.Helper()
	root := t.TempDir()
	if backend == "object" {
		var remote []byte
		dir := filepath.Join(root, "auths")
		store := &ObjectTokenStore{authDir: dir, putObjectFn: func(_ context.Context, _ string, payload []byte, _ string) error {
			remote = append([]byte(nil), payload...)
			return nil
		}}
		return store, filepath.Join(dir, "credential.json"), func() []byte { return remote }
	}
	remoteDir := filepath.Join(root, "remote.git")
	if _, errInit := git.PlainInit(remoteDir, true); errInit != nil {
		t.Fatal(errInit)
	}
	store := NewGitTokenStore(remoteDir, "", "", "")
	dir := filepath.Join(root, "work", "auths")
	store.SetBaseDir(dir)
	return store, filepath.Join(dir, "credential.json"), func() []byte {
		return readMirroredGitCredential(t, remoteDir, "credential.json")
	}
}

func readMirroredGitCredential(t *testing.T, remoteDir, name string) []byte {
	t.Helper()
	repo, errOpen := git.PlainOpen(remoteDir)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	defer func() {
		if errClose := repo.Close(); errClose != nil {
			t.Error(errClose)
		}
	}()
	head, errHead := repo.Head()
	if errHead != nil {
		t.Fatal(errHead)
	}
	commit, errCommit := repo.CommitObject(head.Hash())
	if errCommit != nil {
		t.Fatal(errCommit)
	}
	file, errFile := commit.File("auths/" + name)
	if errFile != nil {
		t.Fatal(errFile)
	}
	contents, errContents := file.Contents()
	if errContents != nil {
		t.Fatal(errContents)
	}
	return []byte(contents)
}
