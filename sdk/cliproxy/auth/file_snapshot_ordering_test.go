package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type snapshotOrderingStore struct{ path string }

func (s *snapshotOrderingStore) List(context.Context) ([]*Auth, error) { return nil, nil }
func (s *snapshotOrderingStore) Delete(context.Context, string) error  { return os.Remove(s.path) }
func (s *snapshotOrderingStore) Save(_ context.Context, auth *Auth) (string, error) {
	data, errMarshal := json.Marshal(auth.Metadata)
	if errMarshal != nil {
		return "", errMarshal
	}
	return s.path, os.WriteFile(s.path, data, 0o600)
}

func TestFileSnapshotValidationFencedByDurableUpdate(t *testing.T) {
	for _, mutation := range []string{"none", "persist", "reregister", "results"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "credential.json")
			manager := NewManager(&snapshotOrderingStore{path: path}, nil, nil)
			base, errRegister := manager.Register(ctx, &Auth{
				ID: "credential.json", Provider: "audit", Status: StatusActive,
				Metadata: map[string]any{"access_token": "old-token"},
			})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			data, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			snapshot := &Auth{
				ID: base.ID, Provider: base.Provider, Status: StatusActive,
				Label: "file-observation", Metadata: map[string]any{"access_token": "old-token"},
			}
			snapshot.BindFileSnapshot(path, data)

			readStarted := make(chan struct{})
			releaseRead := make(chan struct{})
			updateDone := make(chan struct{})
			var errSnapshot error
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseRead) }) }
			originalRead := readAuthSnapshotFileFunc
			readAuthSnapshotFileFunc = func(path string) ([]byte, error) {
				data, errRead := originalRead(path)
				close(readStarted)
				<-releaseRead
				return data, errRead
			}
			t.Cleanup(func() {
				release()
				<-updateDone
				readAuthSnapshotFileFunc = originalRead
			})
			go func() {
				defer close(updateDone)
				_, errSnapshot = manager.Update(WithSkipPersist(ctx), snapshot)
			}()
			<-readStarted

			switch mutation {
			case "persist", "reregister":
				newer := base.Clone()
				newer.Metadata["access_token"] = "new-token"
				var errUpdate error
				if mutation == "persist" {
					_, errUpdate = manager.Update(ctx, newer)
				} else {
					_, errUpdate = manager.Register(ctx, newer)
				}
				if errUpdate != nil {
					t.Fatal(errUpdate)
				}
			case "results":
				for range 100 {
					manager.MarkResult(ctx, Result{AuthID: base.ID, Provider: base.Provider, Model: "model", Success: true})
				}
			}
			before, _ := manager.GetByID(base.ID)
			release()
			<-updateDone
			if errSnapshot != nil {
				t.Fatal(errSnapshot)
			}
			current, _ := manager.GetByID(base.ID)
			if mutation == "persist" || mutation == "reregister" {
				if current.Metadata["access_token"] != "new-token" || current.Generation != before.Generation || !snapshot.HasFileSnapshot() {
					t.Fatalf("snapshot overwrote intervening %s: token=%v generation=%d, want new-token and generation %d with binding retained", mutation, current.Metadata["access_token"], current.Generation, before.Generation)
				}
			} else if current.Label != "file-observation" || snapshot.HasFileSnapshot() || current.Success != before.Success {
				t.Fatalf("current file observation did not apply with runtime results preserved: label=%q bound=%v successes=%d, want %d", current.Label, snapshot.HasFileSnapshot(), current.Success, before.Success)
			}
		})
	}
}
