package cliproxy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type serviceSnapshotFileStore struct {
	*sdkauth.FileTokenStore
	beforeSave func() error
}

func (s *serviceSnapshotFileStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	if s.beforeSave != nil {
		if errSave := s.beforeSave(); errSave != nil {
			return "", errSave
		}
	}
	return s.FileTokenStore.Save(ctx, auth)
}

func TestPendingFileSnapshotServiceCallbackSkipsModelRegistration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pending-service.json")
	data := []byte(`{"type":"claude","prefix":"old"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	store := &serviceSnapshotFileStore{FileTokenStore: sdkauth.NewFileTokenStore()}
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	service, errBuild := NewBuilder().WithConfig(&internalconfig.Config{AuthDir: dir}).WithConfigPath(filepath.Join(dir, "config.yaml")).WithCoreAuthManager(manager).Build()
	if errBuild != nil {
		t.Fatal(errBuild)
	}
	snapshot := &coreauth.Auth{
		ID: filepath.Base(path), FileName: filepath.Base(path), Provider: "claude", Prefix: "old", Status: coreauth.StatusActive,
		Attributes: map[string]string{coreauth.AttributePath: path},
		Metadata:   map[string]any{"type": "claude", "prefix": "old"},
	}
	snapshot.BindFileSnapshot(path, data)
	base, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), snapshot.Clone())
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(base.ID) })
	rereads := 0
	service.watcher = &WatcherWrapper{rereadDiscardedFileSnapshot: func(auth *coreauth.Auth) {
		rereads++
		if !auth.HasFileSnapshot() || auth.FileSnapshotPersistenceDone() == nil {
			t.Error("deferred snapshot lost its file binding or persistence completion notification")
		}
	}}
	store.beforeSave = func() error {
		ctx := coreauth.WithSkipPersist(context.Background())
		if prepared := service.prepareCoreAuthForModelRegistration(ctx, snapshot); prepared != nil {
			return errors.New("pending snapshot scheduled model registration while Save holds its mutation gate")
		}
		// Exercise the complete synchronous Store -> Service callback after the
		// preparation check, so a regression fails without leaving a deadlocked worker.
		service.handleAuthUpdates(ctx, []watcher.AuthUpdate{{Action: watcher.AuthUpdateActionModify, ID: base.ID, Auth: snapshot}})
		return nil
	}
	newer := base.Clone()
	newer.Prefix = "new"
	newer.Metadata["prefix"] = "new"
	if _, errUpdate := manager.Update(context.Background(), newer); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	current, _ := manager.GetByID(base.ID)
	if current.Prefix != "new" || rereads != 2 || service.authRegistrationWaitCh(base.ID) != nil {
		t.Fatalf("callback did not defer cleanly: prefix=%q rereads=%d registrationWait=%v", current.Prefix, rereads, service.authRegistrationWaitCh(base.ID))
	}
}
