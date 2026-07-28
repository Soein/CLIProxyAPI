package cliproxy

import (
	"context"
	"errors"
	"testing"
)

type recordingClusterConfigSynchronizer struct {
	calls int
	err   error
}

func (s *recordingClusterConfigSynchronizer) SyncConfigAuthoritative(context.Context) error {
	s.calls++
	return s.err
}

func TestSyncClusterConfigRefreshesWatcherAfterAuthoritativeMirror(t *testing.T) {
	synchronizer := &recordingClusterConfigSynchronizer{}
	reloads := 0
	service := &Service{watcher: &WatcherWrapper{
		reloadConfigIfChanged: func() { reloads++ },
	}}

	if errSync := service.syncClusterConfig(context.Background(), synchronizer); errSync != nil {
		t.Fatalf("syncClusterConfig() error: %v", errSync)
	}
	if synchronizer.calls != 1 {
		t.Fatalf("synchronizer calls = %d, want 1", synchronizer.calls)
	}
	if reloads != 1 {
		t.Fatalf("watcher reloads = %d, want 1", reloads)
	}
}

func TestSyncClusterConfigPropagatesMirrorFailure(t *testing.T) {
	wantErr := errors.New("database unavailable")
	synchronizer := &recordingClusterConfigSynchronizer{err: wantErr}
	reloads := 0
	service := &Service{watcher: &WatcherWrapper{
		reloadConfigIfChanged: func() { reloads++ },
	}}

	errSync := service.syncClusterConfig(context.Background(), synchronizer)
	if !errors.Is(errSync, wantErr) {
		t.Fatalf("syncClusterConfig() error = %v, want %v", errSync, wantErr)
	}
	if reloads != 0 {
		t.Fatalf("watcher reloads = %d, want 0 after mirror failure", reloads)
	}
}

func TestSyncClusterConfigIgnoresUnsupportedStore(t *testing.T) {
	service := &Service{}
	if errSync := service.syncClusterConfig(context.Background(), struct{}{}); errSync != nil {
		t.Fatalf("syncClusterConfig() error: %v", errSync)
	}
}
