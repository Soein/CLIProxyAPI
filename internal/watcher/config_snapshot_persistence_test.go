package watcher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type configSnapshotPersisterTest struct {
	started     chan string
	release     chan struct{}
	legacyCalls atomic.Int32
}

func (p *configSnapshotPersisterTest) PersistConfig(context.Context) error {
	p.legacyCalls.Add(1)
	return nil
}

func (p *configSnapshotPersisterTest) PersistAuthFiles(context.Context, string, ...string) error {
	return nil
}

func (p *configSnapshotPersisterTest) PersistConfigSnapshot(_ context.Context, snapshot []byte) error {
	p.started <- string(snapshot)
	<-p.release
	return nil
}

func TestConfigSnapshotPersistenceCopiesAndSerializesAcceptedSnapshots(t *testing.T) {
	p := &configSnapshotPersisterTest{started: make(chan string, 3), release: make(chan struct{}, 3)}
	w := &Watcher{storePersister: p}
	persist := w.persistConfigAsync
	first := []byte("port: 8317\n")
	persist(first)
	copy(first, "port: 9999\n")
	select {
	case got := <-p.started:
		if got != "port: 8317\n" {
			t.Fatalf("first snapshot = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first snapshot was not persisted")
	}
	second := []byte("port: 8318\n")
	persist(second)
	copy(second, "port: 9999\n")
	select {
	case got := <-p.started:
		t.Fatalf("newer snapshot started before the older write completed: %q", got)
	default:
	}
	p.release <- struct{}{}
	select {
	case got := <-p.started:
		if got != "port: 8318\n" {
			t.Fatalf("second snapshot = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second snapshot was not persisted")
	}
	p.release <- struct{}{}
	if p.legacyCalls.Load() != 0 {
		t.Fatal("snapshot persistence fell back to re-reading the config path")
	}
}

func TestConfigSnapshotPersistenceSkipsLegacyOnlyPersister(t *testing.T) {
	p := &legacyConfigPersisterTest{}
	w := &Watcher{storePersister: p}
	w.persistConfigAsync([]byte("port: 8317\n"))
	if got := p.configCalls.Load(); got != 0 {
		t.Fatalf("legacy path persister called %d times", got)
	}
}

type legacyConfigPersisterTest struct {
	configCalls atomic.Int32
}

func (p *legacyConfigPersisterTest) PersistConfig(context.Context) error {
	p.configCalls.Add(1)
	return nil
}

func (*legacyConfigPersisterTest) PersistAuthFiles(context.Context, string, ...string) error {
	return nil
}
