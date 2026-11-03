package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestManagerModifyCommitsAcrossEquivalentFileReplay(t *testing.T) {
	for _, setup := range []string{"registration", "metadata_save", "first_enrichment"} {
		t.Run(setup, func(t *testing.T) {
			ctx := context.Background()
			manager, _, path := newTrafficManager(t)
			base, _ := manager.GetByID(trafficAuthID)
			switch setup {
			case "metadata_save":
				saved, preempted := manager.PersistMetadataEntry(ctx, base.ID, base.RegistrationEpoch, "runtime_note", "persisted note", nil)
				if !saved || preempted {
					t.Fatalf("PersistMetadataEntry() saved=%v preempted=%v", saved, preempted)
				}
			case "first_enrichment":
				bootstrap := base.Clone()
				bootstrap.Label = ""
				delete(bootstrap.Attributes, "auth_kind")
				if _, errRegister := manager.Register(ctx, bootstrap); errRegister != nil {
					t.Fatal(errRegister)
				}
			}
			base, _ = manager.GetByID(trafficAuthID)
			snapshot := readTrafficSnapshot(t, path, "old-model")
			calls := 0
			modified, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
				calls++
				current.Prefix = "pfx-edited"
				current.Metadata["prefix"] = "pfx-edited"
				_, errReplay := manager.Update(coreauth.WithSkipPersist(ctx), snapshot)
				return errReplay
			})
			if errModify != nil || !committed || modified == nil {
				t.Fatalf("Modify() committed=%v auth present=%v error=%v, want the explicit edit committed", committed, modified != nil, errModify)
			}
			if calls != 1 {
				t.Fatalf("edit callback calls=%d, want 1", calls)
			}
			current, ok := manager.GetByID(base.ID)
			if !ok || current == nil {
				t.Fatal("explicit edit removed the runtime auth")
			}
			if current.Prefix != "pfx-edited" || current.Metadata["prefix"] != "pfx-edited" || modified.Prefix != "pfx-edited" {
				t.Fatalf("explicit edit was not published: runtime prefix=%q metadata prefix=%v returned prefix=%q", current.Prefix, current.Metadata["prefix"], modified.Prefix)
			}
			if got := diskField(t, path, "prefix"); got != "pfx-edited" {
				t.Fatalf("persisted prefix=%v, want pfx-edited", got)
			}
			if setup == "metadata_save" && (current.Metadata["runtime_note"] != "persisted note" || diskField(t, path, "runtime_note") != "persisted note") {
				t.Fatal("explicit edit lost the saved metadata")
			}
			if setup == "first_enrichment" && (current.Label != "a@b.c" || current.Attributes["auth_kind"] != "oauth") {
				t.Fatal("explicit edit lost the watcher's label or auth kind enrichment")
			}
		})
	}
}

func TestManagerModifyPreservesConcurrentFileChanges(t *testing.T) {
	for _, change := range []string{"prefix", "access_token"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			manager, _, path := newTrafficManager(t)
			base, _ := manager.GetByID(trafficAuthID)
			calls := 0
			_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
				calls++
				current.Prefix = "pfx-edited"
				current.Metadata["prefix"] = "pfx-edited"
				current.Metadata["note"] = "explicit edit"
				metadata := map[string]any{"type": "claude", "email": "a@b.c", "prefix": "pfx-1"}
				metadata[change] = "external-change"
				data, errMarshal := json.Marshal(metadata)
				if errMarshal != nil {
					return errMarshal
				}
				if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
					return errWrite
				}
				snapshot := readTrafficSnapshot(t, path, "old-model")
				_, errReplay := manager.Update(coreauth.WithSkipPersist(ctx), snapshot)
				return errReplay
			})
			if calls != 1 {
				t.Fatalf("edit callback calls=%d, want 1", calls)
			}
			if !committed && errModify == nil {
				t.Fatal("Modify() silently discarded an edit to an auth that still exists; want an explicit conflict or a merged commit")
			}
			current, ok := manager.GetByID(base.ID)
			if !ok || current == nil {
				t.Fatal("Modify() removed the runtime auth after a concurrent file change")
			}
			if current.Metadata[change] != "external-change" || diskField(t, path, change) != "external-change" {
				t.Fatalf("Modify() overwrote concurrent %s: runtime matches=%v disk matches=%v error=%v", change, current.Metadata[change] == "external-change", diskField(t, path, change) == "external-change", errModify)
			}
			if change == "prefix" && current.Prefix != "external-change" {
				t.Fatalf("runtime prefix=%q, want external-change", current.Prefix)
			}
			if committed {
				if errModify != nil || current.Metadata["note"] != "explicit edit" || diskField(t, path, "note") != "explicit edit" {
					t.Fatalf("committed edit lost its non-conflicting note: runtime=%v disk=%v error=%v", current.Metadata["note"], diskField(t, path, "note"), errModify)
				}
				if change == "access_token" && (current.Prefix != "pfx-edited" || diskField(t, path, "prefix") != "pfx-edited") {
					t.Fatal("merged credential replacement lost the explicit prefix edit")
				}
			}
		})
	}
}

func TestManagerModifyRejectsConcurrentRegistrationReplacement(t *testing.T) {
	ctx := context.Background()
	manager, _, path := newTrafficManager(t)
	base, _ := manager.GetByID(trafficAuthID)
	calls := 0
	_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
		calls++
		current.Prefix = "pfx-stale"
		current.Metadata["prefix"] = "pfx-stale"
		if errWrite := os.WriteFile(path, []byte(`{"type":"claude","email":"a@b.c","prefix":"pfx-recreated"}`), 0o600); errWrite != nil {
			return errWrite
		}
		replacement := readTrafficSnapshot(t, path, "old-model")
		replacement.ClearFileSnapshot()
		_, errRegister := manager.Register(coreauth.WithSkipPersist(ctx), replacement)
		return errRegister
	})
	if committed || errModify == nil || calls != 1 {
		t.Fatalf("stale Modify() committed=%v error=%v callback calls=%d, want rejection without repeating edit", committed, errModify, calls)
	}
	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil {
		t.Fatal("stale edit removed the replacement registration")
	}
	if current.RegistrationEpoch <= base.RegistrationEpoch || current.Prefix != "pfx-recreated" || diskField(t, path, "prefix") != "pfx-recreated" {
		t.Fatalf("stale edit replaced the new registration: epoch=%d prefix=%q disk prefix=%v", current.RegistrationEpoch, current.Prefix, diskField(t, path, "prefix"))
	}
}

func TestManagerModifyRejectsDifferentFileObservation(t *testing.T) {
	for _, source := range []string{"registration", "observed"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			manager, _, path := newTrafficManager(t)
			original := []byte(`{"type":"claude","email":"a@b.c","prefix":"pfx-1","priority":1}`)
			if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			bootstrap := readTrafficSnapshot(t, path, "old-model")
			bootstrap.ClearFileSnapshot()
			if _, errRegister := manager.Register(ctx, bootstrap); errRegister != nil {
				t.Fatal(errRegister)
			}
			snapshot := readTrafficSnapshot(t, path, "old-model")
			originalObservation := snapshot.FileSnapshotIdentity()
			if source == "observed" {
				requireApplied(t, manager, snapshot)
			}
			base, _ := manager.GetByID(trafficAuthID)
			// The parser projects both number spellings to float64(1), while source digests differ.
			external := []byte(`{"disabled":false,"type":"claude","email":"a@b.c","prefix":"pfx-1","priority":1.0}`)
			calls := 0
			_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
				calls++
				current.Prefix = "pfx-edited"
				current.Metadata["prefix"] = "pfx-edited"
				if errWrite := os.WriteFile(path, external, 0o600); errWrite != nil {
					return errWrite
				}
				replay := readTrafficSnapshot(t, path, "old-model")
				if replay.FileSnapshotIdentity() == originalObservation || !reflect.DeepEqual(replay.Metadata, base.Metadata) {
					t.Fatal("fixture must have different source observations with identical parsed metadata")
				}
				_, errReplay := manager.Update(coreauth.WithSkipPersist(ctx), replay)
				return errReplay
			})
			if committed || !errors.Is(errModify, coreauth.ErrAuthStoreConflict) || calls != 1 {
				t.Fatalf("Modify() across a different file observation committed=%v error=%v callback calls=%d, want one edit and a conflict", committed, errModify, calls)
			}
			current, ok := manager.GetByID(base.ID)
			if !ok || current == nil || current.Prefix != "pfx-1" {
				t.Fatal("conflicting edit replaced the external file observation in runtime")
			}
			after, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			if !bytes.Equal(after, external) {
				t.Fatal("conflicting edit overwrote the external file contents")
			}
		})
	}
}

func TestManagerModifyRejectsUnwitnessedReplay(t *testing.T) {
	ctx := context.Background()
	manager := coreauth.NewManager(nil, nil, nil)
	base, errRegister := manager.Register(ctx, &coreauth.Auth{ID: "memory-auth", Provider: "claude", Prefix: "pfx-1", Status: coreauth.StatusActive})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	calls := 0
	_, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
		calls++
		current.Prefix = "pfx-edited"
		_, errReplay := manager.Update(coreauth.WithSkipPersist(ctx), base.Clone())
		return errReplay
	})
	if committed || !errors.Is(errModify, coreauth.ErrAuthStoreConflict) || calls != 1 {
		t.Fatalf("unwitnessed replay committed=%v error=%v callback calls=%d, want one edit and a conflict", committed, errModify, calls)
	}
	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil || current.Prefix != "pfx-1" {
		t.Fatal("unwitnessed replay allowed a stale callback to replace runtime state")
	}
}

func TestManagerModifyKeepsAuthKindEditDuringFileEnrichment(t *testing.T) {
	ctx := context.Background()
	manager, _, path := newTrafficManager(t)
	bootstrap, _ := manager.GetByID(trafficAuthID)
	bootstrap.Label = ""
	delete(bootstrap.Attributes, "auth_kind")
	if _, errRegister := manager.Register(ctx, bootstrap); errRegister != nil {
		t.Fatal(errRegister)
	}
	base, _ := manager.GetByID(trafficAuthID)
	snapshot := readTrafficSnapshot(t, path, "old-model")
	calls := 0
	modified, committed, errModify := manager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
		calls++
		current.Metadata["auth_kind"] = "api_key"
		_, errReplay := manager.Update(coreauth.WithSkipPersist(ctx), snapshot)
		return errReplay
	})
	if calls != 1 {
		t.Fatalf("edit callback calls=%d, want 1", calls)
	}
	current, ok := manager.GetByID(base.ID)
	if !ok || current == nil {
		t.Fatal("auth kind edit removed the runtime auth")
	}
	if committed {
		if errModify != nil || modified == nil || modified.AuthKind() != "api_key" || current.AuthKind() != "api_key" || diskField(t, path, "auth_kind") != "api_key" {
			t.Fatalf("committed auth kind=%q error=%v, want api_key preserved in the returned, runtime and persisted auth", current.AuthKind(), errModify)
		}
	} else if !errors.Is(errModify, coreauth.ErrAuthStoreConflict) || current.AuthKind() != "oauth" || diskField(t, path, "auth_kind") != nil {
		t.Fatalf("auth kind conflict error=%v kind=%q, want an explicit conflict with the previous kind preserved", errModify, current.AuthKind())
	}
}
