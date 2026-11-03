package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"golang.org/x/crypto/bcrypt"
)

type configSnapshotTestPersister interface {
	PersistConfigSnapshot(context.Context, []byte) error
}

func TestGitTokenStorePersistConfigSnapshotPreservesExistingUploadReferences(t *testing.T) {
	for _, failPush := range []bool{false, true} {
		t.Run(fmt.Sprintf("push-fails=%t", failPush), func(t *testing.T) {
			root := t.TempDir()
			remote := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "main\n"})
			store := NewGitTokenStore(remote, "", "", "")
			store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
			if err := store.EnsureRepository(); err != nil {
				t.Fatal(err)
			}
			repo, errOpen := git.PlainOpen(store.repoDirSnapshot())
			if errOpen != nil {
				t.Fatal(errOpen)
			}
			t.Cleanup(func() { _ = repo.Close() })
			head, errHead := repo.Head()
			if errHead != nil {
				t.Fatal(errHead)
			}
			refs := []plumbing.ReferenceName{
				plumbing.NewBranchReferenceName("cliproxy-config-upload/main"),
				plumbing.NewRemoteReferenceName("origin", "cliproxy-config-upload/main"),
				plumbing.NewBranchReferenceName("cliproxy/config-baseline/main"),
			}
			for _, ref := range refs {
				if err := repo.Storer.SetReference(plumbing.NewHashReference(ref, head.Hash())); err != nil {
					t.Fatal(err)
				}
			}
			errRejected := errors.New("rejected config upload")
			if failPush {
				store.pushFn = func(context.Context, *git.Repository, *git.PushOptions) error { return errRejected }
			}
			errPersist := store.PersistConfigSnapshot(context.Background(), []byte("port: 8318\n"))
			if (!failPush && errPersist != nil) || (failPush && !errors.Is(errPersist, errRejected)) {
				t.Fatalf("PersistConfigSnapshot error = %v, failPush = %t", errPersist, failPush)
			}
			for _, ref := range refs {
				current, errRef := repo.Reference(ref, false)
				if errRef != nil || current.Hash() != head.Hash() {
					t.Errorf("existing ref %s changed or disappeared: %v, %v", ref, current, errRef)
				}
			}
		})
	}
}

func TestGitTokenStorePersistConfigSnapshotRejectsRemoteManagementKeyChange(t *testing.T) {
	root := t.TempDir()
	remote := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "main\n"})
	store := NewGitTokenStore(remote, "", "", "")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	if err := store.EnsureRepository(); err != nil {
		t.Fatal(err)
	}
	persister := requireConfigSnapshotPersister(t, store)
	original := seedTestSettledConfigs(t)["hashed secret"]
	if err := persister.PersistConfigSnapshot(context.Background(), []byte(original)); err != nil {
		t.Fatal(err)
	}
	other := NewGitTokenStore(remote, "", "", "")
	other.SetBaseDir(filepath.Join(root, "other", "auths"))
	if err := other.EnsureRepository(); err != nil {
		t.Fatal(err)
	}
	hash, errHash := bcrypt.GenerateFromPassword([]byte("new-remote-management-key"), bcrypt.MinCost)
	if errHash != nil {
		t.Fatal(errHash)
	}
	newRemote := "port: 8319\nmanagement:\n  secret-key: '" + string(hash) + "'\n"
	if err := requireConfigSnapshotPersister(t, other).PersistConfigSnapshot(context.Background(), []byte(newRemote)); err != nil {
		t.Fatal(err)
	}
	const partial = "management:\n  secret-key: local-in-progress\nport: ["
	writeSeedTestFile(t, store.ConfigPath(), partial)
	if err := persister.PersistConfigSnapshot(context.Background(), []byte(original)); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("stale config persist error = %v, want same-path conflict", err)
	}
	assertRemoteFileContents(t, remote, "main", "config/config.yaml", newRemote)
	assertLocalFileContents(t, store.ConfigPath(), partial)
	if err := persister.PersistConfigSnapshot(context.Background(), []byte(newRemote)); err != nil {
		t.Fatalf("idempotent remote snapshot: %v", err)
	}
	assertRemoteFileContents(t, remote, "main", "config/config.yaml", newRemote)
	assertLocalFileContents(t, store.ConfigPath(), partial)
}

func requireConfigSnapshotPersister(t *testing.T, store any) configSnapshotTestPersister {
	t.Helper()
	persister, ok := store.(configSnapshotTestPersister)
	if !ok {
		t.Fatalf("%T cannot persist an accepted config snapshot", store)
	}
	return persister
}

func TestRemoteStoresPersistAcceptedConfigSnapshotWithoutReadingLocalFile(t *testing.T) {
	const accepted = "# accepted\nport: 8318\n"
	const partial = "management:\n  secret-key: operator-plaintext\nport: ["
	t.Run("object", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		var uploads [][]byte
		store := &ObjectTokenStore{configPath: path, putObjectFn: func(_ context.Context, key string, data []byte, _ string) error {
			if key != objectStoreConfigKey {
				t.Fatalf("upload key = %q, want config key", key)
			}
			uploads = append(uploads, append([]byte(nil), data...))
			return nil
		}}
		writeSeedTestFile(t, path, partial)
		if err := requireConfigSnapshotPersister(t, store).PersistConfigSnapshot(context.Background(), []byte(accepted)); err != nil {
			t.Fatalf("PersistConfigSnapshot: %v", err)
		}
		if len(uploads) != 1 || string(uploads[0]) != accepted {
			t.Fatalf("uploaded configs = %q, want accepted snapshot", uploads)
		}
		assertLocalFileContents(t, path, partial)
	})
	t.Run("postgres", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		store, database := newSeedTestPostgresStore(t, path)
		writeSeedTestFile(t, path, partial)
		if err := requireConfigSnapshotPersister(t, store).PersistConfigSnapshot(context.Background(), []byte(accepted)); err != nil {
			t.Fatalf("PersistConfigSnapshot: %v", err)
		}
		if uploads := database.recordedUpserts(); len(uploads) != 1 || uploads[0] != accepted {
			t.Fatalf("uploaded configs = %q, want accepted snapshot", uploads)
		}
		assertLocalFileContents(t, path, partial)
	})
}

func TestGitTokenStorePersistConfigSnapshotPreservesLocalWritesAndRemoteAuth(t *testing.T) {
	for _, branch := range []string{"", "release/config"} {
		t.Run(branch, func(t *testing.T) {
			root := t.TempDir()
			remote := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "main\n"}, testBranchSpec{name: "release/config", contents: "release\n"})
			store := NewGitTokenStore(remote, "", "", branch)
			store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
			if err := store.EnsureRepository(); err != nil {
				t.Fatalf("EnsureRepository: %v", err)
			}
			persister := requireConfigSnapshotPersister(t, store)
			rotatedPath := filepath.Join(store.AuthDir(), "rotated.json")
			writeSeedTestFile(t, rotatedPath, "{\"type\":\"claude\",\"token\":\"old\"}\n")
			if err := store.PersistAuthFiles(context.Background(), "initial auth", rotatedPath); err != nil {
				t.Fatalf("initial auth: %v", err)
			}
			other := NewGitTokenStore(remote, "", "", branch)
			other.SetBaseDir(filepath.Join(root, "other", "auths"))
			if err := other.EnsureRepository(); err != nil {
				t.Fatalf("other EnsureRepository: %v", err)
			}
			remoteAuthPath := filepath.Join(other.AuthDir(), "remote-only.json")
			writeSeedTestFile(t, remoteAuthPath, "{\"type\":\"claude\"}\n")
			if err := other.PersistAuthFiles(context.Background(), "remote auth", remoteAuthPath); err != nil {
				t.Fatalf("remote auth: %v", err)
			}
			const rotatedAuth = "{\"type\":\"claude\",\"token\":\"rotated\"}\n"
			writeSeedTestFile(t, filepath.Join(other.AuthDir(), "rotated.json"), rotatedAuth)
			if err := other.PersistAuthFiles(context.Background(), "rotate auth", filepath.Join(other.AuthDir(), "rotated.json")); err != nil {
				t.Fatalf("rotate auth: %v", err)
			}
			const partial = "management:\n  secret-key: operator-plaintext\nport: ["
			writeSeedTestFile(t, store.ConfigPath(), partial)
			before, errStat := os.Stat(store.ConfigPath())
			if errStat != nil {
				t.Fatal(errStat)
			}
			const localAuth = "{\"type\":\"claude\",\"local\":true}\n"
			writeSeedTestFile(t, filepath.Join(store.AuthDir(), "local-only.json"), localAuth)
			const accepted = "# accepted snapshot\nport: 8318\n"
			if err := persister.PersistConfigSnapshot(context.Background(), []byte(accepted)); err != nil {
				t.Fatalf("PersistConfigSnapshot: %v", err)
			}
			after, errStat := os.Stat(store.ConfigPath())
			if errStat != nil || !os.SameFile(before, after) {
				t.Fatalf("config inode replaced: %v", errStat)
			}
			assertLocalFileContents(t, store.ConfigPath(), partial)
			assertLocalFileContents(t, filepath.Join(store.AuthDir(), "local-only.json"), localAuth)
			remoteBranch := branch
			if remoteBranch == "" {
				remoteBranch = "main"
			}
			assertRemoteFileContents(t, remote, remoteBranch, "config/config.yaml", accepted)
			assertRemoteTreePath(t, remote, remoteBranch, "auths/remote-only.json", true)
			assertRemoteTreePath(t, remote, remoteBranch, "auths/local-only.json", false)
			if err := persister.PersistConfigSnapshot(context.Background(), []byte(accepted)); err != nil {
				t.Fatalf("repeat snapshot before normal auth synchronization: %v", err)
			}
			if err := store.PersistAuthFiles(context.Background(), "later auth", filepath.Join(store.AuthDir(), "local-only.json")); err != nil {
				t.Fatalf("subsequent auth persistence: %v", err)
			}
			assertRemoteFileContents(t, remote, remoteBranch, "auths/rotated.json", rotatedAuth)
			assertLocalFileContents(t, rotatedPath, rotatedAuth)
			assertRemoteFileContents(t, remote, remoteBranch, "config/config.yaml", accepted)
			assertLocalFileContents(t, store.ConfigPath(), partial)
		})
	}
}

func TestGitTokenStorePersistConfigSnapshotRetryKeepsAcceptedBytes(t *testing.T) {
	root := t.TempDir()
	remote := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "main\n"})
	store := NewGitTokenStore(remote, "", "", "")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	if err := store.EnsureRepository(); err != nil {
		t.Fatal(err)
	}
	persister := requireConfigSnapshotPersister(t, store)
	const accepted = "port: 8318\n"
	const partial = "management:\n  secret-key: newer-plaintext\nport: ["
	writeSeedTestFile(t, store.ConfigPath(), "port: 8317\n")
	repo, errOpen := git.PlainOpen(store.repoDirSnapshot())
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	t.Cleanup(func() { _ = repo.Close() })
	headBefore, errHead := repo.Head()
	if errHead != nil {
		t.Fatal(errHead)
	}
	other := NewGitTokenStore(remote, "", "", "")
	other.SetBaseDir(filepath.Join(root, "other", "auths"))
	if err := other.EnsureRepository(); err != nil {
		t.Fatal(err)
	}
	store.pushFn = func(ctx context.Context, repo *git.Repository, opts *git.PushOptions) error {
		writeSeedTestFile(t, store.ConfigPath(), partial)
		path := filepath.Join(other.AuthDir(), "concurrent.json")
		writeSeedTestFile(t, path, "{\"type\":\"claude\"}\n")
		if err := other.PersistAuthFiles(context.Background(), "concurrent remote change", path); err != nil {
			t.Fatalf("concurrent remote persistence: %v", err)
		}
		return repo.PushContext(ctx, opts)
	}
	if err := persister.PersistConfigSnapshot(context.Background(), []byte(accepted)); err == nil || !strings.Contains(err.Error(), "non-fast-forward") {
		t.Fatalf("first persist error = %v, want rejected push", err)
	}
	headAfter, errHead := repo.Head()
	if errHead != nil || headAfter.Hash() != headBefore.Hash() {
		t.Fatalf("rejected push did not restore HEAD: %v", errHead)
	}
	store.pushFn = nil
	if err := persister.PersistConfigSnapshot(context.Background(), []byte(accepted)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	assertLocalFileContents(t, store.ConfigPath(), partial)
	assertRemoteFileContents(t, remote, "main", "config/config.yaml", accepted)
	assertRemoteTreePath(t, remote, "main", "auths/concurrent.json", true)
}

func TestGitTokenStorePersistConfigSnapshotRejectsLocalOnlyCommit(t *testing.T) {
	root := t.TempDir()
	remote := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "main\n"})
	store := NewGitTokenStore(remote, "", "", "")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	if err := store.EnsureRepository(); err != nil {
		t.Fatal(err)
	}
	persister := requireConfigSnapshotPersister(t, store)
	repo, errOpen := git.PlainOpen(store.repoDirSnapshot())
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	t.Cleanup(func() { _ = repo.Close() })
	worktree, errWorktree := repo.Worktree()
	if errWorktree != nil {
		t.Fatal(errWorktree)
	}
	writeSeedTestFile(t, filepath.Join(store.AuthDir(), "unpublished.json"), "{\"type\":\"claude\"}\n")
	if _, errAdd := worktree.Add("auths/unpublished.json"); errAdd != nil {
		t.Fatal(errAdd)
	}
	localHash, errCommit := worktree.Commit("Unpublished auth change", &git.CommitOptions{Author: &object.Signature{Name: "operator", Email: "operator@local", When: time.Now()}})
	if errCommit != nil {
		t.Fatal(errCommit)
	}
	if err := persister.PersistConfigSnapshot(context.Background(), []byte("port: 8318\n")); err == nil {
		t.Fatal("accepted snapshot overwrote a local-only branch state")
	}
	current, errHead := repo.Head()
	if errHead != nil || current.Hash() != localHash {
		t.Fatalf("local-only HEAD changed: %v", errHead)
	}
}
