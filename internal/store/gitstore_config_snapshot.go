package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// PersistConfigSnapshot commits accepted, sanitized config bytes directly into the
// object database. It never checks out, pulls, or rewrites local working files, so
// an external editor or a bind-mounted config remains untouched during retries.
// The repository must already be initialized and have no local-only commits.
func (s *GitTokenStore) PersistConfigSnapshot(ctx context.Context, snapshot []byte) (errResult error) {
	if len(snapshot) == 0 {
		return fmt.Errorf("git token store: empty config snapshot")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errCtx := ctx.Err(); errCtx != nil {
		return errCtx
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	repoDir := s.repoDirSnapshot()
	rel, errRel := s.relativeToRepo(s.ConfigPath())
	if errRel != nil {
		return errRel
	}
	rel = filepath.ToSlash(rel)
	repo, errOpen := git.PlainOpen(repoDir)
	if errOpen != nil {
		return fmt.Errorf("git token store: open snapshot repository: %w", errOpen)
	}
	defer func() {
		if errClose := repo.Close(); errClose != nil {
			errResult = errors.Join(errResult, fmt.Errorf("git token store: close snapshot repository: %w", errClose))
		}
	}()
	head, errHead := repo.Head()
	if errHead != nil {
		return fmt.Errorf("git token store: inspect snapshot head: %w", errHead)
	}
	if !head.Name().IsBranch() || (s.branch != "" && head.Name() != plumbing.NewBranchReferenceName(s.branch)) {
		return fmt.Errorf("git token store: snapshot head is not the configured branch")
	}
	trackingName := plumbing.NewRemoteReferenceName("origin", head.Name().Short())
	previousTracking, errTracking := repo.Reference(trackingName, true)
	if errTracking != nil {
		return fmt.Errorf("git token store: inspect snapshot remote tracking branch: %w", errTracking)
	}
	baselineName := plumbing.ReferenceName("refs/cliproxy/config-baseline/" + head.Name().Short())
	baseline, errBaseline := repo.Reference(baselineName, true)
	if errBaseline != nil && !errors.Is(errBaseline, plumbing.ErrReferenceNotFound) {
		return fmt.Errorf("git token store: inspect snapshot checkout baseline: %w", errBaseline)
	}
	if head.Hash() != previousTracking.Hash() && (baseline == nil || head.Hash() != baseline.Hash()) {
		return fmt.Errorf("git token store: config snapshot conflicts with local-only branch changes")
	}
	localCommit, errLocal := repo.CommitObject(head.Hash())
	if errLocal != nil {
		return fmt.Errorf("git token store: inspect local snapshot base: %w", errLocal)
	}
	localTree, errLocalTree := localCommit.Tree()
	if errLocalTree != nil {
		return fmt.Errorf("git token store: inspect local snapshot tree: %w", errLocalTree)
	}
	// A private ref lets us fetch and push without moving the checkout or its
	// index to a remote auth tree that has not been installed locally.
	var tempName, tempTrackingName plumbing.ReferenceName
	for {
		tempBranch := "cliproxy-config-upload-" + rand.Text()
		tempName = plumbing.NewBranchReferenceName(tempBranch)
		tempTrackingName = plumbing.NewRemoteReferenceName("origin", tempBranch)
		available := true
		for _, ref := range []plumbing.ReferenceName{tempName, tempTrackingName} {
			if _, errRef := repo.Reference(ref, false); errRef == nil {
				available = false
				break
			} else if !errors.Is(errRef, plumbing.ErrReferenceNotFound) {
				return fmt.Errorf("git token store: inspect temporary snapshot reference: %w", errRef)
			}
		}
		if available {
			break
		}
	}
	var ownedRefs []plumbing.ReferenceName
	defer func() {
		for _, ref := range ownedRefs {
			if errRemove := repo.Storer.RemoveReference(ref); errRemove != nil && !errors.Is(errRemove, plumbing.ErrReferenceNotFound) {
				errResult = errors.Join(errResult, fmt.Errorf("git token store: remove snapshot upload reference: %w", errRemove))
			}
		}
	}()
	for _, ref := range []plumbing.ReferenceName{tempName, tempTrackingName} {
		if errRef := repo.Storer.SetReference(plumbing.NewHashReference(ref, head.Hash())); errRef != nil {
			return fmt.Errorf("git token store: reserve temporary snapshot reference: %w", errRef)
		}
		ownedRefs = append(ownedRefs, ref)
	}
	errFetch := repo.FetchContext(ctx, &git.FetchOptions{
		RemoteName: "origin", ClientOptions: s.gitClientOptions(),
		RefSpecs: []config.RefSpec{config.RefSpec("+" + head.Name().String() + ":" + tempName.String())},
	})
	if errFetch != nil && !errors.Is(errFetch, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("git token store: fetch config snapshot base: %w", errFetch)
	}
	remoteRef, errRemote := repo.Reference(tempName, true)
	if errRemote != nil {
		return fmt.Errorf("git token store: inspect fetched snapshot branch: %w", errRemote)
	}
	base, errBase := repo.CommitObject(remoteRef.Hash())
	if errBase != nil {
		return fmt.Errorf("git token store: inspect snapshot base commit: %w", errBase)
	}
	baseTree, errTree := base.Tree()
	if errTree != nil {
		return fmt.Errorf("git token store: inspect snapshot base tree: %w", errTree)
	}
	blob := repo.Storer.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	writer, errWriter := blob.Writer()
	if errWriter != nil {
		return fmt.Errorf("git token store: create snapshot blob: %w", errWriter)
	}
	_, errWrite := writer.Write(snapshot)
	if errClose := writer.Close(); errWrite != nil || errClose != nil {
		return fmt.Errorf("git token store: write snapshot blob: %w", errors.Join(errWrite, errClose))
	}
	blobHash, errBlob := repo.Storer.SetEncodedObject(blob)
	if errBlob != nil {
		return fmt.Errorf("git token store: store snapshot blob: %w", errBlob)
	}
	localConfig, errLocalConfig := configSnapshotTreeEntry(localTree, rel)
	if errLocalConfig != nil {
		return errLocalConfig
	}
	remoteConfig, errRemoteConfig := configSnapshotTreeEntry(baseTree, rel)
	if errRemoteConfig != nil {
		return errRemoteConfig
	}
	if remoteConfig != localConfig && (remoteConfig.Hash != blobHash || remoteConfig.Mode != filemode.Regular) {
		return fmt.Errorf("git token store: remote config conflicts with accepted snapshot")
	}
	remoteTreeHash, errReplace := replaceConfigSnapshotTree(repo, baseTree, strings.Split(rel, "/"), blobHash)
	if errReplace != nil {
		return fmt.Errorf("git token store: build remote snapshot tree: %w", errReplace)
	}
	remoteHash := remoteRef.Hash()
	if remoteTreeHash != base.TreeHash {
		remoteHash, errReplace = writeConfigSnapshotCommit(repo, remoteTreeHash)
		if errReplace != nil {
			return errReplace
		}
		if errRef := repo.Storer.SetReference(plumbing.NewHashReference(tempName, remoteHash)); errRef != nil {
			return fmt.Errorf("git token store: set snapshot upload reference: %w", errRef)
		}
		// go-git requires the source branch's tracking ref even with an explicit
		// lease hash when the destination branch has a different name.
		if errRef := repo.Storer.SetReference(plumbing.NewHashReference(tempTrackingName, remoteRef.Hash())); errRef != nil {
			return fmt.Errorf("git token store: set snapshot upload lease reference: %w", errRef)
		}
		pushOpts := &git.PushOptions{
			ClientOptions:  s.gitClientOptions(),
			RefSpecs:       []config.RefSpec{config.RefSpec(tempName.String() + ":" + head.Name().String())},
			ForceWithLease: &git.ForceWithLease{RefName: head.Name(), Hash: remoteRef.Hash()},
		}
		var errPush error
		if s.pushFn != nil {
			errPush = s.pushFn(ctx, repo, pushOpts)
		} else {
			errPush = repo.PushContext(ctx, pushOpts)
		}
		if errPush != nil && !errors.Is(errPush, git.NoErrAlreadyUpToDate) {
			return fmt.Errorf("git token store: push config snapshot: %w", errPush)
		}
	}
	// Only the config baseline advances locally. Keeping the previous auth tree
	// lets normal reconciliation install remote auth changes without treating an
	// operator's plaintext/partial config as a conflicting remote file change.
	localTreeHash, errLocalReplace := replaceConfigSnapshotTree(repo, localTree, strings.Split(rel, "/"), blobHash)
	if errLocalReplace != nil {
		return fmt.Errorf("git token store: build local snapshot baseline: %w", errLocalReplace)
	}
	localHash := remoteHash
	if localTreeHash != remoteTreeHash {
		localHash, errLocalReplace = writeConfigSnapshotCommit(repo, localTreeHash)
		if errLocalReplace != nil {
			return errLocalReplace
		}
	}
	if localHash != remoteHash {
		if errRef := repo.Storer.SetReference(plumbing.NewHashReference(baselineName, localHash)); errRef != nil {
			return fmt.Errorf("git token store: record snapshot checkout baseline: %w", errRef)
		}
	} else if errRemove := repo.Storer.RemoveReference(baselineName); errRemove != nil && !errors.Is(errRemove, plumbing.ErrReferenceNotFound) {
		return fmt.Errorf("git token store: remove synchronized snapshot baseline: %w", errRemove)
	}
	if errRef := repo.Storer.SetReference(plumbing.NewHashReference(head.Name(), localHash)); errRef != nil {
		return fmt.Errorf("git token store: advance snapshot checkout baseline: %w", errRef)
	}
	worktree, errWorktree := repo.Worktree()
	if errWorktree != nil {
		return fmt.Errorf("git token store: inspect snapshot worktree: %w", errWorktree)
	}
	if errReset := resetIndexToHead(repo, worktree); errReset != nil {
		return fmt.Errorf("git token store: reset snapshot index: %w", errReset)
	}
	if errRef := repo.Storer.SetReference(plumbing.NewHashReference(trackingName, remoteHash)); errRef != nil {
		return fmt.Errorf("git token store: update snapshot remote tracking branch: %w", errRef)
	}
	s.maybeRunGC(repoDir)
	return nil
}

func configSnapshotTreeEntry(tree *object.Tree, path string) (object.TreeEntry, error) {
	entry, errEntry := tree.FindEntry(path)
	if errors.Is(errEntry, object.ErrEntryNotFound) || errors.Is(errEntry, object.ErrDirectoryNotFound) {
		return object.TreeEntry{}, nil
	}
	if errEntry != nil {
		return object.TreeEntry{}, fmt.Errorf("git token store: inspect config snapshot entry: %w", errEntry)
	}
	return *entry, nil
}

func writeConfigSnapshotCommit(repo *git.Repository, tree plumbing.Hash) (plumbing.Hash, error) {
	signature := object.Signature{Name: "CLIProxyAPI", Email: "cliproxy@local", When: time.Now()}
	commit := &object.Commit{Author: signature, Committer: signature, Message: "Update config", TreeHash: tree}
	encoded := repo.Storer.NewEncodedObject()
	if errEncode := commit.Encode(encoded); errEncode != nil {
		return plumbing.ZeroHash, fmt.Errorf("git token store: encode snapshot commit: %w", errEncode)
	}
	return repo.Storer.SetEncodedObject(encoded)
}

func replaceConfigSnapshotTree(repo *git.Repository, tree *object.Tree, path []string, blob plumbing.Hash) (plumbing.Hash, error) {
	entries := append([]object.TreeEntry(nil), tree.Entries...)
	index := -1
	for i := range entries {
		if entries[i].Name == path[0] {
			index = i
			break
		}
	}
	entry := object.TreeEntry{Name: path[0], Mode: filemode.Regular, Hash: blob}
	if len(path) > 1 {
		subtree := &object.Tree{}
		if index >= 0 {
			if entries[index].Mode != filemode.Dir {
				return plumbing.ZeroHash, fmt.Errorf("config parent %s is not a directory", path[0])
			}
			var errTree error
			subtree, errTree = repo.TreeObject(entries[index].Hash)
			if errTree != nil {
				return plumbing.ZeroHash, errTree
			}
		}
		hash, errReplace := replaceConfigSnapshotTree(repo, subtree, path[1:], blob)
		if errReplace != nil {
			return plumbing.ZeroHash, errReplace
		}
		entry.Mode, entry.Hash = filemode.Dir, hash
	} else if index >= 0 && entries[index].Mode == filemode.Dir {
		return plumbing.ZeroHash, fmt.Errorf("config path %s is a directory", path[0])
	}
	if index >= 0 {
		entries[index] = entry
	} else {
		entries = append(entries, entry)
	}
	sort.Sort(object.TreeEntrySorter(entries))
	encoded := repo.Storer.NewEncodedObject()
	if errEncode := (&object.Tree{Entries: entries}).Encode(encoded); errEncode != nil {
		return plumbing.ZeroHash, errEncode
	}
	return repo.Storer.SetEncodedObject(encoded)
}
