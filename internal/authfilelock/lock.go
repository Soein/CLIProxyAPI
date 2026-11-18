// Package authfilelock serializes in-process auth file read-modify-write operations.
package authfilelock

import (
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

var pathLocks sync.Map

type fileLock struct {
	mu             sync.Mutex
	observationMu  sync.RWMutex
	pendingUploads int
	uploadsDone    chan struct{}
}

func locksForPaths(paths []string) []*fileLock {
	keys := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		key := canonicalPath(path)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	locks := make([]*fileLock, 0, len(keys))
	for _, key := range keys {
		locks = append(locks, lockForKey(key))
	}
	return locks
}

func lockForKey(key string) *fileLock {
	if current, ok := pathLocks.Load(key); ok {
		return current.(*fileLock)
	}
	current, _ := pathLocks.LoadOrStore(key, &fileLock{})
	return current.(*fileLock)
}

// Lock acquires canonical path locks in stable order and returns an idempotent release function.
func Lock(paths ...string) func() {
	locks := locksForPaths(paths)
	for _, lock := range locks {
		lock.mu.Lock()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for index := len(locks) - 1; index >= 0; index-- {
				locks[index].mu.Unlock()
			}
		})
	}
}

// BeginUpload hides tentative mirror bytes from file observers. Call the returned
// function after the backend confirms the write or the previous mirror is restored.
func BeginUpload(paths ...string) func() {
	locks := locksForPaths(paths)
	for _, lock := range locks {
		lock.observationMu.Lock()
		if lock.pendingUploads == 0 {
			lock.uploadsDone = make(chan struct{})
		}
		lock.pendingUploads++
		lock.observationMu.Unlock()
	}
	return func() {
		for _, lock := range locks {
			lock.observationMu.Lock()
			lock.pendingUploads--
			if lock.pendingUploads == 0 {
				close(lock.uploadsDone)
				lock.uploadsDone = nil
			}
			lock.observationMu.Unlock()
		}
	}
}

// ReadCommittedFile reads a source only when no upload is modifying its mirror.
// During an upload it returns a completion channel without calling read or waiting.
func ReadCommittedFile(path string, read func(string) ([]byte, error)) (data []byte, pending <-chan struct{}, err error) {
	paths := []string{path}
	if resolved, errResolve := filepath.EvalSymlinks(path); errResolve == nil && resolved != path {
		paths = append(paths, resolved)
	}
	locks := locksForPaths(paths)
	for _, lock := range locks {
		lock.observationMu.RLock()
	}
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].observationMu.RUnlock()
		}
	}()
	for _, lock := range locks {
		if lock.uploadsDone != nil {
			return nil, lock.uploadsDone, nil
		}
	}

	data, err = read(path)
	return data, nil, err
}

func canonicalPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	path = filepath.Clean(path)
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
	}
	return path
}
