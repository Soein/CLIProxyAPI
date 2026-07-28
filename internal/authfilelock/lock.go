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

// Lock acquires canonical path locks in stable order and returns an idempotent release function.
func Lock(paths ...string) func() {
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
	locks := make([]*sync.Mutex, 0, len(keys))
	for _, key := range keys {
		value, _ := pathLocks.LoadOrStore(key, &sync.Mutex{})
		lock, _ := value.(*sync.Mutex)
		if lock == nil {
			lock = &sync.Mutex{}
			pathLocks.Store(key, lock)
		}
		lock.Lock()
		locks = append(locks, lock)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for index := len(locks) - 1; index >= 0; index-- {
				locks[index].Unlock()
			}
		})
	}
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
