package claude

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// MaxToolNameLength 是 Kiro 对工具名称的长度上限。
const MaxToolNameLength = 64

// AliasToolName 返回（可能已别名化的）工具名称和是否发生了别名化的标志。
func AliasToolName(name string) (string, bool) {
	if len(name) <= MaxToolNameLength {
		return name, false
	}
	sum := sha256.Sum256([]byte(name))
	hash := hex.EncodeToString(sum[:])[:12]
	prefixLen := MaxToolNameLength - len(hash) - 1 // 1 为下划线
	if prefixLen < 1 {
		prefixLen = 1
	}
	return name[:prefixLen] + "_" + hash, true
}

// ToolNameMap 存储别名 → 原始名称的映射，用于响应时的反向查找。
// 并发安全；每个请求使用一个实例。
type ToolNameMap struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewToolNameMap 返回一个空映射。
func NewToolNameMap() *ToolNameMap {
	return &ToolNameMap{m: map[string]string{}}
}

// Add 记录 alias → original 的映射关系。
func (t *ToolNameMap) Add(alias, original string) {
	t.mu.Lock()
	t.m[alias] = original
	t.mu.Unlock()
}

// Original 根据别名返回原始名称。
func (t *ToolNameMap) Original(alias string) (string, bool) {
	t.mu.RLock()
	v, ok := t.m[alias]
	t.mu.RUnlock()
	return v, ok
}
