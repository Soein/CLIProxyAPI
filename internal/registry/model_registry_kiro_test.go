package registry

import (
	"strings"
	"testing"
)

func TestKiroModelsContainsExpected(t *testing.T) {
	models := KiroModels()
	want := []string{
		"claude-haiku-4-5",
		"claude-sonnet-4-5",
		"claude-sonnet-4-5-20250929",
		"claude-sonnet-4-6",
		"claude-opus-4-5",
		"claude-opus-4-5-20251101",
		"claude-opus-4-6",
		"claude-opus-4-7",
		"claude-sonnet-4-20250514",
		"claude-3-7-sonnet-20250219",
	}
	gotIDs := map[string]bool{}
	for _, m := range models {
		gotIDs[m.ID] = true
	}
	for _, w := range want {
		if !gotIDs[w] {
			t.Errorf("Kiro model %q not registered", w)
		}
	}
	if len(models) < len(want) {
		t.Errorf("got %d kiro models; want at least %d", len(models), len(want))
	}
}

func TestKiroOpus47HasMillionContext(t *testing.T) {
	for _, m := range KiroModels() {
		if m.ID == "claude-opus-4-7" {
			if m.ContextLength < 1_000_000 {
				t.Errorf("opus-4-7 ContextLength = %d; want >= 1M", m.ContextLength)
			}
			return
		}
	}
	t.Fatal("opus-4-7 not found")
}

func TestKiroOwnedByIsAWSKiro(t *testing.T) {
	for _, m := range KiroModels() {
		if !strings.Contains(strings.ToLower(m.OwnedBy), "kiro") {
			t.Errorf("model %s OwnedBy = %q; want to contain 'kiro'", m.ID, m.OwnedBy)
		}
	}
}
