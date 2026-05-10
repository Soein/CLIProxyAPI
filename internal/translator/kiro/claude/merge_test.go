package claude

import (
	"encoding/json"
	"testing"
)

func TestMergeAdjacentSameRoleStrings(t *testing.T) {
	in := []AnthropicMessage{
		{Role: "user", Content: json.RawMessage(`"hello"`)},
		{Role: "user", Content: json.RawMessage(`"world"`)},
	}
	got := MergeAdjacentSameRole(in)
	if len(got) != 1 {
		t.Fatalf("expected 1 merged message, got %d", len(got))
	}
	var s string
	_ = json.Unmarshal(got[0].Content, &s)
	if s != "hello\nworld" {
		t.Errorf("merged content = %q; want hello\\nworld", s)
	}
}

func TestMergeAdjacentDifferentRolesUnchanged(t *testing.T) {
	in := []AnthropicMessage{
		{Role: "user", Content: json.RawMessage(`"a"`)},
		{Role: "assistant", Content: json.RawMessage(`"b"`)},
		{Role: "user", Content: json.RawMessage(`"c"`)},
	}
	got := MergeAdjacentSameRole(in)
	if len(got) != 3 {
		t.Errorf("len = %d; want 3", len(got))
	}
}

func TestMergeArrayContentConcatenates(t *testing.T) {
	in := []AnthropicMessage{
		{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"a"}]`)},
		{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"b"}]`)},
	}
	got := MergeAdjacentSameRole(in)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
	var blocks []map[string]any
	_ = json.Unmarshal(got[0].Content, &blocks)
	if len(blocks) != 2 {
		t.Errorf("expected 2 merged blocks, got %d (%s)", len(blocks), got[0].Content)
	}
}
