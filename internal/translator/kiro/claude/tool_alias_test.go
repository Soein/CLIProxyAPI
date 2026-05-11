package claude

import (
	"strings"
	"testing"
)

func TestAliasToolNameShort(t *testing.T) {
	short := "get_weather"
	got, alias := AliasToolName(short)
	if got != short || alias {
		t.Errorf("short name should pass through unchanged; got=%q alias=%v", got, alias)
	}
}

func TestAliasToolNameLong(t *testing.T) {
	long := strings.Repeat("very_long_tool_name_", 5) // 100 chars
	got, alias := AliasToolName(long)
	if !alias {
		t.Errorf("long name should be aliased")
	}
	if len(got) > 64 {
		t.Errorf("aliased name length = %d; must be <= 64", len(got))
	}
	// Format: <prefix>_<sha256[:12]>. The 13 trailing chars are "_" + 12 hex.
	if got[len(got)-13] != '_' {
		t.Errorf("expected '_' before hash suffix in %q", got)
	}
}

func TestAliasToolNameDeterministic(t *testing.T) {
	long := strings.Repeat("x", 80)
	a, _ := AliasToolName(long)
	b, _ := AliasToolName(long)
	if a != b {
		t.Errorf("aliasing not deterministic: %q vs %q", a, b)
	}
}

func TestToolNameMapRoundTrip(t *testing.T) {
	m := NewToolNameMap()
	long := strings.Repeat("y", 80)
	alias, _ := AliasToolName(long)
	m.Add(alias, long)
	if got, ok := m.Original(alias); !ok || got != long {
		t.Errorf("Original(%q) = (%q, %v); want (%q, true)", alias, got, ok, long)
	}
	if _, ok := m.Original("nonexistent"); ok {
		t.Errorf("Original on missing key should be ok=false")
	}
}
