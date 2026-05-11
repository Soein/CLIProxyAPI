package claude

import (
	"strings"
	"testing"
)

func TestTruncateDescriptionShort(t *testing.T) {
	in := "short desc"
	if got := TruncateToolDescription(in); got != in {
		t.Errorf("short desc should pass through unchanged: %q", got)
	}
}

func TestTruncateDescriptionLong(t *testing.T) {
	in := strings.Repeat("x", MaxToolDescriptionLength+100)
	got := TruncateToolDescription(in)
	if len(got) != MaxToolDescriptionLength+3 {
		t.Errorf("len = %d; want %d", len(got), MaxToolDescriptionLength+3)
	}
	if !strings.HasSuffix(got, "...") {
		t.Errorf("expected '...' suffix")
	}
}
