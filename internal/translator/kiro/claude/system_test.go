package claude

import (
	"strings"
	"testing"
)

func TestComposeSystemNoUserInput(t *testing.T) {
	got := ComposeSystem(nil)
	if !strings.Contains(got, "<CRITICAL_OVERRIDE>") {
		t.Errorf("expected identity override XML; got: %s", got)
	}
}

func TestComposeSystemWithStringInput(t *testing.T) {
	got := ComposeSystem([]byte(`"You are a helpful coding assistant."`))
	if !strings.Contains(got, "<CRITICAL_OVERRIDE>") {
		t.Errorf("missing identity override")
	}
	if !strings.Contains(got, "helpful coding assistant") {
		t.Errorf("user-supplied system not embedded; got: %s", got)
	}
}

func TestComposeSystemWithBlockArray(t *testing.T) {
	got := ComposeSystem([]byte(`[{"type":"text","text":"role: senior dev"}]`))
	if !strings.Contains(got, "role: senior dev") {
		t.Errorf("block-array text not embedded; got: %s", got)
	}
}
