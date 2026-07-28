package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveConfigPreserveCommentsRemovesCodexAutomationKeys(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	original := `port: 8317
codex-weekly-automation:
  enabled: true
  interval-seconds: 300
codex-hourly-automation:
  enabled: true
  interval-seconds: 300
`
	if errWrite := os.WriteFile(configPath, []byte(original), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}

	if errSave := SaveConfigPreserveComments(configPath, &Config{Port: 8317}); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}

	data, errRead := os.ReadFile(configPath)
	if errRead != nil {
		t.Fatalf("read config: %v", errRead)
	}
	text := string(data)
	for _, removedKey := range []string{"codex-weekly-automation", "codex-hourly-automation"} {
		if strings.Contains(text, removedKey) {
			t.Fatalf("saved config still contains removed key %q:\n%s", removedKey, text)
		}
	}
}
