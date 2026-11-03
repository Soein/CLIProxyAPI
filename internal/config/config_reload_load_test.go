package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

func TestLoadConfigForReloadPreparesExactReadWithoutWriting(t *testing.T) {
	raw := "# snapshot source\nport: 8317\nremote-management: {secret-key: legacy-plaintext}\nmanagement: {secret-key: effective-plaintext}\n... # complete\n# trailing comment\n"
	path := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	cfg, errLoad := LoadConfigForReload(path, "")
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	if cfg.SourceHash != ComputeSourceHash([]byte(raw)) || !cfg.SourceHasDocumentEnd {
		t.Fatal("snapshot lost the actual parsed source identity or completion boundary")
	}
	if len(cfg.SourceSnapshot) == 0 || strings.Contains(string(cfg.SourceSnapshot), "plaintext") {
		t.Fatalf("snapshot retained a management plaintext: %s", cfg.SourceSnapshot)
	}
	var persisted Config
	if errParse := yaml.Unmarshal(cfg.SourceSnapshot, &persisted); errParse != nil {
		t.Fatal(errParse)
	}
	if persisted.Port != 8317 || persisted.RemoteManagement.SecretKey != cfg.RemoteManagement.SecretKey {
		t.Fatal("prepared snapshot changed the effective configuration")
	}
	if errCompare := bcrypt.CompareHashAndPassword([]byte(persisted.RemoteManagement.SecretKey), []byte("effective-plaintext")); errCompare != nil {
		t.Fatal(errCompare)
	}
	_, ended, errSource := reloadConfigSource(cfg.SourceSnapshot)
	if errSource != nil || !ended {
		t.Fatalf("prepared snapshot lost its single-document completion boundary: %v", errSource)
	}
	got, errRead := os.ReadFile(path)
	if errRead != nil || string(got) != raw {
		t.Fatalf("reload rewrote the source: %q, error=%v", got, errRead)
	}
}

func TestConfigReloadRejectsCyclicAliasBeforeExpansion(t *testing.T) {
	if target := os.Getenv("CLIPROXY_CYCLE_TEST_TARGET"); target != "" {
		debug.SetMaxStack(256 << 10)
		raw := []byte("cycle: &a [*a]\nmanagement: {secret-key: \"\"}\n...\n")
		var err error
		if target == "snapshot" {
			_, err = PrepareConfigSnapshot(raw, "")
		} else {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if errWrite := os.WriteFile(path, raw, 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			_, err = LoadConfigForReload(path, "")
		}
		if err == nil {
			t.Fatal("cyclic alias was accepted")
		}
		return
	}
	for _, target := range []string{"reload", "snapshot"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestConfigReloadRejectsCyclicAliasBeforeExpansion$")
		cmd.Env = append(os.Environ(), "CLIPROXY_CYCLE_TEST_TARGET="+target)
		output, errRun := cmd.CombinedOutput()
		if errRun != nil {
			if len(output) > 500 {
				output = output[:500]
			}
			t.Errorf("%s did not return a validation error safely: %v\n%s", target, errRun, output)
		}
	}
}

func TestLoadConfigForReloadPreservesHashbangBlockScalar(t *testing.T) {
	for _, secret := range []string{uploadTestBcrypt(t), "new-plaintext-key"} {
		raw := "management:\n  secret-key: " + secret + "\nplugins:\n  configs:\n    opaque:\n      script: |\n        #!/bin/sh\n        # shell comment\n        echo ready\n...\n"
		path := filepath.Join(t.TempDir(), "config.yaml")
		if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		cfg, errLoad := LoadConfigForReload(path, "")
		if errLoad != nil {
			t.Fatal(errLoad)
		}
		var snapshot map[string]any
		if errParse := yaml.Unmarshal(cfg.SourceSnapshot, &snapshot); errParse != nil {
			t.Fatal(errParse)
		}
		plugins := snapshot["plugins"].(map[string]any)
		configs := plugins["configs"].(map[string]any)
		opaque := configs["opaque"].(map[string]any)
		if got := opaque["script"]; got != "#!/bin/sh\n# shell comment\necho ready\n" {
			t.Fatalf("snapshot changed block scalar semantics: %#v", got)
		}
	}
}

func TestLoadConfigForReloadRejectsSecondDocumentEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := "management: {secret-key: \"\"}\n---\n...\n"
	if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, errLoad := LoadConfigForReload(path, ""); errLoad == nil {
		t.Fatal("a second document's end completed the first configuration")
	}
}

func TestMixedLayoutStartupAndUploadHashExistingLegacySecret(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		raw := "server: {port: 8317}\nremote-management: {secret-key: mixed-layout-plaintext}\n"
		if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		if prepare {
			if errPrepare := PrepareConfigFileForUpload(path); errPrepare != nil {
				t.Fatal(errPrepare)
			}
		} else if _, errLoad := LoadConfig(path); errLoad != nil {
			t.Fatal(errLoad)
		}
		saved, errRead := os.ReadFile(path)
		if errRead != nil || strings.Contains(string(saved), "mixed-layout-plaintext") {
			t.Fatalf("mixed layout retained legacy plaintext: %s, error=%v", saved, errRead)
		}
		var cfg Config
		if errParse := yaml.Unmarshal(saved, &cfg); errParse != nil {
			t.Fatal(errParse)
		}
		if errCompare := bcrypt.CompareHashAndPassword([]byte(cfg.RemoteManagement.SecretKey), []byte("mixed-layout-plaintext")); errCompare != nil {
			t.Fatal(errCompare)
		}
	}
}
