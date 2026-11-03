package management

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

func TestConfigV8HashesSecretBeforeWritingCompletePayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(path, []byte("server: {port: 8317}\n"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	before, errStat := os.Stat(path)
	if errStat != nil {
		t.Fatal(errStat)
	}
	h := &Handler{cfg: &config.Config{}, configFilePath: path}
	router := gin.New()
	router.PUT("/v8/management/config.yaml", h.ConfigV8)
	raw := "# complete submitted configuration\nserver: {port: 8317}\nmanagement:\n  secret-key: new-complete-secret\nplugins:\n  configs: {opaque: {enabled: false, note: preserve}}\n"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/v8/management/config.yaml", strings.NewReader(raw)))
	if rec.Code != http.StatusOK {
		t.Fatalf("save status=%d: %s", rec.Code, rec.Body.String())
	}
	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var persisted config.Config
	if errParse := yaml.Unmarshal(saved, &persisted); errParse != nil {
		t.Fatal(errParse)
	}
	if errCompare := bcrypt.CompareHashAndPassword([]byte(persisted.RemoteManagement.SecretKey), []byte("new-complete-secret")); errCompare != nil {
		t.Fatalf("saved key was not hashed before the file write: %v", errCompare)
	}
	if persisted.RemoteManagement.SecretKey != h.cfg.RemoteManagement.SecretKey {
		t.Fatal("disk and runtime management keys differ")
	}
	after, errStat := os.Stat(path)
	if errStat != nil || !os.SameFile(before, after) {
		t.Fatalf("save replaced the mounted configuration inode: %v", errStat)
	}
	if !strings.Contains(string(saved), "# complete submitted configuration") || !strings.Contains(string(saved), "note: preserve") {
		t.Fatalf("secret hashing dropped unrelated configuration: %s", saved)
	}
}

func TestConfigV8RecordsIntentionalManagementSecretRemoval(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"whole document omission", http.MethodPut, "/v8/management/config.yaml", "server: {port: 8317}\n"},
		{"whole document null", http.MethodPut, "/v8/management/config.yaml", "management: {secret-key: null}\n"},
		{"delete key", http.MethodDelete, "/v8/management/config/management/secret-key", ""},
		{"delete section", http.MethodDelete, "/v8/management/config/management", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			raw := "server: {port: 8317}\nmanagement: {secret-key: previous-secret}\n"
			if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			cfg, errLoad := config.LoadConfig(path)
			if errLoad != nil {
				t.Fatal(errLoad)
			}
			h := &Handler{cfg: cfg, configFilePath: path}
			reloaded := make(chan *config.Config, 1)
			h.SetConfigReloadHook(func(_ context.Context, next *config.Config) { reloaded <- next })
			router := gin.New()
			router.PUT("/v8/management/config.yaml", h.ConfigV8)
			router.DELETE("/v8/management/config/*path", h.ConfigV8)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			if rec.Code != http.StatusOK {
				t.Fatalf("save status=%d: %s", rec.Code, rec.Body.String())
			}
			select {
			case next := <-reloaded:
				if next.RemoteManagement.SecretKey != "" {
					t.Fatal("completed removal kept the previous runtime secret")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("completed removal did not publish its runtime snapshot")
			}
			saved, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			var persisted map[string]any
			if errParse := yaml.Unmarshal(saved, &persisted); errParse != nil {
				t.Fatal(errParse)
			}
			management, _ := persisted["management"].(map[string]any)
			if value, exists := management["secret-key"]; !exists || value != "" {
				t.Fatalf("completed removal did not serialize explicit disable: %s", saved)
			}
		})
	}
}

func TestConfigV8RejectsPlaintextSecretRetainedByAnchor(t *testing.T) {
	for _, raw := range []string{
		"plugins:\n  configs:\n    opaque:\n      secret: &secret anchor-plaintext\nmanagement: {secret-key: *secret}\n",
		"plugins:\n  configs:\n    opaque:\n      credentials: &management {secret-key: anchor-plaintext}\nmanagement: {<<: *management}\n",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		initial := "server: {port: 8317}\n"
		if errWrite := os.WriteFile(path, []byte(initial), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		h := &Handler{cfg: &config.Config{}, configFilePath: path}
		router := gin.New()
		router.PUT("/v8/management/config.yaml", h.ConfigV8)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/v8/management/config.yaml", strings.NewReader(raw)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("save status=%d, want refusal when hashing leaves plaintext: %s", rec.Code, rec.Body.String())
		}
		saved, errRead := os.ReadFile(path)
		if errRead != nil || string(saved) != initial || h.cfg.RemoteManagement.SecretKey != "" {
			t.Fatalf("refused save changed disk or runtime: %q, error=%v", saved, errRead)
		}
	}
}

func TestConfigV8RejectsUnrelatedEditOfIncompleteManagementSource(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPatch, "/v8/management/config", `{"server":{"port":8318}}`},
		{http.MethodPut, "/v8/management/config/server/port", `8318`},
		{http.MethodPatch, "/v8/management/config/management", `{"allow-remote":true}`},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		initial := "server: {port: 8317}\nmanagement: {secret-key: previous-secret}\n"
		if errWrite := os.WriteFile(path, []byte(initial), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		cfg, errLoad := config.LoadConfig(path)
		if errLoad != nil {
			t.Fatal(errLoad)
		}
		h := &Handler{cfg: cfg, configFilePath: path}
		partial := "server: {port: 8317}\n"
		if errWrite := os.WriteFile(path, []byte(partial), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		router := gin.New()
		router.PATCH("/v8/management/config", h.ConfigV8)
		router.PATCH("/v8/management/config/*path", h.ConfigV8)
		router.PUT("/v8/management/config/*path", h.ConfigV8)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if rec.Code != http.StatusConflict {
			t.Fatalf("%s %s status=%d, want conflict: %s", tc.method, tc.path, rec.Code, rec.Body.String())
		}
		saved, errRead := os.ReadFile(path)
		if errRead != nil || string(saved) != partial || h.cfg.RemoteManagement.SecretKey != cfg.RemoteManagement.SecretKey {
			t.Fatalf("unrelated edit changed partial disk or live secret: %q, error=%v", saved, errRead)
		}
	}
}

func TestConfigV8RejectsMultiDocumentKeyChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "server: {port: 8317}\n"
	if errWrite := os.WriteFile(path, []byte(initial), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	h := &Handler{cfg: &config.Config{}, configFilePath: path}
	router := gin.New()
	router.PUT("/v8/management/config.yaml", h.ConfigV8)
	rec := httptest.NewRecorder()
	raw := "management: {secret-key: \"\"}\n---\nmanagement: {secret-key: intended-key}\n...\n"
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/v8/management/config.yaml", strings.NewReader(raw)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("multiple config documents were accepted: status=%d, body=%s", rec.Code, rec.Body.String())
	}
	saved, errRead := os.ReadFile(path)
	if errRead != nil || string(saved) != initial {
		t.Fatalf("refused document stream changed the source: %q, error=%v", saved, errRead)
	}
}

func TestConfigV8PreservesHashbangBlockScalar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(path, []byte("server: {port: 8317}\n"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	h := &Handler{cfg: &config.Config{}, configFilePath: path}
	router := gin.New()
	router.PUT("/v8/management/config.yaml", h.ConfigV8)
	raw := "management: {secret-key: new-key}\nplugins:\n  configs:\n    opaque:\n      script: |\n        #!/bin/sh\n        # comment\n        echo ready\n"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/v8/management/config.yaml", strings.NewReader(raw)))
	if rec.Code != http.StatusOK {
		t.Fatalf("save status=%d: %s", rec.Code, rec.Body.String())
	}
	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var doc map[string]any
	if errParse := yaml.Unmarshal(saved, &doc); errParse != nil {
		t.Fatal(errParse)
	}
	plugins := doc["plugins"].(map[string]any)
	configs := plugins["configs"].(map[string]any)
	opaque := configs["opaque"].(map[string]any)
	if got := opaque["script"]; got != "#!/bin/sh\n# comment\necho ready\n" {
		t.Fatalf("trusted save changed block scalar semantics: %#v", got)
	}
}

func TestConfigV8RejectsUnrelatedEditWhenRemoteAccessWasNotFullyWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "management: {allow-remote: true, secret-key: previous-secret}\n"
	if errWrite := os.WriteFile(path, []byte(initial), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	cfg, errLoad := config.LoadConfig(path)
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	h := &Handler{cfg: cfg, configFilePath: path}
	partial := "management: {secret-key: \"" + cfg.RemoteManagement.SecretKey + "\"}\n"
	if errWrite := os.WriteFile(path, []byte(partial), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	router := gin.New()
	router.PATCH("/v8/management/config", h.ConfigV8)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/v8/management/config", strings.NewReader(`{"server":{"port":8318}}`)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("unrelated edit published incomplete remote access: status=%d", rec.Code)
	}
	saved, errRead := os.ReadFile(path)
	if errRead != nil || string(saved) != partial || !h.cfg.RemoteManagement.AllowRemote {
		t.Fatal("refused edit changed management authentication or disk")
	}
}

func TestConfigV8AllowsExplicitRemoteAccessChanges(t *testing.T) {
	for _, before := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		initial := fmt.Sprintf("management: {allow-remote: %t, secret-key: previous-secret}\n", before)
		if errWrite := os.WriteFile(path, []byte(initial), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		cfg, errLoad := config.LoadConfig(path)
		if errLoad != nil {
			t.Fatal(errLoad)
		}
		h := &Handler{cfg: cfg, configFilePath: path}
		// The explicit field update is authoritative even when the disk writer
		// has not yet published the previous allow-remote value.
		partial := fmt.Sprintf("management: {secret-key: %q}\n", cfg.RemoteManagement.SecretKey)
		if errWrite := os.WriteFile(path, []byte(partial), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		router := gin.New()
		router.PUT("/v8/management/config/*path", h.ConfigV8)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/v8/management/config/management/allow-remote", strings.NewReader(fmt.Sprintf("%t", !before)))
		req.RemoteAddr = "127.0.0.1:1234"
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || h.cfg.RemoteManagement.AllowRemote == before {
			t.Fatalf("explicit access change was refused: status=%d, body=%s", rec.Code, rec.Body.String())
		}
		reloaded, errReload := config.LoadConfigForReload(path, cfg.RemoteManagement.SecretKey)
		if errReload != nil || reloaded.RemoteManagement.AllowRemote == before || !reloaded.SourceHasDocumentEnd {
			t.Fatalf("explicit access change was not published completely: %v", errReload)
		}
	}
}
