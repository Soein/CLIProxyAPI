package cliproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalregistry "github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func managementFileRequest(t *testing.T, h *issue6631Harness, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "/v8/management/"+path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+issue6631ManagementKey)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)
	return recorder
}

func TestManagementCredentialPatchPreservesQueuedExternalRotation(t *testing.T) {
	h := newIssue6631Harness(t)
	h.releaseFirst()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const fileName = "claude-concurrent.json"
	filePath := filepath.Join(h.service.currentConfig().AuthDir, fileName)
	writeCredential := func(token string) {
		t.Helper()
		data := fmt.Sprintf(`{"type":"claude","access_token":%q,"refresh_token":%q}`, token, token)
		if errWrite := os.WriteFile(filePath, []byte(data), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
	writeCredential("initial")
	t.Cleanup(func() {
		for _, auth := range h.service.coreManager.List() {
			internalregistry.GetGlobalRegistry().UnregisterClient(auth.ID)
		}
	})

	// Hold real watcher events outside the service dispatcher so the file rotation
	// is observed before PATCH, but is not yet visible in the runtime manager.
	pending := make(chan watcher.AuthUpdate, 32)
	h.service.currentWatcher().SetAuthUpdateQueue(pending)
	if errStart := h.service.currentWatcher().Start(ctx); errStart != nil {
		t.Fatal(errStart)
	}
	awaitToken := func(token string) watcher.AuthUpdate {
		t.Helper()
		timeout := time.NewTimer(10 * time.Second)
		defer timeout.Stop()
		for {
			select {
			case update := <-pending:
				if update.ID == fileName && update.Auth != nil && update.Auth.Metadata["access_token"] == token {
					return update
				}
				h.service.handleAuthUpdate(coreauth.WithSkipPersist(ctx), update)
			case <-timeout.C:
				t.Fatalf("watcher did not queue credential %q", token)
			}
		}
	}
	h.service.handleAuthUpdate(coreauth.WithSkipPersist(ctx), awaitToken("initial"))
	writeCredential("rotated")
	rotation := awaitToken("rotated")
	if live, ok := h.service.coreManager.GetByID(fileName); !ok || live.Metadata["access_token"] != "initial" {
		t.Fatal("rotation reached runtime before PATCH")
	}

	response := managementFileRequest(t, h, http.MethodPatch, "credentials/fields", fmt.Sprintf(`{"name":%q,"note":"operator"}`, fileName))
	if response.Code != http.StatusOK && response.Code != http.StatusConflict {
		t.Fatalf("PATCH: status=%d body=%s, want success or explicit conflict", response.Code, response.Body.String())
	}
	if response.Code == http.StatusConflict {
		if live, ok := h.service.coreManager.GetByID(fileName); !ok || live.Metadata["note"] != nil {
			t.Error("failed PATCH left an uncommitted runtime note")
		}
	}
	h.service.handleAuthUpdate(coreauth.WithSkipPersist(ctx), rotation)

	data, errRead := os.ReadFile(filePath)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var disk map[string]any
	if errDecode := json.Unmarshal(data, &disk); errDecode != nil {
		t.Fatal(errDecode)
	}
	live, ok := h.service.coreManager.GetByID(fileName)
	if !ok {
		t.Fatal("credential missing after queued rotation")
	}
	for name, metadata := range map[string]map[string]any{"disk": disk, "runtime": live.Metadata} {
		if metadata["access_token"] != "rotated" || metadata["refresh_token"] != "rotated" {
			t.Errorf("%s lost the externally rotated tokens", name)
		}
		wantNote := any(nil)
		if response.Code == http.StatusOK {
			wantNote = "operator"
		}
		if metadata["note"] != wantNote {
			t.Errorf("%s note=%v, want %v after PATCH status %d", name, metadata["note"], wantNote, response.Code)
		}
	}
}

func TestManagementRoutesSurvivePartialConfigWrite(t *testing.T) {
	for _, stage := range []string{"before-management", "null-secret"} {
		t.Run(stage, func(t *testing.T) {
			h := newIssue6631Harness(t)
			h.releaseFirst()
			data, errRead := os.ReadFile(h.configPath)
			if errRead != nil {
				t.Fatal(errRead)
			}
			complete := strings.Replace(string(data), "request-retry: 3", "request-retry: 7", 1)
			marker := "remote-management:"
			if stage == "null-secret" {
				marker = "secret-key:"
			}
			boundary := strings.Index(complete, marker)
			if boundary < 0 {
				t.Fatal("config fixture has no expected management boundary")
			}
			if stage == "null-secret" {
				boundary += len(marker)
			}
			file, errOpen := os.OpenFile(h.configPath, os.O_WRONLY|os.O_TRUNC, 0o600)
			if errOpen != nil {
				t.Fatal(errOpen)
			}
			t.Cleanup(func() {
				if errClose := file.Close(); errClose != nil {
					t.Error(errClose)
				}
			})
			assertRoute := func(stage string) {
				t.Helper()
				if !h.service.reloadConfigFromWatcher() {
					t.Fatal("watcher reload is unavailable")
				}
				response := managementFileRequest(t, h, http.MethodGet, "credentials", "")
				if response.Code != http.StatusOK {
					t.Errorf("%s: GET credentials status=%d body=%s", stage, response.Code, response.Body.String())
				}
			}
			assertRoute("empty file")
			if _, errWrite := file.WriteString(complete[:boundary]); errWrite != nil {
				t.Fatal(errWrite)
			}
			assertRoute("valid YAML prefix")
			if _, errWrite := file.WriteString(complete[boundary:]); errWrite != nil {
				t.Fatal(errWrite)
			}
			assertRoute("complete file")
			if got := h.service.currentConfig().RequestRetry; got != 7 {
				t.Errorf("complete config request-retry=%d, want 7", got)
			}
		})
	}
}

func TestManagementRoutesHonorExplicitConfigDisable(t *testing.T) {
	h := newIssue6631Harness(t)
	h.releaseFirst()
	if response := managementFileRequest(t, h, http.MethodGet, "credentials", ""); response.Code != http.StatusOK {
		t.Fatalf("initial GET credentials status=%d body=%s", response.Code, response.Body.String())
	}
	data, errRead := os.ReadFile(h.configPath)
	if errRead != nil {
		t.Fatal(errRead)
	}
	configWithoutKey, _, ok := strings.Cut(string(data), "  secret-key:")
	if !ok {
		t.Fatal("config fixture has no secret-key")
	}
	if errWrite := os.WriteFile(h.configPath, []byte(configWithoutKey+"  secret-key: \"\"\n"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if !h.service.reloadConfigFromWatcher() {
		t.Fatal("watcher reload is unavailable")
	}
	response := managementFileRequest(t, h, http.MethodGet, "credentials", "")
	if response.Code != http.StatusOK {
		t.Fatalf("unpublished key removal: GET credentials status=%d body=%s", response.Code, response.Body.String())
	}
	// Key changes are published only by the final YAML document marker.
	file, errOpen := os.OpenFile(h.configPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	t.Cleanup(func() {
		if errClose := file.Close(); errClose != nil {
			t.Error(errClose)
		}
	})
	if _, errWrite := file.WriteString("...\n"); errWrite != nil {
		t.Fatal(errWrite)
	}
	if !h.service.reloadConfigFromWatcher() {
		t.Fatal("watcher reload is unavailable")
	}
	response = managementFileRequest(t, h, http.MethodGet, "credentials", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET credentials status=%d body=%s, want disabled route", response.Code, response.Body.String())
	}
}
