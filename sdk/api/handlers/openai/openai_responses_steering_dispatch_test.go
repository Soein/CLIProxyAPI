package openai

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func isTeardownErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure)
}

type fakeDispatchAuthority struct {
	mu            sync.Mutex
	closed        bool
	deny          atomic.Bool
	admitCalls    atomic.Int32
	totalReleases atomic.Int32

	initialAdmitted     atomic.Bool
	initialReleased     atomic.Bool
	initialReleaseCount atomic.Int32
	initialReleasedChan chan struct{}

	successorAdmitted     atomic.Bool
	successorReleased     atomic.Bool
	successorReleaseCount atomic.Int32
	successorReleasedChan chan struct{}

	thirdAdmitted     atomic.Bool
	thirdReleased     atomic.Bool
	thirdReleaseCount atomic.Int32
	thirdReleasedChan chan struct{}

	fourthAdmitted     atomic.Bool
	fourthReleased     atomic.Bool
	fourthReleaseCount atomic.Int32
	fourthReleasedChan chan struct{}
}

func newFakeDispatchAuthority() *fakeDispatchAuthority {
	return &fakeDispatchAuthority{
		initialReleasedChan:   make(chan struct{}),
		successorReleasedChan: make(chan struct{}),
		thirdReleasedChan:     make(chan struct{}),
		fourthReleasedChan:    make(chan struct{}),
	}
}

func (f *fakeDispatchAuthority) Admit(authID string) (func(), bool) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed || f.deny.Load() {
		return nil, false
	}
	call := f.admitCalls.Add(1)
	switch call {
	case 1:
		f.initialAdmitted.Store(true)
		var once sync.Once
		return func() {
			f.initialReleased.Store(true)
			f.initialReleaseCount.Add(1)
			f.totalReleases.Add(1)
			once.Do(func() {
				close(f.initialReleasedChan)
			})
		}, true
	case 2:
		f.successorAdmitted.Store(true)
		var once sync.Once
		return func() {
			f.successorReleased.Store(true)
			f.successorReleaseCount.Add(1)
			f.totalReleases.Add(1)
			once.Do(func() {
				close(f.successorReleasedChan)
			})
		}, true
	case 3:
		f.thirdAdmitted.Store(true)
		var once sync.Once
		return func() {
			f.thirdReleased.Store(true)
			f.thirdReleaseCount.Add(1)
			f.totalReleases.Add(1)
			once.Do(func() {
				close(f.thirdReleasedChan)
			})
		}, true
	case 4:
		f.fourthAdmitted.Store(true)
		var once sync.Once
		return func() {
			f.fourthReleased.Store(true)
			f.fourthReleaseCount.Add(1)
			f.totalReleases.Add(1)
			once.Do(func() {
				close(f.fourthReleasedChan)
			})
		}, true
	default:
		return func() {
			f.totalReleases.Add(1)
		}, true
	}
}

func (f *fakeDispatchAuthority) Wake() {}

func (f *fakeDispatchAuthority) Ready() bool {
	return true
}

func (f *fakeDispatchAuthority) WaitReady(ctx context.Context) error {
	return nil
}

func (f *fakeDispatchAuthority) CloseAdmissions() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func TestResponsesSteeringDispatchRejectNewGenerationAfterCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload func(model, parentID string) []byte
	}{
		{
			name: "create",
			payload: func(model, _ string) []byte {
				return []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))
			},
		},
		{
			name: "append",
			payload: func(_, parentID string) []byte {
				return []byte(fmt.Sprintf(`{"type":"response.append","previous_response_id":%q,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"append"}]}]}`, parentID))
			},
		},
		{
			name: "steer",
			payload: func(_, parentID string) []byte {
				return []byte(fmt.Sprintf(`{"type":"response.steer","previous_response_id":%q,"input":"steer targeting completed parent"}`, parentID))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var frames atomic.Int32
			done := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = c.Close() }()
				_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
				if _, _, err := c.ReadMessage(); err != nil {
					t.Errorf("upstream read initial: %v", err)
					return
				}
				frames.Add(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

				// When unpatched upstream receives unexpected second frame, deliberately close
				// so RED gives a clear assertion instead of deadlocking or timing out.
				if _, _, err := c.ReadMessage(); err == nil {
					frames.Add(1)
					_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "unexpected second frame"))
					return
				}
			}))
			defer upstream.Close()

			cfg := &config.Config{}
			cfg.Codex.ResponseSteering = true
			cfg.CodexResponseSteering = true
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
			authority := newFakeDispatchAuthority()
			manager.SetDispatchAuthority(authority)

			authID := "steering-dispatch-reject-" + tc.name
			model := "steering-dispatch-reject-model-" + tc.name
			if _, err := manager.Register(context.Background(), &coreauth.Auth{
				ID: authID, Provider: "codex", Status: coreauth.StatusActive,
				Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
			}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
			defer registry.GetGlobalRegistry().UnregisterClient(authID)

			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
			router := gin.New()
			router.GET("/v1/responses", h.ResponsesWebsocket)
			downstream := httptest.NewServer(router)
			defer downstream.Close()

			c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

			// Initial create
			if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
				t.Fatal(err)
			}

			// Read r1 created and r1 completed
			for i := 0; i < 2; i++ {
				if _, _, err := c.ReadMessage(); err != nil {
					t.Fatalf("read r1 (%d): %v", i, err)
				}
			}

			// Downstream has observed first completed. Authority now denies new admissions.
			authority.CloseAdmissions()

			// Send followup frame that must NOT reach upstream.
			if err := c.WriteMessage(websocket.TextMessage, tc.payload(model, "r1")); err != nil {
				t.Fatal(err)
			}

			// Connection should terminate promptly with rejection or close.
			_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, msg, errRead := c.ReadMessage()
			if errRead == nil {
				msgType := gjson.GetBytes(msg, "type").String()
				if msgType != "error" && !strings.Contains(msgType, "fail") && !strings.Contains(msgType, "reject") {
					t.Fatalf("expected rejection or close, got message type %s: %s", msgType, msg)
				}
			}

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream cleanup stalled")
			}

			if frames.Load() != 1 {
				t.Fatalf("expected client followup %s not to reach upstream, but upstream received %d frames", tc.name, frames.Load())
			}

			// Check no credential-wide cooldown was fabricated for this local rejection.
			currentAuth, okAuth := manager.GetByID(authID)
			if !okAuth || currentAuth == nil {
				t.Fatalf("failed to get auth: %s", authID)
			}
			if views := coreauth.CooldownSnapshotForAuth(currentAuth, time.Now()); len(views) > 0 {
				t.Fatalf("unexpected credential cooldown fabricated for %s: %#v", authID, views)
			}
			if currentAuth.Disabled {
				t.Fatalf("auth %s was unexpectedly disabled", authID)
			}
		})
	}
}

func TestResponsesSteeringDispatchAllowsInFlightSteer(t *testing.T) {
	var frames atomic.Int32
	done := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Read initial create
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read initial: %v", err)
			return
		}
		frames.Add(1)

		// Send response.created only (initial generation is still running)
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`)); err != nil {
			t.Errorf("upstream write created: %v", err)
			return
		}

		// Read in-flight steer
		_, b, err := c.ReadMessage()
		if err != nil {
			t.Errorf("upstream read steer: %v", err)
			return
		}
		frames.Add(1)
		if gjson.GetBytes(b, "type").String() != "response.steer" {
			t.Errorf("expected response.steer, got: %s", b)
			return
		}
		if gjson.GetBytes(b, "previous_response_id").String() != "r1" {
			t.Errorf("expected previous_response_id r1, got: %s", b)
			return
		}

		// Respond with steer.accepted and then complete r1
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r1"}}`)); err != nil {
			t.Errorf("upstream write accepted: %v", err)
			return
		}
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`)); err != nil {
			t.Errorf("upstream write completed: %v", err)
			return
		}

		// Read until client closes
		_, _, _ = c.ReadMessage()
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	authority := newFakeDispatchAuthority()
	manager.SetDispatchAuthority(authority)

	authID := "steering-dispatch-in-flight"
	model := "steering-dispatch-in-flight-model"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Send initial create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}

	// Read response.created for r1
	_, b, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read created: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r1" {
		t.Fatalf("unexpected first message: %s", b)
	}

	// Authority closes while initial generation is still running
	authority.CloseAdmissions()

	// Steer for active parent r1
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"in-flight steer"}`)); err != nil {
		t.Fatal(err)
	}

	// Read steer.accepted and response.completed
	acceptedSeen := false
	completedSeen := false
	for i := 0; i < 2; i++ {
		_, b, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read after steer (%d): %v", i, err)
		}
		switch gjson.GetBytes(b, "type").String() {
		case "response.steer.accepted":
			acceptedSeen = true
		case "response.completed":
			completedSeen = true
		}
	}
	if !acceptedSeen {
		t.Fatal("response.steer.accepted was not received")
	}
	if !completedSeen {
		t.Fatal("response.completed was not received")
	}

	// Close downstream client
	_ = c.Close()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream cleanup stalled")
	}

	if frames.Load() != 2 {
		t.Fatalf("expected 2 frames at upstream (create + steer), got %d", frames.Load())
	}
	if authority.admitCalls.Load() != 1 {
		t.Fatalf("expected exactly 1 dispatch admission for in-flight steer, got %d", authority.admitCalls.Load())
	}
}

func TestResponsesSteeringDispatchReleaseLifecycle(t *testing.T) {
	var frames atomic.Int32
	done := make(chan struct{})
	readyForTerminal := make(chan struct{})
	var closeOnce sync.Once
	signalReady := func() { closeOnce.Do(func() { close(readyForTerminal) }) }
	defer signalReady()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

		// 1. Initial create
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read initial: %v", err)
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

		// 2. Successor create
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read successor: %v", err)
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2"}}`))

		// Wait for signal from downstream test that it has observed r2 created
		// and verified successor admission is held before terminal.
		select {
		case <-readyForTerminal:
		case <-time.After(3 * time.Second):
			t.Error("upstream timed out waiting for readyForTerminal signal")
			return
		}

		// 3. Successor terminal
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r2","output":[]}}`))

		// Wait for downstream client to close
		_, _, _ = c.ReadMessage()
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	authority := newFakeDispatchAuthority()
	manager.SetDispatchAuthority(authority)

	authID := "steering-dispatch-lifecycle"
	model := "steering-dispatch-lifecycle-model"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Initial create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}

	// Read r1 created and r1 completed
	for i := 0; i < 2; i++ {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("read r1 (%d): %v", i, err)
		}
	}

	// Verify initial admission occurred and has not been released yet
	if !authority.initialAdmitted.Load() {
		t.Fatal("initial request was not admitted through dispatch authority")
	}
	if authority.initialReleased.Load() {
		t.Fatal("initial conductor admission released before stream termination")
	}

	// Client sends successor explicit create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}

	// Read r2 created
	_, b, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read r2 created: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r2" {
		t.Fatalf("unexpected message for r2: %s", b)
	}

	// Assert: After second frame is sent/created but before terminal, successor admission must still be held.
	if !authority.successorAdmitted.Load() {
		t.Fatal("successor generation was not admitted through dispatch authority")
	}
	if authority.successorReleased.Load() {
		t.Fatal("successor admission released before terminal")
	}
	if authority.initialReleased.Load() {
		t.Fatal("initial conductor admission released before stream termination")
	}

	// Signal upstream to send successor terminal
	signalReady()

	// Read r2 completed
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read r2 completed: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.completed" || gjson.GetBytes(b, "response.id").String() != "r2" {
		t.Fatalf("unexpected message for r2 terminal: %s", b)
	}

	// Assert: On its terminal, successor admission must release.
	select {
	case <-authority.successorReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("successor admission was not released on terminal")
	}

	// Initial conductor ownership follows existing stream lifetime (still alive).
	if authority.initialReleased.Load() {
		t.Fatal("initial conductor admission released before stream termination")
	}

	// Socket close/cancellation must release everything eventually.
	_ = c.Close()

	select {
	case <-authority.initialReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("initial conductor admission was not released on stream close")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream cleanup stalled")
	}

	if authority.initialReleaseCount.Load() != 1 {
		t.Fatalf("expected initial release callback called exactly once, got %d", authority.initialReleaseCount.Load())
	}
	if authority.successorReleaseCount.Load() != 1 {
		t.Fatalf("expected successor release callback called exactly once, got %d", authority.successorReleaseCount.Load())
	}
	if authority.admitCalls.Load() != 2 {
		t.Fatalf("expected exactly 2 admissions, got %d", authority.admitCalls.Load())
	}
}

func TestResponsesSteeringDispatchAutomaticSuccessorRetainsOwnedLease(t *testing.T) {
	var frames atomic.Int32
	done := make(chan struct{})
	readyForTerminal := make(chan struct{})
	var closeOnce sync.Once
	signalReady := func() { closeOnce.Do(func() { close(readyForTerminal) }) }
	defer signalReady()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

		// 1. Initial create
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read initial: %v", err)
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

		// 2. Steer targeting completed parent r1
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read steer: %v", err)
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r1"}}`))

		// 3. Upstream creates automatic successor r2
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2","previous_response_id":"r1"}}`))

		// Wait for signal from downstream test
		select {
		case <-readyForTerminal:
		case <-time.After(3 * time.Second):
			t.Error("upstream timed out waiting for readyForTerminal")
			return
		}

		// 4. Successor r2 completed
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r2","output":[]}}`))

		// Wait for downstream client to close
		_, _, _ = c.ReadMessage()
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	authority := newFakeDispatchAuthority()
	manager.SetDispatchAuthority(authority)

	authID := "steering-dispatch-auto-successor"
	model := "steering-dispatch-auto-successor-model"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Initial create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}

	// Read r1 created and r1 completed
	for i := 0; i < 2; i++ {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("read r1 (%d): %v", i, err)
		}
	}

	// Steer targeting completed parent r1 (requires fresh admission)
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"steer successor"}`)); err != nil {
		t.Fatal(err)
	}

	// Read steer.accepted
	_, b, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read steer.accepted: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
		t.Fatalf("expected response.steer.accepted, got: %s", b)
	}

	// Read r2 created (automatic successor)
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read r2 created: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r2" {
		t.Fatalf("unexpected message for r2: %s", b)
	}

	// Assert: Successor lease must be held while r2 is running
	if !authority.successorAdmitted.Load() {
		t.Fatal("steer successor generation was not admitted")
	}
	if authority.successorReleased.Load() {
		t.Fatal("successor admission was released before terminal")
	}

	// Signal upstream to complete r2
	signalReady()

	// Read r2 completed
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read r2 completed: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.completed" || gjson.GetBytes(b, "response.id").String() != "r2" {
		t.Fatalf("unexpected message for r2 terminal: %s", b)
	}

	// Assert: On terminal, successor lease must release
	select {
	case <-authority.successorReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("successor admission was not released on terminal")
	}

	// Close downstream client
	_ = c.Close()

	select {
	case <-authority.initialReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("initial conductor admission was not released on stream close")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream cleanup stalled")
	}

	if authority.initialReleaseCount.Load() != 1 {
		t.Fatalf("expected initial release callback called exactly once, got %d", authority.initialReleaseCount.Load())
	}
	if authority.successorReleaseCount.Load() != 1 {
		t.Fatalf("expected successor release callback called exactly once, got %d", authority.successorReleaseCount.Load())
	}
	if authority.admitCalls.Load() != 2 {
		t.Fatalf("expected exactly 2 admissions, got %d", authority.admitCalls.Load())
	}
}

func TestResponsesSteeringDispatchRequiredInputRefusesContinuationWhenDenied(t *testing.T) {
	var frames atomic.Int32
	done := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

		// 1. Initial create
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read initial: %v", err)
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))

		// 2. Read in-flight steer
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read steer: %v", err)
			return
		}
		frames.Add(1)

		// 3. Upstream responds with pending (waiting for required input) and completes r1
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.pending","steer":{"id":"s1","previous_response_id":"r1"},"reason":"waiting_for_required_input"}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

		// Upstream should NOT receive a third frame when continuation is denied
		if _, _, err := c.ReadMessage(); err == nil {
			frames.Add(1)
			_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "unexpected continuation frame"))
			return
		}
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	authority := newFakeDispatchAuthority()
	manager.SetDispatchAuthority(authority)

	authID := "steering-dispatch-required-input"
	model := "steering-dispatch-required-input-model"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Initial create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}

	// Read r1 created
	if _, _, err := c.ReadMessage(); err != nil {
		t.Fatalf("read r1 created: %v", err)
	}

	// Send in-flight steer
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"in-flight steer"}`)); err != nil {
		t.Fatal(err)
	}

	// Read steer.pending and r1 completed
	for i := 0; i < 2; i++ {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("read message (%d): %v", i, err)
		}
	}

	// Deny new admissions
	authority.CloseAdmissions()

	// Send continuation for required input
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.append","previous_response_id":"r1","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"tool_output"}]}]}`)); err != nil {
		t.Fatal(err)
	}

	// Downstream should receive rejection or socket close
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, errRead := c.ReadMessage()
	if errRead == nil {
		msgType := gjson.GetBytes(msg, "type").String()
		if msgType != "error" && !strings.Contains(msgType, "fail") && !strings.Contains(msgType, "reject") {
			t.Fatalf("expected rejection or close, got message type %s: %s", msgType, msg)
		}
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream cleanup stalled")
	}

	if frames.Load() != 2 {
		t.Fatalf("expected exactly 2 frames at upstream (create + steer), got %d", frames.Load())
	}
}

func TestResponsesSteeringDispatchCleanupReleasesAllOwnership(t *testing.T) {
	var frames atomic.Int32
	done := make(chan struct{})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

		// 1. Initial create
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read initial: %v", err)
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

		// 2. Successor create
		if _, _, err := c.ReadMessage(); err != nil {
			t.Errorf("upstream read successor: %v", err)
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2"}}`))

		// Wait for downstream client to abruptly disconnect
		_, _, _ = c.ReadMessage()
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	authority := newFakeDispatchAuthority()
	manager.SetDispatchAuthority(authority)

	authID := "steering-dispatch-cleanup"
	model := "steering-dispatch-cleanup-model"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Initial create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}

	// Read r1 created and r1 completed
	for i := 0; i < 2; i++ {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("read r1 (%d): %v", i, err)
		}
	}

	// Successor explicit create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}

	// Read r2 created
	if _, _, err := c.ReadMessage(); err != nil {
		t.Fatalf("read r2 created: %v", err)
	}

	// Assert: Successor is admitted and not yet released
	if !authority.successorAdmitted.Load() {
		t.Fatal("successor was not admitted")
	}
	if authority.successorReleased.Load() {
		t.Fatal("successor was prematurely released")
	}

	// Abruptly close client while r2 is still running
	_ = c.Close()

	// Assert: Both initial and successor release callbacks must be called on cleanup
	select {
	case <-authority.successorReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("successor admission was not released on client disconnect")
	}

	select {
	case <-authority.initialReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("initial admission was not released on stream close")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream cleanup stalled")
	}

	if authority.initialReleaseCount.Load() != 1 {
		t.Fatalf("expected initial release called exactly once, got %d", authority.initialReleaseCount.Load())
	}
	if authority.successorReleaseCount.Load() != 1 {
		t.Fatalf("expected successor release called exactly once, got %d", authority.successorReleaseCount.Load())
	}
	if authority.totalReleases.Load() != 2 {
		t.Fatalf("expected exactly 2 total releases, got %d", authority.totalReleases.Load())
	}
}

func TestResponsesSteeringDispatchAutomaticSuccessorGroupLifecycle(t *testing.T) {
	var frames atomic.Int32
	done := make(chan struct{})
	readyForR3Terminal := make(chan struct{})
	var closeR3TerminalOnce sync.Once
	signalR3Terminal := func() { closeR3TerminalOnce.Do(func() { close(readyForR3Terminal) }) }
	defer signalR3Terminal()

	var upstreamConn atomic.Pointer[websocket.Conn]
	upstreamErrs := make(chan error, 16)
	recordErr := func(err error) {
		if err != nil && !isTeardownErr(err) {
			select {
			case upstreamErrs <- err:
			default:
			}
		}
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			recordErr(err)
			return
		}
		upstreamConn.Store(c)
		defer func() {
			upstreamConn.Store(nil)
			_ = c.Close()
		}()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

		// 1. Initial create
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read initial: %w", err))
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

		// 2. Explicit create r2
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read explicit r2: %w", err))
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2"}}`))

		// 3. In-flight active steer s1 targeting active r2
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read active steer s1: %w", err))
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r2"}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r2","output":[]}}`))

		// 4. Idle steer s2 targeting completed r2
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read idle steer s2: %w", err))
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s2","previous_response_id":"r2"}}`))

		// 5. Automatic successor r3 targeting r2
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r3","previous_response_id":"r2"}}`))

		// Wait for downstream assertion that both L2 and L3 are held before r3 terminal
		select {
		case <-readyForR3Terminal:
		case <-time.After(3 * time.Second):
			recordErr(errors.New("upstream timed out waiting for readyForR3Terminal signal"))
			return
		}

		// 6. Successor r3 terminal
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r3","output":[]}}`))

		// 7. Further generation explicit create r4
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read explicit r4: %w", err))
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r4"}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r4","output":[]}}`))

		// Wait for downstream client to close
		_, _, _ = c.ReadMessage()
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	authority := newFakeDispatchAuthority()
	manager.SetDispatchAuthority(authority)

	authID := "steering-dispatch-group-lifecycle"
	model := "steering-dispatch-group-lifecycle-model"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		signalR3Terminal()
		_ = c.Close()
		if uc := upstreamConn.Load(); uc != nil {
			_ = uc.Close()
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		close(upstreamErrs)
		for err := range upstreamErrs {
			t.Errorf("upstream error: %v", err)
		}
	}()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// 1. Initial create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("read r1 (%d): %v", i, err)
		}
	}
	if !authority.initialAdmitted.Load() {
		t.Fatal("initial request was not admitted through dispatch authority")
	}
	if authority.initialReleased.Load() {
		t.Fatal("initial conductor admission released before stream termination")
	}

	// 2. Explicit create r2 (admits L2)
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}
	_, b, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read r2 created: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r2" {
		t.Fatalf("unexpected message for r2 created: %s", b)
	}
	if !authority.successorAdmitted.Load() {
		t.Fatal("r2 was not admitted (call 2)")
	}

	// 3. Active steer s1 targeting active r2 (borrows L2)
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r2","input":"active steer s1"}`)); err != nil {
		t.Fatal(err)
	}
	// Read s1 accepted and r2 completed
	for i := 0; i < 2; i++ {
		_, b, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read after s1 (%d): %v", i, err)
		}
		switch gjson.GetBytes(b, "type").String() {
		case "response.steer.accepted":
		case "response.completed":
		default:
			t.Fatalf("unexpected event: %s", b)
		}
	}
	// r2 completed retained L2 because s1 was targeting r2; L2 must not be released yet!
	if authority.successorReleased.Load() {
		t.Fatal("L2 was prematurely released when r2 completed with active steer targeting it")
	}
	if authority.admitCalls.Load() != 2 {
		t.Fatalf("expected 2 admission calls so far, got %d", authority.admitCalls.Load())
	}

	// 4. Idle steer s2 targeting completed r2 (admits L3)
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r2","input":"idle steer s2"}`)); err != nil {
		t.Fatal(err)
	}
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read s2 accepted: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
		t.Fatalf("expected response.steer.accepted for s2, got: %s", b)
	}
	if !authority.thirdAdmitted.Load() {
		t.Fatal("idle steer s2 was not admitted (call 3)")
	}
	if authority.thirdReleased.Load() {
		t.Fatal("L3 was prematurely released before r3 terminal")
	}

	// 5. Read automatic successor r3 created
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read r3 created: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r3" {
		t.Fatalf("unexpected message for r3 created: %s", b)
	}

	// Assert: Both L2 and L3 must be held before r3 terminal
	if authority.successorReleased.Load() {
		t.Fatal("L2 released prematurely while automatic successor r3 is active")
	}
	if authority.thirdReleased.Load() {
		t.Fatal("L3 released prematurely while automatic successor r3 is active")
	}
	if authority.initialReleased.Load() {
		t.Fatal("initial conductor lease released before socket close")
	}

	// Signal upstream to complete r3
	signalR3Terminal()

	// 6. Read r3 completed
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read r3 completed: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.completed" || gjson.GetBytes(b, "response.id").String() != "r3" {
		t.Fatalf("unexpected message for r3 completed: %s", b)
	}

	// Assert: Both L2 and L3 released after r3 terminal while socket remains open
	select {
	case <-authority.successorReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("L2 was not released on r3 terminal")
	}
	select {
	case <-authority.thirdReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("L3 was not released on r3 terminal")
	}
	if authority.initialReleased.Load() {
		t.Fatal("initial conductor lease released before socket close")
	}

	// 7. Further generation adds/releases normally (explicit create r4 admits L4)
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}
	// Read r4 created and r4 completed
	for i := 0; i < 2; i++ {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("read r4 (%d): %v", i, err)
		}
	}
	if !authority.fourthAdmitted.Load() {
		t.Fatal("r4 was not admitted (call 4)")
	}
	select {
	case <-authority.fourthReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("L4 was not released on r4 terminal")
	}
	if authority.initialReleased.Load() {
		t.Fatal("initial conductor lease released before socket close")
	}

	// 8. Initial conductor releases on close
	_ = c.Close()
	select {
	case <-authority.initialReleasedChan:
	case <-time.After(3 * time.Second):
		t.Fatal("initial conductor admission was not released on stream close")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream cleanup stalled")
	}

	if authority.initialReleaseCount.Load() != 1 {
		t.Fatalf("expected initial release count = 1, got %d", authority.initialReleaseCount.Load())
	}
	if authority.successorReleaseCount.Load() != 1 {
		t.Fatalf("expected successor (L2) release count = 1, got %d", authority.successorReleaseCount.Load())
	}
	if authority.thirdReleaseCount.Load() != 1 {
		t.Fatalf("expected third (L3) release count = 1, got %d", authority.thirdReleaseCount.Load())
	}
	if authority.fourthReleaseCount.Load() != 1 {
		t.Fatalf("expected fourth (L4) release count = 1, got %d", authority.fourthReleaseCount.Load())
	}
	if authority.totalReleases.Load() != 4 {
		t.Fatalf("expected exactly 4 total releases, got %d", authority.totalReleases.Load())
	}
	if frames.Load() != 5 {
		t.Fatalf("expected 5 frames at upstream, got %d", frames.Load())
	}
}

func TestResponsesSteeringDispatchDelayedAckAndRequiredInputCorrelation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		delayedType string
	}{
		{
			name:        "delayed_accepted",
			delayedType: "response.steer.accepted",
		},
		{
			name:        "delayed_failed",
			delayedType: "response.steer.failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var frames atomic.Int32
			done := make(chan struct{})
			readyForDelayedS2 := make(chan struct{})
			var closeDelayedS2Once sync.Once
			signalDelayedS2 := func() { closeDelayedS2Once.Do(func() { close(readyForDelayedS2) }) }
			defer signalDelayedS2()

			readyForR2Terminal := make(chan struct{})
			var closeR2TerminalOnce sync.Once
			signalR2Terminal := func() { closeR2TerminalOnce.Do(func() { close(readyForR2Terminal) }) }
			defer signalR2Terminal()

			var upstreamConn atomic.Pointer[websocket.Conn]
			upstreamErrs := make(chan error, 16)
			recordErr := func(err error) {
				if err != nil && !isTeardownErr(err) {
					select {
					case upstreamErrs <- err:
					default:
					}
				}
			}

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					recordErr(err)
					return
				}
				upstreamConn.Store(c)
				defer func() {
					upstreamConn.Store(nil)
					_ = c.Close()
				}()
				_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

				// 1. Initial create
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read initial: %w", err))
					return
				}
				frames.Add(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

				// 2. Steer s1 targeting completed r1
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read steer s1: %w", err))
					return
				}
				frames.Add(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r1"}}`))

				// 3. Steer s2 targeting completed r1
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read steer s2: %w", err))
					return
				}
				frames.Add(1)
				// Do NOT ack s2 yet. Upstream sends steer.pending for s1 (waiting for required input)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.pending","steer":{"id":"s1","previous_response_id":"r1"},"reason":"waiting_for_required_input"}`))

				// Gate sending delayed s2 ACK/failure on test signal AFTER held assertion
				select {
				case <-readyForDelayedS2:
				case <-time.After(3 * time.Second):
					recordErr(errors.New("upstream timed out waiting for readyForDelayedS2 signal"))
					return
				}

				// 4. Now send delayed ack/fail for s2
				if tc.delayedType == "response.steer.accepted" {
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s2","previous_response_id":"r1"}}`))
				} else {
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.failed","steer":{"id":"s2","previous_response_id":"r1"},"error":{"message":"steer failed"}}`))
				}

				// 5. Client continuation (response.append) for required input
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read continuation: %w", err))
					return
				}
				frames.Add(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2","previous_response_id":"r1"}}`))

				// Gate r2 completed until controller test has checked L4 held
				select {
				case <-readyForR2Terminal:
				case <-time.After(3 * time.Second):
					recordErr(errors.New("upstream timed out waiting for readyForR2Terminal signal"))
					return
				}

				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r2","output":[]}}`))

				// Wait for downstream client to close
				_, _, _ = c.ReadMessage()
			}))
			defer upstream.Close()

			cfg := &config.Config{}
			cfg.Codex.ResponseSteering = true
			cfg.CodexResponseSteering = true
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
			authority := newFakeDispatchAuthority()
			manager.SetDispatchAuthority(authority)

			authID := "steering-dispatch-delayed-" + tc.name
			model := "steering-dispatch-delayed-model-" + tc.name
			if _, err := manager.Register(context.Background(), &coreauth.Auth{
				ID: authID, Provider: "codex", Status: coreauth.StatusActive,
				Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
			}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
			defer registry.GetGlobalRegistry().UnregisterClient(authID)

			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
			router := gin.New()
			router.GET("/v1/responses", h.ResponsesWebsocket)
			downstream := httptest.NewServer(router)
			defer downstream.Close()

			c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				signalDelayedS2()
				signalR2Terminal()
				_ = c.Close()
				if uc := upstreamConn.Load(); uc != nil {
					_ = uc.Close()
				}
				select {
				case <-done:
				case <-time.After(3 * time.Second):
				}
				close(upstreamErrs)
				for err := range upstreamErrs {
					t.Errorf("upstream error: %v", err)
				}
			}()
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

			// Initial create
			if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if _, _, err := c.ReadMessage(); err != nil {
					t.Fatalf("read r1 (%d): %v", i, err)
				}
			}

			// Steer s1 targeting r1 (admits L2)
			if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"steer s1"}`)); err != nil {
				t.Fatal(err)
			}
			_, b, err := c.ReadMessage()
			if err != nil {
				t.Fatalf("read s1 accepted: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
				t.Fatalf("expected s1 accepted, got: %s", b)
			}
			if !authority.successorAdmitted.Load() {
				t.Fatal("s1 was not admitted (L2)")
			}

			// Steer s2 targeting r1 (admits L3)
			if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"steer s2"}`)); err != nil {
				t.Fatal(err)
			}

			// Read response.steer.pending for s1
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read steer.pending: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.steer.pending" {
				t.Fatalf("expected response.steer.pending, got: %s", b)
			}

			// Assert s2 was admitted (L3)
			if !authority.thirdAdmitted.Load() {
				t.Fatal("s2 was not admitted (L3)")
			}

			// On response.steer.pending:
			// s1's lease (L2) is released
			select {
			case <-authority.successorReleasedChan:
			case <-time.After(3 * time.Second):
				t.Fatal("L2 was not released on steer.pending")
			}
			// s2's lease (L3) stays held!
			if authority.thirdReleased.Load() {
				t.Fatal("L3 was prematurely released on steer.pending")
			}

			// Signal upstream to send delayed ack/fail for s2
			signalDelayedS2()

			// Read delayed ack/fail for s2
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read delayed s2 event: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != tc.delayedType {
				t.Fatalf("expected %s, got: %s", tc.delayedType, b)
			}

			if tc.delayedType == "response.steer.failed" {
				select {
				case <-authority.thirdReleasedChan:
				case <-time.After(3 * time.Second):
					t.Fatal("L3 was not released on s2 failure")
				}
			} else {
				if authority.thirdReleased.Load() {
					t.Fatal("L3 was prematurely released on s2 accepted")
				}
			}

			// Send explicit continuation (response.append) for required input
			if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.append","previous_response_id":"r1","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"tool_result"}]}]}`)); err != nil {
				t.Fatal(err)
			}

			// Read r2 created
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read r2 created: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r2" {
				t.Fatalf("unexpected message for r2: %s", b)
			}

			// Assert: r2 lease (L4) must be admitted and held
			if !authority.fourthAdmitted.Load() {
				t.Fatal("r2 continuation was not admitted (L4)")
			}
			if authority.fourthReleased.Load() {
				t.Fatal("L4 was prematurely released; delayed ACK/fail must not consume or release later generation lease")
			}

			// If s2 was delayed_accepted, s2 was obsolete accepted steer targeting r1, so r2 continuation supersedes it and releases L3
			if tc.delayedType == "response.steer.accepted" {
				select {
				case <-authority.thirdReleasedChan:
				case <-time.After(3 * time.Second):
					t.Fatal("L3 was not released when r2 continuation superseded obsolete accepted s2")
				}
			}

			// Unblock r2 completed
			signalR2Terminal()

			// Read r2 completed
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read r2 completed: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.completed" || gjson.GetBytes(b, "response.id").String() != "r2" {
				t.Fatalf("unexpected message for r2 completed: %s", b)
			}

			// Assert: L4 released on r2 terminal
			select {
			case <-authority.fourthReleasedChan:
			case <-time.After(3 * time.Second):
				t.Fatal("L4 was not released on r2 terminal")
			}

			// Close client connection
			_ = c.Close()
			select {
			case <-authority.initialReleasedChan:
			case <-time.After(3 * time.Second):
				t.Fatal("initial conductor admission was not released on stream close")
			}

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream cleanup stalled")
			}

			if authority.initialReleaseCount.Load() != 1 {
				t.Fatalf("expected initial release count = 1, got %d", authority.initialReleaseCount.Load())
			}
			if authority.successorReleaseCount.Load() != 1 {
				t.Fatalf("expected L2 release count = 1, got %d", authority.successorReleaseCount.Load())
			}
			if authority.thirdReleaseCount.Load() != 1 {
				t.Fatalf("expected L3 release count = 1, got %d", authority.thirdReleaseCount.Load())
			}
			if authority.fourthReleaseCount.Load() != 1 {
				t.Fatalf("expected L4 release count = 1, got %d", authority.fourthReleaseCount.Load())
			}
			if authority.totalReleases.Load() != 4 {
				t.Fatalf("expected total releases = 4, got %d", authority.totalReleases.Load())
			}
		})
	}
}

type authCheckWrapperExecutor struct {
	coreauth.ProviderExecutor
	check func(authID string) bool
}

func (w *authCheckWrapperExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if w.check != nil {
		ctx = cliproxyexecutor.WithWebsocketAuthCheck(ctx, w.check)
	}
	return w.ProviderExecutor.ExecuteStream(ctx, auth, req, opts)
}

func TestResponsesSteeringDispatchPendingBeforeWrite(t *testing.T) {
	for _, tc := range []struct {
		name     string
		activeR2 bool
	}{
		{
			name:     "fresh_admission_idle_steer",
			activeR2: false,
		},
		{
			name:     "active_inherited_lease",
			activeR2: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var frames atomic.Int32
			done := make(chan struct{})

			pausedWriter := make(chan struct{})
			resumeWriter := make(chan struct{})
			var closePausedOnce, closeResumeOnce sync.Once
			signalPaused := func() { closePausedOnce.Do(func() { close(pausedWriter) }) }
			signalResume := func() { closeResumeOnce.Do(func() { close(resumeWriter) }) }
			defer signalResume()

			var pauseSteer2 atomic.Bool
			var steer2CheckCalls atomic.Int32

			var upstreamConn atomic.Pointer[websocket.Conn]
			upstreamErrs := make(chan error, 16)
			recordErr := func(err error) {
				if err != nil && !isTeardownErr(err) {
					select {
					case upstreamErrs <- err:
					default:
					}
				}
			}

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					recordErr(err)
					return
				}
				upstreamConn.Store(c)
				defer func() {
					upstreamConn.Store(nil)
					_ = c.Close()
				}()
				_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

				// 1. Initial create
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read initial: %w", err))
					return
				}
				frames.Add(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

				if tc.activeR2 {
					// 2. Explicit create r2 (leaves r2 active)
					if _, _, err := c.ReadMessage(); err != nil {
						recordErr(fmt.Errorf("upstream read explicit r2: %w", err))
						return
					}
					frames.Add(1)
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2"}}`))

					// 3. Active steer s1 targeting active r2
					if _, _, err := c.ReadMessage(); err != nil {
						recordErr(fmt.Errorf("upstream read steer s1: %w", err))
						return
					}
					frames.Add(1)
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r2"}}`))

					// Wait until downstream confirms writer is paused for s2 before sending r2 completed and s1 pending
					select {
					case <-pausedWriter:
					case <-time.After(3 * time.Second):
						recordErr(errors.New("upstream timed out waiting for pausedWriter signal"))
						return
					}

					// Complete r2 (releases active handle) and send pending for s1 (releases s1 handle)
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r2","output":[]}}`))
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.pending","steer":{"id":"s1","previous_response_id":"r2"},"reason":"waiting_for_required_input"}`))

					// 4. Read resumed s2
					if _, _, err := c.ReadMessage(); err != nil {
						recordErr(fmt.Errorf("upstream read resumed s2: %w", err))
						return
					}
					frames.Add(1)
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s2","previous_response_id":"r2"}}`))
				} else {
					// 2. Steer s1 targeting completed r1
					if _, _, err := c.ReadMessage(); err != nil {
						recordErr(fmt.Errorf("upstream read steer s1: %w", err))
						return
					}
					frames.Add(1)
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r1"}}`))

					// Wait until downstream confirms writer is paused for s2 before sending s1 pending
					select {
					case <-pausedWriter:
					case <-time.After(3 * time.Second):
						recordErr(errors.New("upstream timed out waiting for pausedWriter signal"))
						return
					}

					// Send pending for s1 (releases s1 handle)
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.pending","steer":{"id":"s1","previous_response_id":"r1"},"reason":"waiting_for_required_input"}`))

					// 3. Read resumed s2
					if _, _, err := c.ReadMessage(); err != nil {
						recordErr(fmt.Errorf("upstream read resumed s2: %w", err))
						return
					}
					frames.Add(1)
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s2","previous_response_id":"r1"}}`))
				}

				// Wait for downstream client to close
				_, _, _ = c.ReadMessage()
			}))
			defer upstream.Close()

			cfg := &config.Config{}
			cfg.Codex.ResponseSteering = true
			cfg.CodexResponseSteering = true
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)

			wrapper := &authCheckWrapperExecutor{
				ProviderExecutor: runtimeexecutor.NewCodexAutoExecutor(cfg),
				check: func(authID string) bool {
					if pauseSteer2.Load() {
						call := steer2CheckCalls.Add(1)
						if call == 2 {
							// Paused AFTER state registration in unacknowledgedSteers but before WriteMessage
							signalPaused()
							<-resumeWriter
						}
					}
					return true
				},
			}
			manager.RegisterExecutor(wrapper)
			authority := newFakeDispatchAuthority()
			manager.SetDispatchAuthority(authority)

			authID := "steering-dispatch-pbw-" + tc.name
			model := "steering-dispatch-pbw-model-" + tc.name
			if _, err := manager.Register(context.Background(), &coreauth.Auth{
				ID: authID, Provider: "codex", Status: coreauth.StatusActive,
				Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
			}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
			defer registry.GetGlobalRegistry().UnregisterClient(authID)

			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
			router := gin.New()
			router.GET("/v1/responses", h.ResponsesWebsocket)
			downstream := httptest.NewServer(router)
			defer downstream.Close()

			c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				signalPaused()
				signalResume()
				_ = c.Close()
				if uc := upstreamConn.Load(); uc != nil {
					_ = uc.Close()
				}
				select {
				case <-done:
				case <-time.After(3 * time.Second):
				}
				close(upstreamErrs)
				for err := range upstreamErrs {
					t.Errorf("upstream error: %v", err)
				}
			}()
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

			// Initial create
			if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if _, _, err := c.ReadMessage(); err != nil {
					t.Fatalf("read r1 (%d): %v", i, err)
				}
			}

			if tc.activeR2 {
				// Explicit create r2
				if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
					t.Fatal(err)
				}
				_, b, err := c.ReadMessage()
				if err != nil {
					t.Fatalf("read r2 created: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r2" {
					t.Fatalf("unexpected message for r2: %s", b)
				}
				if !authority.successorAdmitted.Load() {
					t.Fatal("r2 was not admitted (L2)")
				}

				// Active steer s1 targeting active r2
				if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r2","input":"steer s1"}`)); err != nil {
					t.Fatal(err)
				}
				_, b, err = c.ReadMessage()
				if err != nil {
					t.Fatalf("read s1 accepted: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
					t.Fatalf("expected s1 accepted, got: %s", b)
				}

				// Arm pause for s2, then send s2 targeting active r2
				pauseSteer2.Store(true)
				if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r2","input":"steer s2"}`)); err != nil {
					t.Fatal(err)
				}

				// Wait for writer to pause after s2 state registration
				select {
				case <-pausedWriter:
				case <-time.After(3 * time.Second):
					t.Fatal("timed out waiting for writer to pause at s2")
				}

				// Assert L2 still live before upstream events
				if authority.successorReleased.Load() {
					t.Fatal("L2 prematurely released before r2 completion / s1 pending")
				}

				// Read r2 completed and s1 pending
				for i := 0; i < 2; i++ {
					_, b, err := c.ReadMessage()
					if err != nil {
						t.Fatalf("read after s2 paused (%d): %v", i, err)
					}
					switch gjson.GetBytes(b, "type").String() {
					case "response.completed":
					case "response.steer.pending":
					default:
						t.Fatalf("unexpected event: %s", b)
					}
				}

				// Assert: Even after r2 completed AND older s1 pending, unsent s2 still preserves L2!
				if authority.successorReleased.Load() {
					t.Fatal("L2 was prematurely released: unsent s2 retained handle must keep root alive")
				}

				// Resume writer to send s2 frame
				signalResume()

				// Read s2 accepted
				_, b, err = c.ReadMessage()
				if err != nil {
					t.Fatalf("read s2 accepted: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
					t.Fatalf("expected s2 accepted, got: %s", b)
				}

				// Close connection; now L2 is released on stream cleanup
				_ = c.Close()
				select {
				case <-authority.successorReleasedChan:
				case <-time.After(3 * time.Second):
					t.Fatal("L2 was not released on stream close")
				}
			} else {
				// Steer s1 targeting completed r1 (admits L2)
				if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"steer s1"}`)); err != nil {
					t.Fatal(err)
				}
				_, b, err := c.ReadMessage()
				if err != nil {
					t.Fatalf("read s1 accepted: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
					t.Fatalf("expected s1 accepted, got: %s", b)
				}
				if !authority.successorAdmitted.Load() {
					t.Fatal("s1 was not admitted (L2)")
				}

				// Arm pause for s2, then send s2 targeting completed r1 (admits L3)
				pauseSteer2.Store(true)
				if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"steer s2"}`)); err != nil {
					t.Fatal(err)
				}

				// Wait for writer to pause after s2 state registration
				select {
				case <-pausedWriter:
				case <-time.After(3 * time.Second):
					t.Fatal("timed out waiting for writer to pause at s2")
				}

				// At this point s2 was admitted (L3) and registered
				if !authority.thirdAdmitted.Load() {
					t.Fatal("s2 was not admitted (L3)")
				}
				if authority.thirdReleased.Load() {
					t.Fatal("L3 was prematurely released before write")
				}

				// Read s1 pending
				_, b, err = c.ReadMessage()
				if err != nil {
					t.Fatalf("read s1 pending: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.steer.pending" {
					t.Fatalf("expected response.steer.pending for s1, got: %s", b)
				}

				// Older s1 lease (L2) released
				select {
				case <-authority.successorReleasedChan:
				case <-time.After(3 * time.Second):
					t.Fatal("L2 was not released on s1 steer.pending")
				}

				// Assert: s2 admission (L3) MUST STILL BE HELD!
				if authority.thirdReleased.Load() {
					t.Fatal("L3 was prematurely released by older s1 steer.pending while s2 was paused before write")
				}

				// Resume writer to send s2 frame
				signalResume()

				// Read s2 accepted
				_, b, err = c.ReadMessage()
				if err != nil {
					t.Fatalf("read s2 accepted: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
					t.Fatalf("expected s2 accepted, got: %s", b)
				}

				// Close connection; now L3 is released on stream cleanup
				_ = c.Close()
				select {
				case <-authority.thirdReleasedChan:
				case <-time.After(3 * time.Second):
					t.Fatal("L3 was not released on stream close")
				}
			}

			// Initial conductor lease releases on close
			select {
			case <-authority.initialReleasedChan:
			case <-time.After(3 * time.Second):
				t.Fatal("initial conductor admission was not released on stream close")
			}

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream cleanup stalled")
			}
		})
	}
}

func TestResponsesSteeringDispatchPendingFailedWithUnacknowledgedSuccessor(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		pendingBeforeAccepted bool
	}{
		{
			name:                  "pending_after_accepted",
			pendingBeforeAccepted: false,
		},
		{
			name:                  "pending_before_accepted",
			pendingBeforeAccepted: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var frames atomic.Int32
			done := make(chan struct{})
			readyForR2Terminal := make(chan struct{})
			var closeR2TerminalOnce sync.Once
			signalR2Terminal := func() { closeR2TerminalOnce.Do(func() { close(readyForR2Terminal) }) }
			defer signalR2Terminal()

			var upstreamConn atomic.Pointer[websocket.Conn]
			upstreamErrs := make(chan error, 16)
			recordErr := func(err error) {
				if err != nil && !isTeardownErr(err) {
					select {
					case upstreamErrs <- err:
					default:
					}
				}
			}

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					recordErr(err)
					return
				}
				upstreamConn.Store(c)
				defer func() {
					upstreamConn.Store(nil)
					_ = c.Close()
				}()
				_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

				// 1. Initial create
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read initial: %w", err))
					return
				}
				frames.Add(1)
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

				// 2. Steer s1 targeting r1
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read steer s1: %w", err))
					return
				}
				frames.Add(1)

				if tc.pendingBeforeAccepted {
					// Send steer.pending BEFORE steer.accepted for s1
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.pending","steer":{"id":"s1","previous_response_id":"r1"},"reason":"waiting_for_required_input"}`))
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r1"}}`))
				} else {
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r1"}}`))
				}

				// 3. Steer s2 targeting r1 (remains unacked while s1 fails)
				if _, _, err := c.ReadMessage(); err != nil {
					recordErr(fmt.Errorf("upstream read steer s2: %w", err))
					return
				}
				frames.Add(1)

				if !tc.pendingBeforeAccepted {
					// Send steer.pending for s1
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.pending","steer":{"id":"s1","previous_response_id":"r1"},"reason":"waiting_for_required_input"}`))
				}

				// Upstream sends steer.failed for s1 while s2 is still unacked
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.failed","steer":{"id":"s1","previous_response_id":"r1"},"error":{"message":"s1 failed"}}`))

				// Now ack s2
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s2","previous_response_id":"r1"}}`))

				// Upstream triggers automatic successor r2 targeting r1
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2","previous_response_id":"r1"}}`))

				// Gate r2 completed until downstream asserts L3 held
				select {
				case <-readyForR2Terminal:
				case <-time.After(3 * time.Second):
					recordErr(errors.New("upstream timed out waiting for readyForR2Terminal signal"))
					return
				}

				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r2","output":[]}}`))

				// Wait for downstream close
				_, _, _ = c.ReadMessage()
			}))
			defer upstream.Close()

			cfg := &config.Config{}
			cfg.Codex.ResponseSteering = true
			cfg.CodexResponseSteering = true
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
			authority := newFakeDispatchAuthority()
			manager.SetDispatchAuthority(authority)

			authID := "steering-dispatch-pending-failed-" + tc.name
			model := "steering-dispatch-pending-failed-model-" + tc.name
			if _, err := manager.Register(context.Background(), &coreauth.Auth{
				ID: authID, Provider: "codex", Status: coreauth.StatusActive,
				Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
			}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
			defer registry.GetGlobalRegistry().UnregisterClient(authID)

			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
			router := gin.New()
			router.GET("/v1/responses", h.ResponsesWebsocket)
			downstream := httptest.NewServer(router)
			defer downstream.Close()

			c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				signalR2Terminal()
				_ = c.Close()
				if uc := upstreamConn.Load(); uc != nil {
					_ = uc.Close()
				}
				select {
				case <-done:
				case <-time.After(3 * time.Second):
				}
				close(upstreamErrs)
				for err := range upstreamErrs {
					t.Errorf("upstream error: %v", err)
				}
			}()
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

			// Initial create
			if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if _, _, err := c.ReadMessage(); err != nil {
					t.Fatalf("read r1 (%d): %v", i, err)
				}
			}

			// Steer s1 targeting r1 (admits L2)
			var b []byte
			if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"steer s1"}`)); err != nil {
				t.Fatal(err)
			}
			if tc.pendingBeforeAccepted {
				// Read s1 pending then s1 accepted
				for i := 0; i < 2; i++ {
					_, b, err = c.ReadMessage()
					if err != nil {
						t.Fatalf("read s1 pending/accepted (%d): %v", i, err)
					}
					tStr := gjson.GetBytes(b, "type").String()
					if tStr != "response.steer.pending" && tStr != "response.steer.accepted" {
						t.Fatalf("unexpected message: %s", b)
					}
				}
			} else {
				// Read s1 accepted
				_, b, err = c.ReadMessage()
				if err != nil {
					t.Fatalf("read s1 accepted: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
					t.Fatalf("expected s1 accepted, got: %s", b)
				}
			}
			if !authority.successorAdmitted.Load() {
				t.Fatal("s1 was not admitted (L2)")
			}

			// Steer s2 targeting r1 (admits L3)
			if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r1","input":"steer s2"}`)); err != nil {
				t.Fatal(err)
			}

			if !tc.pendingBeforeAccepted {
				// Read s1 pending
				_, b, err = c.ReadMessage()
				if err != nil {
					t.Fatalf("read s1 pending: %v", err)
				}
				if gjson.GetBytes(b, "type").String() != "response.steer.pending" {
					t.Fatalf("expected s1 pending, got: %s", b)
				}
			}

			// L2 released on s1 pending
			select {
			case <-authority.successorReleasedChan:
			case <-time.After(3 * time.Second):
				t.Fatal("L2 was not released on s1 pending")
			}

			// Read s1 failed
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read s1 failed: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.steer.failed" {
				t.Fatalf("expected s1 failed, got: %s", b)
			}

			// Assert: s2 admission (L3) MUST STILL BE HELD! It must not have been consumed/released by s1 failed!
			if !authority.thirdAdmitted.Load() {
				t.Fatal("s2 was not admitted (L3)")
			}
			if authority.thirdReleased.Load() {
				t.Fatal("L3 was prematurely released by s1 failed while s2 was unacknowledged")
			}

			// Read s2 accepted
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read s2 accepted: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
				t.Fatalf("expected s2 accepted, got: %s", b)
			}

			// Read automatic successor r2 created
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read r2 created: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r2" {
				t.Fatalf("unexpected message for r2 created: %s", b)
			}

			// Assert: L3 is still held while automatic successor r2 is active
			if authority.thirdReleased.Load() {
				t.Fatal("L3 was prematurely released while automatic successor r2 is active")
			}

			// Signal upstream to complete r2
			signalR2Terminal()

			// Read r2 completed
			_, b, err = c.ReadMessage()
			if err != nil {
				t.Fatalf("read r2 completed: %v", err)
			}
			if gjson.GetBytes(b, "type").String() != "response.completed" || gjson.GetBytes(b, "response.id").String() != "r2" {
				t.Fatalf("unexpected message for r2 completed: %s", b)
			}

			// Assert: L3 released strictly after r2 terminal
			select {
			case <-authority.thirdReleasedChan:
			case <-time.After(3 * time.Second):
				t.Fatal("L3 was not released on automatic successor r2 terminal")
			}
		})
	}
}

func TestResponsesSteeringDispatchExplicitCreateAllowsSubsequentFramesWhileActive(t *testing.T) {
	var frames atomic.Int32
	done := make(chan struct{})
	readyForR2Terminal := make(chan struct{})
	var closeR2TerminalOnce sync.Once
	signalR2Terminal := func() { closeR2TerminalOnce.Do(func() { close(readyForR2Terminal) }) }
	defer signalR2Terminal()

	var upstreamConn atomic.Pointer[websocket.Conn]
	upstreamErrs := make(chan error, 16)
	recordErr := func(err error) {
		if err != nil && !isTeardownErr(err) {
			select {
			case upstreamErrs <- err:
			default:
			}
		}
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			recordErr(err)
			return
		}
		upstreamConn.Store(c)
		defer func() {
			upstreamConn.Store(nil)
			_ = c.Close()
		}()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

		// 1. Initial create
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read initial: %w", err))
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1"}}`))
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r1","output":[]}}`))

		// 2. Explicit create r2
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read explicit r2: %w", err))
			return
		}
		frames.Add(1)
		// Upstream creates r2, but does NOT complete it yet (r2 is active)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r2"}}`))

		// 3. Upstream expects to read subsequent steer s1 forwarded while r2 is STILL ACTIVE!
		if _, _, err := c.ReadMessage(); err != nil {
			recordErr(fmt.Errorf("upstream read active steer while r2 active: %w", err))
			return
		}
		frames.Add(1)
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r2"}}`))

		// Wait for downstream signal to complete r2
		select {
		case <-readyForR2Terminal:
		case <-time.After(3 * time.Second):
			recordErr(errors.New("upstream timed out waiting for readyForR2Terminal signal"))
			return
		}

		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"r2","output":[]}}`))

		// Wait for downstream close
		_, _, _ = c.ReadMessage()
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	authority := newFakeDispatchAuthority()
	manager.SetDispatchAuthority(authority)

	authID := "steering-dispatch-subsequent-frames"
	model := "steering-dispatch-subsequent-frames-model"
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: authID, Provider: "codex", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "test-key", "base_url": upstream.URL, "websockets": "true"},
	}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	defer downstream.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		signalR2Terminal()
		_ = c.Close()
		if uc := upstreamConn.Load(); uc != nil {
			_ = uc.Close()
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		close(upstreamErrs)
		for err := range upstreamErrs {
			t.Errorf("upstream error: %v", err)
		}
	}()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Initial create
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Fatalf("read r1 (%d): %v", i, err)
		}
	}

	// Explicit create r2
	if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
		t.Fatal(err)
	}
	_, b, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read r2 created: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.created" || gjson.GetBytes(b, "response.id").String() != "r2" {
		t.Fatalf("unexpected message for r2 created: %s", b)
	}

	// Send steer s1 targeting r2 while r2 is STILL ACTIVE
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.steer","previous_response_id":"r2","input":"steer s1"}`)); err != nil {
		t.Fatal(err)
	}

	// Read steer s1 accepted (must succeed and be forwarded while r2 is active)
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read s1 accepted: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.steer.accepted" {
		t.Fatalf("expected s1 accepted, got: %s", b)
	}

	// Signal upstream to complete r2
	signalR2Terminal()

	// Read r2 completed
	_, b, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("read r2 completed: %v", err)
	}
	if gjson.GetBytes(b, "type").String() != "response.completed" || gjson.GetBytes(b, "response.id").String() != "r2" {
		t.Fatalf("unexpected message for r2 completed: %s", b)
	}
}
