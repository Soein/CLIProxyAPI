package auth

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestManagerMetaConcurrentSuccess(t *testing.T) {
	cases := []struct {
		name     string
		mode     string
		modelKey string
	}{
		{
			name:     "UpdateRefreshedAuth_NonEmptyModel",
			mode:     "refresh",
			modelKey: "meta-llama-3",
		},
		{
			name:     "UpdateRefreshedAuth_EmptyModel",
			mode:     "refresh",
			modelKey: "",
		},
		{
			name:     "UpdatePreparedAuth_NonEmptyModel",
			mode:     "prepare",
			modelKey: "meta-llama-3",
		},
		{
			name:     "UpdatePreparedAuth_EmptyModel",
			mode:     "prepare",
			modelKey: "",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &blockingSaveStore{
				lifecycleVersionedStore: lifecycleVersionedStore{restoreGeneration: 7},
				saveStarted:             make(chan struct{}, 1),
				saveRelease:             make(chan struct{}),
			}
			manager := NewManager(store, &RoundRobinSelector{}, nil)
			manager.RegisterExecutor(schedulerTestExecutor{provider: "meta"})

			err503 := &Error{
				Code:       "503",
				Message:    "service unavailable",
				HTTPStatus: http.StatusServiceUnavailable,
			}
			retryAfter := time.Now().Add(5 * time.Minute)

			var modelStates map[string]*ModelState
			if tc.modelKey != "" {
				modelStates = map[string]*ModelState{
					tc.modelKey: {
						Unavailable:    true,
						Status:         StatusError,
						LastError:      cloneError(err503),
						NextRetryAfter: retryAfter,
						UpdatedAt:      time.Now(),
					},
				}
			}

			baseAuth := &Auth{
				ID:             "meta-success-" + tc.name,
				Provider:       "meta",
				Status:         StatusError,
				Unavailable:    true,
				LastError:      cloneError(err503),
				NextRetryAfter: retryAfter,
				Metadata: map[string]any{
					"access_token": "original-token",
				},
				ModelStates: modelStates,
			}

			base, errRegister := manager.Register(ctx, baseAuth)
			if errRegister != nil {
				t.Fatalf("Register error: %v", errRegister)
			}

			updated := base.Clone()
			updated.Metadata["access_token"] = "refreshed-token"
			updated.Metadata["api_key"] = "minted-secret-key"
			updated.LastError = nil
			updated.StatusMessage = ""
			updated.Status = StatusActive
			updated.Unavailable = false
			updated.NextRetryAfter = time.Time{}

			updateDone := make(chan struct{})
			var (
				saved     *Auth
				errUpdate error
			)
			go func() {
				defer close(updateDone)
				if tc.mode == "refresh" {
					saved, errUpdate = manager.UpdateRefreshedAuth(ctx, base, updated)
				} else {
					saved, errUpdate = manager.UpdatePreparedAuth(ctx, base, updated)
				}
			}()

			var saveReleased bool
			t.Cleanup(func() {
				if !saveReleased {
					close(store.saveRelease)
				}
				<-updateDone
			})

			select {
			case <-store.saveStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for saveStarted")
			}

			manager.MarkResult(ctx, Result{
				AuthID:   base.ID,
				Provider: "meta",
				Model:    tc.modelKey,
				Success:  true,
			})

			midAuth, ok := manager.GetByID(base.ID)
			if !ok || midAuth == nil {
				t.Fatal("expected current auth to exist while save is blocked")
			}
			if midAuth.LastError != nil {
				t.Fatalf("expected immediate current LastError to be nil, got: %v", midAuth.LastError)
			}
			if midAuth.Unavailable {
				t.Fatalf("expected immediate current Unavailable to be false, got: %v", midAuth.Unavailable)
			}
			if midAuth.Status != StatusActive {
				t.Fatalf("expected immediate current Status to be StatusActive, got: %v", midAuth.Status)
			}
			if !midAuth.NextRetryAfter.IsZero() {
				t.Fatalf("expected immediate current NextRetryAfter to be zero, got: %v", midAuth.NextRetryAfter)
			}
			if tc.modelKey != "" {
				if ms, ok := midAuth.ModelStates[tc.modelKey]; !ok || ms.LastError != nil || ms.Unavailable || ms.Status != StatusActive {
					t.Fatalf("expected immediate model state to be cleared, got: %+v", ms)
				}
			}

			saveReleased = true
			close(store.saveRelease)

			select {
			case <-updateDone:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for updateDone")
			}
			if errUpdate != nil {
				t.Fatalf("update error: %v", errUpdate)
			}
			if saved == nil {
				t.Fatal("expected saved auth to be non-nil")
			}

			if saved.Metadata["access_token"] != "refreshed-token" {
				t.Fatalf("saved access_token = %v, want refreshed-token", saved.Metadata["access_token"])
			}
			if saved.Metadata["api_key"] != "minted-secret-key" {
				t.Fatalf("saved api_key = %v, want minted-secret-key", saved.Metadata["api_key"])
			}
			if saved.Generation <= base.Generation {
				t.Fatalf("saved generation %d <= base generation %d", saved.Generation, base.Generation)
			}
			if saved.LastError != nil {
				t.Fatalf("saved LastError = %v, want nil", saved.LastError)
			}
			if saved.Unavailable {
				t.Fatalf("saved Unavailable = true, want false")
			}
			if saved.Status != StatusActive {
				t.Fatalf("saved Status = %v, want %v", saved.Status, StatusActive)
			}
			if !saved.NextRetryAfter.IsZero() {
				t.Fatalf("saved NextRetryAfter = %v, want zero", saved.NextRetryAfter)
			}
			if tc.modelKey != "" {
				if ms, ok := saved.ModelStates[tc.modelKey]; !ok || ms.LastError != nil || ms.Unavailable || ms.Status != StatusActive {
					t.Fatalf("saved model state not cleared: %+v", ms)
				}
			}

			finalAuth, ok := manager.GetByID(base.ID)
			if !ok || finalAuth == nil {
				t.Fatal("expected final auth to exist")
			}
			if finalAuth.Metadata["access_token"] != "refreshed-token" {
				t.Fatalf("final access_token = %v, want refreshed-token", finalAuth.Metadata["access_token"])
			}
			if finalAuth.Metadata["api_key"] != "minted-secret-key" {
				t.Fatalf("final api_key = %v, want minted-secret-key", finalAuth.Metadata["api_key"])
			}
			if finalAuth.Generation <= base.Generation {
				t.Fatalf("final generation %d <= base generation %d", finalAuth.Generation, base.Generation)
			}
			if finalAuth.LastError != nil {
				t.Fatalf("final LastError = %v, want nil", finalAuth.LastError)
			}
			if finalAuth.Unavailable {
				t.Fatalf("final Unavailable = true, want false")
			}
			if finalAuth.Status != StatusActive {
				t.Fatalf("final Status = %v, want %v", finalAuth.Status, StatusActive)
			}
			if !finalAuth.NextRetryAfter.IsZero() {
				t.Fatalf("final NextRetryAfter = %v, want zero", finalAuth.NextRetryAfter)
			}
			if tc.modelKey != "" {
				if ms, ok := finalAuth.ModelStates[tc.modelKey]; !ok || ms.LastError != nil || ms.Unavailable || ms.Status != StatusActive {
					t.Fatalf("final model state not cleared: %+v", ms)
				}
			}
		})
	}
}

func TestManagerMetaConcurrentAuthFailure(t *testing.T) {
	cases := []struct {
		name       string
		mode       string
		statusCode int
		err        *Error
	}{
		{
			name:       "UpdatePreparedAuth_401",
			mode:       "prepare",
			statusCode: http.StatusUnauthorized,
			err: &Error{
				Code:       "401",
				Message:    "unauthorized",
				HTTPStatus: http.StatusUnauthorized,
			},
		},
		{
			name:       "UpdatePreparedAuth_503",
			mode:       "prepare",
			statusCode: http.StatusServiceUnavailable,
			err: &Error{
				Code:       "503",
				Message:    "service unavailable",
				HTTPStatus: http.StatusServiceUnavailable,
			},
		},
		{
			name:       "UpdateRefreshedAuth_401",
			mode:       "refresh",
			statusCode: http.StatusUnauthorized,
			err: &Error{
				Code:       "401",
				Message:    "unauthorized",
				HTTPStatus: http.StatusUnauthorized,
			},
		},
		{
			name:       "UpdateRefreshedAuth_503",
			mode:       "refresh",
			statusCode: http.StatusServiceUnavailable,
			err: &Error{
				Code:       "503",
				Message:    "service unavailable",
				HTTPStatus: http.StatusServiceUnavailable,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &blockingSaveStore{
				lifecycleVersionedStore: lifecycleVersionedStore{restoreGeneration: 7},
				saveStarted:             make(chan struct{}, 1),
				saveRelease:             make(chan struct{}),
			}
			manager := NewManager(store, &RoundRobinSelector{}, nil)
			manager.RegisterExecutor(schedulerTestExecutor{provider: "meta"})

			modelKey := "meta-llama-3"
			baseAuth := &Auth{
				ID:          "meta-failure-" + tc.name,
				Provider:    "meta",
				Status:      StatusActive,
				Unavailable: false,
				Metadata: map[string]any{
					"access_token": "original-token",
				},
				ModelStates: map[string]*ModelState{
					modelKey: {
						Unavailable: false,
						Status:      StatusActive,
						UpdatedAt:   time.Now(),
					},
				},
			}

			base, errRegister := manager.Register(ctx, baseAuth)
			if errRegister != nil {
				t.Fatalf("Register error: %v", errRegister)
			}

			updated := base.Clone()
			updated.Metadata["access_token"] = "refreshed-token"
			updated.Metadata["api_key"] = "minted-secret-key"

			updateDone := make(chan struct{})
			var (
				saved     *Auth
				errUpdate error
			)
			go func() {
				defer close(updateDone)
				if tc.mode == "refresh" {
					saved, errUpdate = manager.UpdateRefreshedAuth(ctx, base, updated)
				} else {
					saved, errUpdate = manager.UpdatePreparedAuth(ctx, base, updated)
				}
			}()

			var saveReleased bool
			t.Cleanup(func() {
				if !saveReleased {
					close(store.saveRelease)
				}
				<-updateDone
			})

			select {
			case <-store.saveStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for saveStarted")
			}

			manager.MarkResult(ctx, Result{
				AuthID:   base.ID,
				Provider: "meta",
				Model:    "",
				Success:  false,
				Error:    cloneError(tc.err),
			})

			midAuth, ok := manager.GetByID(base.ID)
			if !ok || midAuth == nil {
				t.Fatal("expected current auth to exist while save is blocked")
			}
			if midAuth.LastError == nil {
				t.Fatal("expected mid LastError to be non-nil")
			}
			if !reflect.DeepEqual(midAuth.LastError, tc.err) {
				t.Fatalf("mid LastError = %v, want %v", midAuth.LastError, tc.err)
			}
			if !midAuth.Unavailable {
				t.Fatal("expected mid Unavailable to be true")
			}
			if !midAuth.NextRetryAfter.After(time.Now()) {
				t.Fatalf("expected mid NextRetryAfter to be in the future, got %v", midAuth.NextRetryAfter)
			}
			if tc.statusCode == http.StatusUnauthorized {
				if !hasUnauthorizedAuthFailure(midAuth) {
					t.Fatal("expected mid hasUnauthorizedAuthFailure to be true for 401")
				}
			} else {
				if hasUnauthorizedAuthFailure(midAuth) {
					t.Fatal("expected mid hasUnauthorizedAuthFailure to be false for 503")
				}
			}

			saveReleased = true
			close(store.saveRelease)

			select {
			case <-updateDone:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for updateDone")
			}
			if errUpdate != nil {
				t.Fatalf("update error: %v", errUpdate)
			}
			if saved == nil {
				t.Fatal("expected saved auth to be non-nil")
			}

			if saved.Metadata["access_token"] != "refreshed-token" {
				t.Fatalf("saved access_token = %v, want refreshed-token", saved.Metadata["access_token"])
			}
			if saved.Metadata["api_key"] != "minted-secret-key" {
				t.Fatalf("saved api_key = %v, want minted-secret-key", saved.Metadata["api_key"])
			}
			if saved.Generation <= base.Generation {
				t.Fatalf("saved generation %d <= base generation %d", saved.Generation, base.Generation)
			}
			if !reflect.DeepEqual(saved.LastError, midAuth.LastError) {
				t.Fatalf("saved LastError = %v, want %v", saved.LastError, midAuth.LastError)
			}
			if !saved.Unavailable {
				t.Fatalf("saved Unavailable = false, want true")
			}
			if !saved.NextRetryAfter.Equal(midAuth.NextRetryAfter) {
				t.Fatalf("saved NextRetryAfter = %v, want %v", saved.NextRetryAfter, midAuth.NextRetryAfter)
			}
			if hasUnauthorizedAuthFailure(saved) != hasUnauthorizedAuthFailure(midAuth) {
				t.Fatalf("saved hasUnauthorizedAuthFailure = %v, want %v", hasUnauthorizedAuthFailure(saved), hasUnauthorizedAuthFailure(midAuth))
			}

			finalAuth, ok := manager.GetByID(base.ID)
			if !ok || finalAuth == nil {
				t.Fatal("expected final auth to exist")
			}
			if finalAuth.Metadata["access_token"] != "refreshed-token" {
				t.Fatalf("final access_token = %v, want refreshed-token", finalAuth.Metadata["access_token"])
			}
			if finalAuth.Metadata["api_key"] != "minted-secret-key" {
				t.Fatalf("final api_key = %v, want minted-secret-key", finalAuth.Metadata["api_key"])
			}
			if finalAuth.Generation <= base.Generation {
				t.Fatalf("final generation %d <= base generation %d", finalAuth.Generation, base.Generation)
			}
			if !reflect.DeepEqual(finalAuth.LastError, midAuth.LastError) {
				t.Fatalf("final LastError = %v, want %v", finalAuth.LastError, midAuth.LastError)
			}
			if !finalAuth.Unavailable {
				t.Fatalf("final Unavailable = false, want true")
			}
			if !finalAuth.NextRetryAfter.Equal(midAuth.NextRetryAfter) {
				t.Fatalf("final NextRetryAfter = %v, want %v", finalAuth.NextRetryAfter, midAuth.NextRetryAfter)
			}
			if hasUnauthorizedAuthFailure(finalAuth) != hasUnauthorizedAuthFailure(midAuth) {
				t.Fatalf("final hasUnauthorizedAuthFailure = %v, want %v", hasUnauthorizedAuthFailure(finalAuth), hasUnauthorizedAuthFailure(midAuth))
			}
		})
	}
}
