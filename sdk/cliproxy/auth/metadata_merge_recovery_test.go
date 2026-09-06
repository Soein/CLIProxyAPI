package auth

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestMergeRefreshedAuthRecoversUnauthorizedAggregation(t *testing.T) {
	for _, scenario := range []string{"recovered", "unauthorized code only", "removed model", "remaining model quota", "credential quota", "concurrent unauthorized", "concurrent second unauthorized", "concurrent same-message error", "concurrent disable"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now()
			unauthorized := &Error{Code: "unauthorized", Message: "request failed", HTTPStatus: http.StatusUnauthorized}
			if scenario == "unauthorized code only" {
				unauthorized.HTTPStatus = 0
				unauthorized.Code = "UnAuThOrIzEd"
			}
			base := &Auth{
				ID: "refresh-recovery", Provider: "codex", Status: StatusError,
				Unavailable: true, LastError: unauthorized, StatusMessage: unauthorized.Message,
				NextRetryAfter: now.Add(25 * time.Minute), NextRefreshAfter: now.Add(-time.Second),
				Metadata: map[string]any{"access_token": "old", "refresh_interval_seconds": 900},
				ModelStates: map[string]*ModelState{"model": {
					Status: StatusError, Unavailable: true, LastError: cloneError(unauthorized),
					NextRetryAfter: now.Add(25 * time.Minute), UpdatedAt: now.Add(-5 * time.Minute),
				}},
			}
			quota := QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: now.Add(time.Hour)}
			if scenario == "remaining model quota" {
				base.ModelStates["limited"] = &ModelState{
					Status: StatusError, Unavailable: true, NextRetryAfter: quota.NextRecoverAt, Quota: quota,
					LastError: &Error{Message: "quota exceeded", HTTPStatus: http.StatusTooManyRequests},
				}
				base.Quota = quota
			}
			if scenario == "credential quota" {
				quota.Reason = "credential_quota"
				base.Quota = quota
			}
			current := base.Clone()
			updated := base.Clone()
			updated.Metadata["access_token"] = "fresh"
			updated.LastRefreshedAt = now
			updated.NextRefreshAfter = time.Time{}
			updated.LastError = nil
			updated.StatusMessage = ""
			updated.Status = StatusActive
			updated.Unavailable = false
			clearUnauthorizedModelStates(updated, now)
			switch scenario {
			case "removed model":
				delete(updated.ModelStates, "model")
			case "concurrent unauthorized":
				current.ModelStates["model"].UpdatedAt = now
				current.ModelStates["model"].NextRetryAfter = now.Add(30 * time.Minute)
				current.NextRetryAfter = now.Add(30 * time.Minute)
			case "concurrent second unauthorized":
				current.ModelStates["other"] = &ModelState{
					Status: StatusError, Unavailable: true, LastError: cloneError(unauthorized),
					NextRetryAfter: now.Add(30 * time.Minute), UpdatedAt: now,
				}
			case "concurrent same-message error":
				current.LastError = &Error{Message: unauthorized.Message, HTTPStatus: http.StatusServiceUnavailable}
			case "concurrent disable":
				current.Disabled = true
				current.Status = StatusDisabled
			}
			beforeCurrent, beforeUpdated := current.Clone(), updated.Clone()
			merged := MergeRefreshedAuth(base, current, updated)
			if merged.Metadata["access_token"] != "fresh" {
				t.Fatal("refreshed token was not merged")
			}
			switch scenario {
			case "concurrent unauthorized", "concurrent second unauthorized", "concurrent same-message error":
				if !reflect.DeepEqual(merged.LastError, current.LastError) || merged.Status != current.Status || merged.Unavailable != current.Unavailable {
					t.Fatalf("concurrent failure was cleared: %+v", merged)
				}
				if scenario == "concurrent unauthorized" && !reflect.DeepEqual(merged.ModelStates, current.ModelStates) {
					t.Fatal("concurrent model failure was cleared")
				}
				if scenario == "concurrent second unauthorized" && !reflect.DeepEqual(merged.ModelStates["other"], current.ModelStates["other"]) {
					t.Fatal("concurrent second model failure was cleared")
				}
			case "concurrent disable":
				if !merged.Disabled || merged.Status != StatusDisabled {
					t.Fatal("concurrent disable was cleared")
				}
			default:
				if merged.LastError != nil || merged.Status != StatusActive || merged.StatusMessage != "" {
					t.Fatalf("recovered 401 remains in aggregate: status=%s unavailable=%v error=%v", merged.Status, merged.Unavailable, merged.LastError)
				}
				if _, scheduled := nextRefreshCheckAt(now, merged, time.Minute); !scheduled {
					t.Fatal("successful refresh was removed from automatic refresh scheduling")
				}
				if scenario == "credential quota" {
					if !merged.Unavailable || !reflect.DeepEqual(merged.Quota, quota) {
						t.Fatal("credential quota cooldown was cleared")
					}
				} else if merged.Unavailable || !merged.NextRetryAfter.IsZero() {
					t.Fatal("recovered model remains unavailable")
				}
				if scenario == "remaining model quota" && !reflect.DeepEqual(merged.ModelStates["limited"], base.ModelStates["limited"]) {
					t.Fatal("unrelated model quota was cleared")
				}
			}
			if !reflect.DeepEqual(current, beforeCurrent) || !reflect.DeepEqual(updated, beforeUpdated) {
				t.Fatal("merge mutated an input snapshot")
			}
		})
	}
}
