# Upstream Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix every blocking review finding and its directly coupled regressions before merging the upstream sync into `origin/main`.

**Architecture:** Preserve existing provider boundaries while adding bounded resource ownership at the selector, schema, credential, and WebSocket edges. Each task follows red-green-refactor and owns non-overlapping production files so implementation can proceed in parallel.

**Tech Stack:** Go 1.26, Gin, gorilla/websocket, gjson/sjson, Go race detector, GitHub Actions, Docker.

---

### Task 1: Make selector replacement race-free and lifetime-safe

**Files:**
- Modify: `sdk/cliproxy/auth/conductor.go`
- Modify: `sdk/cliproxy/auth/conductor_selection.go`
- Modify: `sdk/cliproxy/auth/conductor_cooldown.go`
- Modify: `sdk/cliproxy/auth/conductor_lifecycle.go`
- Modify: `sdk/cliproxy/auth/conductor_refresh.go`
- Test: `sdk/cliproxy/auth/selector_lifecycle_test.go`

**Steps:**
1. Add a race regression that concurrently replaces selectors while executing `Pick`, `MarkResult`, invalidation, and shutdown paths; verify `go test -race` fails.
2. Add selector read leases and retain them for every selector method call.
3. Make `SetSelector` wait for active leases before stopping the old selector.
4. Run the focused race test and the full auth race suite.

### Task 2: Make session-affinity and request-scoped model-pool outcomes coherent

**Files:**
- Modify: `sdk/cliproxy/auth/selector.go`
- Modify: `sdk/cliproxy/auth/conductor_execution.go`
- Modify: `sdk/cliproxy/auth/conductor_stream.go`
- Modify: `sdk/cliproxy/auth/conductor_home_execution.go`
- Test: `sdk/cliproxy/auth/session_affinity_metadata_test.go`
- Test: `sdk/cliproxy/auth/conductor_request_scoped_errors_test.go`

**Steps:**
1. Add failing tests for xAI execution/prompt-cache keys, caller isolation, model-pool fail-then-success binding, `continue*` credential advancement, and Home actions.
2. Propagate exact affinity keys through internal metadata and consume them in `OnResult`.
3. Record affinity once for the final credential outcome and route `continue*` to the next credential.
4. Run focused tests and `go test -race ./sdk/cliproxy/auth`.

### Task 3: Bound and correctly merge local JSON Schema references

**Files:**
- Modify: `internal/util/gemini_schema.go`
- Test: `internal/util/gemini_schema_test.go`

**Steps:**
1. Add failing tests for exponential DAG expansion, depth/node/output budgets, and `$ref` sibling `properties`/`required` semantics.
2. Add a budgeted resolver that falls back to typed hints when limits are exhausted.
3. Merge object properties recursively and required names as a stable union.
4. Run util and executor schema tests, including race tests.

### Task 4: Preserve credential extensions without overriding new identities

**Files:**
- Modify: `internal/misc/credentials.go`
- Modify: `sdk/cliproxy/auth/metadata_merge.go`
- Modify: `sdk/auth/manager.go`
- Modify: `internal/api/handlers/management/auth_files_fields.go`
- Test: `internal/misc/credentials_test.go`
- Test: `sdk/auth/manager_test.go`
- Test: `internal/api/handlers/management/auth_files_relogin_preserve_test.go`

**Steps:**
1. Add failing relogin tests where old `account_id` must not replace the new storage value.
2. Add failing traversal tests for relative, absolute, and symlink escapes from `AuthDir`.
3. Make serialized storage fields authoritative and restrict existing-file reads to local contained paths.
4. Run focused credential and management tests with the race detector.

### Task 5: Validate request-scoped error configuration and report hot-reload changes

**Files:**
- Modify: `internal/config/config_types.go`
- Modify: `internal/api/handlers/management/config_lists.go`
- Modify: `internal/watcher/diff/config_diff.go`
- Modify: `internal/watcher/diff/openai_compat.go`
- Test: `internal/config/request_scoped_errors_test.go`
- Test: `internal/api/handlers/management/config_openai_compat_test.go`
- Test: `internal/watcher/diff/config_diff_test.go`

**Steps:**
1. Add failing tests for invalid status, action, empty matchers, and malformed regex.
2. Implement one shared validator and call it from config load and management updates.
3. Add a redacted stable change summary for rule updates.
4. Run config, management, synthesizer, and watcher tests.

### Task 6: Bound and isolate Responses WebSocket state

**Files:**
- Modify: `sdk/api/handlers/openai/openai_responses_websocket.go`
- Modify: `sdk/api/handlers/openai/openai_responses_websocket_toolcall_repair.go`
- Test: `sdk/api/handlers/openai/openai_responses_websocket_test.go`

**Steps:**
1. Add failing tests for oversized messages/items, global/session byte budgets, maximum session count, and cross-caller cache isolation.
2. Set the downstream read limit and reject oversized cached items.
3. Track per-session and global bytes with deterministic eviction.
4. Namespace session keys with a hashed caller scope and run WebSocket race tests.

### Task 7: Update and verify the release toolchain

**Files:**
- Modify: `go.mod`
- Modify: `.github/workflows/release.yaml`
- Modify: `Dockerfile`

**Steps:**
1. Verify the patched Go release against the official Go download/security metadata.
2. Pin local, CI, and container toolchains consistently.
3. Run `govulncheck ./...`, documenting any remaining unreachable or third-party findings.

### Task 8: Integrate, review, and deliver

**Steps:**
1. Run `gofmt` on changed Go files and `git diff --check`.
2. Run focused race suites, `go vet` on changed packages, `go test ./...`, and the required server build.
3. Run a fresh multi-domain code and security review of `origin/main...HEAD`.
4. Commit logical changes, merge the repair branch into local `main`, push `main` to `origin`, and verify the remote SHA.

