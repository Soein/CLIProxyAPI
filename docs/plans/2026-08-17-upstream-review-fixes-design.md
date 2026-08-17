# Upstream Review Fixes Design

## Goal

Remove the blocking correctness and security regressions found while reviewing the pending upstream sync, then re-review the complete `origin/main...HEAD` snapshot before it reaches the fork's default branch.

## Design

Selector replacement needs a lifetime guarantee, not only a synchronized pointer swap. The manager will expose selector read leases backed by an `RWMutex`. Selection, result notification, invalidation, and shutdown paths retain a read lease for the full selector call. `SetSelector` takes the write lease, swaps the selector, waits for existing readers by construction, and only then stops the old selector. This avoids both interface data races and use-after-stop behavior without introducing a second selector registry or deferred cleanup goroutine.

Session affinity will carry the exact provider-aware, caller-scoped cache keys selected for a request into result metadata. Result handling will consume those keys instead of reconstructing them from a different algorithm. Model-pool attempts will publish one final affinity outcome per credential, and request-scoped `continue*` actions will advance to the next credential as documented.

Local JSON Schema references will remain supported, but expansion will have explicit depth, node, and output-size budgets. Sibling `properties` will merge recursively and `required` values will form a stable union. Exceeding a budget degrades to the existing typed reference hint instead of allocating an unbounded tree.

Credential relogin preservation will treat freshly acquired storage fields as authoritative. Historical metadata may fill missing extension fields but may not overwrite serialized provider fields such as `account_id`. Existing credential files will be read only through paths proven to remain beneath `AuthDir`.

Responses WebSocket handling will scope repair caches by a one-way caller identity and enforce message, item, per-session, session-count, and global byte limits. The release toolchain will move to the verified patched Go release used by CI and container builds.

## Alternatives Rejected

- Atomic selector swaps alone do not protect an old selector from being stopped while a request still uses it.
- Memoizing `$ref` expansion reduces repeated computation but does not cap the final exponentially large output.
- Extending only a token-field denylist remains fragile when providers add new authoritative fields; source-first merging is the safer central rule.
- A WebSocket frame limit alone does not bound long-lived global repair caches.

