package management

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

// openTestDB mirrors the helper in internal/usage; duplicated here to keep
// the management package self-contained (no test cycle on internal/usage's
// unexported helpers).
func openClusterTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_PGSTORE_DSN")
	if dsn == "" {
		t.Skip("TEST_PGSTORE_DSN not set; skipping handler integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("ping: %v", err)
	}
	return db
}

func TestGetUsageStatistics_PGBackend_ServesClusterPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openClusterTestDB(t)
	defer db.Close()
	ctx := context.Background()
	store := usage.NewPGStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Seed one bucket so the payload is non-trivial. Cleanup at end so
	// repeated runs don't leak rows.
	bucket := time.Now().UTC().Truncate(time.Minute)
	defer db.ExecContext(ctx,
		`DELETE FROM usage_minute_rollup WHERE node_id LIKE 'hctest-%'`)
	defer db.ExecContext(ctx,
		`DELETE FROM usage_events WHERE node_id LIKE 'hctest-%'`)

	if err := store.UpsertRollupBatch(ctx, []usage.UsageRollupDelta{
		{UsageRollupKey: usage.UsageRollupKey{
			BucketStart: bucket, NodeID: "hctest-n1", APIKey: "kA", Model: "mA",
		}, RequestCount: 10, SuccessCount: 8, FailureCount: 2,
			TotalTokens: 1000, LatencyMsSum: 5000},
		{UsageRollupKey: usage.UsageRollupKey{
			BucketStart: bucket, NodeID: "hctest-n2", APIKey: "kA", Model: "mA",
		}, RequestCount: 5, SuccessCount: 5, TotalTokens: 500, LatencyMsSum: 2500},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cfg := &config.Config{}
	cfg.Usage.Backend = "pg"
	h := NewHandler(cfg, "", nil)
	h.SetPGUsage(store)

	router := gin.New()
	router.GET("/usage", h.GetUsageStatistics)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/usage", nil)
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Usage-Backend"); got != "pg" {
		t.Errorf("X-Usage-Backend = %q, want pg", got)
	}
	if got := w.Header().Get("X-Usage-Cache"); got != "miss" {
		t.Errorf("first call X-Usage-Cache = %q, want miss", got)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cluster, ok := body["cluster"].(map[string]any)
	if !ok {
		t.Fatalf("cluster block missing: %s", w.Body.String())
	}
	if agg, _ := cluster["aggregated"].(bool); !agg {
		t.Errorf("cluster.aggregated = false")
	}
	if nc, _ := cluster["node_count"].(float64); nc < 2 {
		t.Errorf("cluster.node_count = %v, want >= 2", nc)
	}
	usageBlock, _ := body["usage"].(map[string]any)
	if tr, _ := usageBlock["total_requests"].(float64); tr != 15 {
		t.Errorf("usage.total_requests = %v, want 15", tr)
	}

	// Second call: same range → cache hit.
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req)
	if got := w2.Header().Get("X-Usage-Cache"); got != "hit" {
		t.Errorf("second call X-Usage-Cache = %q, want hit", got)
	}
}

func TestGetUsageStatistics_MemoryBackend_FallsThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.Usage.Backend = "memory"
	h := NewHandler(cfg, "", nil)
	// Don't attach pgUsage — even if backend says pg, missing PGStore
	// must NOT crash.

	router := gin.New()
	router.GET("/usage", h.GetUsageStatistics)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/usage", nil))
	if w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	}
	if got := w.Header().Get("X-Usage-Backend"); got != "memory" {
		t.Errorf("X-Usage-Backend = %q, want memory", got)
	}
	// Cluster block should be absent on memory path.
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if _, ok := body["cluster"]; ok {
		t.Errorf("memory path leaked cluster block: %s", w.Body.String())
	}
}

func TestGetUsageStatistics_DualBackend_StaysOnMemoryPath(t *testing.T) {
	// dual mode means we're double-writing to PG but read path hasn't
	// flipped yet — must serve in-memory snapshot, no cluster block.
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.Usage.Backend = "dual"
	h := NewHandler(cfg, "", nil)
	// Even with pgUsage attached, dual mode must NOT serve the PG payload.
	if dsn := os.Getenv("TEST_PGSTORE_DSN"); dsn != "" {
		db, err := sql.Open("pgx", dsn)
		if err == nil {
			h.SetPGUsage(usage.NewPGStore(db))
			defer db.Close()
		}
	}

	router := gin.New()
	router.GET("/usage", h.GetUsageStatistics)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/usage", nil))
	if got := w.Header().Get("X-Usage-Backend"); got != "dual" {
		t.Errorf("X-Usage-Backend = %q, want dual", got)
	}
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if _, ok := body["cluster"]; ok {
		t.Errorf("dual path leaked cluster block: %s", w.Body.String())
	}
}

func TestGetUsageStatistics_PGBackend_NilHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	var h *Handler
	router.GET("/usage", h.GetUsageStatistics)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/usage", nil))
	if w.Code != 200 {
		t.Fatalf("nil handler must not crash: %d", w.Code)
	}
}

func TestExportImportUsage_PGBackend_RoundTrip(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openClusterTestDB(t)
	defer db.Close()
	ctx := context.Background()
	store := usage.NewPGStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	defer db.ExecContext(ctx,
		`DELETE FROM usage_minute_rollup WHERE node_id IN ('imported','exporttest-n')`)
	defer db.ExecContext(ctx,
		`DELETE FROM usage_events WHERE node_id IN ('imported','exporttest-n')`)

	// Seed a few raw events under a known node.
	t0 := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	seed := []usage.UsageEventRow{
		{OccurredAt: t0, NodeID: "exporttest-n", APIKey: "kX", Model: "mY",
			Source: "openai", AuthIndex: "0",
			Failed: false, LatencyMs: 100, TotalTokens: 50,
			InputTokens: 30, OutputTokens: 20,
			DedupHash: usage.DedupHash("kX", "mY", usage.RequestDetail{
				Timestamp: t0, LatencyMs: 100, Source: "openai", AuthIndex: "0",
				Tokens: usage.TokenStats{InputTokens: 30, OutputTokens: 20, TotalTokens: 50}})},
		{OccurredAt: t0.Add(time.Minute), NodeID: "exporttest-n", APIKey: "kX", Model: "mY",
			Source: "openai", AuthIndex: "0",
			Failed: true, TotalTokens: 0,
			DedupHash: usage.DedupHash("kX", "mY", usage.RequestDetail{
				Timestamp: t0.Add(time.Minute), Source: "openai", AuthIndex: "0",
				Tokens: usage.TokenStats{}, Failed: true})},
	}
	if err := store.InsertEventsBatch(ctx, seed); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	// Build rollup so QueryClusterTotals returns non-zero on export.
	if err := store.RebuildRollupForRange(ctx, t0.Add(-time.Minute), t0.Add(2*time.Minute)); err != nil {
		t.Fatalf("rebuild rollup: %v", err)
	}

	cfg := &config.Config{}
	cfg.Usage.Backend = "pg"
	h := NewHandler(cfg, "", nil)
	h.SetPGUsage(store)

	router := gin.New()
	router.GET("/usage/export", h.ExportUsageStatistics)
	router.POST("/usage/import", h.ImportUsageStatistics)

	// 1) Export with from/to that includes our seeded window.
	expURL := "/usage/export?from=" +
		t0.Add(-5*time.Minute).Format(time.RFC3339) +
		"&to=" + t0.Add(5*time.Minute).Format(time.RFC3339)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", expURL, nil))
	if w.Code != 200 {
		t.Fatalf("export status %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Usage-Backend"); got != "pg" {
		t.Errorf("export X-Usage-Backend = %q, want pg", got)
	}
	var exp usageExportPayload
	if err := json.Unmarshal(w.Body.Bytes(), &exp); err != nil {
		t.Fatalf("export unmarshal: %v", err)
	}
	if exp.Usage.TotalRequests < 2 {
		t.Errorf("export total_requests = %d, want >= 2", exp.Usage.TotalRequests)
	}
	// At least one detail per (api,model) reachable.
	api, ok := exp.Usage.APIs["kX"]
	if !ok {
		t.Fatalf("export missing api kX: %+v", exp.Usage.APIs)
	}
	if model, ok := api.Models["mY"]; !ok || len(model.Details) < 2 {
		t.Fatalf("export model mY missing details: %+v", api)
	}

	// 2) Build a synthetic import payload with NEW timestamps (not
	//    overlapping the seed) so DedupHash doesn't silently absorb them.
	//    DedupHash by design ignores node_id — the cluster is logically
	//    one event stream — so re-importing the same exported snapshot
	//    no-ops, which is the desired idempotency.
	syntheticTS := t0.Add(2 * time.Hour) // 2h after seed window
	syntheticSnap := usage.StatisticsSnapshot{
		APIs: map[string]usage.APISnapshot{
			"kImport": {
				TotalRequests: 1, TotalTokens: 99,
				Models: map[string]usage.ModelSnapshot{
					"mImport": {
						TotalRequests: 1, TotalTokens: 99,
						Details: []usage.RequestDetail{{
							Timestamp: syntheticTS,
							Source:    "openai",
							AuthIndex: "0",
							Failed:    false,
							LatencyMs: 12,
							Tokens:    usage.TokenStats{InputTokens: 50, OutputTokens: 49, TotalTokens: 99},
						}},
					},
				},
			},
		},
	}
	importBody, _ := json.Marshal(usageImportPayload{Version: 1, Usage: syntheticSnap})
	w2 := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/usage/import", bytesReader(importBody))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w2, req)
	if w2.Code != 200 {
		t.Fatalf("import status %d body=%s", w2.Code, w2.Body.String())
	}
	if got := w2.Header().Get("X-Usage-Backend"); got != "pg" {
		t.Errorf("import X-Usage-Backend = %q, want pg", got)
	}

	// Confirm row landed under "imported" node_id.
	var importedN int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_events WHERE node_id='imported' AND api_key='kImport'`,
	).Scan(&importedN); err != nil {
		t.Fatal(err)
	}
	if importedN != 1 {
		t.Errorf("imported rows = %d, want 1", importedN)
	}

	// 3) Replay the import — dedup_hash must absorb the duplicate,
	//    no double-write.
	w3 := httptest.NewRecorder()
	req2 := httptest.NewRequest("POST", "/usage/import", bytesReader(importBody))
	req2.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w3, req2)
	if w3.Code != 200 {
		t.Fatalf("re-import status %d", w3.Code)
	}
	var importedN2 int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_events WHERE node_id='imported' AND api_key='kImport'`,
	).Scan(&importedN2); err != nil {
		t.Fatal(err)
	}
	if importedN2 != importedN {
		t.Errorf("re-import double-wrote: %d → %d", importedN, importedN2)
	}

	// 4) Re-importing a snapshot whose timestamps overlap the seeded
	//    events (different api_key) must ALSO dedup if the (api,model,
	//    detail) signature matches an existing row. Verify by importing
	//    the original exported snapshot — none should land under
	//    "imported" because dedup_hash collides with seed's exporttest-n
	//    rows (DedupHash ignores node_id by design).
	exportImportBody, _ := json.Marshal(usageImportPayload{Version: 1, Usage: exp.Usage})
	w4 := httptest.NewRecorder()
	req3 := httptest.NewRequest("POST", "/usage/import", bytesReader(exportImportBody))
	req3.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w4, req3)
	if w4.Code != 200 {
		t.Fatalf("export-roundtrip import status %d", w4.Code)
	}
	var collisionN int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM usage_events WHERE node_id='imported' AND api_key='kX'`,
	).Scan(&collisionN); err != nil {
		t.Fatal(err)
	}
	if collisionN != 0 {
		t.Errorf("dedup violated: %d kX rows landed under imported", collisionN)
	}
}

// bytesReader builds an *bytes.Reader for httptest.NewRequest body.
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
