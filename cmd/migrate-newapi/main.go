// Package main: one-shot migration from new-api SQLite → shared PostgreSQL.
//
// Assumes the target PG database has already been schema-initialized by
// running new-api once against it (GORM AutoMigrate). This tool only
// copies DATA, never DDL. It TRUNCATEs each target table (CASCADE) before
// INSERT so re-runs are idempotent.
//
// Scope:
//   - Iterates every user table in sqlite_master (skips sqlite_* internal).
//   - Per-table: SELECT * in batches, pgx CopyFrom into target.
//   - After all data: setval() on <table>_id_seq so GORM INSERTs start at max+1.
//
// Usage:
//
//	migrate-newapi --sqlite=/path/to/one-api.db --pg=postgres://... [--dry-run|--execute]
//
// Exit codes:
//
//	0  success
//	1  arg error
//	2  sqlite error
//	3  pg error
//	4  mid-migration error (some tables migrated, others not)
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"
)

func main() {
	var (
		sqlitePath = flag.String("sqlite", "", "path to sqlite db (e.g. /data/one-api.db)")
		pgDSN      = flag.String("pg", "", "postgres DSN (e.g. postgres://u:p@h:5000/newapi?sslmode=require)")
		dryRun     = flag.Bool("dry-run", true, "print row counts without writing (default)")
		execute    = flag.Bool("execute", false, "actually migrate (overrides --dry-run)")
		skipTables = flag.String("skip", "", "comma-separated table names to skip (e.g. sqlite_sequence)")
		batchSize  = flag.Int("batch", 1000, "rows per CopyFrom batch")
	)
	flag.Parse()

	if *sqlitePath == "" || *pgDSN == "" {
		fmt.Fprintln(os.Stderr, "migrate-newapi: --sqlite and --pg are required")
		flag.Usage()
		os.Exit(1)
	}
	if *execute {
		*dryRun = false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Open sqlite read-only.
	sqliteDB, err := sql.Open("sqlite", "file:"+*sqlitePath+"?mode=ro")
	if err != nil {
		fatal(2, "sqlite open: %v", err)
	}
	defer sqliteDB.Close()
	if err := sqliteDB.PingContext(ctx); err != nil {
		fatal(2, "sqlite ping: %v", err)
	}

	// Open PG via pgxpool (pgx handles sslmode correctly).
	cfg, err := pgxpool.ParseConfig(*pgDSN)
	if err != nil {
		fatal(3, "pg parse DSN: %v", err)
	}
	pgPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fatal(3, "pg connect: %v", err)
	}
	defer pgPool.Close()

	// Enumerate sqlite tables.
	tables, err := listTables(ctx, sqliteDB)
	if err != nil {
		fatal(2, "list sqlite tables: %v", err)
	}
	skip := parseCSVSet(*skipTables)
	skip["sqlite_sequence"] = struct{}{} // never useful

	filtered := make([]string, 0, len(tables))
	for _, t := range tables {
		if _, s := skip[t]; s {
			continue
		}
		filtered = append(filtered, t)
	}

	fmt.Printf("source sqlite: %s (%d tables; %d after skip)\n", *sqlitePath, len(tables), len(filtered))
	if *dryRun {
		fmt.Println("DRY RUN — counts only")
	}

	var totalRows int64
	failed := []string{}
	for _, t := range filtered {
		n, err := sqliteRowCount(ctx, sqliteDB, t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  [ERR] %s: count: %v\n", t, err)
			continue
		}
		fmt.Printf("  %-40s  rows=%d\n", t, n)
		totalRows += n

		if *dryRun {
			continue
		}

		// TRUNCATE target table (CASCADE handles FK references from children).
		if _, err := pgPool.Exec(ctx, `TRUNCATE TABLE "`+t+`" RESTART IDENTITY CASCADE`); err != nil {
			fmt.Fprintf(os.Stderr, "  [FAIL] %s: truncate: %v\n", t, err)
			failed = append(failed, t)
			continue
		}
		// Copy data.
		if err := copyTable(ctx, sqliteDB, pgPool, t, *batchSize); err != nil {
			fmt.Fprintf(os.Stderr, "  [FAIL] %s: copy: %v\n", t, err)
			failed = append(failed, t)
			continue
		}
		// Reset sequence if table has an id column.
		if err := resetSequence(ctx, pgPool, t); err != nil {
			// Non-fatal: many tables may not have an id seq. Log only if
			// the error isn't "relation ... does not exist".
			if !strings.Contains(err.Error(), "does not exist") {
				fmt.Fprintf(os.Stderr, "  [WARN] %s: setval: %v\n", t, err)
			}
		}
	}

	fmt.Printf("\nTotal rows observed: %d\n", totalRows)
	if *dryRun {
		fmt.Println("Re-run with --execute=true to perform migration.")
		return
	}
	if len(failed) > 0 {
		fatal(4, "migration completed with %d failed tables: %v", len(failed), failed)
	}
	fmt.Println("Migration complete.")
}

// listTables returns every user-defined table in sqlite, alphabetically.
func listTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out, rows.Err()
}

// sqliteRowCount returns the row count for fast progress reporting. For
// multi-hundred-MB DBs this is cheap (sqlite maintains COUNT via b-tree).
func sqliteRowCount(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var n int64
	// Identifier quoting: sqlite uses double quotes for table names when
	// they conflict with keywords (e.g. "group").
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM "`+table+`"`).Scan(&n)
	return n, err
}

// listColumns returns the column names of the sqlite table in declaration
// order — which is the same as SELECT *'s column order. We use the exact
// list for CopyFrom so PG never guesses.
func listColumns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info("`+table+`")`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// boolColumnsFor queries information_schema to find which columns of the
// target PG table are of type 'boolean'. GORM maps Go `bool` struct fields
// to PG BOOLEAN, but SQLite stores them as INTEGER 0/1. Without this
// conversion pgx CopyFrom fails with "unable to encode 1 into binary
// format for bool (OID 16)".
func boolColumnsFor(ctx context.Context, pgPool *pgxpool.Pool, table string) (map[string]bool, error) {
	rows, err := pgPool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'boolean'
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out[c] = true
	}
	return out, rows.Err()
}

// copyTable streams rows from sqlite into the PG table via pgx CopyFrom,
// which uses the PostgreSQL COPY protocol for maximum throughput.
func copyTable(ctx context.Context, sqliteDB *sql.DB, pgPool *pgxpool.Pool, table string, batchSize int) error {
	cols, err := listColumns(ctx, sqliteDB, table)
	if err != nil {
		return fmt.Errorf("list columns: %w", err)
	}
	if len(cols) == 0 {
		return nil // empty table definition, nothing to copy
	}

	boolCols, err := boolColumnsFor(ctx, pgPool, table)
	if err != nil {
		return fmt.Errorf("fetch bool cols: %w", err)
	}

	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + c + `"`
	}
	selectSQL := `SELECT ` + strings.Join(quoted, ",") + ` FROM "` + table + `"`
	rows, err := sqliteDB.QueryContext(ctx, selectSQL)
	if err != nil {
		return fmt.Errorf("select: %w", err)
	}
	defer rows.Close()

	// Use a source that yields []any per row so pgx.CopyFromRows can stream.
	// We accumulate in memory up to batchSize then flush, to bound peak RAM.
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	var (
		batch     [][]any
		totalSent int64
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, err := conn.Conn().CopyFrom(ctx,
			pgx.Identifier{table},
			cols,
			pgx.CopyFromRows(batch),
		)
		if err != nil {
			return err
		}
		totalSent += int64(len(batch))
		batch = batch[:0]
		return nil
	}

	for rows.Next() {
		vals := make([]any, len(cols))
		valPtrs := make([]any, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		// sqlite numerics come back as int64/float64/string/[]byte. PG via
		// pgx COPY will coerce as long as the target type accepts. For
		// NUMERIC (e.g. balance) pgx needs the Go value to be a string or
		// *big.Rat or similar; the sqlite driver returns int64/float64
		// which pgx serializes fine for NUMERIC if no digit overflow.
		//
		// BOOLEAN columns need an explicit int64→bool coercion because
		// SQLite stored them as 0/1. nil (NULL) values pass through.
		if len(boolCols) > 0 {
			for i, col := range cols {
				if !boolCols[col] {
					continue
				}
				switch v := vals[i].(type) {
				case int64:
					vals[i] = v != 0
				case []byte:
					vals[i] = string(v) != "0" && string(v) != ""
				case string:
					vals[i] = v != "0" && v != ""
				case bool:
					// already bool — no-op
				case nil:
					// preserve NULL
				}
			}
		}
		batch = append(batch, vals)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return fmt.Errorf("copyfrom flush: %w", err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows.Err: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("copyfrom final: %w", err)
	}
	fmt.Printf("    copied %d rows\n", totalSent)
	return nil
}

// resetSequence resets <table>_id_seq to max(id)+1 if the table has an id
// column. Returns the raw error from PG (caller checks for "does not exist").
func resetSequence(ctx context.Context, pgPool *pgxpool.Pool, table string) error {
	seq := table + "_id_seq"
	q := fmt.Sprintf(`SELECT setval('%s', COALESCE((SELECT MAX(id) FROM "%s"), 0)+1, false)`, seq, table)
	_, err := pgPool.Exec(ctx, q)
	return err
}

func parseCSVSet(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out[p] = struct{}{}
		}
	}
	return out
}

func fatal(code int, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "migrate-newapi: "+format+"\n", args...)
	os.Exit(code)
}
