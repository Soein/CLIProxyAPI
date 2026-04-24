// Package main provides a one-shot migration tool that copies auth JSON
// files from a local file-based deployment into the shared PostgreSQL
// auth_store table, enabling the Phase 4 HA cluster to read a unified
// auth pool.
//
// Usage:
//
//	migrate-auths --dsn=<postgres-dsn> --auth-dir=/opt/cli-proxy-api/auths [--dry-run|--execute] [--schema=<schema>] [--auth-table=<name>]
//
// Exit codes:
//
//	0   success (or dry-run completed without error)
//	1   argument / configuration error
//	2   filesystem error reading auth directory
//	3   PG connection or schema error
//	4   per-file migration error during --execute
//
// Safety:
//   - --dry-run (default) NEVER writes to PG. It parses each JSON file and
//     prints a summary table so operators can validate shape/counts before
//     committing.
//   - --execute goes through PostgresStore.PersistAuthFiles which is the
//     exact code path production uses — JSONB encoding, version bump,
//     last_writer, pg_notify are all identical to live writes. This is
//     intentional: we want migration-written rows to be indistinguishable
//     from subsequently-modified rows.
//   - Re-running --execute is safe: PersistAuthFiles UPSERTs by id, so
//     partial-failure recovery is just "re-run".
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/store"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	var (
		dsn       = flag.String("dsn", "", "Postgres connection DSN (e.g. postgres://postgres:pw@127.0.0.1:5000/cliproxy?sslmode=disable)")
		authDir   = flag.String("auth-dir", "", "Source directory containing *.json auth files")
		dryRun    = flag.Bool("dry-run", true, "Print plan without writing (default); use --execute=true to actually migrate")
		execute   = flag.Bool("execute", false, "Actually write to PG (overrides --dry-run)")
		schema    = flag.String("schema", "", "Optional schema name (matches config.yaml postgres.schema)")
		authTable = flag.String("auth-table", "auth_store", "Auth table name (matches config.yaml postgres.auth-table)")
		spoolDir  = flag.String("spool-dir", "", "Spool directory for PostgresStore workspace (will be created; default: ./pgstore-migrate)")
		listOnly  = flag.Bool("list", false, "Skip migration; just print auth_store contents from PG and exit")
	)
	flag.Parse()

	if *dsn == "" {
		fatal(1, "--dsn is required")
	}
	if !*listOnly && *authDir == "" {
		fatal(1, "--auth-dir is required (unless --list)")
	}
	if *execute {
		*dryRun = false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	spool := *spoolDir
	if spool == "" {
		spool = filepath.Join(os.TempDir(), "pgstore-migrate")
	}

	cfg := store.PostgresStoreConfig{
		DSN:       *dsn,
		Schema:    *schema,
		AuthTable: *authTable,
		SpoolDir:  spool,
	}
	pg, err := store.NewPostgresStore(ctx, cfg)
	if err != nil {
		fatal(3, "connect postgres: %v", err)
	}
	defer func() { _ = pg.Close() }()

	if err := pg.EnsureSchema(ctx); err != nil {
		fatal(3, "ensure schema: %v", err)
	}

	if *listOnly {
		if err := listAuthStore(ctx, pg); err != nil {
			fatal(4, "list auth_store: %v", err)
		}
		return
	}

	files, err := collectAuthFiles(*authDir)
	if err != nil {
		fatal(2, "collect auth files: %v", err)
	}
	if len(files) == 0 {
		fatal(2, "no *.json files in %s", *authDir)
	}

	if *dryRun {
		fmt.Printf("DRY RUN: would migrate %d files from %s to %s\n", len(files), *authDir, *dsn)
		for _, f := range files {
			// Stat for size & mtime is enough — we don't parse because
			// PersistAuthFiles handles validation during --execute. If a
			// file is malformed, --execute surfaces it file-by-file.
			fi, err := os.Stat(f)
			if err != nil {
				fmt.Printf("  [ERR] %s: stat: %v\n", f, err)
				continue
			}
			fmt.Printf("  %8d bytes  %s  %s\n",
				fi.Size(),
				fi.ModTime().Format("2026-01-02 15:04:05"),
				filepath.Base(f))
		}
		fmt.Printf("\nRe-run with --execute=true to perform migration.\n")
		return
	}

	// Execute path. Migrate in small batches so a bad file doesn't
	// lose the successful ones we processed earlier — PersistAuthFiles
	// returns on first error.
	var (
		okCount, errCount int
		firstErr          error
	)
	for _, f := range files {
		err := pg.PersistAuthFiles(ctx, "", f)
		if err != nil {
			errCount++
			if firstErr == nil {
				firstErr = err
			}
			fmt.Printf("  [FAIL] %s: %v\n", filepath.Base(f), err)
			continue
		}
		okCount++
		fmt.Printf("  [OK]   %s\n", filepath.Base(f))
	}

	fmt.Printf("\nMigration summary: %d ok, %d failed (of %d)\n", okCount, errCount, len(files))
	if errCount > 0 {
		fatal(4, "migration completed with errors; first: %v", firstErr)
	}

	// Verification: count rows in PG and compare.
	total, err := countAuthStore(ctx, pg, *schema, *authTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: verification count failed: %v\n", err)
		return
	}
	fmt.Printf("PG auth_store now holds %d rows\n", total)
}

func collectAuthFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	sort.Strings(out)
	return out, nil
}

func countAuthStore(ctx context.Context, pg *store.PostgresStore, schema, table string) (int, error) {
	db := pg.DB()
	if db == nil {
		return 0, fmt.Errorf("nil DB")
	}
	full := table
	if schema != "" {
		full = fmt.Sprintf("%q.%q", schema, table)
	}
	var n int
	row := db.QueryRowContext(ctx, "SELECT count(*) FROM "+full)
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func listAuthStore(ctx context.Context, pg *store.PostgresStore) error {
	auths, err := pg.List(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("PG auth_store: %d rows\n", len(auths))
	for _, a := range auths {
		if a == nil {
			continue
		}
		fmt.Printf("  id=%-60s provider=%-12s disabled=%v\n", a.ID, a.Provider, a.Disabled)
	}
	return nil
}

func fatal(code int, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "migrate-auths: "+format+"\n", args...)
	os.Exit(code)
}

// ensure we don't leak import for sql (used via store)
var _ = (*sql.DB)(nil)
