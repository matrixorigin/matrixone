// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

const (
	catalogAttributionEnabledEnv = "MO_CATALOG_INVALIDATION_ATTRIBUTION"
	catalogAttributionReportEnv  = "MO_CATALOG_INVALIDATION_REPORT_DIR"
	catalogAttributionSHAEnv     = "MO_CATALOG_INVALIDATION_MATRIXONE_SHA"
	catalogAttributionSamples    = 128
)

type catalogAttributionScenario struct {
	Name        string                        `json:"name"`
	DDLType     string                        `json:"ddl_type"`
	ConsumerCN  string                        `json:"consumer_cn"`
	DDLCN       string                        `json:"ddl_cn"`
	Protocol    string                        `json:"protocol,omitempty"`
	Samples     int                           `json:"stable_terminal_samples"`
	Status      string                        `json:"status"`
	Observation catalogAttributionObservation `json:"observation"`
}

// Observation is a counter delta captured around one scenario. Global totals
// cannot prove that a lifecycle or collision scenario exercised its intended
// consumer, so every scenario carries its own stable terminal evidence.
type catalogAttributionObservation struct {
	PreparedChecks           uint64 `json:"prepared_checks"`
	PreparedStableChecks     uint64 `json:"prepared_stable_checks"`
	PreparedTerminalOutcomes uint64 `json:"prepared_terminal_outcomes"`
	RCChecks                 uint64 `json:"rc_checks"`
	RCStableChecks           uint64 `json:"rc_stable_checks"`
	RCTerminalOutcomes       uint64 `json:"rc_terminal_outcomes"`
	BucketFalsePositives     uint64 `json:"bucket_false_positives"`
	BucketFalseNegatives     uint64 `json:"bucket_false_negatives"`
	PreciseFalsePositives    uint64 `json:"precise_false_positives"`
	PreciseFalseNegatives    uint64 `json:"precise_false_negatives"`
	ShadowOverflow           bool   `json:"shadow_overflow"`
}

type catalogAttributionNode struct {
	ServiceID string                          `json:"service_id"`
	Report    cache.CatalogInvalidationReport `json:"report"`
}

type catalogAttributionLatency struct {
	Count   uint64                                   `json:"count"`
	Success uint64                                   `json:"successes"`
	Error   uint64                                   `json:"errors"`
	Miss    uint64                                   `json:"misses"`
	P50NS   int64                                    `json:"p50_ns"`
	P95NS   int64                                    `json:"p95_ns"`
	P99NS   int64                                    `json:"p99_ns"`
	Buckets []cache.CatalogInvalidationLatencyBucket `json:"buckets"`
}

type catalogAttributionReportV2 struct {
	SchemaVersion  int                          `json:"schema_version"`
	MatrixONESHA   string                       `json:"matrixone_sha"`
	Config         string                       `json:"config"`
	WindowStart    string                       `json:"window_start_utc"`
	WindowEnd      string                       `json:"window_end_utc"`
	Integrity      string                       `json:"integrity"`
	Nodes          []catalogAttributionNode     `json:"nodes"`
	Scenarios      []catalogAttributionScenario `json:"scenarios"`
	Prepared       catalogAttributionLatency    `json:"prepared_plan_rebuild"`
	RC             catalogAttributionLatency    `json:"rc_table_cache_reload"`
	DecisionTotals map[string]uint64            `json:"decision_totals"`
}

func requireCatalogAttributionBuild(t *testing.T, expected string) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if ok {
		settings := make(map[string]string, len(info.Settings))
		for _, setting := range info.Settings {
			settings[setting.Key] = setting.Value
		}
		if revision := settings["vcs.revision"]; revision != "" {
			if revision != expected {
				t.Fatalf("measurement SHA %s does not match executed build revision %q", expected, revision)
			}
			if settings["vcs.modified"] == "true" {
				t.Fatal("catalog attribution requires a clean VCS-attested build")
			}
			return
		}
	}
	// Some CGo test-link modes omit Go's VCS build settings. In that mode use
	// the exact clean checkout that owns the running test package as the
	// equivalent attestation, rejecting both ref drift and dirty source.
	root := findCatalogAttributionRepoRoot(t)
	revision, err := outputCatalogAttributionGit(root, "rev-parse", "HEAD")
	if err != nil || revision != expected {
		t.Fatalf("measurement SHA %s does not match clean checkout revision %q", expected, revision)
	}
	status, err := outputCatalogAttributionGit(root, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		t.Fatalf("cannot verify clean measurement checkout: %v", err)
	}
	if status != "" {
		t.Fatalf("catalog attribution requires a clean checkout, got %q", status)
	}
}

func findCatalogAttributionRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get measurement working directory: %v", err)
	}
	for {
		if info, err := os.Stat(filepath.Join(dir, ".git")); err == nil && (info.IsDir() || info.Mode().IsRegular()) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("measurement checkout root is not discoverable")
		}
		dir = parent
	}
}

func outputCatalogAttributionGit(root string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := cmd.Output()
	return strings.TrimSpace(string(output)), err
}

func TestCatalogInvalidationAttributionMultiCN(t *testing.T) {
	if os.Getenv(catalogAttributionEnabledEnv) != "1" {
		t.Skip("opt-in catalog invalidation attribution workload")
	}
	reportDir := os.Getenv(catalogAttributionReportEnv)
	if reportDir == "" || !filepath.IsAbs(reportDir) {
		t.Fatal("MO_CATALOG_INVALIDATION_REPORT_DIR must be an absolute path")
	}
	sha := os.Getenv(catalogAttributionSHAEnv)
	if len(sha) != 40 || strings.Trim(sha, "0123456789abcdefABCDEF") != "" {
		t.Fatal("MO_CATALOG_INVALIDATION_MATRIXONE_SHA must be a complete 40-byte hexadecimal SHA")
	}
	requireCatalogAttributionBuild(t, sha)
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		t.Fatal(err)
	}

	started := time.Now().UTC()
	var (
		cluster   Cluster
		db0, db1  *sql.DB
		nodes     []catalogAttributionNode
		scenarios []catalogAttributionScenario
		passed    bool
		cleanupOK = true
		closeOnce sync.Once
	)
	defer func() {
		windowEnd := time.Now().UTC()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if db0 != nil {
			if _, err := db0.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS catalog_attr_27235"); err != nil {
				cleanupOK = false
				t.Errorf("catalog fixture cleanup failed: %v", err)
			}
		}
		cancel()
		if db0 != nil {
			if err := db0.Close(); err != nil {
				cleanupOK = false
				t.Errorf("CN0 database close failed: %v", err)
			}
		}
		if db1 != nil {
			if err := db1.Close(); err != nil {
				cleanupOK = false
				t.Errorf("CN1 database close failed: %v", err)
			}
		}
		nodeIntegrity := "incomplete"
		if passed && cleanupOK {
			nodeIntegrity = "complete"
		}
		if cluster != nil {
			nodes = snapshotCatalogAttributionNodes(t, cluster, sha, started, windowEnd, nodeIntegrity)
			closeOnce.Do(func() {
				if err := cluster.Close(); err != nil {
					cleanupOK = false
					t.Errorf("embedded cluster cleanup failed: %v", err)
				}
			})
		}
		integrity := "incomplete"
		if passed && cleanupOK {
			integrity = "complete"
		}
		report := catalogAttributionReportV2{
			SchemaVersion:  3,
			MatrixONESHA:   sha,
			Config:         "pkg/embed:1log-1tn-2cn;exact-authoritative",
			WindowStart:    started.Format(time.RFC3339Nano),
			WindowEnd:      windowEnd.Format(time.RFC3339Nano),
			Integrity:      integrity,
			Nodes:          nodes,
			Scenarios:      scenarios,
			Prepared:       mergeCatalogLatency(nodes, func(r cache.CatalogInvalidationReport) cache.CatalogInvalidationLatency { return r.PreparedPlanRebuild }),
			RC:             mergeCatalogLatency(nodes, func(r cache.CatalogInvalidationReport) cache.CatalogInvalidationLatency { return r.RCTableCacheReload }),
			DecisionTotals: mergeDecisionTotals(nodes),
		}
		if err := writeCatalogAttributionReport(reportDir, report); err != nil {
			t.Errorf("write catalog invalidation report: %v", err)
		}
	}()

	var err error
	cluster, err = StartTestCluster(WithCNCount(2), WithPreStart(adjustBasicClusterService))
	if err != nil {
		t.Fatalf("start two-CN attribution cluster: %v", err)
	}

	cn0, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	db0 = openCatalogAttributionDB(t, cn0)
	db1 = openCatalogAttributionDB(t, cn1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	databaseName := "catalog_attr_27235"
	if err := resetCatalogAttributionFixture(ctx, db0, databaseName); err != nil {
		t.Fatal(err)
	}
	runMeasured := func(run func() error) error {
		before := snapshotCatalogAttributionNodes(t, cluster, sha, started, time.Now().UTC(), "measured")
		startIndex := len(scenarios)
		if err := run(); err != nil {
			return err
		}
		return annotateCatalogAttributionScenario(t, cluster, sha, started, &scenarios, startIndex, before)
	}
	if err := runMeasured(func() error { return runPreparedAttribution(ctx, db0, db1, databaseName, &scenarios) }); err != nil {
		t.Fatal(err)
	}
	if err := runMeasured(func() error { return runRCAttribution(ctx, db0, db1, databaseName, &scenarios) }); err != nil {
		t.Fatal(err)
	}
	if err := runMeasured(func() error { return runUnrelatedDDLAttribution(ctx, db0, db1, databaseName, &scenarios) }); err != nil {
		t.Fatal(err)
	}
	if err := runLifecycleAttribution(t, cluster, sha, started, ctx, db0, db1, databaseName, &scenarios); err != nil {
		t.Fatal(err)
	}

	// Snapshot and validate before the deferred writer marks the report complete.
	nodes = snapshotCatalogAttributionNodes(t, cluster, sha, started, time.Now().UTC(), "measured")
	if err := validateCatalogAttributionSamples(nodes, scenarios, sha); err != nil {
		t.Fatal(err)
	}
	passed = true
}

func openCatalogAttributionDB(t *testing.T, svc ServiceOperator) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", svc.GetServiceConfig().CN.Frontend.Port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	return db
}

func resetCatalogAttributionFixture(ctx context.Context, db *sql.DB, name string) error {
	for _, stmt := range []string{
		"DROP DATABASE IF EXISTS " + name,
		"CREATE DATABASE " + name,
		"CREATE TABLE " + name + ".prepared_table (id INT PRIMARY KEY, payload INT)",
		"CREATE TABLE " + name + ".rc_table (id INT PRIMARY KEY, payload INT)",
		"CREATE TABLE " + name + ".unrelated_table (id INT PRIMARY KEY, payload INT)",
		"CREATE TABLE " + name + ".lifecycle_table (id INT PRIMARY KEY, payload INT)",
		"INSERT INTO " + name + ".prepared_table VALUES (1, 1)",
		"INSERT INTO " + name + ".rc_table VALUES (1, 1)",
		"INSERT INTO " + name + ".unrelated_table VALUES (1, 1)",
		"INSERT INTO " + name + ".lifecycle_table VALUES (1, 1)",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("fixture %q: %w", stmt, err)
		}
	}
	return nil
}

func runPreparedAttribution(ctx context.Context, db0, db1 *sql.DB, database string, scenarios *[]catalogAttributionScenario) error {
	stmt, err := db0.PrepareContext(ctx, "SELECT payload FROM "+database+".prepared_table WHERE id = ?")
	if err != nil {
		return fmt.Errorf("prepare binary statement: %w", err)
	}
	defer stmt.Close()
	conn, err := db0.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open text prepared connection: %w", err)
	}
	defer conn.Close()
	textName := "catalog_attr_text_stmt"
	if _, err := conn.ExecContext(ctx, "PREPARE "+textName+" FROM 'SELECT payload FROM "+database+".prepared_table WHERE id = ?'"); err != nil {
		return fmt.Errorf("prepare text statement: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, _ = conn.ExecContext(cleanupCtx, "DEALLOCATE PREPARE "+textName)
		cancel()
	}()
	for i := 0; i < catalogAttributionSamples; i++ {
		column := fmt.Sprintf("prepared_c%03d", i)
		if _, err := db1.ExecContext(ctx, "ALTER TABLE "+database+".prepared_table ADD COLUMN "+column+" INT"); err != nil {
			return fmt.Errorf("prepared DDL %d: %w", i, err)
		}
		if err := waitCatalogObservation(ctx, db0, database, column); err != nil {
			return err
		}
		var payload int
		if err := stmt.QueryRowContext(ctx, 1).Scan(&payload); err != nil {
			return fmt.Errorf("prepared execute %d: %w", i, err)
		}
		if _, err := conn.ExecContext(ctx, "SET @catalog_attr_id = 1"); err != nil {
			return fmt.Errorf("set text prepared argument %d: %w", i, err)
		}
		if err := conn.QueryRowContext(ctx, "EXECUTE "+textName+" USING @catalog_attr_id").Scan(&payload); err != nil {
			return fmt.Errorf("text prepared execute %d: %w", i, err)
		}
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "prepared_schema_rebuild", DDLType: "alter", ConsumerCN: "cn-0", DDLCN: "cn-1",
		Protocol: "binary-and-text-prepare", Samples: catalogAttributionSamples, Status: "completed",
	})
	return nil
}

func runRCAttribution(ctx context.Context, db0, db1 *sql.DB, database string, scenarios *[]catalogAttributionScenario) error {
	for i := 0; i < catalogAttributionSamples; i++ {
		tx, err := db0.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return fmt.Errorf("begin RC transaction %d: %w", i, err)
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+database+".rc_table").Scan(&count); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("RC initial read %d: %w", i, err)
		}
		column := fmt.Sprintf("rc_c%03d", i)
		if _, err := db1.ExecContext(ctx, "ALTER TABLE "+database+".rc_table ADD COLUMN "+column+" INT"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("RC DDL %d: %w", i, err)
		}
		if err := waitCatalogObservation(ctx, db0, database, column); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+database+".rc_table").Scan(&count); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("RC reload %d: %w", i, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("RC commit %d: %w", i, err)
		}
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "rc_table_cache_reload", DDLType: "alter", ConsumerCN: "cn-0", DDLCN: "cn-1",
		Protocol: "read-committed", Samples: catalogAttributionSamples, Status: "completed",
	})
	return nil
}

func runUnrelatedDDLAttribution(ctx context.Context, db0, db1 *sql.DB, database string, scenarios *[]catalogAttributionScenario) error {
	stmt, err := db0.PrepareContext(ctx, "SELECT payload FROM "+database+".prepared_table WHERE id = ?")
	if err != nil {
		return fmt.Errorf("prepare unrelated-DDL control statement: %w", err)
	}
	defer stmt.Close()
	for i := 0; i < catalogAttributionSamples; i++ {
		column := fmt.Sprintf("unrelated_c%03d", i)
		if _, err := db1.ExecContext(ctx, "ALTER TABLE "+database+".unrelated_table ADD COLUMN "+column+" INT"); err != nil {
			return fmt.Errorf("unrelated DDL %d: %w", i, err)
		}
		if err := waitCatalogObservation(ctx, db0, database, column); err != nil {
			return err
		}
		var payload int
		if err := stmt.QueryRowContext(ctx, 1).Scan(&payload); err != nil {
			return fmt.Errorf("unrelated read %d: %w", i, err)
		}
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "same_account_unrelated_ddl", DDLType: "alter-unrelated", ConsumerCN: "cn-0", DDLCN: "cn-1",
		Samples: catalogAttributionSamples, Status: "completed",
	})
	return nil
}

func runLifecycleScenario(
	t *testing.T,
	cluster Cluster,
	sha string,
	started time.Time,
	ctx context.Context,
	scenarios *[]catalogAttributionScenario,
	name, ddlType, protocol string,
	db0, db1 *sql.DB,
	fn func() error,
) error {
	t.Helper()
	before := snapshotCatalogAttributionNodes(t, cluster, sha, started, time.Now().UTC(), "measured")
	if err := fn(); err != nil {
		return err
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: name, DDLType: ddlType, ConsumerCN: "cn-0", DDLCN: "cn-1",
		Protocol: protocol, Samples: 1, Status: "completed",
	})
	return annotateCatalogAttributionScenario(t, cluster, sha, started, scenarios, len(*scenarios)-1, before)
}

func runLifecycleAttribution(
	t *testing.T,
	cluster Cluster,
	sha string,
	started time.Time,
	ctx context.Context,
	db0, db1 *sql.DB,
	database string,
	scenarios *[]catalogAttributionScenario,
) error {
	base := database + ".lifecycle_table"
	if err := runLifecycleScenario(t, cluster, sha, started, ctx, scenarios,
		"no_change", "none", "binary-prepare", db0, db1, func() error {
			stmt, err := db0.PrepareContext(ctx, "SELECT payload FROM "+base+" WHERE id = ?")
			if err != nil {
				return fmt.Errorf("no-change prepare: %w", err)
			}
			defer stmt.Close()
			var payload int
			if err := stmt.QueryRowContext(ctx, 1).Scan(&payload); err != nil {
				return fmt.Errorf("no-change execute: %w", err)
			}
			return nil
		}); err != nil {
		return err
	}

	if err := runLifecycleScenario(t, cluster, sha, started, ctx, scenarios,
		"truncate", "truncate", "binary-prepare", db0, db1, func() error {
			stmt, err := db0.PrepareContext(ctx, "SELECT payload FROM "+base+" WHERE id = ?")
			if err != nil {
				return fmt.Errorf("truncate prepare: %w", err)
			}
			defer stmt.Close()
			if _, err := db1.ExecContext(ctx, "TRUNCATE TABLE "+base); err != nil {
				return fmt.Errorf("truncate: %w", err)
			}
			var payload sql.NullInt64
			err = stmt.QueryRowContext(ctx, 1).Scan(&payload)
			if err != nil && err != sql.ErrNoRows {
				return fmt.Errorf("truncate stale execute: %w", err)
			}
			if _, err := db1.ExecContext(ctx, "INSERT INTO "+base+" VALUES (1, 2)"); err != nil {
				return fmt.Errorf("restore after truncate: %w", err)
			}
			return nil
		}); err != nil {
		return err
	}

	rename := database + ".lifecycle_renamed"
	if err := runLifecycleScenario(t, cluster, sha, started, ctx, scenarios,
		"rename", "rename", "binary-prepare", db0, db1, func() error {
			stmt, err := db0.PrepareContext(ctx, "SELECT payload FROM "+base+" WHERE id = ?")
			if err != nil {
				return fmt.Errorf("rename prepare: %w", err)
			}
			defer stmt.Close()
			if _, err := db1.ExecContext(ctx, "ALTER TABLE "+base+" RENAME TO "+rename); err != nil {
				return fmt.Errorf("rename forward: %w", err)
			}
			var payload int
			if err := stmt.QueryRowContext(ctx, 1).Scan(&payload); err == nil {
				return fmt.Errorf("rename stale dependency unexpectedly executed")
			}
			if _, err := db1.ExecContext(ctx, "ALTER TABLE "+rename+" RENAME TO "+base); err != nil {
				return fmt.Errorf("rename backward: %w", err)
			}
			return waitTableAvailable(ctx, db0, base)
		}); err != nil {
		return err
	}

	if err := runLifecycleScenario(t, cluster, sha, started, ctx, scenarios,
		"table_drop_recreate", "drop-recreate", "binary-prepare", db0, db1, func() error {
			stmt, err := db0.PrepareContext(ctx, "SELECT payload FROM "+base+" WHERE id = ?")
			if err != nil {
				return fmt.Errorf("drop prepare: %w", err)
			}
			defer stmt.Close()
			if _, err := db1.ExecContext(ctx, "DROP TABLE "+base); err != nil {
				return fmt.Errorf("drop table: %w", err)
			}
			var payload int
			if err := stmt.QueryRowContext(ctx, 1).Scan(&payload); err == nil {
				return fmt.Errorf("drop stale dependency unexpectedly executed")
			}
			if _, err := db1.ExecContext(ctx, "CREATE TABLE "+base+" (id INT PRIMARY KEY, payload INT)"); err != nil {
				return fmt.Errorf("recreate table: %w", err)
			}
			return waitTableAvailable(ctx, db0, base)
		}); err != nil {
		return err
	}

	databaseRecreated := database + "_recreated"
	if err := runLifecycleScenario(t, cluster, sha, started, ctx, scenarios,
		"database_drop_recreate", "database-drop-recreate", "binary-prepare", db0, db1, func() error {
			if _, err := db1.ExecContext(ctx, "CREATE DATABASE "+databaseRecreated); err != nil {
				return fmt.Errorf("create recreation database: %w", err)
			}
			if _, err := db1.ExecContext(ctx, "CREATE TABLE "+databaseRecreated+".lifecycle_table (id INT PRIMARY KEY, payload INT)"); err != nil {
				return fmt.Errorf("create recreation table: %w", err)
			}
			stmt, err := db0.PrepareContext(ctx, "SELECT payload FROM "+databaseRecreated+".lifecycle_table WHERE id = ?")
			if err != nil {
				return fmt.Errorf("database prepare: %w", err)
			}
			defer stmt.Close()
			if _, err := db1.ExecContext(ctx, "DROP DATABASE "+databaseRecreated); err != nil {
				return fmt.Errorf("drop recreation database: %w", err)
			}
			var payload int
			if err := stmt.QueryRowContext(ctx, 1).Scan(&payload); err == nil {
				return fmt.Errorf("database stale dependency unexpectedly executed")
			}
			if _, err := db1.ExecContext(ctx, "CREATE DATABASE "+databaseRecreated); err != nil {
				return fmt.Errorf("recreate database: %w", err)
			}
			if _, err := db1.ExecContext(ctx, "CREATE TABLE "+databaseRecreated+".lifecycle_table (id INT PRIMARY KEY, payload INT)"); err != nil {
				return fmt.Errorf("recreate database table: %w", err)
			}
			if err := waitTableAvailable(ctx, db0, databaseRecreated+".lifecycle_table"); err != nil {
				return fmt.Errorf("recreate database observation: %w", err)
			}
			return nil
		}); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, _ = db1.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+databaseRecreated)
		cancel()
		return err
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, _ = db1.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+databaseRecreated)
	cancel()
	return nil
}

func waitTableAvailable(ctx context.Context, db *sql.DB, qualifiedTable string) error {
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	query := "SELECT COUNT(*) FROM " + qualifiedTable
	for {
		var count int
		err := db.QueryRowContext(deadline, query).Scan(&count)
		if err == nil {
			return nil
		}
		if deadline.Err() != nil {
			return err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-deadline.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func waitCatalogObservation(ctx context.Context, db *sql.DB, database, column string) error {
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	table := "prepared_table"
	if strings.HasPrefix(column, "rc_") {
		table = "rc_table"
	} else if strings.HasPrefix(column, "unrelated_") {
		table = "unrelated_table"
	}
	query := "SELECT " + column + " FROM " + database + "." + table + " LIMIT 1"
	for {
		var value sql.NullInt64
		row := db.QueryRowContext(deadline, query)
		err := row.Scan(&value)
		if err == nil {
			return nil
		}
		if deadline.Err() != nil {
			if err != nil {
				return fmt.Errorf("catalog observation %s: %w", column, err)
			}
			return fmt.Errorf("catalog observation %s timed out", column)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-deadline.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func snapshotCatalogAttributionNodes(t *testing.T, cluster Cluster, sha string, start, end time.Time, integrity string) []catalogAttributionNode {
	t.Helper()
	nodes := make([]catalogAttributionNode, 0, 2)
	for i := 0; i < 2; i++ {
		svc, err := cluster.GetCNService(i)
		if err != nil {
			t.Errorf("get CN %d: %v", i, err)
			continue
		}
		raw, ok := svc.RawService().(cnservice.Service)
		if !ok {
			t.Errorf("CN %d does not expose cnservice.Service", i)
			continue
		}
		eng, ok := raw.GetEngine().(*disttae.Engine)
		if !ok {
			t.Errorf("CN %d does not expose disttae.Engine", i)
			continue
		}
		cc := eng.GetLatestCatalogCache()
		if cc == nil || !cc.CatalogInvalidationAttributionEnabled() {
			t.Errorf("CN %d attribution is not enabled", i)
			continue
		}
		cc.SetCatalogInvalidationReportMetadata(cache.CatalogInvalidationReportMetadata{
			MatrixONESHA: sha,
			Config:       "pkg/embed:1log-1tn-2cn;exact-authoritative",
			Window:       start.Format(time.RFC3339Nano) + "/" + end.Format(time.RFC3339Nano),
			Integrity:    integrity,
		})
		nodes = append(nodes, catalogAttributionNode{ServiceID: svc.ServiceID(), Report: cc.SnapshotCatalogInvalidationReport()})
	}
	return nodes
}

func catalogAttributionObservationFor(nodes []catalogAttributionNode) catalogAttributionObservation {
	var out catalogAttributionObservation
	for _, node := range nodes {
		if node.Report.Shadow.Overflow {
			out.ShadowOverflow = true
		}
		prepared := node.Report.Consumers["prepared_plan"]
		rc := node.Report.Consumers["rc_table_cache"]
		out.PreparedChecks += prepared.Checks
		out.PreparedStableChecks += prepared.StableChecks
		out.PreparedTerminalOutcomes += node.Report.PreparedPlanRebuild.Count
		out.RCChecks += rc.Checks
		out.RCStableChecks += rc.StableChecks
		out.RCTerminalOutcomes += node.Report.RCTableCacheReload.Count
		out.BucketFalsePositives += prepared.BucketFalsePositives + rc.BucketFalsePositives
		out.BucketFalseNegatives += prepared.BucketFalseNegatives + rc.BucketFalseNegatives
		out.PreciseFalsePositives += prepared.PreciseFalsePositives + rc.PreciseFalsePositives
		out.PreciseFalseNegatives += prepared.PreciseFalseNegatives + rc.PreciseFalseNegatives
	}
	return out
}

func subtractCatalogAttributionObservation(after, before catalogAttributionObservation) catalogAttributionObservation {
	return catalogAttributionObservation{
		PreparedChecks:           after.PreparedChecks - before.PreparedChecks,
		PreparedStableChecks:     after.PreparedStableChecks - before.PreparedStableChecks,
		PreparedTerminalOutcomes: after.PreparedTerminalOutcomes - before.PreparedTerminalOutcomes,
		RCChecks:                 after.RCChecks - before.RCChecks,
		RCStableChecks:           after.RCStableChecks - before.RCStableChecks,
		RCTerminalOutcomes:       after.RCTerminalOutcomes - before.RCTerminalOutcomes,
		BucketFalsePositives:     after.BucketFalsePositives - before.BucketFalsePositives,
		BucketFalseNegatives:     after.BucketFalseNegatives - before.BucketFalseNegatives,
		PreciseFalsePositives:    after.PreciseFalsePositives - before.PreciseFalsePositives,
		PreciseFalseNegatives:    after.PreciseFalseNegatives - before.PreciseFalseNegatives,
		ShadowOverflow:           after.ShadowOverflow,
	}
}

func annotateCatalogAttributionScenario(
	t *testing.T,
	cluster Cluster,
	sha string,
	started time.Time,
	scenarios *[]catalogAttributionScenario,
	startIndex int,
	before []catalogAttributionNode,
) error {
	t.Helper()
	if startIndex >= len(*scenarios) {
		return fmt.Errorf("scenario did not publish a result")
	}
	after := snapshotCatalogAttributionNodes(t, cluster, sha, started, time.Now().UTC(), "measured")
	delta := subtractCatalogAttributionObservation(
		catalogAttributionObservationFor(after),
		catalogAttributionObservationFor(before),
	)
	for i := startIndex; i < len(*scenarios); i++ {
		(*scenarios)[i].Observation = delta
	}
	return nil
}

func validateCatalogAttributionSamples(nodes []catalogAttributionNode, scenarios []catalogAttributionScenario, sha string) error {
	if len(nodes) != 2 {
		return fmt.Errorf("expected two CN reports, got %d", len(nodes))
	}
	for _, node := range nodes {
		if !node.Report.Enabled || node.Report.Metadata.MatrixONESHA != sha || node.Report.Metadata.Integrity != "measured" {
			return fmt.Errorf("node %s has unbound report identity or integrity", node.ServiceID)
		}
	}
	required := map[string]bool{
		"no_change": false, "truncate": false, "rename": false,
		"table_drop_recreate": false, "database_drop_recreate": false,
		"prepared_schema_rebuild": false, "rc_table_cache_reload": false,
		"same_account_unrelated_ddl": false,
	}
	for _, scenario := range scenarios {
		if scenario.Status != "completed" || scenario.Samples < 1 {
			return fmt.Errorf("scenario %s is incomplete", scenario.Name)
		}
		if _, ok := required[scenario.Name]; !ok {
			return fmt.Errorf("unexpected scenario %s", scenario.Name)
		}
		if scenario.Samples >= catalogAttributionSamples || scenario.Samples == 1 {
			required[scenario.Name] = true
		}
		if scenario.Observation.ShadowOverflow {
			return fmt.Errorf("scenario %s observed shadow overflow", scenario.Name)
		}
		if scenario.Observation.PreciseFalseNegatives != 0 {
			return fmt.Errorf("scenario %s observed precise false negatives: %d", scenario.Name, scenario.Observation.PreciseFalseNegatives)
		}
		if scenario.Observation.PreciseFalsePositives != 0 {
			return fmt.Errorf("scenario %s observed precise false positives: %d", scenario.Name, scenario.Observation.PreciseFalsePositives)
		}
		if scenario.Name == "prepared_schema_rebuild" &&
			(scenario.Observation.PreparedStableChecks < uint64(scenario.Samples) ||
				scenario.Observation.PreparedTerminalOutcomes < uint64(scenario.Samples)) {
			return fmt.Errorf("prepared scenario lacks per-scenario terminal evidence: %+v", scenario.Observation)
		}
		if scenario.Name == "rc_table_cache_reload" &&
			(scenario.Observation.RCStableChecks < uint64(scenario.Samples) ||
				scenario.Observation.RCTerminalOutcomes < uint64(scenario.Samples)) {
			return fmt.Errorf("RC scenario lacks per-scenario terminal evidence: %+v", scenario.Observation)
		}
		if scenario.Name == "same_account_unrelated_ddl" && scenario.Observation.PreparedStableChecks < uint64(scenario.Samples) {
			return fmt.Errorf("unrelated DDL did not exercise a prepared dependency: %+v", scenario.Observation)
		}
		if scenario.Name == "same_account_unrelated_ddl" && scenario.Observation.BucketFalsePositives == 0 {
			return fmt.Errorf("unrelated DDL did not produce a bucket false-positive opportunity")
		}
	}
	for name, seen := range required {
		if !seen {
			return fmt.Errorf("required scenario %s is missing", name)
		}
	}
	var prepared, rc uint64
	for _, node := range nodes {
		for name, histogram := range map[string]cache.CatalogInvalidationLatency{
			"prepared": node.Report.PreparedPlanRebuild,
			"rc":       node.Report.RCTableCacheReload,
		} {
			var bucketCount uint64
			for _, bucket := range histogram.Buckets {
				bucketCount += bucket.Count
			}
			if bucketCount != histogram.Count || histogram.Success+histogram.Error+histogram.Miss != histogram.Count {
				return fmt.Errorf("node %s %s histogram is inconsistent", node.ServiceID, name)
			}
		}
		for _, consumer := range []string{"prepared_plan", "rc_table_cache"} {
			counter, ok := node.Report.Consumers[consumer]
			if !ok || counter.StableChecks == 0 || counter.InconclusiveChecks != 0 {
				return fmt.Errorf("node %s consumer %s lacks stable observations", node.ServiceID, consumer)
			}
		}
		prepared += node.Report.PreparedPlanRebuild.Count
		rc += node.Report.RCTableCacheReload.Count
		if node.Report.Shadow.Overflow {
			return fmt.Errorf("node %s reports shadow overflow", node.ServiceID)
		}
		for consumer, counter := range node.Report.Consumers {
			if counter.StableChecks+counter.InconclusiveChecks != counter.Checks {
				return fmt.Errorf("node %s consumer %s has inconsistent decision counters", node.ServiceID, consumer)
			}
		}
	}
	if prepared < catalogAttributionSamples || rc < catalogAttributionSamples {
		return fmt.Errorf("terminal samples below 128: prepared=%d rc=%d", prepared, rc)
	}
	return nil
}

func mergeDecisionTotals(nodes []catalogAttributionNode) map[string]uint64 {
	totals := make(map[string]uint64)
	for _, node := range nodes {
		for consumer, counter := range node.Report.Consumers {
			prefix := consumer + "."
			totals[prefix+"checks"] += counter.Checks
			totals[prefix+"stable_checks"] += counter.StableChecks
			totals[prefix+"inconclusive_checks"] += counter.InconclusiveChecks
			totals[prefix+"bucket_false_positives"] += counter.BucketFalsePositives
			totals[prefix+"bucket_false_negatives"] += counter.BucketFalseNegatives
			totals[prefix+"precise_false_positives"] += counter.PreciseFalsePositives
			totals[prefix+"precise_false_negatives"] += counter.PreciseFalseNegatives
		}
	}
	return totals
}

func mergeCatalogLatency(nodes []catalogAttributionNode, pick func(cache.CatalogInvalidationReport) cache.CatalogInvalidationLatency) catalogAttributionLatency {
	var out catalogAttributionLatency
	for _, node := range nodes {
		value := pick(node.Report)
		out.Count += value.Count
		out.Success += value.Success
		out.Error += value.Error
		out.Miss += value.Miss
		if len(out.Buckets) == 0 {
			out.Buckets = make([]cache.CatalogInvalidationLatencyBucket, len(value.Buckets))
			copy(out.Buckets, value.Buckets)
		} else {
			for i := range value.Buckets {
				if i >= len(out.Buckets) {
					out.Buckets = append(out.Buckets, value.Buckets[i])
				} else {
					out.Buckets[i].Count += value.Buckets[i].Count
				}
			}
		}
	}
	out.P50NS, out.P95NS, out.P99NS = latencyQuantiles(out.Buckets, out.Count)
	return out
}

func latencyQuantiles(buckets []cache.CatalogInvalidationLatencyBucket, count uint64) (int64, int64, int64) {
	quantile := func(q float64) int64 {
		if count == 0 {
			return 0
		}
		target := uint64(float64(count-1)*q) + 1
		var seen uint64
		for _, bucket := range buckets {
			seen += bucket.Count
			if seen >= target {
				return bucket.UpperBoundNS
			}
		}
		return -1
	}
	return quantile(0.50), quantile(0.95), quantile(0.99)
}

func writeCatalogAttributionReport(dir string, report catalogAttributionReportV2) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(dir, ".catalog-invalidation-report-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, "catalog-invalidation-report.json"))
}
