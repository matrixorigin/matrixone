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
	"path/filepath"
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
	Name       string `json:"name"`
	DDLType    string `json:"ddl_type"`
	ConsumerCN string `json:"consumer_cn"`
	DDLCN      string `json:"ddl_cn"`
	Protocol   string `json:"protocol,omitempty"`
	Samples    int    `json:"stable_terminal_samples"`
	Status     string `json:"status"`
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
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		t.Fatal(err)
	}

	started := time.Now().UTC()
	var (
		cluster   Cluster
		nodes     []catalogAttributionNode
		scenarios []catalogAttributionScenario
		passed    bool
		closeOnce sync.Once
	)
	defer func() {
		windowEnd := time.Now().UTC()
		nodeIntegrity := "incomplete"
		if passed {
			nodeIntegrity = "complete"
		}
		if cluster != nil {
			nodes = snapshotCatalogAttributionNodes(t, cluster, sha, started, windowEnd, nodeIntegrity)
			closeOnce.Do(func() { _ = cluster.Close() })
		}
		integrity := "incomplete"
		if passed {
			integrity = "complete"
		}
		report := catalogAttributionReportV2{
			SchemaVersion:  2,
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
	db0 := openCatalogAttributionDB(t, cn0)
	db1 := openCatalogAttributionDB(t, cn1)
	defer db0.Close()
	defer db1.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	databaseName := "catalog_attr_27235"
	if err := resetCatalogAttributionFixture(ctx, db0, databaseName); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db0.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+databaseName) }()

	if err := runPreparedAttribution(ctx, db0, db1, databaseName, &scenarios); err != nil {
		t.Fatal(err)
	}
	if err := runRCAttribution(ctx, db0, db1, databaseName, &scenarios); err != nil {
		t.Fatal(err)
	}
	if err := runUnrelatedDDLAttribution(ctx, db0, db1, databaseName, &scenarios); err != nil {
		t.Fatal(err)
	}
	if err := runLifecycleAttribution(ctx, db0, db1, databaseName, &scenarios); err != nil {
		t.Fatal(err)
	}

	// Snapshot and validate before the deferred writer marks the report complete.
	nodes = snapshotCatalogAttributionNodes(t, cluster, sha, started, time.Now().UTC(), "complete")
	if err := validateCatalogAttributionSamples(nodes, scenarios); err != nil {
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
	defer func() { _, _ = conn.ExecContext(context.Background(), "DEALLOCATE PREPARE "+textName) }()
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
	for i := 0; i < catalogAttributionSamples; i++ {
		column := fmt.Sprintf("unrelated_c%03d", i)
		if _, err := db1.ExecContext(ctx, "ALTER TABLE "+database+".unrelated_table ADD COLUMN "+column+" INT"); err != nil {
			return fmt.Errorf("unrelated DDL %d: %w", i, err)
		}
		if err := waitCatalogObservation(ctx, db0, database, column); err != nil {
			return err
		}
		var payload int
		if err := db0.QueryRowContext(ctx, "SELECT payload FROM "+database+".prepared_table WHERE id = 1").Scan(&payload); err != nil {
			return fmt.Errorf("unrelated read %d: %w", i, err)
		}
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "same_account_unrelated_ddl", DDLType: "alter-unrelated", ConsumerCN: "cn-0", DDLCN: "cn-1",
		Samples: catalogAttributionSamples, Status: "completed",
	})
	return nil
}

func runLifecycleAttribution(ctx context.Context, db0, db1 *sql.DB, database string, scenarios *[]catalogAttributionScenario) error {
	base := database + ".lifecycle_table"
	if err := waitTableAvailable(ctx, db0, base); err != nil {
		return fmt.Errorf("no-change observation: %w", err)
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "no_change", DDLType: "none", ConsumerCN: "cn-0", DDLCN: "none", Samples: 1, Status: "completed",
	})

	if _, err := db1.ExecContext(ctx, "TRUNCATE TABLE "+base); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	if err := waitTableAvailable(ctx, db0, base); err != nil {
		return fmt.Errorf("truncate observation: %w", err)
	}
	if _, err := db1.ExecContext(ctx, "INSERT INTO "+base+" VALUES (1, 2)"); err != nil {
		return fmt.Errorf("restore after truncate: %w", err)
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "truncate", DDLType: "truncate", ConsumerCN: "cn-0", DDLCN: "cn-1", Samples: 1, Status: "completed",
	})

	rename := database + ".lifecycle_renamed"
	if _, err := db1.ExecContext(ctx, "ALTER TABLE "+base+" RENAME TO "+rename); err != nil {
		return fmt.Errorf("rename forward: %w", err)
	}
	if err := waitTableAvailable(ctx, db0, rename); err != nil {
		return fmt.Errorf("rename forward observation: %w", err)
	}
	if _, err := db1.ExecContext(ctx, "ALTER TABLE "+rename+" RENAME TO "+base); err != nil {
		return fmt.Errorf("rename backward: %w", err)
	}
	if err := waitTableAvailable(ctx, db0, base); err != nil {
		return fmt.Errorf("rename backward observation: %w", err)
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "rename", DDLType: "rename", ConsumerCN: "cn-0", DDLCN: "cn-1", Samples: 1, Status: "completed",
	})

	if _, err := db1.ExecContext(ctx, "DROP TABLE "+base); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	if _, err := db1.ExecContext(ctx, "CREATE TABLE "+base+" (id INT PRIMARY KEY, payload INT)"); err != nil {
		return fmt.Errorf("recreate table: %w", err)
	}
	if err := waitTableAvailable(ctx, db0, base); err != nil {
		return fmt.Errorf("recreate table observation: %w", err)
	}
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "table_drop_recreate", DDLType: "drop-recreate", ConsumerCN: "cn-0", DDLCN: "cn-1", Samples: 1, Status: "completed",
	})

	databaseRecreated := database + "_recreated"
	if _, err := db1.ExecContext(ctx, "CREATE DATABASE "+databaseRecreated); err != nil {
		return fmt.Errorf("create recreation database: %w", err)
	}
	defer func() { _, _ = db1.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+databaseRecreated) }()
	if _, err := db1.ExecContext(ctx, "CREATE TABLE "+databaseRecreated+".lifecycle_table (id INT PRIMARY KEY, payload INT)"); err != nil {
		return fmt.Errorf("create recreation table: %w", err)
	}
	if _, err := db1.ExecContext(ctx, "DROP DATABASE "+databaseRecreated); err != nil {
		return fmt.Errorf("drop recreation database: %w", err)
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
	_, _ = db1.ExecContext(ctx, "DROP DATABASE IF EXISTS "+databaseRecreated)
	*scenarios = append(*scenarios, catalogAttributionScenario{
		Name: "database_drop_recreate", DDLType: "database-drop-recreate", ConsumerCN: "cn-0", DDLCN: "cn-1", Samples: 1, Status: "completed",
	})
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

func validateCatalogAttributionSamples(nodes []catalogAttributionNode, scenarios []catalogAttributionScenario) error {
	if len(nodes) != 2 {
		return fmt.Errorf("expected two CN reports, got %d", len(nodes))
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
	}
	for name, seen := range required {
		if !seen {
			return fmt.Errorf("required scenario %s is missing", name)
		}
	}
	var prepared, rc uint64
	for _, node := range nodes {
		prepared += node.Report.PreparedPlanRebuild.Count
		rc += node.Report.RCTableCacheReload.Count
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
