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

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type localE2EConfig struct {
	CatalogURI string
	Warehouse  string
	DSN        string
	ReportDir  string
	Namespace  string
	Catalog    string
	Database   string
}

type caseResult struct {
	ID       string            `json:"case_id"`
	Name     string            `json:"name"`
	Status   string            `json:"status"`
	SQL      []string          `json:"sql,omitempty"`
	Expected []string          `json:"expected,omitempty"`
	Actual   []string          `json:"actual,omitempty"`
	Details  map[string]string `json:"details,omitempty"`
	Error    string            `json:"error,omitempty"`
}

type runSummary struct {
	RunID     string       `json:"run_id"`
	Namespace string       `json:"namespace"`
	Database  string       `json:"database"`
	Catalog   string       `json:"catalog"`
	StartedAt string       `json:"started_at"`
	EndedAt   string       `json:"ended_at"`
	Cases     []caseResult `json:"cases"`
}

func main() {
	cfg := localE2EConfig{}
	flag.StringVar(&cfg.CatalogURI, "catalog-uri", envOr("MO_ICEBERG_E2E_CATALOG_URI", "http://127.0.0.1:19120/iceberg"), "Iceberg REST catalog URI")
	flag.StringVar(&cfg.Warehouse, "warehouse", envOr("MO_ICEBERG_E2E_WAREHOUSE", "s3://mo-iceberg/warehouse"), "Iceberg warehouse location")
	flag.StringVar(&cfg.DSN, "dsn", envOr("MO_ICEBERG_E2E_DSN", "root:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=30s&writeTimeout=30s&multiStatements=false"), "MatrixOne MySQL DSN")
	flag.StringVar(&cfg.ReportDir, "report-dir", envOr("MO_ICEBERG_REPORT_DIR", "test/iceberg/reports/e2e-local"), "report output directory")
	flag.StringVar(&cfg.Namespace, "namespace", envOr("MO_ICEBERG_E2E_NAMESPACE", ""), "Iceberg namespace to create")
	flag.StringVar(&cfg.Catalog, "mo-catalog", envOr("MO_ICEBERG_E2E_MO_CATALOG", ""), "MatrixOne Iceberg catalog name")
	flag.StringVar(&cfg.Database, "mo-db", envOr("MO_ICEBERG_E2E_MO_DB", ""), "MatrixOne database name")
	flag.Parse()

	runID := time.Now().UTC().Format("20060102t150405z")
	if cfg.Namespace == "" {
		cfg.Namespace = "ci_e2e_" + runID
	}
	if cfg.Catalog == "" {
		cfg.Catalog = "iceci_" + runID
	}
	if cfg.Database == "" {
		cfg.Database = "iceci_" + runID
	}
	if err := validateIdentifier(cfg.Namespace, "namespace"); err != nil {
		fatal(err)
	}
	if err := validateIdentifier(cfg.Catalog, "catalog"); err != nil {
		fatal(err)
	}
	if err := validateIdentifier(cfg.Database, "database"); err != nil {
		fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := os.MkdirAll(cfg.ReportDir, 0o755); err != nil {
		fatal(fmt.Errorf("create report dir: %w", err))
	}
	started := time.Now().UTC()
	summary := runSummary{
		RunID:     runID,
		Namespace: cfg.Namespace,
		Database:  cfg.Database,
		Catalog:   cfg.Catalog,
		StartedAt: started.Format(time.RFC3339),
	}

	if err := seedRESTTables(ctx, cfg); err != nil {
		result := caseResult{ID: "ICE-CI-E2E-000", Name: "rest-seed", Status: "failed", Error: err.Error()}
		summary.Cases = append(summary.Cases, result)
		_ = writeCaseReport(cfg.ReportDir, result)
		_ = writeRunSummary(cfg.ReportDir, summary)
		fatal(err)
	}

	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		fatal(fmt.Errorf("open MO connection: %w", err))
	}
	defer db.Close()
	if err := waitForDB(ctx, db); err != nil {
		fatal(err)
	}

	runner := &caseRunner{cfg: cfg, db: db}
	if err := runner.setup(ctx); err != nil {
		result := caseResult{ID: "ICE-CI-E2E-001", Name: "mo-setup", Status: "failed", Error: err.Error()}
		summary.Cases = append(summary.Cases, result)
		_ = writeCaseReport(cfg.ReportDir, result)
		_ = writeRunSummary(cfg.ReportDir, summary)
		fatal(err)
	}

	cases := []func(context.Context) caseResult{
		runner.catalogAndMappingCase,
		runner.accessLifecycleCase,
		runner.concurrentCreateMappingAndDropCase,
		runner.appendReadAndTimeTravelCase,
		runner.emptyStringReadCase,
		runner.partitionFilterCase,
		runner.yearPartitionDateCase,
		runner.mergeOnReadDeleteCase,
	}
	failed := false
	for _, fn := range cases {
		result := fn(ctx)
		if result.Status != "passed" {
			failed = true
		}
		summary.Cases = append(summary.Cases, result)
		if err := writeCaseReport(cfg.ReportDir, result); err != nil {
			fatal(err)
		}
	}
	summary.EndedAt = time.Now().UTC().Format(time.RFC3339)
	if err := writeRunSummary(cfg.ReportDir, summary); err != nil {
		fatal(err)
	}
	if failed {
		fatal(fmt.Errorf("one or more Iceberg E2E local cases failed"))
	}
}

type caseRunner struct {
	cfg localE2EConfig
	db  *sql.DB
}

func (r *caseRunner) setup(ctx context.Context) error {
	statements := []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", ident(r.cfg.Database)),
		fmt.Sprintf("DROP ICEBERG CATALOG IF EXISTS %s", ident(r.cfg.Catalog)),
		fmt.Sprintf("CREATE DATABASE %s", ident(r.cfg.Database)),
		fmt.Sprintf("CREATE ICEBERG CATALOG %s WITH ('type'='rest','uri'=%s,'warehouse'=%s,'auth_mode'='none')",
			ident(r.cfg.Catalog), sqlString(r.cfg.CatalogURI), sqlString(r.cfg.Warehouse)),
		fmt.Sprintf("CALL iceberg_register_access(%s, %s)",
			sqlString(r.cfg.Catalog),
			sqlString("scope=cluster,account_id=0,external_principal=ci-local,endpoint=localhost,region=us-east-1,bucket=mo-iceberg")),
		fmt.Sprintf(`CREATE EXTERNAL TABLE %s.%s (
  order_id BIGINT,
  bucket INT,
  amount BIGINT,
  region TEXT
) ENGINE = ICEBERG WITH ('catalog'=%s,'namespace'=%s,'table'='append_orders','ref'='main','read_mode'='append_only','write_mode'='append_only')`,
			ident(r.cfg.Database), ident("append_orders"), sqlString(r.cfg.Catalog), sqlString(r.cfg.Namespace)),
		fmt.Sprintf(`CREATE EXTERNAL TABLE %s.%s (
  order_id BIGINT,
  bucket INT,
  amount BIGINT,
  region TEXT
) ENGINE = ICEBERG WITH ('catalog'=%s,'namespace'=%s,'table'='partition_orders','ref'='main','read_mode'='append_only','write_mode'='append_only')`,
			ident(r.cfg.Database), ident("partition_orders"), sqlString(r.cfg.Catalog), sqlString(r.cfg.Namespace)),
		fmt.Sprintf(`CREATE EXTERNAL TABLE %s.%s (
  id BIGINT,
  obs_date DATE,
  value BIGINT
) ENGINE = ICEBERG WITH ('catalog'=%s,'namespace'=%s,'table'='year_partition_tiny','ref'='main','read_mode'='append_only','write_mode'='append_only')`,
			ident(r.cfg.Database), ident("year_partition_tiny"), sqlString(r.cfg.Catalog), sqlString(r.cfg.Namespace)),
		fmt.Sprintf(`CREATE EXTERNAL TABLE %s.%s (
  account_id BIGINT,
  balance BIGINT,
  region TEXT
) ENGINE = ICEBERG WITH ('catalog'=%s,'namespace'=%s,'table'='mor_accounts','ref'='main','read_mode'='merge_on_read','write_mode'='merge_on_read')`,
			ident(r.cfg.Database), ident("mor_accounts"), sqlString(r.cfg.Catalog), sqlString(r.cfg.Namespace)),
	}
	for _, stmt := range statements {
		if _, err := r.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute setup statement %q: %w", redactText(stmt), err)
		}
	}
	return nil
}

func (r *caseRunner) catalogAndMappingCase(ctx context.Context) caseResult {
	sqls := []string{
		fmt.Sprintf("CREATE ICEBERG CATALOG bad_%s WITH ('type'='rest','uri'=%s,'warehouse'=%s,'token_secret'='raw-token')",
			r.cfg.Catalog, sqlString(r.cfg.CatalogURI), sqlString(r.cfg.Warehouse)),
		fmt.Sprintf("SHOW ICEBERG NAMESPACES FROM %s", ident(r.cfg.Catalog)),
		fmt.Sprintf("SHOW ICEBERG TABLES FROM %s.%s", ident(r.cfg.Catalog), ident(r.cfg.Namespace)),
		fmt.Sprintf("SHOW CREATE TABLE %s.%s", ident(r.cfg.Database), ident("append_orders")),
	}
	details := map[string]string{}
	if _, err := r.db.ExecContext(ctx, sqls[0]); err == nil {
		return failedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, nil, nil, "inline token_secret was accepted")
	} else if !strings.Contains(strings.ToLower(err.Error()), "secret://") && !strings.Contains(strings.ToLower(err.Error()), "secret") {
		return failedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, nil, nil, "inline token_secret failed with unexpected error: "+err.Error())
	}
	details["inline_secret"] = "rejected"

	namespaces, err := queryLines(ctx, r.db, sqls[1])
	if err != nil {
		return failedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, nil, namespaces, err.Error())
	}
	if !linesContain(namespaces, r.cfg.Namespace) {
		return failedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, []string{r.cfg.Namespace}, namespaces, "seed namespace not listed")
	}
	tables, err := queryLines(ctx, r.db, sqls[2])
	if err != nil {
		return failedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, nil, tables, err.Error())
	}
	for _, table := range []string{"append_orders", "partition_orders", "year_partition_tiny", "mor_accounts"} {
		if !linesContain(tables, table) {
			return failedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, []string{table}, tables, "seed table not listed")
		}
	}
	showCreate, err := queryLines(ctx, r.db, sqls[3])
	if err != nil {
		return failedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, nil, showCreate, err.Error())
	}
	actual := append(append([]string{}, namespaces...), tables...)
	actual = append(actual, showCreate...)
	return passedCase("ICE-CI-E2E-010", "catalog-ddl-and-discovery", sqls, []string{"namespace and tables visible; inline secret rejected"}, actual, details)
}

func (r *caseRunner) accessLifecycleCase(ctx context.Context) (result caseResult) {
	catalogName := r.cfg.Catalog + "_lifecycle"
	mappingName := "access_lifecycle_mapping"
	mapping := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident(mappingName))
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		if cleanupErr := r.cleanupAccessLifecycle(ctx, mapping, catalogName); cleanupErr != nil {
			if result.Details == nil {
				result.Details = make(map[string]string)
			}
			result.Details["cleanup_error"] = cleanupErr.Error()
			if result.Error == "" {
				result.Error = "cleanup: " + cleanupErr.Error()
			} else {
				result.Error += "; cleanup: " + cleanupErr.Error()
			}
			result.Status = "failed"
		}
	}()
	sqls := make([]string, 0, 10)
	fail := func(expected, actual []string, msg string) caseResult {
		return failedCase("ICE-CI-E2E-015", "access-register-unregister-lifecycle", sqls, expected, actual, msg)
	}
	exec := func(stmt string) error {
		sqls = append(sqls, stmt)
		_, err := r.db.ExecContext(ctx, stmt)
		return err
	}

	if err := exec(fmt.Sprintf(
		"CREATE ICEBERG CATALOG %s WITH ('type'='rest','uri'=%s,'warehouse'=%s,'auth_mode'='none')",
		ident(catalogName), sqlString(r.cfg.CatalogURI), sqlString(r.cfg.Warehouse),
	)); err != nil {
		return fail(nil, nil, err.Error())
	}
	if err := exec(fmt.Sprintf(
		"CALL iceberg_register_access(%s, %s)",
		sqlString(catalogName),
		sqlString("scope=cluster,account_id=0,external_principal=ci-local,endpoint=localhost,region=us-east-1,bucket=mo-iceberg"),
	)); err != nil {
		return fail(nil, nil, err.Error())
	}
	catalogIDSQL := fmt.Sprintf(
		"select catalog_id from mo_catalog.mo_iceberg_catalogs where account_id = 0 and name = %s",
		sqlString(catalogName),
	)
	sqls = append(sqls, catalogIDSQL)
	catalogIDRows, err := queryLines(ctx, r.db, catalogIDSQL)
	if err != nil {
		return fail(nil, catalogIDRows, err.Error())
	}
	if len(catalogIDRows) != 1 {
		return fail([]string{"one catalog id"}, catalogIDRows, "lifecycle catalog id was not uniquely resolved")
	}
	catalogID, err := strconv.ParseUint(catalogIDRows[0], 10, 64)
	if err != nil || catalogID == 0 {
		return fail([]string{"non-zero catalog id"}, catalogIDRows, "lifecycle catalog id was invalid")
	}

	if err := exec(fmt.Sprintf(`CREATE EXTERNAL TABLE %s (
  order_id BIGINT,
  bucket INT,
  amount BIGINT,
  region TEXT
) ENGINE = ICEBERG WITH ('catalog'=%s,'namespace'=%s,'table'='append_orders','ref'='main','read_mode'='append_only','write_mode'='append_only')`,
		mapping, sqlString(catalogName), sqlString(r.cfg.Namespace),
	)); err != nil {
		return fail(nil, nil, err.Error())
	}
	dropCatalogSQL := fmt.Sprintf("DROP ICEBERG CATALOG %s", ident(catalogName))
	sqls = append(sqls, dropCatalogSQL)
	dropErr := func() error {
		_, err := r.db.ExecContext(ctx, dropCatalogSQL)
		return err
	}()
	if dropErr == nil {
		return fail([]string{"ordinary DROP rejected while table mapping exists"}, nil, "ordinary DROP removed a catalog with a live table mapping")
	}
	if !strings.Contains(strings.ToLower(dropErr.Error()), "table mappings") {
		return fail([]string{"table mappings dependency error"}, []string{dropErr.Error()}, "ordinary DROP failed for an unexpected reason")
	}

	beforeSQL := fmt.Sprintf(
		"select (select count(*) from mo_catalog.mo_iceberg_catalogs where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_tables where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_principal_map where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_residency_policy where catalog_id = %d and (scope_type = 'cluster' or account_id = 0))",
		catalogID, catalogID, catalogID, catalogID,
	)
	sqls = append(sqls, beforeSQL)
	before, err := queryLines(ctx, r.db, beforeSQL)
	if err != nil {
		return fail([]string{"1\t1\t1\t1"}, before, err.Error())
	}
	if !sameLines([]string{"1\t1\t1\t1"}, before) {
		return fail([]string{"1\t1\t1\t1"}, before, "rejected DROP was not atomic")
	}

	if err := exec("DROP TABLE " + mapping); err != nil {
		return fail(nil, before, err.Error())
	}
	if err := exec(fmt.Sprintf("CALL iceberg_unregister_access(%s)", sqlString(catalogName))); err != nil {
		return fail(nil, before, err.Error())
	}
	if err := exec(dropCatalogSQL); err != nil {
		return fail(nil, before, err.Error())
	}

	afterSQL := fmt.Sprintf(
		"select (select count(*) from mo_catalog.mo_iceberg_catalogs where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_tables where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_principal_map where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_residency_policy where catalog_id = %d and (scope_type = 'cluster' or account_id = 0)), (select count(*) from mo_catalog.mo_iceberg_refs where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_publish_jobs where account_id = 0 and target_catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_orphan_files where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_maintenance_jobs where account_id = 0 and catalog_id = %d)",
		catalogID, catalogID, catalogID, catalogID, catalogID, catalogID, catalogID, catalogID,
	)
	sqls = append(sqls, afterSQL)
	after, err := queryLines(ctx, r.db, afterSQL)
	wantAfter := []string{"0\t0\t0\t0\t0\t0\t0\t0"}
	if err != nil {
		return fail(wantAfter, after, err.Error())
	}
	if !sameLines(wantAfter, after) {
		return fail(wantAfter, after, "catalog-owned metadata remained after unregister and drop")
	}
	cleanupNeeded = false
	return passedCase(
		"ICE-CI-E2E-015",
		"access-register-unregister-lifecycle",
		sqls,
		[]string{"1\t1\t1\t1", wantAfter[0]},
		append(before, after...),
		map[string]string{"catalog_id": strconv.FormatUint(catalogID, 10), "blocked_drop": "atomic"},
	)
}

// cleanupAccessLifecycle deliberately does not reuse the case context.  The
// runner's five-minute context may be canceled precisely when teardown is most
// important; cleanup gets its own bounded control path and returns every
// failure to the caller for inclusion in the case report.
func (r *caseRunner) cleanupAccessLifecycle(_ context.Context, mapping, catalogName string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	statements := []string{
		"DROP TABLE IF EXISTS " + mapping,
		fmt.Sprintf("CALL iceberg_unregister_access(%s)", sqlString(catalogName)),
		fmt.Sprintf("DROP ICEBERG CATALOG IF EXISTS %s", ident(catalogName)),
	}
	failures := make([]string, 0, len(statements))
	for _, stmt := range statements {
		if _, err := r.db.ExecContext(cleanupCtx, stmt); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", redactText(stmt), err))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(failures, "; "))
}

// concurrentCreateMappingAndDropCase pauses CREATE after its FOR UPDATE catalog
// lookup and before mapping publication.  DROP must stay behind that row lock;
// after CREATE is released, exactly one DDL commits and the durable catalog and
// mapping counts prove that no orphan mapping was published.
func (r *caseRunner) concurrentCreateMappingAndDropCase(ctx context.Context) (result caseResult) {
	catalogName := r.cfg.Catalog + "_concurrent"
	mappingName := "concurrent_mapping"
	mapping := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident(mappingName))
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		if cleanupErr := r.cleanupAccessLifecycle(ctx, mapping, catalogName); cleanupErr != nil {
			if result.Details == nil {
				result.Details = make(map[string]string)
			}
			result.Details["cleanup_error"] = cleanupErr.Error()
			if result.Error == "" {
				result.Error = "cleanup: " + cleanupErr.Error()
			} else {
				result.Error += "; cleanup: " + cleanupErr.Error()
			}
			result.Status = "failed"
		}
	}()
	sqls := make([]string, 0, 8)
	fail := func(expected, actual []string, msg string) caseResult {
		return failedCase("ICE-CI-E2E-016", "concurrent-create-mapping-and-drop", sqls, expected, actual, msg)
	}
	createCatalogSQL := fmt.Sprintf(
		"CREATE ICEBERG CATALOG %s WITH ('type'='rest','uri'=%s,'warehouse'=%s,'auth_mode'='none')",
		ident(catalogName), sqlString(r.cfg.CatalogURI), sqlString(r.cfg.Warehouse),
	)
	sqls = append(sqls, createCatalogSQL)
	if _, err := r.db.ExecContext(ctx, createCatalogSQL); err != nil {
		return fail(nil, nil, err.Error())
	}
	catalogIDSQL := fmt.Sprintf("select catalog_id from mo_catalog.mo_iceberg_catalogs where account_id = 0 and name = %s", sqlString(catalogName))
	sqls = append(sqls, catalogIDSQL)
	catalogIDs, err := queryLines(ctx, r.db, catalogIDSQL)
	if err != nil || len(catalogIDs) != 1 {
		return fail([]string{"one catalog id"}, catalogIDs, fmt.Sprintf("resolve concurrent catalog id: %v", err))
	}
	catalogID, err := strconv.ParseUint(catalogIDs[0], 10, 64)
	if err != nil || catalogID == 0 {
		return fail([]string{"non-zero catalog id"}, catalogIDs, "concurrent catalog id was invalid")
	}
	lockIdentity, err := captureCatalogLifecycleLockIdentity(ctx, r.db, catalogName)
	if err != nil {
		return fail(nil, nil, fmt.Sprintf("resolve target catalog lifecycle lock: %v", err))
	}
	if err := installLifecycleFaults(ctx, r.db); err != nil {
		return fail(nil, nil, fmt.Sprintf("install lifecycle synchronization points: %v", err))
	}
	defer func() {
		if cleanupErr := cleanupLifecycleFaults(r.db); cleanupErr != nil {
			if result.Details == nil {
				result.Details = make(map[string]string)
			}
			result.Details["fault_cleanup_error"] = cleanupErr.Error()
			if result.Error == "" {
				result.Error = "fault cleanup: " + cleanupErr.Error()
			} else {
				result.Error += "; fault cleanup: " + cleanupErr.Error()
			}
			result.Status = "failed"
		}
	}()

	createSQL := fmt.Sprintf(`CREATE EXTERNAL TABLE %s (
  order_id BIGINT,
  bucket INT,
  amount BIGINT,
  region TEXT
) ENGINE = ICEBERG WITH ('catalog'=%s,'namespace'=%s,'table'='append_orders','ref'='main','read_mode'='append_only','write_mode'='append_only')`,
		mapping, sqlString(catalogName), sqlString(r.cfg.Namespace))
	dropSQL := fmt.Sprintf("DROP ICEBERG CATALOG %s", ident(catalogName))
	sqls = append(sqls, createSQL, dropSQL)
	type ddlResult struct {
		name string
		err  error
	}
	results := make(chan ddlResult, 2)
	var workers sync.WaitGroup
	caseCtx, cancelCase := context.WithCancel(ctx)
	defer cancelCase()
	stopWorkers := func() error {
		cancelCase()
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancelRelease()
		var errs []error
		for _, point := range []string{
			icebergCreateAfterCatalogLockFault,
			icebergDropBeforeCatalogLockFault,
			icebergDropAfterCatalogLockFault,
		} {
			if err := releaseLifecycleFault(releaseCtx, r.db, point); err != nil {
				errs = append(errs, err)
			}
		}
		if err := waitForLifecycleWorkers(releaseCtx, &workers); err != nil {
			errs = append(errs, fmt.Errorf("wait for lifecycle workers: %w", err))
		}
		return errors.Join(errs...)
	}
	stopAndFail := func(expected, actual []string, msg string) caseResult {
		if stopErr := stopWorkers(); stopErr != nil {
			msg += "; release lifecycle workers: " + stopErr.Error()
		}
		return fail(expected, actual, msg)
	}
	collectWorkers := func(waitCtx context.Context) (map[string]error, error) {
		if err := waitForLifecycleWorkers(waitCtx, &workers); err != nil {
			return nil, err
		}
		close(results)
		resultByName := make(map[string]error, 2)
		for ddlResult := range results {
			resultByName[ddlResult.name] = ddlResult.err
		}
		return resultByName, nil
	}
	stateSQL := fmt.Sprintf("select (select count(*) from mo_catalog.mo_iceberg_catalogs where account_id = 0 and catalog_id = %d), (select count(*) from mo_catalog.mo_iceberg_tables where account_id = 0 and catalog_id = %d)", catalogID, catalogID)
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := r.db.ExecContext(caseCtx, dropSQL)
		results <- ddlResult{name: "drop", err: err}
	}()
	barrierCtx, cancelBarrier := context.WithTimeout(ctx, 30*time.Second)
	defer cancelBarrier()
	if err := waitForLifecycleFaultWaiters(barrierCtx, r.db, icebergDropBeforeCatalogLockWaitersFault, 1); err != nil {
		return stopAndFail(nil, nil, fmt.Sprintf("DROP did not reach the pre-lock phase: %v", err))
	}
	if err := releaseLifecycleFault(barrierCtx, r.db, icebergDropBeforeCatalogLockFault); err != nil {
		return stopAndFail(nil, nil, fmt.Sprintf("release DROP pre-lock phase: %v", err))
	}
	if err := waitForLifecycleFaultWaiters(barrierCtx, r.db, icebergDropAfterCatalogLockWaitersFault, 1); err != nil {
		return stopAndFail(nil, nil, fmt.Sprintf("DROP did not acquire the target catalog lifecycle lock: %v", err))
	}
	createConn, err := r.db.Conn(ctx)
	if err != nil {
		return stopAndFail(nil, nil, fmt.Sprintf("reserve CREATE connection: %v", err))
	}
	defer createConn.Close()
	previousLockWaitTimeout, err := sessionLockWaitTimeout(barrierCtx, createConn)
	if err != nil {
		return stopAndFail(nil, nil, fmt.Sprintf("capture CREATE lock wait timeout: %v", err))
	}
	defer func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancelCleanup()
		restoreErr, discardErr := restoreOrDiscardSessionLockWaitTimeout(cleanupCtx, createConn, previousLockWaitTimeout)
		if restoreErr != nil {
			if result.Details == nil {
				result.Details = make(map[string]string)
			}
			result.Details["lock_wait_timeout_restore_error"] = restoreErr.Error()
			message := "restore CREATE lock wait timeout: " + restoreErr.Error()
			if discardErr != nil {
				result.Details["lock_wait_timeout_discard_error"] = discardErr.Error()
				message += "; discard CREATE connection: " + discardErr.Error()
			} else {
				result.Details["lock_wait_timeout_connection_discarded"] = "true"
			}
			if result.Error == "" {
				result.Error = message
			} else {
				result.Error += "; " + message
			}
			result.Status = "failed"
			return
		}
		if result.Details == nil {
			result.Details = make(map[string]string)
		}
		result.Details["lock_wait_timeout_restored"] = strconv.FormatUint(previousLockWaitTimeout, 10)
	}()
	if _, err := createConn.ExecContext(barrierCtx, "set session lock_wait_timeout = 1"); err != nil {
		return stopAndFail(nil, nil, fmt.Sprintf("set CREATE lock wait timeout: %v", err))
	}
	createDone := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := createConn.ExecContext(caseCtx, createSQL)
		results <- ddlResult{name: "create", err: err}
		createDone <- err
	}()
	select {
	case createErr := <-createDone:
		if createErr == nil {
			return stopAndFail([]string{"CREATE lock wait failure while DROP holds the catalog row"}, []string{"CREATE committed"}, "CREATE bypassed the catalog lifecycle lock")
		}
	case <-barrierCtx.Done():
		return stopAndFail(nil, nil, fmt.Sprintf("CREATE did not terminate at the catalog lifecycle lock: %v", barrierCtx.Err()))
	}
	if err := releaseLifecycleFault(barrierCtx, r.db, icebergDropAfterCatalogLockFault); err != nil {
		return stopAndFail(nil, nil, fmt.Sprintf("release DROP post-lock phase: %v", err))
	}
	resultByName, err := collectWorkers(barrierCtx)
	if err != nil {
		return fail(nil, nil, fmt.Sprintf("wait for lifecycle workers: %v", err))
	}
	createErr, dropErr := resultByName["create"], resultByName["drop"]
	if createErr == nil || dropErr != nil {
		return fail([]string{"CREATE lock failure and DROP commit"}, []string{fmt.Sprintf("create=%v", createErr), fmt.Sprintf("drop=%v", dropErr)}, "catalog lifecycle lock did not reject CREATE while DROP owned the target row")
	}

	sqls = append(sqls, stateSQL)
	state, err := queryLines(ctx, r.db, stateSQL)
	if err != nil {
		return fail(nil, state, err.Error())
	}
	if !sameLines([]string{"0\t0"}, state) {
		return fail([]string{"0\t0"}, state, "DROP committed but catalog-owned mapping remained")
	}
	cleanupNeeded = false
	return passedCase(
		"ICE-CI-E2E-016",
		"concurrent-create-mapping-and-drop",
		sqls,
		[]string{"CREATE lock failure", "DROP commit", "0\t0"},
		state,
		map[string]string{"catalog_id": strconv.FormatUint(catalogID, 10), "catalog_lock_table_id": lockIdentity.tableID, "catalog_lock_content": lockIdentity.content, "create_error": fmt.Sprint(createErr), "drop_error": fmt.Sprint(dropErr), "phase_barrier": "fault injection: DROP acquires the captured target catalog lock; CREATE uses a bounded lock wait and must be rejected before DROP commits"},
	)
}

const (
	icebergCreateAfterCatalogLockFault        = "iceberg-create-mapping-after-catalog-lock"
	icebergDropBeforeCatalogLockFault         = "iceberg-drop-catalog-before-lifecycle-lock"
	icebergDropAfterCatalogLockFault          = "iceberg-drop-catalog-after-lifecycle-lock"
	icebergCreateAfterCatalogLockWaitersFault = "iceberg-create-mapping-after-catalog-lock-waiters"
	icebergDropBeforeCatalogLockWaitersFault  = "iceberg-drop-catalog-before-lifecycle-lock-waiters"
	icebergDropAfterCatalogLockWaitersFault   = "iceberg-drop-catalog-after-lifecycle-lock-waiters"
	icebergCreateAfterCatalogLockNotifyFault  = "iceberg-create-mapping-after-catalog-lock-notify"
	icebergDropBeforeCatalogLockNotifyFault   = "iceberg-drop-catalog-before-lifecycle-lock-notify"
	icebergDropAfterCatalogLockNotifyFault    = "iceberg-drop-catalog-after-lifecycle-lock-notify"
)

func installLifecycleFaults(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "select enable_fault_injection()"); err != nil {
		return err
	}
	points := []struct{ name, action, target string }{
		{icebergCreateAfterCatalogLockFault, "wait", ""},
		{icebergDropBeforeCatalogLockFault, "wait", ""},
		{icebergDropAfterCatalogLockFault, "wait", ""},
		{icebergCreateAfterCatalogLockWaitersFault, "getwaiters", icebergCreateAfterCatalogLockFault},
		{icebergDropBeforeCatalogLockWaitersFault, "getwaiters", icebergDropBeforeCatalogLockFault},
		{icebergDropAfterCatalogLockWaitersFault, "getwaiters", icebergDropAfterCatalogLockFault},
		{icebergCreateAfterCatalogLockNotifyFault, "notifyall", icebergCreateAfterCatalogLockFault},
		{icebergDropBeforeCatalogLockNotifyFault, "notifyall", icebergDropBeforeCatalogLockFault},
		{icebergDropAfterCatalogLockNotifyFault, "notifyall", icebergDropAfterCatalogLockFault},
	}
	for _, point := range points {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("select add_fault_point(%s, ':::', %s, 0, %s)", sqlString(point.name), sqlString(point.action), sqlString(point.target))); err != nil {
			if cleanupErr := cleanupLifecycleFaults(db); cleanupErr != nil {
				return fmt.Errorf("add lifecycle fault %s: %w; cleanup: %v", point.name, err, cleanupErr)
			}
			return err
		}
	}
	return nil
}

func cleanupLifecycleFaults(db *sql.DB) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var errs []error
	for _, name := range []string{icebergCreateAfterCatalogLockFault, icebergDropBeforeCatalogLockFault, icebergDropAfterCatalogLockFault} {
		if err := releaseLifecycleFault(cleanupCtx, db, name); err != nil {
			errs = append(errs, fmt.Errorf("release %s: %w", name, err))
		}
	}
	for _, name := range []string{
		icebergCreateAfterCatalogLockFault, icebergDropBeforeCatalogLockFault, icebergDropAfterCatalogLockFault,
		icebergCreateAfterCatalogLockWaitersFault, icebergDropBeforeCatalogLockWaitersFault, icebergDropAfterCatalogLockWaitersFault,
		icebergCreateAfterCatalogLockNotifyFault, icebergDropBeforeCatalogLockNotifyFault, icebergDropAfterCatalogLockNotifyFault,
	} {
		if _, err := db.ExecContext(cleanupCtx, fmt.Sprintf("select fault_inject('all.', 'REMOVE_FAULT_POINT', %s)", sqlString(name))); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", name, err))
		}
	}
	if _, err := db.ExecContext(cleanupCtx, "select disable_fault_injection()"); err != nil {
		errs = append(errs, fmt.Errorf("disable fault injection: %w", err))
	}
	return errors.Join(errs...)
}

func releaseLifecycleFault(ctx context.Context, db *sql.DB, waitPoint string) error {
	notifyPoint := map[string]string{
		icebergCreateAfterCatalogLockFault: icebergCreateAfterCatalogLockNotifyFault,
		icebergDropBeforeCatalogLockFault:  icebergDropBeforeCatalogLockNotifyFault,
		icebergDropAfterCatalogLockFault:   icebergDropAfterCatalogLockNotifyFault,
	}[waitPoint]
	_, err := db.ExecContext(ctx, fmt.Sprintf("select trigger_fault_point(%s)", sqlString(notifyPoint)))
	return err
}

func waitForLifecycleWorkers(ctx context.Context, workers *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sessionLockWaitTimeout reads the value from the physical connection that is
// used for the bounded CREATE probe.  database/sql may return this connection
// to its pool, so the caller must restore the value before Conn.Close.
func sessionLockWaitTimeout(ctx context.Context, conn *sql.Conn) (uint64, error) {
	var value string
	if err := conn.QueryRowContext(ctx, "select @@session.lock_wait_timeout").Scan(&value); err != nil {
		return 0, err
	}
	timeout, err := strconv.ParseUint(value, 10, 64)
	if err != nil || timeout == 0 {
		return 0, fmt.Errorf("invalid session lock_wait_timeout %q", value)
	}
	return timeout, nil
}

// restoreSessionLockWaitTimeout restores and then reads the same physical
// connection, proving that the session returned to the pool has its original
// timeout rather than the one-second lifecycle-test override.
func restoreSessionLockWaitTimeout(ctx context.Context, conn *sql.Conn, want uint64) error {
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("set session lock_wait_timeout = %d", want)); err != nil {
		return err
	}
	got, err := sessionLockWaitTimeout(ctx, conn)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("lock_wait_timeout after restore = %d, want %d", got, want)
	}
	return nil
}

// restoreOrDiscardSessionLockWaitTimeout prevents a failed restore from leaking
// the test's one-second timeout through database/sql's reusable connection
// pool. driver.ErrBadConn is the documented database/sql signal to discard the
// physical connection rather than returning it to that pool.
func restoreOrDiscardSessionLockWaitTimeout(ctx context.Context, conn *sql.Conn, want uint64) (restoreErr, discardErr error) {
	if restoreErr = restoreSessionLockWaitTimeout(ctx, conn, want); restoreErr == nil {
		return nil, nil
	}
	return restoreErr, discardSessionConnection(conn)
}

func discardSessionConnection(conn *sql.Conn) error {
	err := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(err, driver.ErrBadConn) {
		return nil
	}
	if err == nil {
		return errors.New("database/sql retained a connection after discard request")
	}
	return fmt.Errorf("discard physical connection: %w", err)
}

type catalogLifecycleLockIdentity struct {
	tableID string
	content string
}

func captureCatalogLifecycleLockIdentity(ctx context.Context, db *sql.DB, catalogName string) (catalogLifecycleLockIdentity, error) {
	const tableIDSQL = "select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = 'mo_catalog' and relname = 'mo_iceberg_catalogs'"
	tableIDs, err := queryLines(ctx, db, tableIDSQL)
	if err != nil {
		return catalogLifecycleLockIdentity{}, err
	}
	if len(tableIDs) != 1 {
		return catalogLifecycleLockIdentity{}, fmt.Errorf("catalog table lookup returned %d rows", len(tableIDs))
	}
	if _, err := strconv.ParseUint(tableIDs[0], 10, 64); err != nil {
		return catalogLifecycleLockIdentity{}, fmt.Errorf("parse catalog table id: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return catalogLifecycleLockIdentity{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	lockSQL := fmt.Sprintf("select catalog_id from mo_catalog.mo_iceberg_catalogs where account_id = 0 and name = %s for update", sqlString(catalogName))
	rows, err := tx.QueryContext(ctx, lockSQL)
	if err != nil {
		return catalogLifecycleLockIdentity{}, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return catalogLifecycleLockIdentity{}, err
		}
		return catalogLifecycleLockIdentity{}, errors.New("catalog lifecycle lock probe selected no row")
	}
	var catalogID uint64
	if err := rows.Scan(&catalogID); err != nil {
		return catalogLifecycleLockIdentity{}, err
	}
	if err := rows.Close(); err != nil {
		return catalogLifecycleLockIdentity{}, err
	}
	if catalogID == 0 {
		return catalogLifecycleLockIdentity{}, errors.New("catalog lifecycle lock probe selected zero catalog id")
	}

	identitySQL := fmt.Sprintf("select lock_content from mo_catalog.mo_locks where table_id = %s and lock_key = 'point' and (lock_wait is null or lock_wait = '')", tableIDs[0])
	contents, err := queryLines(ctx, db, identitySQL)
	if err != nil {
		return catalogLifecycleLockIdentity{}, err
	}
	if len(contents) != 1 || contents[0] == "" {
		return catalogLifecycleLockIdentity{}, fmt.Errorf("catalog lifecycle lock probe returned %d identities", len(contents))
	}
	if err := tx.Commit(); err != nil {
		return catalogLifecycleLockIdentity{}, err
	}
	committed = true
	return catalogLifecycleLockIdentity{tableID: tableIDs[0], content: contents[0]}, nil
}

func waitForLifecycleFaultWaiters(ctx context.Context, db *sql.DB, waitersPoint string, want uint64) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		// A slow query or instrumented test run can make the ticker and
		// context deadline ready at the same time. Do not start another
		// database poll after the context has expired.
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := queryLines(ctx, db, fmt.Sprintf("select trigger_fault_point(%s)", sqlString(waitersPoint)))
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("fault point %s returned %d rows", waitersPoint, len(rows))
		}
		waiters, err := strconv.ParseUint(rows[0], 10, 64)
		if err != nil {
			return fmt.Errorf("parse waiter count for %s: %w", waitersPoint, err)
		}
		if waiters >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *caseRunner) appendReadAndTimeTravelCase(ctx context.Context) caseResult {
	caseStarted := time.Now()
	details := map[string]string{}
	fail := func(sqls, expected, actual []string, msg string) caseResult {
		details["case_wall_s"] = formatSeconds(time.Since(caseStarted))
		res := failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, expected, actual, msg)
		res.Details = details
		return res
	}
	table := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident("append_orders"))
	insertFirstSQL := fmt.Sprintf("INSERT INTO %s VALUES (1,1,10,'ksa'),(2,1,20,'uae'),(3,2,30,'ksa'),(4,2,40,'qat')", table)
	selectFirstSQL := fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s", table)
	insertSecondSQL := fmt.Sprintf("INSERT INTO %s VALUES (5,3,50,'ksa')", table)
	selectCurrentSQL := fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s", table)
	sqls := []string{insertFirstSQL, selectFirstSQL}

	opStarted := time.Now()
	if _, err := r.db.ExecContext(ctx, insertFirstSQL); err != nil {
		return fail(sqls, nil, nil, err.Error())
	}
	details["insert_first_s"] = formatSeconds(time.Since(opStarted))
	opStarted = time.Now()
	oldSnapshot, err := currentSnapshotID(ctx, r.cfg, "append_orders")
	if err != nil {
		return fail(sqls, nil, nil, err.Error())
	}
	details["old_snapshot_lookup_s"] = formatSeconds(time.Since(opStarted))
	opStarted = time.Now()
	oldSnapshotRef, err := createNessieBranchAtMain(ctx, r.cfg, "mo_e2e_t1_"+strconv.FormatInt(oldSnapshot, 10))
	if err != nil {
		return fail(sqls, nil, nil, err.Error())
	}
	details["create_nessie_branch_s"] = formatSeconds(time.Since(opStarted))
	oldTable := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident("append_orders_old"))
	createOldMappingSQL := fmt.Sprintf(`CREATE EXTERNAL TABLE %s (
  order_id BIGINT,
  bucket INT,
  amount BIGINT,
  region TEXT
	) ENGINE = ICEBERG WITH ('catalog'=%s,'namespace'=%s,'table'='append_orders','ref'=%s,'read_mode'='append_only','write_mode'='append_only')`,
		oldTable, sqlString(r.cfg.Catalog), sqlString(r.cfg.Namespace), sqlString(oldSnapshotRef))
	sqls = append(sqls, createOldMappingSQL)
	opStarted = time.Now()
	if _, err := r.db.ExecContext(ctx, createOldMappingSQL); err != nil {
		return fail(sqls, nil, nil, err.Error())
	}
	details["create_old_mapping_s"] = formatSeconds(time.Since(opStarted))
	opStarted = time.Now()
	first, err := queryLines(ctx, r.db, selectFirstSQL)
	details["select_first_s"] = formatSeconds(time.Since(opStarted))
	if err != nil {
		return fail(sqls, nil, first, err.Error())
	}
	sqls = append(sqls, insertSecondSQL, selectCurrentSQL)
	opStarted = time.Now()
	if _, err := r.db.ExecContext(ctx, insertSecondSQL); err != nil {
		return fail(sqls, nil, first, err.Error())
	}
	details["insert_second_s"] = formatSeconds(time.Since(opStarted))
	opStarted = time.Now()
	current, err := queryLines(ctx, r.db, selectCurrentSQL)
	details["select_current_s"] = formatSeconds(time.Since(opStarted))
	if err != nil {
		return fail(sqls, nil, current, err.Error())
	}
	opStarted = time.Now()
	currentSnapshot, err := currentSnapshotID(ctx, r.cfg, "append_orders")
	if err != nil {
		return fail(sqls, nil, current, err.Error())
	}
	details["current_snapshot_lookup_s"] = formatSeconds(time.Since(opStarted))
	details["old_snapshot_id"] = fmt.Sprintf("%d", oldSnapshot)
	details["old_snapshot_ref"] = oldSnapshotRef
	details["current_snapshot_id"] = fmt.Sprintf("%d", currentSnapshot)
	timeTravelSQL := fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s FOR ICEBERG SNAPSHOT %d", oldTable, oldSnapshot)
	opStarted = time.Now()
	retained, err := snapshotAvailableOnRef(ctx, r.cfg, "append_orders", oldSnapshotRef, oldSnapshot)
	details["snapshot_ref_check_s"] = formatSeconds(time.Since(opStarted))
	if err != nil {
		return fail(sqls, nil, current, err.Error())
	}
	details["historical_snapshot_retained"] = fmt.Sprintf("%t", retained)
	opStarted = time.Now()
	old, err := queryLines(ctx, r.db, timeTravelSQL)
	details["time_travel_select_s"] = formatSeconds(time.Since(opStarted))
	sqls = append(sqls, timeTravelSQL)
	if err != nil {
		return fail(sqls, nil, old, err.Error())
	}
	expected := []string{"4\t100", "5\t150", "4\t100"}
	actual := append(append([]string{}, first...), current...)
	actual = append(actual, old...)
	if !sameLines(expected, actual) {
		return fail(sqls, expected, actual, "append/time-travel result mismatch")
	}
	details["case_wall_s"] = formatSeconds(time.Since(caseStarted))
	return passedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, expected, actual, details)
}

func (r *caseRunner) emptyStringReadCase(ctx context.Context) caseResult {
	table := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident("append_orders"))
	sqls := []string{
		fmt.Sprintf("INSERT INTO %s VALUES (101,10,10,''),(102,10,20,'中文'),(103,10,30,NULL)", table),
		fmt.Sprintf("SELECT COUNT(*), COUNT(region), COUNT(NULLIF(region,'')) FROM %s WHERE order_id BETWEEN 101 AND 103", table),
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE order_id BETWEEN 101 AND 103 AND region = ''", table),
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE order_id BETWEEN 101 AND 103 AND region = '中文'", table),
	}
	if _, err := r.db.ExecContext(ctx, sqls[0]); err != nil {
		return failedCase("ICE-CI-E2E-025", "empty-string-read", sqls, nil, nil, err.Error())
	}
	expected := []string{"3\t2\t1", "1", "1"}
	actual := make([]string, 0, len(expected))
	for _, stmt := range sqls[1:] {
		lines, err := queryLines(ctx, r.db, stmt)
		actual = append(actual, lines...)
		if err != nil {
			return failedCase("ICE-CI-E2E-025", "empty-string-read", sqls, expected, actual, err.Error())
		}
	}
	if !sameLines(expected, actual) {
		return failedCase("ICE-CI-E2E-025", "empty-string-read", sqls, expected, actual, "empty string read result mismatch")
	}
	return passedCase("ICE-CI-E2E-025", "empty-string-read", sqls, expected, actual, nil)
}

func (r *caseRunner) partitionFilterCase(ctx context.Context) caseResult {
	table := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident("partition_orders"))
	sqls := []string{
		fmt.Sprintf("INSERT INTO %s VALUES (10,1,10,'ksa'),(11,1,20,'uae'),(12,2,30,'ksa'),(13,2,40,'qat')", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s WHERE region = 'ksa'", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s WHERE region IN ('ksa','uae')", table),
	}
	if _, err := r.db.ExecContext(ctx, sqls[0]); err != nil {
		return failedCase("ICE-CI-E2E-030", "partition-filter", sqls, nil, nil, err.Error())
	}
	one, err := queryLines(ctx, r.db, sqls[1])
	if err != nil {
		return failedCase("ICE-CI-E2E-030", "partition-filter", sqls, nil, one, err.Error())
	}
	two, err := queryLines(ctx, r.db, sqls[2])
	if err != nil {
		return failedCase("ICE-CI-E2E-030", "partition-filter", sqls, nil, two, err.Error())
	}
	expected := []string{"2\t40", "3\t60"}
	actual := append(append([]string{}, one...), two...)
	if !sameLines(expected, actual) {
		return failedCase("ICE-CI-E2E-030", "partition-filter", sqls, expected, actual, "partition filter result mismatch")
	}
	return passedCase("ICE-CI-E2E-030", "partition-filter", sqls, expected, actual, nil)
}

func (r *caseRunner) yearPartitionDateCase(ctx context.Context) caseResult {
	table := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident("year_partition_tiny"))
	rangeSQL := fmt.Sprintf("SELECT COUNT(*), SUM(value) FROM %s WHERE obs_date >= DATE '2020-01-01' AND obs_date < DATE '2021-01-01'", table)
	sqls := []string{
		fmt.Sprintf("INSERT INTO %s VALUES (0,DATE '1969-06-01',5),(1,DATE '2019-06-01',10),(2,DATE '2020-02-01',20),(3,DATE '2020-12-31',30),(4,DATE '2021-01-01',40),(5,NULL,50)", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(value) FROM %s", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(value) FROM %s WHERE obs_date = DATE '2020-02-01'", table),
		rangeSQL,
		rangeSQL,
		rangeSQL,
		fmt.Sprintf("SELECT COUNT(*), SUM(value) FROM %s WHERE obs_date IN (DATE '2019-06-01', DATE '2021-01-01')", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(value) FROM %s WHERE obs_date < DATE '1970-01-01'", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(value) FROM %s WHERE obs_date IS NULL", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(value) FROM %s WHERE obs_date >= DATE '2030-01-01'", table),
		"EXPLAIN " + rangeSQL,
	}
	if _, err := r.db.ExecContext(ctx, sqls[0]); err != nil {
		return failedCase("ICE-CI-E2E-035", "year-partition-date-filter", sqls, nil, nil, err.Error())
	}
	expected := []string{
		"6\t155",
		"1\t20",
		"2\t50",
		"2\t50",
		"2\t50",
		"2\t50",
		"1\t5",
		"1\t50",
		"0\tNULL",
	}
	actual := make([]string, 0, len(expected))
	for _, stmt := range sqls[1 : len(sqls)-1] {
		lines, err := queryLines(ctx, r.db, stmt)
		actual = append(actual, lines...)
		if err != nil {
			return failedCase("ICE-CI-E2E-035", "year-partition-date-filter", sqls, expected, actual, err.Error())
		}
	}
	if !sameLines(expected, actual) {
		return failedCase("ICE-CI-E2E-035", "year-partition-date-filter", sqls, expected, actual, "year partition date filter result mismatch")
	}
	explain, err := queryLines(ctx, r.db, sqls[len(sqls)-1])
	if err != nil {
		return failedCase("ICE-CI-E2E-035", "year-partition-date-filter", sqls, expected, actual, err.Error())
	}
	if !linesContain(explain, "Iceberg:") || !linesContain(explain, "residual_filter=true") {
		return failedCase("ICE-CI-E2E-035", "year-partition-date-filter", sqls, expected, actual, "EXPLAIN omitted Iceberg residual filter")
	}
	return passedCase("ICE-CI-E2E-035", "year-partition-date-filter", sqls, expected, actual, map[string]string{
		"range_repetitions": "3",
		"explain":           strings.Join(explain, "\n"),
	})
}

func (r *caseRunner) mergeOnReadDeleteCase(ctx context.Context) caseResult {
	table := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident("mor_accounts"))
	sqls := []string{
		fmt.Sprintf("INSERT INTO %s VALUES (1,100,'ksa'),(2,200,'uae'),(3,300,'ksa'),(4,300,'qat')", table),
		fmt.Sprintf("DELETE FROM %s WHERE account_id = 2", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(balance) FROM %s", table),
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE account_id = 2", table),
	}
	if _, err := r.db.ExecContext(ctx, sqls[0]); err != nil {
		return failedCase("ICE-CI-E2E-040", "merge-on-read-delete", sqls, nil, nil, err.Error())
	}
	res, err := r.db.ExecContext(ctx, sqls[1])
	if err != nil {
		return failedCase("ICE-CI-E2E-040", "merge-on-read-delete", sqls, nil, nil, err.Error())
	}
	affected, _ := res.RowsAffected()
	agg, err := queryLines(ctx, r.db, sqls[2])
	if err != nil {
		return failedCase("ICE-CI-E2E-040", "merge-on-read-delete", sqls, nil, agg, err.Error())
	}
	deleted, err := queryLines(ctx, r.db, sqls[3])
	if err != nil {
		return failedCase("ICE-CI-E2E-040", "merge-on-read-delete", sqls, nil, deleted, err.Error())
	}
	expected := []string{"3\t700", "0"}
	actual := append(append([]string{}, agg...), deleted...)
	if !sameLines(expected, actual) {
		return failedCase("ICE-CI-E2E-040", "merge-on-read-delete", sqls, expected, actual, "delete apply result mismatch")
	}
	return passedCase("ICE-CI-E2E-040", "merge-on-read-delete", sqls, expected, actual, map[string]string{"rows_affected": fmt.Sprintf("%d", affected)})
}

func seedRESTTables(ctx context.Context, cfg localE2EConfig) error {
	client := catalog.NewRESTClient(catalog.WithAllowPlainHTTP(true))
	req := api.CatalogRequest{
		Catalog: model.Catalog{
			AccountID: 1,
			CatalogID: 1,
			Name:      cfg.Catalog,
			Type:      "rest",
			URI:       cfg.CatalogURI,
			Warehouse: cfg.Warehouse,
			AuthMode:  "none",
		},
		ExternalPrincipal: "ci-local",
	}
	reqWithPrefix, err := catalogRequestWithPrefix(ctx, client, req, cfg.Warehouse)
	if err != nil {
		return err
	}
	if err := createNamespace(ctx, cfg, reqWithPrefix.Prefix); err != nil {
		return err
	}
	for _, spec := range e2eTableSpecs(cfg.Namespace) {
		_, err := client.CreateTable(ctx, api.CreateTableRequest{
			CatalogRequest: reqWithPrefix,
			Namespace:      api.Namespace{cfg.Namespace},
			Table:          spec.name,
			Schema:         spec.schema,
			PartitionSpec:  spec.partitionSpec,
			Location:       fmt.Sprintf("%s/%s/%s", strings.TrimRight(cfg.Warehouse, "/"), cfg.Namespace, spec.name),
			Properties: map[string]string{
				"format-version":                       "2",
				"history.expire.min-snapshots-to-keep": "2",
				"owner":                                "matrixone-ci",
			},
		})
		if err != nil {
			return fmt.Errorf("create Iceberg table %s.%s: %w", cfg.Namespace, spec.name, err)
		}
	}
	return nil
}

type e2eTableSpec struct {
	name          string
	schema        api.Schema
	partitionSpec api.PartitionSpec
}

func e2eTableSpecs(namespace string) []e2eTableSpec {
	orderSchema := api.Schema{SchemaID: 0, Fields: []api.SchemaField{
		{ID: 1, Name: "order_id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "bucket", Required: false, Type: api.IcebergType{Kind: api.TypeInt}},
		{ID: 3, Name: "amount", Required: false, Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 4, Name: "region", Required: false, Type: api.IcebergType{Kind: api.TypeString}},
	}}
	partitioned := api.PartitionSpec{SpecID: 0, Fields: []api.PartitionField{
		{SourceID: 4, FieldID: 1000, Name: "region", Transform: "identity"},
	}}
	accountSchema := api.Schema{SchemaID: 0, Fields: []api.SchemaField{
		{ID: 1, Name: "account_id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "balance", Required: false, Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 3, Name: "region", Required: false, Type: api.IcebergType{Kind: api.TypeString}},
	}}
	yearPartitionSchema := api.Schema{SchemaID: 0, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
		{ID: 2, Name: "obs_date", Required: false, Type: api.IcebergType{Kind: api.TypeDate}},
		{ID: 3, Name: "value", Required: false, Type: api.IcebergType{Kind: api.TypeLong}},
	}}
	yearPartitioned := api.PartitionSpec{SpecID: 0, Fields: []api.PartitionField{
		{SourceID: 2, FieldID: 1000, Name: "obs_year", Transform: "year"},
	}}
	return []e2eTableSpec{
		{name: "append_orders", schema: orderSchema, partitionSpec: api.PartitionSpec{SpecID: 0}},
		{name: "partition_orders", schema: orderSchema, partitionSpec: partitioned},
		{name: "year_partition_tiny", schema: yearPartitionSchema, partitionSpec: yearPartitioned},
		{name: "mor_accounts", schema: accountSchema, partitionSpec: api.PartitionSpec{SpecID: 0}},
	}
}

func catalogRequestWithPrefix(ctx context.Context, client *catalog.RESTClient, req api.CatalogRequest, warehouse string) (api.CatalogRequest, error) {
	config, err := client.GetConfig(ctx, api.GetConfigRequest{
		CatalogRequest: req,
		Warehouse:      warehouse,
		NoCache:        true,
	})
	if err != nil {
		return api.CatalogRequest{}, fmt.Errorf("get Iceberg REST catalog config: %w", err)
	}
	req.Prefix = config.Prefix
	return req, nil
}

func createNamespace(ctx context.Context, cfg localE2EConfig, prefix string) error {
	body, err := json.Marshal(map[string]any{
		"namespace":  []string{cfg.Namespace},
		"properties": map[string]string{"owner": "matrixone-ci"},
	})
	if err != nil {
		return err
	}
	target, err := createNamespaceURL(cfg.CatalogURI, prefix)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("create namespace request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusConflict {
		return nil
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("create namespace %s returned HTTP %d: %s", cfg.Namespace, resp.StatusCode, strings.TrimSpace(string(data)))
}

func currentSnapshotID(ctx context.Context, cfg localE2EConfig, table string) (int64, error) {
	client := catalog.NewRESTClient(catalog.WithAllowPlainHTTP(true))
	req := api.CatalogRequest{
		Catalog:           model.Catalog{Name: cfg.Catalog, Type: "rest", URI: cfg.CatalogURI, Warehouse: cfg.Warehouse, AuthMode: "none"},
		ExternalPrincipal: "ci-local",
	}
	reqWithPrefix, err := catalogRequestWithPrefix(ctx, client, req, cfg.Warehouse)
	if err != nil {
		return 0, err
	}
	resp, err := client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: reqWithPrefix,
		Namespace:      api.Namespace{cfg.Namespace},
		Table:          table,
	})
	if err != nil {
		return 0, err
	}
	var metadata struct {
		CurrentSnapshotID int64 `json:"current-snapshot-id"`
	}
	if len(resp.MetadataJSON) == 0 {
		return 0, fmt.Errorf("load table %s returned empty metadata JSON", table)
	}
	if err := json.Unmarshal(resp.MetadataJSON, &metadata); err != nil {
		return 0, fmt.Errorf("decode table metadata for %s: %w", table, err)
	}
	if metadata.CurrentSnapshotID <= 0 {
		return 0, fmt.Errorf("table %s does not have a current snapshot", table)
	}
	return metadata.CurrentSnapshotID, nil
}

func snapshotAvailable(ctx context.Context, cfg localE2EConfig, table string, snapshotID int64) (bool, error) {
	return snapshotAvailableOnRef(ctx, cfg, table, model.DefaultRefMain, snapshotID)
}

func snapshotAvailableOnRef(ctx context.Context, cfg localE2EConfig, table, ref string, snapshotID int64) (bool, error) {
	client := catalog.NewRESTClient(catalog.WithAllowPlainHTTP(true))
	req := api.CatalogRequest{
		Catalog:           model.Catalog{Name: cfg.Catalog, Type: "rest", URI: cfg.CatalogURI, Warehouse: cfg.Warehouse, AuthMode: "none"},
		ExternalPrincipal: "ci-local",
	}
	reqWithPrefix, err := catalogRequestWithPrefixForRef(ctx, client, req, cfg.Warehouse, ref)
	if err != nil {
		return false, err
	}
	resp, err := client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: reqWithPrefix,
		Namespace:      api.Namespace{cfg.Namespace},
		Table:          table,
		Snapshots:      "all",
	})
	if err != nil {
		return false, err
	}
	return snapshotRetainedInMetadata(resp.MetadataJSON, snapshotID)
}

func catalogRequestWithPrefixForRef(ctx context.Context, client *catalog.RESTClient, req api.CatalogRequest, warehouse, ref string) (api.CatalogRequest, error) {
	reqWithPrefix, err := catalogRequestWithPrefix(ctx, client, req, warehouse)
	if err != nil {
		return api.CatalogRequest{}, err
	}
	ref = strings.TrimSpace(ref)
	if ref == "" || ref == model.DefaultRefMain {
		return reqWithPrefix, nil
	}
	prefix := strings.TrimSpace(reqWithPrefix.Prefix)
	if prefix == "" || prefix == model.DefaultRefMain {
		reqWithPrefix.Prefix = ref
		return reqWithPrefix, nil
	}
	parts := strings.SplitN(prefix, "|", 2)
	if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
		reqWithPrefix.Prefix = ref + "|" + strings.TrimSpace(parts[1])
		return reqWithPrefix, nil
	}
	return api.CatalogRequest{}, fmt.Errorf("nessie prefix %q cannot be rewritten for ref %q", prefix, ref)
}

func createNessieBranchAtMain(ctx context.Context, cfg localE2EConfig, branch string) (string, error) {
	ref, err := getNessieReference(ctx, cfg, model.DefaultRefMain)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(map[string]string{
		"type": "BRANCH",
		"name": branch,
		"hash": ref.Hash,
	})
	if err != nil {
		return "", err
	}
	target, err := nessieAPIURL(cfg.CatalogURI, "trees", "tree")
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("create Nessie branch %s request failed: %w", branch, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("create Nessie branch %s returned HTTP %d: %s", branch, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return branch, nil
}

type nessieReference struct {
	Type string `json:"type"`
	Name string `json:"name"`
	Hash string `json:"hash"`
}

func getNessieReference(ctx context.Context, cfg localE2EConfig, ref string) (nessieReference, error) {
	target, err := nessieAPIURL(cfg.CatalogURI, "trees", "tree", ref)
	if err != nil {
		return nessieReference{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nessieReference{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nessieReference{}, fmt.Errorf("load Nessie ref %s request failed: %w", ref, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nessieReference{}, fmt.Errorf("load Nessie ref %s returned HTTP %d: %s", ref, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var out nessieReference
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nessieReference{}, fmt.Errorf("decode Nessie ref %s: %w", ref, err)
	}
	if strings.TrimSpace(out.Hash) == "" {
		return nessieReference{}, fmt.Errorf("nessie ref %s returned empty hash", ref)
	}
	return out, nil
}

func snapshotRetainedInMetadata(metadataJSON []byte, snapshotID int64) (bool, error) {
	if len(metadataJSON) == 0 {
		return false, fmt.Errorf("load table returned empty metadata JSON")
	}
	var metadata struct {
		Snapshots []struct {
			SnapshotID int64 `json:"snapshot-id"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		return false, fmt.Errorf("decode table metadata snapshots: %w", err)
	}
	for _, snapshot := range metadata.Snapshots {
		if snapshot.SnapshotID == snapshotID {
			return true, nil
		}
	}
	return false, nil
}

func nessieAPIURL(rawCatalogURI string, parts ...string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(rawCatalogURI))
	if err != nil {
		return "", fmt.Errorf("invalid Iceberg REST catalog URI %q: %w", rawCatalogURI, err)
	}
	if base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid Iceberg REST catalog URI %q", rawCatalogURI)
	}
	pathParts := []string{"api", "v1"}
	escapedParts := []string{"api", "v1"}
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			pathParts = append(pathParts, part)
			escapedParts = append(escapedParts, url.PathEscape(part))
		}
	}
	base.Path = "/" + strings.Join(pathParts, "/")
	base.RawPath = "/" + strings.Join(escapedParts, "/")
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
}

func createNamespaceURL(rawBase string, prefix string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(rawBase))
	if err != nil {
		return "", fmt.Errorf("invalid Iceberg REST catalog URI %q: %w", rawBase, err)
	}
	if base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid Iceberg REST catalog URI %q", rawBase)
	}
	parts := splitURLPath(base.Path)
	if len(parts) == 0 || parts[len(parts)-1] != "v1" {
		parts = append(parts, "v1")
	}
	if strings.TrimSpace(prefix) != "" {
		parts = append(parts, strings.TrimSpace(prefix))
	}
	parts = append(parts, "namespaces")
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		escaped = append(escaped, url.PathEscape(part))
	}
	base.Path = "/" + strings.Join(parts, "/")
	base.RawPath = "/" + strings.Join(escaped, "/")
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
}

func splitURLPath(path string) []string {
	raw := strings.Split(strings.Trim(path, "/"), "/")
	parts := make([]string, 0, len(raw))
	for _, part := range raw {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func waitForDB(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(90 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		last = db.PingContext(pingCtx)
		cancel()
		if last == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("MO health check timed out: %w", last)
}

func queryLines(ctx context.Context, db *sql.DB, stmt string) ([]string, error) {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return out, err
		}
		parts := make([]string, len(values))
		for i, value := range values {
			parts[i] = sqlValueString(value)
		}
		out = append(out, strings.Join(parts, "\t"))
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func sqlValueString(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}

func formatSeconds(duration time.Duration) string {
	return fmt.Sprintf("%.3f", duration.Seconds())
}

func passedCase(id, name string, sqls, expected, actual []string, details map[string]string) caseResult {
	return caseResult{ID: id, Name: name, Status: "passed", SQL: sqls, Expected: expected, Actual: actual, Details: details}
}

func failedCase(id, name string, sqls, expected, actual []string, msg string) caseResult {
	return caseResult{ID: id, Name: name, Status: "failed", SQL: sqls, Expected: expected, Actual: actual, Error: msg}
}

func writeCaseReport(reportDir string, result caseResult) error {
	dir := filepath.Join(reportDir, safeFileName(result.ID+"_"+result.Name))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	actual := strings.Join(result.Actual, "\n")
	if actual == "" && result.Error != "" {
		actual = result.Error
	}
	if err := os.WriteFile(filepath.Join(dir, "mo.out"), []byte(redactText(actual)+"\n"), 0o644); err != nil {
		return err
	}
	metadata, err := json.MarshalIndent(struct {
		CaseID   string            `json:"case_id"`
		Name     string            `json:"name"`
		SQL      []string          `json:"sql"`
		Expected []string          `json:"expected,omitempty"`
		Details  map[string]string `json:"details,omitempty"`
		Error    string            `json:"error,omitempty"`
	}{
		CaseID:   result.ID,
		Name:     result.Name,
		SQL:      redactStrings(result.SQL),
		Expected: redactStrings(result.Expected),
		Details:  redactMap(result.Details),
		Error:    redactText(result.Error),
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), append(metadata, '\n'), 0o644); err != nil {
		return err
	}
	diff, err := json.MarshalIndent(map[string]any{
		"case_id": result.ID,
		"status":  result.Status,
		"engines": []map[string]any{{
			"engine":         "mo",
			"row_count":      len(result.Actual),
			"checksum":       checksumLines(result.Actual),
			"expected_error": result.Status != "passed",
		}},
		"sample_mismatch": sampleMismatch(result),
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "diff.json"), append(diff, '\n'), 0o644); err != nil {
		return err
	}
	status := "passed"
	if result.Status != "passed" {
		status = "failed"
	}
	summary := fmt.Sprintf("# %s\n\n- status: `%s`\n- rows: `%d`\n- checksum: `%s`\n", result.Name, status, len(result.Actual), checksumLines(result.Actual))
	if result.Error != "" {
		summary += "\n## Error\n\n```text\n" + redactText(result.Error) + "\n```\n"
	}
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(summary), 0o644)
}

func writeRunSummary(reportDir string, summary runSummary) error {
	summary.EndedAt = time.Now().UTC().Format(time.RFC3339)
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(reportDir, "summary.json"), []byte(redactText(string(data))+"\n"), 0o644); err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Iceberg E2E Local Summary\n\n")
	fmt.Fprintf(&b, "- namespace: `%s`\n", summary.Namespace)
	fmt.Fprintf(&b, "- database: `%s`\n", summary.Database)
	fmt.Fprintf(&b, "- catalog: `%s`\n\n", summary.Catalog)
	fmt.Fprintf(&b, "| case | status |\n| --- | --- |\n")
	for _, c := range summary.Cases {
		fmt.Fprintf(&b, "| `%s` %s | `%s` |\n", c.ID, c.Name, c.Status)
	}
	return os.WriteFile(filepath.Join(reportDir, "summary.md"), []byte(redactText(b.String())), 0o644)
}

func sampleMismatch(result caseResult) []string {
	if result.Status == "passed" {
		return nil
	}
	if result.Error != "" {
		return []string{redactText(result.Error)}
	}
	return []string{"actual output did not match expected output"}
}

func checksumLines(lines []string) string {
	h := sha256.New()
	for _, line := range lines {
		_, _ = h.Write([]byte(redactText(line)))
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func redactStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		out = append(out, redactText(item))
	}
	return out
}

func redactMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = redactText(v)
	}
	return out
}

var objectPathRe = regexp.MustCompile(`(?i)\b(?:s3|gs|abfs|abfss)://[^\s,;)` + "`" + `]+`)

func redactText(s string) string {
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, "raw-token", "<redacted>")
	s = strings.ReplaceAll(s, "raw-secret-key", "<redacted>")
	return objectPathRe.ReplaceAllStringFunc(s, func(path string) string {
		sum := sha256.Sum256([]byte(path))
		return "<redacted:path:" + hex.EncodeToString(sum[:])[:8] + ">"
	})
}

func ident(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func sqlString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `''`)
	return "'" + value + "'"
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateIdentifier(value, label string) error {
	if !identRe.MatchString(value) {
		return fmt.Errorf("%s %q must match %s", label, value, identRe.String())
	}
	return nil
}

func safeFileName(value string) string {
	value = regexp.MustCompile(`[^A-Za-z0-9_.-]+`).ReplaceAllString(value, "_")
	return strings.Trim(value, "_")
}

func linesContain(lines []string, needle string) bool {
	for _, line := range lines {
		if strings.Contains(line, needle) {
			return true
		}
	}
	return false
}

func sameLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "iceberg e2e local: %v\n", err)
	os.Exit(1)
}
