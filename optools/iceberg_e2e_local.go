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
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
		runner.appendReadAndTimeTravelCase,
		runner.partitionFilterCase,
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
	for _, table := range []string{"append_orders", "partition_orders", "mor_accounts"} {
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

func (r *caseRunner) appendReadAndTimeTravelCase(ctx context.Context) caseResult {
	table := fmt.Sprintf("%s.%s", ident(r.cfg.Database), ident("append_orders"))
	sqls := []string{
		fmt.Sprintf("INSERT INTO %s VALUES (1,1,10,'ksa'),(2,1,20,'uae'),(3,2,30,'ksa'),(4,2,40,'qat')", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s", table),
		fmt.Sprintf("INSERT INTO %s VALUES (5,3,50,'ksa')", table),
		fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s", table),
	}
	if _, err := r.db.ExecContext(ctx, sqls[0]); err != nil {
		return failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, nil, nil, err.Error())
	}
	oldSnapshot, err := currentSnapshotID(ctx, r.cfg, "append_orders")
	if err != nil {
		return failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, nil, nil, err.Error())
	}
	first, err := queryLines(ctx, r.db, sqls[1])
	if err != nil {
		return failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, nil, first, err.Error())
	}
	if _, err := r.db.ExecContext(ctx, sqls[2]); err != nil {
		return failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, nil, first, err.Error())
	}
	current, err := queryLines(ctx, r.db, sqls[3])
	if err != nil {
		return failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, nil, current, err.Error())
	}
	timeTravelSQL := fmt.Sprintf("SELECT COUNT(*), SUM(amount) FROM %s FOR ICEBERG SNAPSHOT %d", table, oldSnapshot)
	old, err := queryLines(ctx, r.db, timeTravelSQL)
	sqls = append(sqls, timeTravelSQL)
	if err != nil {
		return failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, nil, old, err.Error())
	}
	expected := []string{"4\t100", "5\t150", "4\t100"}
	actual := append(append([]string{}, first...), current...)
	actual = append(actual, old...)
	if !sameLines(expected, actual) {
		return failedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, expected, actual, "append/time-travel result mismatch")
	}
	return passedCase("ICE-CI-E2E-020", "append-read-time-travel", sqls, expected, actual, map[string]string{"old_snapshot_id": fmt.Sprintf("%d", oldSnapshot)})
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
				"format-version": "2",
				"owner":          "matrixone-ci",
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
	return []e2eTableSpec{
		{name: "append_orders", schema: orderSchema, partitionSpec: api.PartitionSpec{SpecID: 0}},
		{name: "partition_orders", schema: orderSchema, partitionSpec: partitioned},
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
	if metadata.CurrentSnapshotID == 0 {
		return 0, fmt.Errorf("table %s does not have a current snapshot", table)
	}
	return metadata.CurrentSnapshotID, nil
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
