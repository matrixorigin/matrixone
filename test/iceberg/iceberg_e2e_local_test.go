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
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCreateNamespaceURLUsesNegotiatedPrefix(t *testing.T) {
	tests := []struct {
		name    string
		base    string
		prefix  string
		want    string
		wantErr bool
	}{
		{
			name:   "adds v1 and prefix",
			base:   "http://127.0.0.1:19120/iceberg",
			prefix: "main",
			want:   "http://127.0.0.1:19120/iceberg/v1/main/namespaces",
		},
		{
			name:   "does not duplicate existing v1",
			base:   "http://127.0.0.1:19120/iceberg/v1",
			prefix: "main",
			want:   "http://127.0.0.1:19120/iceberg/v1/main/namespaces",
		},
		{
			name:   "escapes composite prefix as one segment",
			base:   "http://127.0.0.1:19120/iceberg",
			prefix: "main|s3://warehouse",
			want:   "http://127.0.0.1:19120/iceberg/v1/main%7Cs3:%2F%2Fwarehouse/namespaces",
		},
		{
			name: "supports catalogs without prefix",
			base: "http://127.0.0.1:19120/iceberg",
			want: "http://127.0.0.1:19120/iceberg/v1/namespaces",
		},
		{
			name:    "rejects invalid uri",
			base:    "://bad",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createNamespaceURL(tt.base, tt.prefix)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected URL: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestSnapshotRetainedInMetadata(t *testing.T) {
	tests := []struct {
		name       string
		metadata   string
		snapshotID int64
		want       bool
		wantErr    bool
	}{
		{
			name: "retained",
			metadata: `{
				"snapshots": [
					{"snapshot-id": 101},
					{"snapshot-id": 102}
				]
			}`,
			snapshotID: 101,
			want:       true,
		},
		{
			name: "not retained",
			metadata: `{
				"snapshots": [
					{"snapshot-id": 102}
				]
			}`,
			snapshotID: 101,
			want:       false,
		},
		{
			name:       "empty metadata",
			snapshotID: 101,
			wantErr:    true,
		},
		{
			name:       "invalid metadata",
			metadata:   `{`,
			snapshotID: 101,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := snapshotRetainedInMetadata([]byte(tt.metadata), tt.snapshotID)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected retained result: got %v want %v", got, tt.want)
			}
		})
	}
}

func TestLocalE2EStringHelpers(t *testing.T) {
	t.Setenv("MO_ICEBERG_TEST_ENV", "  from-env  ")
	if got := envOr("MO_ICEBERG_TEST_ENV", "fallback"); got != "from-env" {
		t.Fatalf("envOr returned %q", got)
	}
	if got := envOr("MO_ICEBERG_TEST_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("envOr fallback returned %q", got)
	}
	if got := ident("a`b"); got != "`a``b`" {
		t.Fatalf("ident returned %q", got)
	}
	if got := sqlString(`a\b'c`); got != `'a\\b''c'` {
		t.Fatalf("sqlString returned %q", got)
	}
	if err := validateIdentifier("ok_123", "catalog"); err != nil {
		t.Fatalf("validateIdentifier rejected a valid identifier: %v", err)
	}
	if err := validateIdentifier("bad-name", "catalog"); err == nil {
		t.Fatalf("validateIdentifier accepted an invalid identifier")
	}
	if got := safeFileName(" ICE/CI:E2E 020 "); got != "ICE_CI_E2E_020" {
		t.Fatalf("safeFileName returned %q", got)
	}
	if !linesContain([]string{"abc", "namespace=x"}, "namespace") {
		t.Fatalf("linesContain did not find substring")
	}
	if !sameLines([]string{"a", "b"}, []string{"a", "b"}) || sameLines([]string{"a"}, []string{"a", "b"}) {
		t.Fatalf("sameLines returned an unexpected result")
	}
}

func TestLocalE2EQueryLinesAndSQLValueString(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("new sqlmock: %v", err)
	}
	defer db.Close()

	ts := time.Date(2026, 7, 6, 1, 2, 3, 4, time.FixedZone("KSA", 3*3600))
	rows := sqlmock.NewRows([]string{"name", "amount", "ts", "nil"}).
		AddRow([]byte("ksa"), int64(42), ts, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	got, err := queryLines(context.Background(), db, "SELECT name, amount, ts, nil FROM t")
	if err != nil {
		t.Fatalf("queryLines failed: %v", err)
	}
	want := []string{"ksa\t42\t2026-07-05T22:02:03.000000004Z\tNULL"}
	if !sameLines(want, got) {
		t.Fatalf("queryLines mismatch: got %#v want %#v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2ECaseReportsRedactAndSummarize(t *testing.T) {
	dir := t.TempDir()
	result := failedCase(
		"ICE-CI/E2E 999",
		"secret report",
		[]string{"SELECT 'raw-token', 's3://bucket/path/file.parquet'"},
		[]string{"safe"},
		[]string{"raw-token s3://bucket/path/file.parquet"},
		"raw-secret-key at s3://bucket/path/file.parquet",
	)
	result.Details = map[string]string{"scope": "s3://bucket/path/file.parquet"}

	if err := writeCaseReport(dir, result); err != nil {
		t.Fatalf("writeCaseReport failed: %v", err)
	}
	caseDir := filepath.Join(dir, safeFileName(result.ID+"_"+result.Name))
	for _, name := range []string{"mo.out", "metadata.json", "diff.json", "summary.md"} {
		data, err := os.ReadFile(filepath.Join(caseDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(data)
		if strings.Contains(text, "raw-token") || strings.Contains(text, "raw-secret-key") || strings.Contains(text, "s3://bucket") {
			t.Fatalf("%s leaked sensitive text:\n%s", name, text)
		}
		if !strings.Contains(text, "<redacted") && !strings.Contains(text, `\u003credacted`) {
			t.Fatalf("%s did not contain a redaction marker:\n%s", name, text)
		}
	}

	summary := runSummary{
		RunID:     "run",
		Namespace: "ns",
		Database:  "db",
		Catalog:   "cat",
		Cases: []caseResult{
			passedCase("ICE-CI-E2E-001", "passed", nil, []string{"ok"}, []string{"ok"}, nil),
			result,
		},
	}
	if err := writeRunSummary(dir, summary); err != nil {
		t.Fatalf("writeRunSummary failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "summary.md"))
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if !strings.Contains(string(data), "ICE-CI-E2E-001") || !strings.Contains(string(data), "failed") {
		t.Fatalf("summary missing case rows:\n%s", string(data))
	}
}

func TestLocalE2ESetupExecutesExpectedStatements(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()

	for _, pattern := range []string{
		"DROP DATABASE IF EXISTS",
		"DROP ICEBERG CATALOG IF EXISTS",
		"CREATE DATABASE",
		"CREATE ICEBERG CATALOG",
		"CALL iceberg_register_access",
		"CREATE EXTERNAL TABLE",
		"CREATE EXTERNAL TABLE",
		"CREATE EXTERNAL TABLE",
	} {
		mock.ExpectExec(pattern).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	runner := &caseRunner{cfg: localE2ETestConfig(), db: db}
	if err := runner.setup(context.Background()); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2ECatalogAndMappingCase(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	cfg := localE2ETestConfig()

	mock.ExpectExec("CREATE ICEBERG CATALOG bad_").
		WillReturnError(errors.New("token_secret must be a secret:// reference"))
	mock.ExpectQuery("SHOW ICEBERG NAMESPACES").
		WillReturnRows(sqlmock.NewRows([]string{"namespace"}).AddRow(cfg.Namespace))
	mock.ExpectQuery("SHOW ICEBERG TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"table"}).
			AddRow("append_orders").
			AddRow("partition_orders").
			AddRow("mor_accounts"))
	mock.ExpectQuery("SHOW CREATE TABLE").
		WillReturnRows(sqlmock.NewRows([]string{"table", "create"}).AddRow("append_orders", "CREATE EXTERNAL TABLE ..."))

	result := (&caseRunner{cfg: cfg, db: db}).catalogAndMappingCase(context.Background())
	if result.Status != "passed" {
		t.Fatalf("catalog case failed: %+v", result)
	}
	if result.Details["inline_secret"] != "rejected" {
		t.Fatalf("inline secret detail missing: %+v", result.Details)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2ECatalogAndMappingCaseFailures(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	cfg := localE2ETestConfig()

	mock.ExpectExec("CREATE ICEBERG CATALOG bad_").
		WillReturnResult(sqlmock.NewResult(0, 1))
	result := (&caseRunner{cfg: cfg, db: db}).catalogAndMappingCase(context.Background())
	if result.Status != "failed" || !strings.Contains(result.Error, "inline token_secret") {
		t.Fatalf("expected inline secret failure, got %+v", result)
	}
}

func TestLocalE2EAppendReadAndTimeTravelCase(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	cfg := localE2ETestConfig()
	cfg.CatalogURI = newSnapshotRESTServer(t).URL + "/iceberg"

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectQuery("COUNT\\(\\*\\), SUM\\(amount\\)").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(4), int64(100)))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("COUNT\\(\\*\\), SUM\\(amount\\)").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(5), int64(150)))
	mock.ExpectQuery("FOR ICEBERG SNAPSHOT").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(4), int64(100)))

	result := (&caseRunner{cfg: cfg, db: db}).appendReadAndTimeTravelCase(context.Background())
	if result.Status != "passed" {
		t.Fatalf("append/time-travel case failed: %+v", result)
	}
	if result.Details["old_snapshot_id"] != "100" || result.Details["current_snapshot_id"] != "101" {
		t.Fatalf("unexpected snapshot details: %+v", result.Details)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2ESnapshotHelpersAgainstREST(t *testing.T) {
	cfg := localE2ETestConfig()
	cfg.CatalogURI = newSnapshotRESTServer(t).URL + "/iceberg"
	ctx := context.Background()
	got, err := currentSnapshotID(ctx, cfg, "append_orders")
	if err != nil || got != 100 {
		t.Fatalf("currentSnapshotID got=%d err=%v", got, err)
	}
	ok, err := snapshotAvailable(ctx, cfg, "append_orders", 100)
	if err != nil || !ok {
		t.Fatalf("snapshotAvailable got=%v err=%v", ok, err)
	}
}

func TestLocalE2EAppendReadAndTimeTravelCaseFallbackWhenOldSnapshotExpired(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	cfg := localE2ETestConfig()
	cfg.CatalogURI = newSnapshotRESTServerWithoutOldSnapshot(t).URL + "/iceberg"

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectQuery("COUNT\\(\\*\\), SUM\\(amount\\)").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(4), int64(100)))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("COUNT\\(\\*\\), SUM\\(amount\\)").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(5), int64(150)))
	mock.ExpectQuery("FOR ICEBERG SNAPSHOT").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(5), int64(150)))

	result := (&caseRunner{cfg: cfg, db: db}).appendReadAndTimeTravelCase(context.Background())
	if result.Status != "passed" {
		t.Fatalf("append/time-travel fallback failed: %+v", result)
	}
	if result.Details["historical_snapshot_retained"] != "false" {
		t.Fatalf("expected expired snapshot fallback detail: %+v", result.Details)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2EPartitionFilterCase(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectQuery("WHERE region = 'ksa'").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(2), int64(40)))
	mock.ExpectQuery("WHERE region IN").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(3), int64(60)))

	result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).partitionFilterCase(context.Background())
	if result.Status != "passed" {
		t.Fatalf("partition case failed: %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2EMergeOnReadDeleteCase(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SUM\\(balance\\)").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(3), int64(700)))
	mock.ExpectQuery("account_id = 2").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))

	result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).mergeOnReadDeleteCase(context.Background())
	if result.Status != "passed" {
		t.Fatalf("delete case failed: %+v", result)
	}
	if result.Details["rows_affected"] != "1" {
		t.Fatalf("rows affected detail mismatch: %+v", result.Details)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2ECaseFailurePaths(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()

	mock.ExpectExec("INSERT INTO").WillReturnError(errors.New("insert failed"))
	result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).partitionFilterCase(context.Background())
	if result.Status != "failed" || !strings.Contains(result.Error, "insert failed") {
		t.Fatalf("expected insert failure, got %+v", result)
	}
}

func TestLocalE2ETableSpecs(t *testing.T) {
	specs := e2eTableSpecs("ns")
	if len(specs) != 3 {
		t.Fatalf("expected 3 specs, got %d", len(specs))
	}
	if specs[1].partitionSpec.Fields[0].Name != "region" {
		t.Fatalf("partition spec did not preserve region identity: %+v", specs[1].partitionSpec.Fields)
	}
	if specs[2].schema.Fields[0].Name != "account_id" {
		t.Fatalf("account schema mismatch: %+v", specs[2].schema.Fields)
	}
}

func TestLocalE2ECreateNamespace(t *testing.T) {
	var body string
	server := httptestServer(t, httpStatusCreated, &body)
	cfg := localE2ETestConfig()
	cfg.CatalogURI = server.URL + "/iceberg"
	if err := createNamespace(context.Background(), cfg, "main"); err != nil {
		t.Fatalf("createNamespace failed: %v", err)
	}
	if !strings.Contains(body, cfg.Namespace) {
		t.Fatalf("namespace request body missing namespace: %s", body)
	}

	server = httptestServer(t, httpStatusInternalServerError, nil)
	cfg.CatalogURI = server.URL
	err := createNamespace(context.Background(), cfg, "")
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("expected HTTP 500 error, got %v", err)
	}
}

func TestLocalE2ESeedRESTTables(t *testing.T) {
	var created []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/v1/config"):
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"main"}}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/v1/main/namespaces"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/v1/main/namespaces/ci_ns/tables"):
			data, _ := io.ReadAll(r.Body)
			body := string(data)
			for _, name := range []string{"append_orders", "partition_orders", "mor_accounts"} {
				if strings.Contains(body, `"name":"`+name+`"`) {
					created = append(created, name)
				}
			}
			if !strings.Contains(body, `"schema-id":0`) || !strings.Contains(body, `"spec-id":0`) {
				t.Fatalf("create table body omitted zero-valued Iceberg fields: %s", body)
			}
			_, _ = w.Write([]byte(`{"metadata-location":"s3://warehouse/ci_ns/table/metadata/v1.json","config":{"table-uuid":"uuid-1"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	cfg := localE2ETestConfig()
	cfg.CatalogURI = server.URL + "/iceberg"
	if err := seedRESTTables(context.Background(), cfg); err != nil {
		t.Fatalf("seedRESTTables failed: %v", err)
	}
	if !sameLines(created, []string{"append_orders", "partition_orders", "mor_accounts"}) {
		t.Fatalf("unexpected created tables: %v", created)
	}
}

func localE2ETestConfig() localE2EConfig {
	return localE2EConfig{
		CatalogURI: "http://127.0.0.1:19120/iceberg",
		Warehouse:  "s3://mo-iceberg/warehouse",
		DSN:        "root:111@tcp(127.0.0.1:6001)/",
		ReportDir:  "test/iceberg/reports/e2e-local",
		Namespace:  "ci_ns",
		Catalog:    "ci_catalog",
		Database:   "ci_db",
	}
}

func newLocalE2ESQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("new sqlmock: %v", err)
	}
	return db, mock
}

const (
	httpStatusCreated             = 201
	httpStatusInternalServerError = 500
)

func httptestServer(t *testing.T, status int, bodyOut *string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bodyOut != nil {
			data, _ := io.ReadAll(r.Body)
			*bodyOut = string(data)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("response body"))
	}))
	t.Cleanup(server.Close)
	return server
}

func newSnapshotRESTServer(t *testing.T) *httptest.Server {
	t.Helper()
	return newSnapshotRESTServerWithRetention(t, true)
}

func newSnapshotRESTServerWithoutOldSnapshot(t *testing.T) *httptest.Server {
	t.Helper()
	return newSnapshotRESTServerWithRetention(t, false)
}

func newSnapshotRESTServerWithRetention(t *testing.T, retainOld bool) *httptest.Server {
	t.Helper()
	loads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/v1/config"):
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"main"}}`))
		case strings.Contains(r.URL.Path, "/namespaces/ci_ns/tables/append_orders"):
			loads++
			current := int64(100)
			if loads >= 2 {
				current = 101
			}
			snapshots := `[{"snapshot-id":100},{"snapshot-id":101}]`
			if r.URL.Query().Get("snapshots") == "all" && !retainOld {
				snapshots = `[{"snapshot-id":101}]`
			}
			_, _ = w.Write([]byte(`{
				"metadata-location":"s3://warehouse/ci_ns/append_orders/metadata/v1.json",
				"metadata":{
					"format-version":2,
					"table-uuid":"uuid-1",
					"current-snapshot-id":` + sqlValueString(current) + `,
					"snapshots":` + snapshots + `
				}
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}
