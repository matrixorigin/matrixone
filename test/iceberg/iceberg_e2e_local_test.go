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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
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
	if got := redactText(""); got != "" {
		t.Fatalf("redactText returned %q for an empty string", got)
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

func TestLocalE2EQueryLinesFailurePaths(t *testing.T) {
	t.Run("query fails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("new sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT").WillReturnError(errors.New("catalog offline"))
		if _, err := queryLines(context.Background(), db, "SELECT * FROM t"); err == nil || !strings.Contains(err.Error(), "catalog offline") {
			t.Fatalf("expected query failure, got %v", err)
		}
	})

	t.Run("iteration fails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("new sqlmock: %v", err)
		}
		defer db.Close()
		rows := sqlmock.NewRows([]string{"name"}).AddRow("first").AddRow("second").RowError(1, errors.New("stream interrupted"))
		mock.ExpectQuery("SELECT").WillReturnRows(rows)
		got, err := queryLines(context.Background(), db, "SELECT name FROM t")
		if err == nil || !strings.Contains(err.Error(), "stream interrupted") {
			t.Fatalf("expected iteration failure, got rows=%v err=%v", got, err)
		}
		if len(got) != 1 || got[0] != "first" {
			t.Fatalf("unexpected rows before iteration failure: %v", got)
		}
	})
}

func TestLocalE2EWaitForDBUsesPing(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("new sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectPing()
	if err := waitForDB(context.Background(), db); err != nil {
		t.Fatalf("waitForDB failed: %v", err)
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

	if err := writeCaseReport(dir, failedCase("ICE-CI-E2E-998", "error-only", nil, nil, nil, "raw-secret-key")); err != nil {
		t.Fatalf("write error-only report: %v", err)
	}
	errorOnly, err := os.ReadFile(filepath.Join(dir, safeFileName("ICE-CI-E2E-998_error-only"), "mo.out"))
	if err != nil {
		t.Fatalf("read error-only report: %v", err)
	}
	if strings.Contains(string(errorOnly), "raw-secret-key") || !strings.Contains(string(errorOnly), "<redacted>") {
		t.Fatalf("error-only report did not redact the error: %s", errorOnly)
	}
}

func TestLocalE2EReportErrorBranches(t *testing.T) {
	dir := t.TempDir()
	blockingFile := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}
	if err := writeCaseReport(blockingFile, passedCase("ICE-CI-E2E-999", "blocked", nil, nil, nil, nil)); err == nil {
		t.Fatalf("expected writeCaseReport to fail when report root is a file")
	}
	if err := writeRunSummary(blockingFile, runSummary{Namespace: "ns", Database: "db", Catalog: "cat"}); err == nil {
		t.Fatalf("expected writeRunSummary to fail when report root is a file")
	}

	if mismatch := sampleMismatch(passedCase("ok", "ok", nil, nil, nil, nil)); mismatch != nil {
		t.Fatalf("passed case should not have mismatch: %v", mismatch)
	}
	failed := failedCase("bad", "bad", nil, nil, nil, "")
	if mismatch := sampleMismatch(failed); len(mismatch) != 1 || !strings.Contains(mismatch[0], "did not match") {
		t.Fatalf("unexpected generic mismatch: %v", mismatch)
	}
	if redacted := redactMap(map[string]string{"scope": "s3://bucket/path/file.parquet"}); !strings.Contains(redacted["scope"], "<redacted:path:") {
		t.Fatalf("map value was not redacted: %v", redacted)
	}
	if redactMap(nil) != nil {
		t.Fatalf("nil map should stay nil")
	}
	left := checksumLines([]string{"raw-token", "s3://bucket/path/file.parquet"})
	right := checksumLines([]string{"raw-token", "s3://bucket/path/file.parquet"})
	if left != right {
		t.Fatalf("checksumLines should be deterministic: %s != %s", left, right)
	}
}

func TestLocalE2ECaseReportFileWriteFailures(t *testing.T) {
	for _, blockedFile := range []string{"mo.out", "metadata.json", "diff.json", "summary.md"} {
		t.Run(blockedFile, func(t *testing.T) {
			root := t.TempDir()
			result := passedCase("ICE-CI-E2E-997", "write-failure", nil, nil, nil, nil)
			caseDir := filepath.Join(root, safeFileName(result.ID+"_"+result.Name))
			if err := os.MkdirAll(filepath.Join(caseDir, blockedFile), 0o755); err != nil {
				t.Fatalf("create blocking directory: %v", err)
			}
			if err := writeCaseReport(root, result); err == nil {
				t.Fatalf("expected writeCaseReport to fail when %s is a directory", blockedFile)
			}
		})
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

func TestLocalE2ESetupReportsStatementFailure(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	mock.ExpectExec("DROP DATABASE IF EXISTS").WillReturnError(errors.New("database locked"))
	if err := (&caseRunner{cfg: localE2ETestConfig(), db: db}).setup(context.Background()); err == nil || !strings.Contains(err.Error(), "database locked") {
		t.Fatalf("expected setup statement failure, got %v", err)
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

func TestLocalE2EPartitionFilterCaseReportsInsertFailure(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	mock.ExpectExec("INSERT INTO").WillReturnError(errors.New("write unavailable"))
	result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).partitionFilterCase(context.Background())
	if result.Status != "failed" || !strings.Contains(result.Error, "write unavailable") {
		t.Fatalf("expected insert failure, got %+v", result)
	}
}

func TestLocalE2EPartitionFilterCaseReportsQueryAndMismatchFailures(t *testing.T) {
	t.Run("query fails", func(t *testing.T) {
		db, mock := newLocalE2ESQLMock(t)
		defer db.Close()
		mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
		mock.ExpectQuery("WHERE region = 'ksa'").WillReturnError(errors.New("catalog offline"))
		result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).partitionFilterCase(context.Background())
		if result.Status != "failed" || !strings.Contains(result.Error, "catalog offline") {
			t.Fatalf("expected query failure, got %+v", result)
		}
	})

	t.Run("result mismatches", func(t *testing.T) {
		db, mock := newLocalE2ESQLMock(t)
		defer db.Close()
		mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
		mock.ExpectQuery("WHERE region = 'ksa'").WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(1), int64(10)))
		mock.ExpectQuery("WHERE region IN").WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(2), int64(30)))
		result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).partitionFilterCase(context.Background())
		if result.Status != "failed" || !strings.Contains(result.Error, "result mismatch") {
			t.Fatalf("expected mismatch failure, got %+v", result)
		}
	})
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

func TestLocalE2ECatalogAndMappingCaseFailureBranches(t *testing.T) {
	cfg := localE2ETestConfig()
	tests := []struct {
		name   string
		mock   func(sqlmock.Sqlmock)
		errSub string
	}{
		{
			name: "inline secret unexpected error",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE ICEBERG CATALOG bad_").WillReturnError(errors.New("syntax error"))
			},
			errSub: "unexpected error",
		},
		{
			name: "namespaces query fails",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE ICEBERG CATALOG bad_").WillReturnError(errors.New("secret:// required"))
				mock.ExpectQuery("SHOW ICEBERG NAMESPACES").WillReturnError(errors.New("catalog offline"))
			},
			errSub: "catalog offline",
		},
		{
			name: "namespace missing",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE ICEBERG CATALOG bad_").WillReturnError(errors.New("secret:// required"))
				mock.ExpectQuery("SHOW ICEBERG NAMESPACES").
					WillReturnRows(sqlmock.NewRows([]string{"namespace"}).AddRow("other_ns"))
			},
			errSub: "seed namespace not listed",
		},
		{
			name: "table missing",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE ICEBERG CATALOG bad_").WillReturnError(errors.New("secret:// required"))
				mock.ExpectQuery("SHOW ICEBERG NAMESPACES").
					WillReturnRows(sqlmock.NewRows([]string{"namespace"}).AddRow(cfg.Namespace))
				mock.ExpectQuery("SHOW ICEBERG TABLES").
					WillReturnRows(sqlmock.NewRows([]string{"table"}).AddRow("append_orders"))
			},
			errSub: "seed table not listed",
		},
		{
			name: "show create fails",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE ICEBERG CATALOG bad_").WillReturnError(errors.New("secret:// required"))
				mock.ExpectQuery("SHOW ICEBERG NAMESPACES").
					WillReturnRows(sqlmock.NewRows([]string{"namespace"}).AddRow(cfg.Namespace))
				mock.ExpectQuery("SHOW ICEBERG TABLES").
					WillReturnRows(sqlmock.NewRows([]string{"table"}).
						AddRow("append_orders").
						AddRow("partition_orders").
						AddRow("mor_accounts"))
				mock.ExpectQuery("SHOW CREATE TABLE").WillReturnError(errors.New("show create failed"))
			},
			errSub: "show create failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newLocalE2ESQLMock(t)
			defer db.Close()
			tt.mock(mock)
			result := (&caseRunner{cfg: cfg, db: db}).catalogAndMappingCase(context.Background())
			if result.Status != "failed" || !strings.Contains(result.Error, tt.errSub) {
				t.Fatalf("expected failure containing %q, got %+v", tt.errSub, result)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestLocalE2EAppendReadAndTimeTravelCase(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	cfg := localE2ETestConfig()
	server := newSnapshotRESTServer(t)
	cfg.CatalogURI = server.URL + "/iceberg"

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("CREATE EXTERNAL TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
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
	if result.Details["old_snapshot_ref"] != "mo_e2e_t1_100" {
		t.Fatalf("unexpected snapshot ref detail: %+v", result.Details)
	}
	if result.Details["historical_snapshot_retained"] != "true" {
		t.Fatalf("expected retained historical snapshot detail: %+v", result.Details)
	}
	for _, key := range []string{"case_wall_s", "select_first_s", "select_current_s", "time_travel_select_s"} {
		if result.Details[key] == "" {
			t.Fatalf("missing timing detail %s: %+v", key, result.Details)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestLocalE2EAppendReadAndTimeTravelCaseReportsInsertFailure(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	mock.ExpectExec("INSERT INTO").WillReturnError(errors.New("write unavailable"))
	result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).appendReadAndTimeTravelCase(context.Background())
	if result.Status != "failed" || !strings.Contains(result.Error, "write unavailable") {
		t.Fatalf("expected insert failure, got %+v", result)
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
	branch, err := createNessieBranchAtMain(ctx, cfg, "mo_e2e_t1_100")
	if err != nil || branch != "mo_e2e_t1_100" {
		t.Fatalf("createNessieBranchAtMain got=%q err=%v", branch, err)
	}
	ok, err = snapshotAvailableOnRef(ctx, cfg, "append_orders", branch, 100)
	if err != nil || !ok {
		t.Fatalf("snapshotAvailableOnRef got=%v err=%v", ok, err)
	}
}

func TestNessieAPIURLEscapesReferenceAsOnePathSegment(t *testing.T) {
	got, err := nessieAPIURL("http://127.0.0.1:19120/iceberg", "trees", "tree", "feature/ksa orders")
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://127.0.0.1:19120/api/v1/trees/tree/feature%2Fksa%20orders" {
		t.Fatalf("unexpected Nessie API URL: %s", got)
	}
}

func TestLocalE2EURLHelpersRejectMissingSchemeOrHost(t *testing.T) {
	for _, rawURI := range []string{"", "/iceberg", "http:///iceberg", "://bad"} {
		t.Run("nessie "+strconv.Quote(rawURI), func(t *testing.T) {
			if _, err := nessieAPIURL(rawURI, "trees", "tree"); err == nil {
				t.Fatalf("nessieAPIURL(%q) unexpectedly succeeded", rawURI)
			}
		})
		t.Run("namespace "+strconv.Quote(rawURI), func(t *testing.T) {
			if _, err := createNamespaceURL(rawURI, "main"); err == nil {
				t.Fatalf("createNamespaceURL(%q) unexpectedly succeeded", rawURI)
			}
		})
	}
}

func TestLocalE2EHTTPHelperFailurePaths(t *testing.T) {
	ctx := context.Background()

	t.Run("create namespace reports non-success status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("namespace create failed"))
		}))
		t.Cleanup(server.Close)
		cfg := localE2ETestConfig()
		cfg.CatalogURI = server.URL
		if err := createNamespace(ctx, cfg, "main"); err == nil || !strings.Contains(err.Error(), "returned HTTP 500") {
			t.Fatalf("expected namespace HTTP error, got %v", err)
		}
	})

	t.Run("create namespace rejects invalid catalog URI", func(t *testing.T) {
		cfg := localE2ETestConfig()
		cfg.CatalogURI = "/not-a-url"
		if err := createNamespace(ctx, cfg, "main"); err == nil {
			t.Fatal("expected createNamespace to reject an invalid catalog URI")
		}
	})

	t.Run("create namespace reports request failures", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		catalogURI := server.URL
		server.Close()
		cfg := localE2ETestConfig()
		cfg.CatalogURI = catalogURI
		if err := createNamespace(ctx, cfg, "main"); err == nil || !strings.Contains(err.Error(), "request failed") {
			t.Fatalf("expected namespace request failure, got %v", err)
		}
	})

	t.Run("catalog helpers propagate config and load-table failures", func(t *testing.T) {
		configDown := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(configDown.Close)
		cfg := localE2ETestConfig()
		cfg.CatalogURI = configDown.URL + "/iceberg"
		if _, err := currentSnapshotID(ctx, cfg, "append_orders"); err == nil {
			t.Fatal("expected currentSnapshotID config failure")
		}
		if _, err := snapshotAvailableOnRef(ctx, cfg, "append_orders", "feature/ksa", 100); err == nil {
			t.Fatal("expected snapshotAvailableOnRef config failure")
		}

		loadTableDown := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/v1/config") {
				_, _ = w.Write([]byte(`{"defaults":{"prefix":"main"}}`))
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(loadTableDown.Close)
		cfg.CatalogURI = loadTableDown.URL + "/iceberg"
		if _, err := currentSnapshotID(ctx, cfg, "append_orders"); err == nil {
			t.Fatal("expected currentSnapshotID load-table failure")
		}
		if _, err := snapshotAvailableOnRef(ctx, cfg, "append_orders", "feature/ksa", 100); err == nil {
			t.Fatal("expected snapshotAvailableOnRef load-table failure")
		}
	})

	t.Run("nessie helpers reject invalid catalog URI", func(t *testing.T) {
		cfg := localE2ETestConfig()
		cfg.CatalogURI = "/not-a-url"
		if _, err := getNessieReference(ctx, cfg, model.DefaultRefMain); err == nil {
			t.Fatal("expected getNessieReference to reject an invalid catalog URI")
		}
		if _, err := createNessieBranchAtMain(ctx, cfg, "feature/ksa"); err == nil {
			t.Fatal("expected createNessieBranchAtMain to reject an invalid catalog URI")
		}
	})

	t.Run("nessie reference reports request failure", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		catalogURI := server.URL
		server.Close()
		cfg := localE2ETestConfig()
		cfg.CatalogURI = catalogURI
		if _, err := getNessieReference(ctx, cfg, model.DefaultRefMain); err == nil || !strings.Contains(err.Error(), "request failed") {
			t.Fatalf("expected Nessie request failure, got %v", err)
		}
	})

	t.Run("nessie branch reports request failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				w.Header().Set("Connection", "close")
				_, _ = w.Write([]byte(`{"type":"BRANCH","name":"main","hash":"main-hash"}`))
			case http.MethodPost:
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
		}))
		t.Cleanup(server.Close)
		cfg := localE2ETestConfig()
		cfg.CatalogURI = server.URL
		if _, err := createNessieBranchAtMain(ctx, cfg, "feature/ksa"); err == nil || !strings.Contains(err.Error(), "request failed") {
			t.Fatalf("expected branch request failure, got %v", err)
		}
	})
}

func TestSeedRESTTablesFailurePaths(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "config request fails",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			wantErr: "get Iceberg REST catalog config",
		},
		{
			name: "namespace creation fails",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/v1/config") {
					_, _ = w.Write([]byte(`{"defaults":{"prefix":"main"}}`))
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr: "create namespace",
		},
		{
			name: "table creation fails",
			handler: func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/v1/config"):
					_, _ = w.Write([]byte(`{"defaults":{"prefix":"main"}}`))
				case strings.HasSuffix(r.URL.Path, "/namespaces"):
					w.WriteHeader(http.StatusCreated)
				default:
					w.WriteHeader(http.StatusInternalServerError)
				}
			},
			wantErr: "create Iceberg table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			t.Cleanup(server.Close)
			cfg := localE2ETestConfig()
			cfg.CatalogURI = server.URL + "/iceberg"
			err := seedRESTTables(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestCatalogRequestWithPrefixForRef(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		status     int
		wantPrefix string
		wantErr    string
	}{
		{
			name:       "replaces main prefix",
			prefix:     "main",
			status:     http.StatusOK,
			wantPrefix: "feature/ksa",
		},
		{
			name:       "replaces ref inside warehouse prefix",
			prefix:     "main|s3://warehouse",
			status:     http.StatusOK,
			wantPrefix: "feature/ksa|s3://warehouse",
		},
		{
			name:       "uses target ref for empty prefix",
			prefix:     "",
			status:     http.StatusOK,
			wantPrefix: "feature/ksa",
		},
		{
			name:    "rejects an unstructured prefix",
			prefix:  "warehouse-only",
			status:  http.StatusOK,
			wantErr: "nessie prefix",
		},
		{
			name:    "propagates config failure",
			status:  http.StatusServiceUnavailable,
			wantErr: "get Iceberg REST catalog config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/v1/config") {
					http.NotFound(w, r)
					return
				}
				if tt.status != http.StatusOK {
					w.WriteHeader(tt.status)
					_, _ = w.Write([]byte("catalog unavailable"))
					return
				}
				_, _ = w.Write([]byte(`{"defaults":{"prefix":` + strconv.Quote(tt.prefix) + `}}`))
			}))
			t.Cleanup(server.Close)

			req := api.CatalogRequest{Catalog: model.Catalog{
				Name:      "ci_catalog",
				Type:      "rest",
				URI:       server.URL + "/iceberg",
				Warehouse: "s3://warehouse",
				AuthMode:  "none",
			}}
			got, err := catalogRequestWithPrefixForRef(
				context.Background(),
				catalog.NewRESTClient(catalog.WithAllowPlainHTTP(true)),
				req,
				req.Catalog.Warehouse,
				"feature/ksa",
			)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("catalogRequestWithPrefixForRef failed: %v", err)
			}
			if got.Prefix != tt.wantPrefix {
				t.Fatalf("unexpected prefix: got %q want %q", got.Prefix, tt.wantPrefix)
			}
		})
	}
}

func TestNessieReferenceFailurePaths(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{
			name:    "non-success status",
			status:  http.StatusConflict,
			body:    "ref conflict",
			wantErr: "returned HTTP 409",
		},
		{
			name:    "invalid JSON",
			status:  http.StatusOK,
			body:    "{",
			wantErr: "decode Nessie ref",
		},
		{
			name:    "empty hash",
			status:  http.StatusOK,
			body:    `{"type":"BRANCH","name":"main"}`,
			wantErr: "nessie ref main returned empty hash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)
			cfg := localE2ETestConfig()
			cfg.CatalogURI = server.URL
			_, err := getNessieReference(context.Background(), cfg, model.DefaultRefMain)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}

	t.Run("branch creation propagates POST failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				_, _ = w.Write([]byte(`{"type":"BRANCH","name":"main","hash":"main-hash"}`))
			case http.MethodPost:
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("branch create failed"))
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(server.Close)
		cfg := localE2ETestConfig()
		cfg.CatalogURI = server.URL
		_, err := createNessieBranchAtMain(context.Background(), cfg, "feature/ksa")
		if err == nil || !strings.Contains(err.Error(), "returned HTTP 500") {
			t.Fatalf("expected branch create HTTP error, got %v", err)
		}
	})
}

func TestLocalE2ESnapshotHelperFailures(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		errSub string
	}{
		{
			name:   "empty metadata",
			body:   `{"metadata-location":"s3://warehouse/t/metadata/v1.json"}`,
			errSub: "empty metadata JSON",
		},
		{
			name:   "invalid metadata",
			body:   `{"metadata-location":"s3://warehouse/t/metadata/v1.json","metadata":"not-json"}`,
			errSub: "decode table metadata",
		},
		{
			name:   "no current snapshot",
			body:   `{"metadata-location":"s3://warehouse/t/metadata/v1.json","metadata":{"format-version":2,"table-uuid":"u"}}`,
			errSub: "does not have a current snapshot",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/v1/config"):
					_, _ = w.Write([]byte(`{"defaults":{"prefix":"main"}}`))
				case strings.Contains(r.URL.Path, "/namespaces/ci_ns/tables/append_orders"):
					_, _ = w.Write([]byte(tt.body))
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			cfg := localE2ETestConfig()
			cfg.CatalogURI = server.URL + "/iceberg"
			_, err := currentSnapshotID(context.Background(), cfg, "append_orders")
			if err == nil || !strings.Contains(err.Error(), tt.errSub) {
				t.Fatalf("expected %q error, got %v", tt.errSub, err)
			}
		})
	}
}

func TestLocalE2EAppendReadAndTimeTravelCaseReadsOldSnapshotWhenMetadataOmitsHistory(t *testing.T) {
	db, mock := newLocalE2ESQLMock(t)
	defer db.Close()
	cfg := localE2ETestConfig()
	cfg.CatalogURI = newSnapshotRESTServerWithoutOldSnapshot(t).URL + "/iceberg"

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("CREATE EXTERNAL TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("COUNT\\(\\*\\), SUM\\(amount\\)").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(4), int64(100)))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("COUNT\\(\\*\\), SUM\\(amount\\)").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(5), int64(150)))
	mock.ExpectQuery("FOR ICEBERG SNAPSHOT").
		WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(4), int64(100)))

	result := (&caseRunner{cfg: cfg, db: db}).appendReadAndTimeTravelCase(context.Background())
	if result.Status != "passed" {
		t.Fatalf("expected append/time-travel to read old snapshot through planner fallback, got %+v", result)
	}
	if result.Details["historical_snapshot_retained"] != "false" {
		t.Fatalf("expected expired snapshot detail: %+v", result.Details)
	}
	if !sameLines([]string{"4\t100", "5\t150", "4\t100"}, result.Actual) {
		t.Fatalf("unexpected actual results: %+v", result)
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

func TestLocalE2EMergeOnReadDeleteCaseFailures(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(sqlmock.Sqlmock)
		errSub string
	}{
		{
			name: "insert fails",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO").WillReturnError(errors.New("insert failed"))
			},
			errSub: "insert failed",
		},
		{
			name: "delete fails",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
				mock.ExpectExec("DELETE FROM").WillReturnError(errors.New("delete failed"))
			},
			errSub: "delete failed",
		},
		{
			name: "aggregate query fails",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
				mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SUM\\(balance\\)").WillReturnError(errors.New("agg failed"))
			},
			errSub: "agg failed",
		},
		{
			name: "deleted row query fails",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
				mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SUM\\(balance\\)").
					WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(3), int64(700)))
				mock.ExpectQuery("account_id = 2").WillReturnError(errors.New("deleted query failed"))
			},
			errSub: "deleted query failed",
		},
		{
			name: "result mismatch",
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 4))
				mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SUM\\(balance\\)").
					WillReturnRows(sqlmock.NewRows([]string{"count", "sum"}).AddRow(int64(4), int64(900)))
				mock.ExpectQuery("account_id = 2").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
			},
			errSub: "delete apply result mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := newLocalE2ESQLMock(t)
			defer db.Close()
			tt.mock(mock)
			result := (&caseRunner{cfg: localE2ETestConfig(), db: db}).mergeOnReadDeleteCase(context.Background())
			if result.Status != "failed" || !strings.Contains(result.Error, tt.errSub) {
				t.Fatalf("expected failure containing %q, got %+v", tt.errSub, result)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
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
		case strings.HasSuffix(r.URL.Path, "/api/v1/trees/tree/main") && r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"type":"BRANCH","name":"main","hash":"main-hash-1"}`))
		case strings.HasSuffix(r.URL.Path, "/api/v1/trees/tree") && r.Method == http.MethodPost:
			var body struct {
				Type string `json:"type"`
				Name string `json:"name"`
				Hash string `json:"hash"`
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read Nessie branch body: %v", err)
			}
			if err := json.Unmarshal(data, &body); err != nil {
				t.Fatalf("decode Nessie branch body: %v body=%s", err, string(data))
			}
			if body.Type != "BRANCH" || !strings.HasPrefix(body.Name, "mo_e2e_t1_") || body.Hash != "main-hash-1" {
				t.Fatalf("unexpected Nessie branch body: %+v", body)
			}
			_, _ = w.Write(data)
		case strings.HasSuffix(r.URL.Path, "/v1/config"):
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"main"}}`))
		case strings.Contains(r.URL.Path, "/namespaces/ci_ns/tables/append_orders"):
			if r.Method == http.MethodPost {
				http.NotFound(w, r)
				return
			}
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
