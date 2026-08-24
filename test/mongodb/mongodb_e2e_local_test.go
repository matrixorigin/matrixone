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
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestMongoDBLocalE2ERunnerDoesNotImportKernelPackages(t *testing.T) {
	repoRoot := mongoDBTestRepoRoot(t)
	file, err := parser.ParseFile(
		token.NewFileSet(),
		filepath.Join(repoRoot, "test/mongodb/mongodb_e2e_local.go"),
		nil,
		parser.ImportsOnly,
	)
	require.NoError(t, err)
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		require.NoError(t, err)
		require.Falsef(t, strings.HasPrefix(path, "github.com/matrixorigin/matrixone/"),
			"standalone E2E runner must not import kernel package %s", path)
	}
}

func TestMongoDBLocalE2ERunContract(t *testing.T) {
	repoRoot := mongoDBTestRepoRoot(t)
	previous, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { require.NoError(t, os.Chdir(previous)) })

	manifest, err := loadFixtureManifest("test/mongodb/fixture_manifest.json")
	require.NoError(t, err)
	db, mock := newMongoDBE2ESQLMock(t)

	for range 8 {
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery("show mongodb connections").WillReturnRows(sqlmock.NewRows([]string{
		"name", "discovery_mode", "auth_mechanism", "tls_mode",
		"read_preference", "read_concern", "version", "disabled",
	}).
		AddRow("existing", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0).
		AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 3, 0))
	mock.ExpectQuery("show create table").WillReturnRows(sqlmock.NewRows([]string{"table", "ddl"}).AddRow(
		"events", "CREATE EXTERNAL TABLE events (id CHAR(24) MONGODB_PATH '_id') ENGINE = MONGODB WITH ('connection'='mongodb_ci')"))
	expectMongoDBE2EScalar(mock, `"text"`)
	expectMongoDBE2EScalar(mock, "2")
	expectMongoDBE2EScalar(mock, "1")
	expectMongoDBE2EScalar(mock, "4")
	expectMongoDBE2EScalar(mock, "0")
	expectMongoDBE2EScalar(mock, "5")
	fixtureRows := sqlmock.NewRows([]string{"id", "device_id", "site_id", "ts", "measurement", "source_batch"})
	for _, row := range manifest.Rows {
		require.Len(t, row, 6)
		fixtureRows.AddRow(row[0], row[1], row[2], row[3], row[4], row[5])
	}
	mock.ExpectQuery("select mongo_id").WillReturnRows(fixtureRows)
	expectMongoDBE2EScalar(mock, "3")
	expectMongoDBE2EScalar(mock, "3")
	mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
	expectMongoDBE2EScalar(mock, "1")
	expectMongoDBE2EScalar(mock, "1")
	mock.ExpectQuery("select payload_1").WillReturnError(errors.New("MongoDB decoded batch byte limit exceeded"))
	// A pre-canceled context is rejected by database/sql before it reaches the
	// driver, so no sqlmock expectation is consumed here.
	expectMongoDBE2EScalar(mock, "5")
	expectMongoDBE2EScalar(mock, "4")
	for range 3 {
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectBegin()
	mock.ExpectExec("replace into mongodb_ci.minute_aggregate").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("update mongodb_ci.ingest_watermark").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectMongoDBE2EScalar(mock, "2026-07-27 10:03:00")
	mock.ExpectExec("create external table mongodb_ci.events_strict").WillReturnResult(sqlmock.NewResult(0, 0))
	expectMongoDBE2EScalar(mock, "4")
	expectMongoDBE2EScalar(mock, "2026-07-27 10:03:00")
	mock.ExpectBegin()
	mock.ExpectExec("replace into mongodb_ci.minute_aggregate.*events_strict").WillReturnError(errors.New("strict conversion failed"))
	mock.ExpectRollback()
	expectMongoDBE2EScalar(mock, "4")
	expectMongoDBE2EScalar(mock, "2026-07-27 10:03:00")
	mock.ExpectBegin()
	mock.ExpectExec("replace into mongodb_ci.minute_aggregate").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("update mongodb_ci.ingest_watermark").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectMongoDBE2EScalar(mock, "4")
	mock.ExpectExec("alter mongodb connection mongodb_ci set").WillReturnResult(sqlmock.NewResult(0, 1))
	expectMongoDBE2EScalar(mock, "5")
	mock.ExpectExec("alter mongodb connection mongodb_ci disable").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("select count").WillReturnError(errors.New("MongoDB connection is disabled"))
	mock.ExpectExec("alter mongodb connection mongodb_ci enable").WillReturnResult(sqlmock.NewResult(0, 1))
	expectMongoDBE2EScalar(mock, "5")

	result := report{}
	require.NoError(t, run(t.Context(), db, "127.0.0.1:27017", &result))
	require.Equal(t, []string{
		"secret-backed-ddl",
		"show-connections-admin-metadata-redaction",
		"show-create-redaction-roundtrip",
		"json-relaxed-extended-conversion",
		"fixed-binary-padding",
		"scan-projection-pushdown-null-conversion",
		"insert-select-primary-key-target",
		"low-precision-temporal-residual",
		"decoded-vector-budget-enforced",
		"multi-batch-cancel-recovery",
		"mongoscan-timewin-gapfill",
		"atomic-aggregate-watermark",
		"conversion-error-atomic-rollback",
		"bounded-idempotent-replay",
		"credential-generation-rotation",
		"connection-disable-enable",
	}, result.Cases)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMongoDBLocalE2ERunPropagatesRelaxedJSONQueryFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		failedQuery string
	}{
		{name: "payload", failedQuery: "json_unquote"},
		{name: "array", failedQuery: "json_contains"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot := mongoDBTestRepoRoot(t)
			previous, err := os.Getwd()
			require.NoError(t, err)
			require.NoError(t, os.Chdir(repoRoot))
			t.Cleanup(func() { require.NoError(t, os.Chdir(previous)) })

			db, mock := newMongoDBE2ESQLMock(t)
			for range 8 {
				mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
			}
			mock.ExpectQuery("show mongodb connections").WillReturnRows(sqlmock.NewRows([]string{
				"name", "discovery_mode", "auth_mechanism", "tls_mode",
				"read_preference", "read_concern", "version", "disabled",
			}).AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 3, 0))
			mock.ExpectQuery("show create table").WillReturnRows(sqlmock.NewRows([]string{"table", "ddl"}).AddRow(
				"events", "CREATE EXTERNAL TABLE events (id CHAR(24) MONGODB_PATH '_id') ENGINE = MONGODB WITH ('connection'='mongodb_ci')"))
			expectMongoDBE2EScalar(mock, `"text"`)
			if tc.failedQuery == "json_contains" {
				expectMongoDBE2EScalar(mock, "2")
			}
			mock.ExpectQuery(tc.failedQuery).WillReturnError(errors.New("relaxed JSON query failed"))

			err = run(t.Context(), db, "127.0.0.1:27017", &report{})
			require.ErrorContains(t, err, "relaxed JSON query failed")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMongoDBPrimaryKeyInsertSelect(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(sqlmock.Sqlmock)
		wantErr string
	}{
		{
			name: "create target fails",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("create table mongodb_ci.events_insert_target").
					WillReturnError(errors.New("catalog unavailable"))
			},
			wantErr: "create primary-key insert target: catalog unavailable",
		},
		{
			name: "insert select fails",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").
					WillReturnError(errors.New("source scan failed"))
			},
			wantErr: "insert-select into primary-key target: source scan failed",
		},
		{
			name: "inserted row count is validated",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("select count\\(\\*\\) from mongodb_ci.events_insert_target").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("0"))
			},
			wantErr: "expected \"1\"",
		},
		{
			name: "single source row is inserted",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("select count\\(\\*\\) from mongodb_ci.events_insert_target").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("1"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			test.prepare(mock)
			err := verifyPrimaryKeyInsertSelect(t.Context(), db)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMongoDBLocalE2EHelpers(t *testing.T) {
	t.Run("wait succeeds", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectPing()
		require.NoError(t, waitForMO(t.Context(), db))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("wait observes cancellation", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectPing().WillDelayFor(10 * time.Millisecond).WillReturnError(errors.New("not ready"))
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		require.ErrorIs(t, waitForMO(ctx, db), context.DeadlineExceeded)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("fixture manifest failures", func(t *testing.T) {
		_, err := loadFixtureManifest(filepath.Join(t.TempDir(), "missing.json"))
		require.ErrorContains(t, err, "read MongoDB fixture manifest")
		invalid := filepath.Join(t.TempDir(), "invalid.json")
		require.NoError(t, os.WriteFile(invalid, []byte("{"), 0o600))
		_, err = loadFixtureManifest(invalid)
		require.ErrorContains(t, err, "decode MongoDB fixture manifest")
		empty := filepath.Join(t.TempDir(), "empty.json")
		require.NoError(t, os.WriteFile(empty, []byte(`{"rows":[]}`), 0o600))
		_, err = loadFixtureManifest(empty)
		require.ErrorContains(t, err, "has no rows")
	})

	t.Run("row comparison failures", func(t *testing.T) {
		db, mock := newMongoDBE2ESQLMock(t)
		mock.ExpectQuery("query-error").WillReturnError(errors.New("source offline"))
		require.ErrorContains(t, expectRows(t.Context(), db, "query-error", nil), "source offline")
		mock.ExpectQuery("scan-error").WillReturnRows(sqlmock.NewRows([]string{"only"}).AddRow("value"))
		require.Error(t, expectRows(t.Context(), db, "scan-error", nil))
		mock.ExpectQuery("row-error").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b", "c", "d", "e", "f"}).
				AddRow("1", "2", "3", "4", "5", "6").
				AddRow("1", "2", "3", "4", "5", "6").
				RowError(1, errors.New("getMore failed")))
		require.ErrorContains(t, expectRows(t.Context(), db, "row-error", nil), "getMore failed")
		mock.ExpectQuery("mismatch").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c", "d", "e", "f"}))
		require.ErrorContains(t, expectRows(t.Context(), db, "mismatch", [][]string{{"expected"}}), "fixture result mismatch")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("show create validation", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			row  *sqlmock.Rows
			err  error
		}{
			{name: "query error", err: errors.New("catalog offline")},
			{name: "incomplete", row: sqlmock.NewRows([]string{"table", "ddl"}).AddRow("raw", "CREATE TABLE raw(a int)")},
			{name: "secret leak", row: sqlmock.NewRows([]string{"table", "ddl"}).AddRow("raw", "ENGINE = MONGODB MONGODB_PATH connection secret://env/key")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, mock := newMongoDBE2ESQLMock(t)
				expectation := mock.ExpectQuery("show create table")
				if tc.err != nil {
					expectation.WillReturnError(tc.err)
				} else {
					expectation.WillReturnRows(tc.row)
				}
				require.Error(t, verifyShowCreate(t.Context(), db))
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("show connections validation", func(t *testing.T) {
		columns := []string{
			"name", "discovery_mode", "auth_mechanism", "tls_mode",
			"read_preference", "read_concern", "version", "disabled",
		}
		tests := []struct {
			name string
			rows *sqlmock.Rows
			err  error
			want string
		}{
			{name: "query error", err: errors.New("catalog offline"), want: "catalog offline"},
			{name: "unexpected columns", rows: sqlmock.NewRows([]string{"name"}).AddRow("mongodb_ci"), want: "unexpected columns"},
			{name: "metadata mismatch", rows: sqlmock.NewRows(columns).AddRow("mongodb_ci", "srv", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0), want: "metadata mismatch"},
			{name: "invalid version", rows: sqlmock.NewRows(columns).AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 0, 0), want: "invalid version"},
			{name: "row error", rows: sqlmock.NewRows(columns).AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0).RowError(0, errors.New("getMore failed")), want: "getMore failed"},
			{name: "connection omitted", rows: sqlmock.NewRows(columns).AddRow("existing", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0), want: "omitted mongodb_ci"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				db, mock := newMongoDBE2ESQLMock(t)
				expectation := mock.ExpectQuery("show mongodb connections")
				if tc.err != nil {
					expectation.WillReturnError(tc.err)
				} else {
					expectation.WillReturnRows(tc.rows)
				}
				require.ErrorContains(t, verifyShowMongoDBConnections(t.Context(), db), tc.want)
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("scalar and expected failure", func(t *testing.T) {
		db, mock := newMongoDBE2ESQLMock(t)
		mock.ExpectQuery("scalar-error").WillReturnError(errors.New("query failed"))
		require.ErrorContains(t, expectScalar(t.Context(), db, "scalar-error", "1"), "query failed")
		mock.ExpectQuery("scalar-mismatch").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("2"))
		require.ErrorContains(t, expectScalar(t.Context(), db, "scalar-mismatch", "1"), "expected")
		mock.ExpectQuery("unexpected-success").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("1"))
		require.ErrorContains(t, expectQueryFailure(t.Context(), db, "unexpected-success", ""), "unexpectedly succeeded")
		mock.ExpectQuery("wrong-error").WillReturnError(errors.New("different"))
		require.ErrorContains(t, expectQueryFailure(t.Context(), db, "wrong-error", "disabled"), "without")
		mock.ExpectQuery("expected-error").WillReturnError(errors.New("connection disabled"))
		require.NoError(t, expectQueryFailure(t.Context(), db, "expected-error", "DISABLED"))
		mock.ExpectQuery("unexpected-statement-success").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("1"))
		require.ErrorContains(t, expectStatementRejected(t.Context(), db, "unexpected-statement-success", "privilege"), "unexpectedly succeeded")
		mock.ExpectQuery("statement-row-error").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("1").RowError(0, errors.New("read failed")))
		require.ErrorContains(t, expectStatementRejected(t.Context(), db, "statement-row-error", "privilege"), "failed while reading rows")
		mock.ExpectQuery("wrong-statement-error").WillReturnError(errors.New("network offline"))
		require.ErrorContains(t, expectStatementRejected(t.Context(), db, "wrong-statement-error", "privilege"), "without")
		mock.ExpectQuery("denied-statement").WillReturnError(errors.New("do not have privilege to execute the statement"))
		require.NoError(t, expectStatementRejected(t.Context(), db, "denied-statement", "PRIVILEGE"))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("redaction and report", func(t *testing.T) {
		require.Equal(t, "<redacted MongoDB DDL>", redact("CREDENTIAL_SECRET_REF='secret'"))
		require.Equal(t, "select 1", redact("select 1"))
		dir := t.TempDir()
		require.NoError(t, writeReport(dir, report{Status: "passed", Cases: []string{"scan"}}))
		data, err := os.ReadFile(filepath.Join(dir, "report.json"))
		require.NoError(t, err)
		require.Contains(t, string(data), `"status": "passed"`)
		summary, err := os.ReadFile(filepath.Join(dir, "summary.md"))
		require.NoError(t, err)
		require.Contains(t, string(summary), "Status: **passed**")
		fileParent := filepath.Join(t.TempDir(), "not-a-directory")
		require.NoError(t, os.WriteFile(fileParent, []byte("x"), 0o600))
		require.Error(t, writeReport(filepath.Join(fileParent, "child"), report{}))
	})
}

func mongoDBTestRepoRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func newMongoDBE2ESQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func expectMongoDBE2EScalar(mock sqlmock.Sqlmock, value string) {
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(value))
}

func TestMongoDBLocalE2ERedactionIsCaseInsensitive(t *testing.T) {
	require.True(t, strings.HasPrefix(redact("CrEdEnTiAl_SeCrEt_ReF=x"), "<redacted"))
}
