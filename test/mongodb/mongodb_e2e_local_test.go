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
	"os/exec"
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

func TestConnectorE2EWorkflowsUseRevisionPinnedDockerImage(t *testing.T) {
	repoRoot := mongoDBTestRepoRoot(t)
	for _, name := range []string{"mongodb-connector.yml", "iceberg-connector.yml"} {
		data, err := os.ReadFile(filepath.Join(repoRoot, ".github", "workflows", name))
		require.NoError(t, err)
		workflow := string(data)
		require.NotContains(t, workflow, "actions/setup-go", "%s must not download Go modules on the host", name)
		require.NotContains(t, workflow, "\n  pull_request:", "%s must not run for every PR", name)
		require.NotContains(t, workflow, "if: ${{ false }}", "%s must remain manually runnable", name)
		require.NotContains(t, workflow, "github.event.pull_request")
		require.Contains(t, workflow, "workflow_dispatch:")
		require.Contains(t, workflow, "MO_CONNECTOR_CI_REVISION: ${{ github.sha }}")
		require.Contains(t, workflow, "uses: ./.github/actions/build-connector-ci")
		require.Contains(t, workflow, "ignore-cache-export-error: 'true'")
	}
	buildAction, err := os.ReadFile(filepath.Join(repoRoot, ".github", "actions", "build-connector-ci", "action.yml"))
	require.NoError(t, err)
	sharedBuild := string(buildAction)
	require.Contains(t, sharedBuild, "target: connector-ci")
	require.Contains(t, sharedBuild, "cache-from: |")
	require.Contains(t, sharedBuild, "type=gha,scope=connector-ci-")
	require.Contains(t, sharedBuild, "scope=matrixone-native-")
	require.Contains(t, sharedBuild, "cache-to: type=gha,mode=max,scope=connector-ci-")
	require.Contains(t, sharedBuild, "ignore-error=${{ inputs['ignore-cache-export-error'] }}")
	require.Contains(t, sharedBuild, "Retry connector CI image build")
	require.Contains(t, sharedBuild, "GO_MOD_DOWNLOAD_TIMEOUT=10m")
	require.Contains(t, sharedBuild, "python3 pkg/iceberg/metadata/testdata/generate_golden_vectors.py --check")

	cacheWorkflow, err := os.ReadFile(filepath.Join(repoRoot, ".github", "workflows", "connector-ci-cache.yml"))
	require.NoError(t, err)
	cacheWarmup := string(cacheWorkflow)
	require.Contains(t, cacheWarmup, "branches: [main]")
	require.Contains(t, cacheWarmup, "uses: ./.github/actions/build-connector-ci")
	require.NotContains(t, cacheWarmup, "ignore-cache-export-error: 'true'")

	dockerfile, err := os.ReadFile(filepath.Join(repoRoot, "optools", "images", "Dockerfile"))
	require.NoError(t, err)
	imageContract := string(dockerfile)
	require.NotContains(t, imageContract, "python3", "the pinned builder image does not contain Python")
	for _, required := range []string{
		"FROM builder AS connector-builder",
		"FROM build-base AS connector-modules",
		"COPY --from=connector-modules /go/pkg/mod /go/pkg/mod",
		"FROM runtime-base AS connector-ci",
		"org.opencontainers.image.revision",
		"/connector-bin/mongodb-e2e",
		"/connector-bin/iceberg-e2e",
		"go test -tags iceberggo",
		"ENV CGO_LDFLAGS=",
		"id=matrixone-go-modules",
		"sharing=locked",
		"cp -a /tmp/gomod-cache/. /go/pkg/mod/",
		"ARG GO_MOD_DOWNLOAD_TIMEOUT=0",
		"timeout \"$GO_MOD_DOWNLOAD_TIMEOUT\" env GOMODCACHE=/tmp/gomod-cache go mod download",
	} {
		require.Contains(t, imageContract, required)
	}

	for _, tc := range []struct {
		path     string
		required []string
	}{
		{
			path: "optools/mongodb_ci.bash",
			required: []string{
				"--user \"$container_user\"",
				"--entrypoint /mo-service",
				"--entrypoint /connector-bin/mongodb-e2e",
				"--mongo-host \"mongo:27017\"",
			},
		},
		{
			path: "optools/iceberg_ci.bash",
			required: []string{
				"--user \"$container_user\"",
				"--entrypoint /mo-service",
				"--entrypoint /connector-bin/iceberg-e2e",
				"http://nessie:19120/iceberg",
				"http://minio:9000",
				"--object-endpoint \"${MO_ICEBERG_E2E_OBJECT_ENDPOINT:-minio}\"",
			},
		},
	} {
		data, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(tc.path)))
		require.NoError(t, err)
		for _, required := range tc.required {
			require.Contains(t, string(data), required, "%s is missing its container contract", tc.path)
		}
		require.Equal(t, 2, strings.Count(string(data), `--user "$container_user"`),
			"%s must run both MatrixOne and its E2E runner as the host user", tc.path)
	}

	compose, err := os.ReadFile(filepath.Join(repoRoot, "etc", "launch-minio-local", "docker-compose.yml"))
	require.NoError(t, err)
	require.NotContains(t, string(compose), "projectnessie/nessie:latest")
	require.Contains(t, string(compose), "NESSIE_EXTERNAL_ENDPOINT:-http://127.0.0.1:9000")
}

func TestConnectorContainerConfigGenerators(t *testing.T) {
	repoRoot := mongoDBTestRepoRoot(t)

	t.Run("mongodb", func(t *testing.T) {
		tmpDir := t.TempDir()
		cmd := exec.Command("bash", "-c", `source "$1"; TMP_DIR="$2"; MO_PORT=6001; generate_mo_config`,
			"connector-config-test", filepath.Join(repoRoot, "optools", "mongodb_ci.bash"), tmpDir)
		output, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "generate MongoDB container config: %s", output)

		for _, name := range []string{"log.toml", "tn.toml", "cn.toml"} {
			data, err := os.ReadFile(filepath.Join(tmpDir, "mo-config", name))
			require.NoError(t, err)
			require.Contains(t, string(data), filepath.Join(tmpDir, "mo-data"))
		}
		cn, err := os.ReadFile(filepath.Join(tmpDir, "mo-config", "cn.toml"))
		require.NoError(t, err)
		require.Contains(t, string(cn), "[cn.frontend]\nport = 6001")
		require.Contains(t, string(cn), "[cn.frontend.mongodb]\n")
	})

	t.Run("iceberg", func(t *testing.T) {
		tmpDir := t.TempDir()
		cmd := exec.Command("bash", "-c", `source "$1"; ICEBERG_E2E_TMP_DIR="$2"; generate_iceberg_container_config`,
			"connector-config-test", filepath.Join(repoRoot, "optools", "iceberg_ci.bash"), tmpDir)
		output, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "generate Iceberg container config: %s", output)

		for _, name := range []string{"log.toml", "tn.toml", "cn.toml"} {
			data, err := os.ReadFile(filepath.Join(tmpDir, "mo-config", name))
			require.NoError(t, err)
			config := string(data)
			require.Contains(t, config, filepath.Join(tmpDir, "mo-data"))
			require.NotContains(t, config, "http://127.0.0.1:9000")
			require.Contains(t, config, "http://minio:9000")
		}
		launch, err := os.ReadFile(filepath.Join(tmpDir, "mo-config", "launch.toml"))
		require.NoError(t, err)
		for _, name := range []string{"log.toml", "tn.toml", "cn.toml"} {
			require.Contains(t, string(launch), filepath.Join(tmpDir, "mo-config", name))
		}
	})
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

	for range 6 {
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery("show create table").WillReturnRows(sqlmock.NewRows([]string{"table", "ddl"}).AddRow(
		"events", "CREATE EXTERNAL TABLE events (id CHAR(24) MONGODB_PATH '_id') ENGINE = MONGODB WITH ('connection'='mongodb_ci')"))
	expectMongoDBE2EScalar(mock, "5")
	fixtureRows := sqlmock.NewRows([]string{"id", "device_id", "site_id", "ts", "measurement", "source_batch"})
	for _, row := range manifest.Rows {
		require.Len(t, row, 6)
		fixtureRows.AddRow(row[0], row[1], row[2], row[3], row[4], row[5])
	}
	mock.ExpectQuery("select mongo_id").WillReturnRows(fixtureRows)
	expectMongoDBE2EScalar(mock, "3")
	expectMongoDBE2EScalar(mock, "3")
	expectMongoDBE2EScalar(mock, "2")
	expectMongoDBE2EScalar(mock, "1")
	expectMongoDBE2EScalar(mock, "2")
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
		"show-create-redaction-roundtrip",
		"scan-projection-pushdown-null-conversion",
		"compound-predicate-objectid-null-boundaries",
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
