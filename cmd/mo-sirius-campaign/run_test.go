// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func publicRunFixture(t *testing.T) (configuration, runSpec, string) {
	t.Helper()
	root := t.TempDir()
	scratch := filepath.Join(root, "scratch")
	require.NoError(t, os.Mkdir(scratch, 0700))
	statement := "select amount from t"
	require.NoError(t, os.WriteFile(filepath.Join(root, "q1.sql"), []byte(statement), 0600))
	digest := sha256.Sum256([]byte(statement))
	return configuration{DSN: "runner:private@tcp(127.0.0.1:3306)/", QueryDirectory: root, Scratch: scratch,
			Databases: map[string]string{"1": "tpch"}, Session: []string{"set time_zone='+00:00'"}, TimeoutSeconds: 1},
		runSpec{Sequence: 1, Route: "mo-native", Query: 1, QuerySHA256: hex.EncodeToString(digest[:]), ScaleFactor: 1, SpoolBudget: 1 << 20}, statement
}

func TestPublicRunOwnsOneConnectionAndPublishesOnlyCompleteSpools(t *testing.T) {
	failed := errors.New("owned SQL failure")
	for _, scenario := range []string{"native", "embedded", "query-error", "late-error", "quota", "identity-error"} {
		t.Run(scenario, func(t *testing.T) {
			cfg, spec, statement := publicRunFixture(t)
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectExec("use `tpch`").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(cfg.Session[0]).WillReturnResult(sqlmock.NewResult(0, 0))
			if scenario == "embedded" {
				spec.Route, statement = "embedded-mo", "/*+ SIDECAR GPU */ "+statement
			}
			query := mock.ExpectQuery(statement)
			if scenario == "query-error" {
				query.WillReturnError(failed)
			} else {
				data := sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("amount").OfType("DECIMAL", "").WithPrecisionAndScale(6, 2)).AddRow("1.20")
				if scenario == "late-error" {
					data.AddRow("2.30").RowError(1, failed)
				}
				query.WillReturnRows(data).RowsWillBeClosed()
				if scenario == "quota" {
					spec.SpoolBudget = 1
				}
				if scenario != "late-error" && scenario != "quota" {
					identity := mock.ExpectQuery("select last_query_id()")
					if scenario == "identity-error" {
						identity.WillReturnError(failed)
					} else {
						identity.WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(strings.Repeat("a", 32)))
					}
				}
			}
			mock.ExpectClose()
			out, err := runWithOpener(t.Context(), cfg, spec, func(driver, dsn string) (*sql.DB, error) {
				require.Equal(t, "mysql", driver)
				require.Equal(t, cfg.DSN, dsn)
				return db, nil
			})
			entries, readErr := os.ReadDir(cfg.Scratch)
			require.NoError(t, readErr)
			if scenario == "native" || scenario == "embedded" {
				require.NoError(t, err)
				require.Len(t, entries, 1)
				require.Equal(t, 2, out.SchemaVersion)
				require.Equal(t, spec.Route, out.Route)
				require.Equal(t, uint64(1), out.Result.Rows)
				require.Equal(t, strings.Repeat("a", 32), out.QueryID)
				require.NotNil(t, out.FirstRow)
				path := filepath.Join(cfg.Scratch, out.Result.Spool)
				data, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				require.Contains(t, string(data), `"value":"120"`)
				digest := sha256.Sum256(data)
				require.Equal(t, hex.EncodeToString(digest[:]), out.Result.SHA256)
				info, statErr := os.Stat(path)
				require.NoError(t, statErr)
				require.Equal(t, os.FileMode(0600), info.Mode().Perm())
			} else {
				require.Error(t, err)
				require.Empty(t, entries, "failed queries cannot retain a partial spool")
				require.Zero(t, out.SchemaVersion)
				if scenario != "quota" {
					require.ErrorIs(t, err, failed)
				}
			}
			require.Equal(t, 1, db.Stats().MaxOpenConnections)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPublicRunRejectsImmutableInputsBeforeOpeningSQL(t *testing.T) {
	for _, scenario := range []string{"sequence", "query", "route", "budget", "missing-query", "digest", "database", "dsn", "driver-options"} {
		t.Run(scenario, func(t *testing.T) {
			cfg, spec, _ := publicRunFixture(t)
			switch scenario {
			case "sequence":
				spec.Sequence = 0
			case "query":
				spec.Query = 23
			case "route":
				spec.Route = "unknown"
			case "budget":
				spec.SpoolBudget = (4 << 30) + 1
			case "missing-query":
				require.NoError(t, os.Remove(filepath.Join(cfg.QueryDirectory, "q1.sql")))
			case "digest":
				spec.QuerySHA256 = strings.Repeat("0", 64)
			case "database":
				cfg.Databases["1"] = "unsafe`database"
			case "dsn":
				cfg.DSN = "not-a-dsn"
			case "driver-options":
				cfg.DSN += "?multiStatements=true"
			}
			_, err := runWithOpener(t.Context(), cfg, spec, func(string, string) (*sql.DB, error) {
				t.Fatal("invalid input reached database acquisition")
				return nil, nil
			})
			require.Error(t, err)
		})
	}
}

func TestPrivateConfigurationRequiresBoundedRegularPrivateInput(t *testing.T) {
	cfg, _, _ := publicRunFixture(t)
	path := filepath.Join(t.TempDir(), "runner.json")
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	actual, err := readConfiguration(path)
	require.NoError(t, err)
	require.Equal(t, cfg, actual)
	for _, scenario := range []string{"public", "oversize", "malformed", "incomplete", "symlink", "missing"} {
		t.Run(scenario, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "input")
			require.NoError(t, os.WriteFile(name, data, 0600))
			switch scenario {
			case "public":
				require.NoError(t, os.Chmod(name, 0644))
			case "oversize":
				require.NoError(t, os.Truncate(name, maxRowBytes+1))
			case "malformed":
				require.NoError(t, os.WriteFile(name, []byte("{"), 0600))
			case "incomplete":
				require.NoError(t, os.WriteFile(name, []byte("{}"), 0600))
			case "symlink":
				require.NoError(t, os.Remove(name))
				require.NoError(t, os.Symlink(path, name))
			case "missing":
				require.NoError(t, os.Remove(name))
			}
			_, err := readConfiguration(name)
			require.Error(t, err)
		})
	}
}

func TestPublicRunAcquisitionFailurePreservesOwner(t *testing.T) {
	cfg, spec, _ := publicRunFixture(t)
	failed := errors.New("open owner")
	_, err := runWithOpener(context.Background(), cfg, spec, func(string, string) (*sql.DB, error) { return nil, failed })
	require.ErrorIs(t, err, failed)
}
