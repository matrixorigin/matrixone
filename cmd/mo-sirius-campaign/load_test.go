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
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

var datasetTestTables = []string{"nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"}

func datasetVerificationRows(amount string) *sqlmock.Rows {
	return sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("amount").OfType("DECIMAL", "").Nullable(true).WithPrecisionAndScale(6, 2),
		sqlmock.NewColumn("text").OfType("VARCHAR", "").Nullable(true),
	).AddRow(amount, "quoted|text").AddRow(nil, "")
}

func datasetFixture(t *testing.T) (string, datasetManifest) {
	t.Helper()
	root := t.TempDir()
	ddl := filepath.Join(root, "schema.sql")
	require.NoError(t, os.WriteFile(ddl, []byte("create table fixture(a decimal(6,2),b varchar(32));"), 0600))
	digest, err := fileDigest(ddl)
	require.NoError(t, err)
	manifest := datasetManifest{Version: 2, Scale: 1, DDL: digest, Digest: "frozen-dataset", Tables: make(map[string]datasetTable)}
	for _, table := range datasetTestTables {
		name := table + ".csv"
		path := filepath.Join(root, name)
		require.NoError(t, os.WriteFile(path, []byte("1.20|\"quoted|text\"\n\\N|\"\"\n"), 0600))
		digest, err := fileDigest(path)
		require.NoError(t, err)
		manifest.Tables[table] = datasetTable{File: name, SHA256: digest, Rows: 2}
	}
	return filepath.Join(root, "manifest.json"), manifest
}

func writeDatasetManifest(t *testing.T, path string, manifest datasetManifest) {
	t.Helper()
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
}

func TestDatasetProvisioningVerifiesEverySourceAndTypedLoadedRow(t *testing.T) {
	failed := errors.New("load operation owner")
	for _, scenario := range []string{"success", "source-checksum", "traversal", "load-error", "same-count-different-value"} {
		t.Run(scenario, func(t *testing.T) {
			path, manifest := datasetFixture(t)
			last := manifest.Tables["lineitem"]
			if scenario == "source-checksum" {
				last.SHA256 = "wrong"
			}
			if scenario == "traversal" {
				last.File = "../outside.csv"
			}
			manifest.Tables["lineitem"] = last
			writeDatasetManifest(t, path, manifest)
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectExec("create database `tpch`").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("use `tpch`").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("create table fixture(a decimal(6,2),b varchar(32))").WillReturnResult(sqlmock.NewResult(0, 0))
			for _, table := range datasetTestTables {
				if table == "lineitem" && (scenario == "source-checksum" || scenario == "traversal") {
					break
				}
				file := filepath.Join(filepath.Dir(path), manifest.Tables[table].File)
				statement := "load data local infile '" + file + "' into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\\n'"
				load := mock.ExpectExec(statement)
				if table == "lineitem" && scenario == "load-error" {
					load.WillReturnError(failed)
					break
				}
				load.WillReturnResult(sqlmock.NewResult(0, 2))
				amount := "1.20"
				if table == "lineitem" && scenario == "same-count-different-value" {
					amount = "2.30"
				}
				mock.ExpectQuery("select * from " + table).WillReturnRows(datasetVerificationRows(amount)).RowsWillBeClosed()
			}
			mock.ExpectClose()
			result, err := loadDatasetWithOpener(t.Context(), configuration{DSN: "private"}, path, "tpch", func(driver, dsn string) (*sql.DB, error) {
				require.Equal(t, "mysql", driver)
				require.Equal(t, "private", dsn)
				return db, nil
			})
			if scenario == "success" {
				require.NoError(t, err)
				require.Equal(t, manifest.Digest, result["dataset_sha256"])
				tables := result["tables"].(map[string]any)
				require.Len(t, tables, 8)
				for _, value := range tables {
					record := value.(map[string]any)
					require.Equal(t, uint64(2), record["row_count"])
					require.Len(t, record["loaded_data_sha256"], 64)
				}
			} else {
				require.Error(t, err)
				require.Nil(t, result)
				if scenario == "load-error" {
					require.ErrorIs(t, err, failed)
				}
				if scenario == "same-count-different-value" {
					require.ErrorContains(t, err, "semantic identity")
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDatasetManifestAdmissionRejectsUntrustedInputsBeforeSQL(t *testing.T) {
	for _, scenario := range []string{"database", "public-file", "malformed", "version", "table-count", "ddl"} {
		t.Run(scenario, func(t *testing.T) {
			path, manifest := datasetFixture(t)
			database := "tpch"
			switch scenario {
			case "database":
				database = "bad`name"
			case "version":
				manifest.Version = 1
			case "table-count":
				delete(manifest.Tables, "lineitem")
			case "ddl":
				manifest.DDL = "wrong"
			}
			writeDatasetManifest(t, path, manifest)
			if scenario == "public-file" {
				require.NoError(t, os.Chmod(path, 0644))
			}
			if scenario == "malformed" {
				require.NoError(t, os.WriteFile(path, []byte("{"), 0600))
			}
			_, err := loadDatasetWithOpener(t.Context(), configuration{}, path, database, func(string, string) (*sql.DB, error) {
				t.Fatal("invalid manifest reached SQL acquisition")
				return nil, nil
			})
			require.Error(t, err)
		})
	}
}

func TestDatasetRowIdentityPreservesMultiplicityAndIgnoresOrder(t *testing.T) {
	a, b := []cell{{Type: "varchar", Value: "a"}}, []cell{{Type: "varchar", Value: "b"}}
	var first, reversed, duplicate rowIdentity
	for _, row := range [][]cell{a, b} {
		require.NoError(t, first.add(row))
	}
	for _, row := range [][]cell{b, a} {
		require.NoError(t, reversed.add(row))
	}
	for _, row := range [][]cell{a, a} {
		require.NoError(t, duplicate.add(row))
	}
	require.Equal(t, first.digest(), reversed.digest())
	require.NotEqual(t, first.digest(), duplicate.digest())
}

func TestDatasetVerificationCancelsUnreadRowsOnEncodingFailure(t *testing.T) {
	data := &cancellationRows{payload: "\xff"}
	db := sql.OpenDB(cancellationConnector{rows: data})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	connection, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	_, err = verifyDatasetTable(ctx, connection, "fixture", filepath.Join(t.TempDir(), "not-read.csv"), 2)
	require.ErrorContains(t, err, "UTF-8")
	require.Equal(t, 1, data.next)
	require.True(t, data.canceled.Load(), "failed provisioning scan cancels before driver close")
	require.EqualValues(t, 1, data.closed.Load())
}
