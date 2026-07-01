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
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestIcebergSQLEngineEmbeddedDDLAndRedaction(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		db := openIcebergTestDB(t, c)
		defer db.Close()

		suffix := time.Now().UnixNano()
		catalogName := fmt.Sprintf("icecat_%d", suffix)
		databaseName := fmt.Sprintf("iceberg_it_%d", suffix)
		tableName := fmt.Sprintf("gold_orders_%d", suffix)

		mustExec(t, db, fmt.Sprintf("create database %s", databaseName))
		defer db.Exec(fmt.Sprintf("drop database if exists %s", databaseName))
		defer db.Exec(fmt.Sprintf("drop iceberg catalog if exists %s", catalogName))

		mustExec(t, db, fmt.Sprintf(
			"create iceberg catalog %s with ('type'='rest','uri'='https://catalog.example/v1','warehouse'='s3://warehouse/gold','token_secret'='secret://catalog/token')",
			catalogName,
		))
		require.True(t, queryContains(t, db, "show iceberg catalogs", catalogName))

		mustExec(t, db, fmt.Sprintf(
			"alter iceberg catalog %s set ('uri'='https://catalog2.example/v1','token_secret'='secret://catalog/token2')",
			catalogName,
		))
		require.True(t, queryContains(t, db, "show iceberg catalogs", "https://catalog2.example/v1"))
		require.False(t, queryContains(t, db, "show iceberg catalogs", "catalog/token2"))

		mustExec(t, db, fmt.Sprintf(
			"create external table %s.%s (id int) engine = iceberg with ('catalog'='%s','namespace'='sales','table'='orders','ref'='main','read_mode'='append_only','write_mode'='read_only')",
			databaseName, tableName, catalogName,
		))
		defer db.Exec(fmt.Sprintf("drop table if exists %s.%s", databaseName, tableName))

		require.True(t, queryContains(t, db, fmt.Sprintf("show iceberg namespaces from %s", catalogName), "sales"))
		require.True(t, queryContains(t, db, fmt.Sprintf("show iceberg tables from %s.sales", catalogName), "orders"))

		createSQL := showCreateTable(t, db, databaseName, tableName)
		require.Contains(t, createSQL, "ENGINE = ICEBERG")
		require.Contains(t, createSQL, "\"catalog\" = '"+catalogName+"'")
		require.Contains(t, createSQL, "\"namespace\" = 'sales'")
		require.Contains(t, createSQL, "\"table\" = 'orders'")
		require.NotContains(t, createSQL, "secret://")

		mustExec(t, db, fmt.Sprintf("drop table %s.%s", databaseName, tableName))
		mustExec(t, db, fmt.Sprintf("drop iceberg catalog %s", catalogName))
	})
}

func TestIcebergSQLEngineEmbeddedRejectsInlineCatalogSecret(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		db := openIcebergTestDB(t, c)
		defer db.Close()

		_, err := db.Exec("create iceberg catalog bad_inline_secret with ('type'='rest','uri'='https://catalog.example/v1','token_secret'='Bearer raw-token')")
		require.Error(t, err)
		require.Contains(t, err.Error(), "secret://")
		require.NotContains(t, err.Error(), "raw-token")
	})
}

func openIcebergTestDB(t *testing.T, c Cluster) *sql.DB {
	t.Helper()
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn0.GetServiceConfig().CN.Frontend.Port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	return db
}

func mustExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	_, err := db.Exec(stmt)
	require.NoError(t, err, stmt)
}

func queryContains(t *testing.T, db *sql.DB, query, needle string) bool {
	t.Helper()
	rows, err := db.Query(query)
	require.NoError(t, err, query)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	values := make([]sql.NullString, len(columns))
	scan := make([]any, len(columns))
	for i := range values {
		scan[i] = &values[i]
	}
	for rows.Next() {
		require.NoError(t, rows.Scan(scan...))
		for _, value := range values {
			if value.Valid && strings.Contains(value.String, needle) {
				return true
			}
		}
	}
	require.NoError(t, rows.Err())
	return false
}

func showCreateTable(t *testing.T, db *sql.DB, databaseName, tableName string) string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("show create table %s.%s", databaseName, tableName))
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	values := make([]sql.NullString, len(columns))
	scan := make([]any, len(columns))
	for i := range values {
		scan[i] = &values[i]
	}
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(scan...))
	require.NoError(t, rows.Err())
	for _, value := range values {
		if value.Valid && strings.Contains(value.String, "CREATE EXTERNAL TABLE") {
			return value.String
		}
	}
	t.Fatalf("SHOW CREATE TABLE did not return create SQL: %+v", values)
	return ""
}
