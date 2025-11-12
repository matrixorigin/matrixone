// Copyright 2021 - 2024 Matrix Origin
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

package dml

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestDeleteAndSelect(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			table := "debug"

			res, err := exec.Exec(
				ctx,
				"create database "+db,
				executor.Options{},
			)
			require.NoError(t, err)
			res.Close()

			res, err = exec.Exec(
				ctx,
				"create table "+table+" (a varchar primary key, b varchar)",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			//insert 3 blocks into t;
			res, err = exec.Exec(
				ctx,
				"insert into "+table+" select *, * from generate_series(1,24576)g",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(false)
			//select * from t where a > 24500;
			res, err = exec.Exec(
				ctx,
				"select * from "+table+" where a > 24500",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			//res, err = exec.Exec(
			//	ctx,
			//	"delete from "+table+" where a > 3",
			//	executor.Options{}.WithDatabase(db),
			//)
			//require.NoError(t, err)
			//res.Close()

			//select b from t2 where a between 1 and 3 order by b asc;
			//res, err = exec.Exec(
			//	ctx,
			//	"select b from "+table+" where a between 1 and 3 order by b asc",
			//	executor.Options{}.WithDatabase(db),
			//)
			//require.NoError(t, err)
			//rows := 0
			//for _, b := range res.Batches {
			//	rows += b.RowCount()
			//}
			//require.Equal(t, 3, rows)
			//res.Close()
		},
	)
}

func TestDataBranchDiffAsFile(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*240)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn1.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
			sqlDB, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer sqlDB.Close()

			t.Log("single primary key diff with base snapshot")
			runSinglePKWithBase(t, ctx, sqlDB)

			t.Log("multi primary key diff with base snapshot")
			runMultiPKWithBase(t, ctx, sqlDB)

			t.Log("single primary key diff without branch base relationship")
			runSinglePKNoBase(t, ctx, sqlDB)

			t.Log("multi primary key diff without branch base relationship")
			runMultiPKNoBase(t, ctx, sqlDB)

			t.Log("large composite diff with multi column workload")
			runLargeCompositeDiff(t, ctx, sqlDB)

			t.Log("sql diff handles rows containing NULL values")
			runSQLDiffHandlesNulls(t, ctx, sqlDB)

			t.Log("csv diff emits large range dataset that can be loaded back")
			runCSVLoadSimple(t, ctx, sqlDB)

			t.Log("csv diff covers rich data type payloads")
			runCSVLoadRichTypes(t, ctx, sqlDB)
		})
}

func runSinglePKWithBase(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "single_pk_base"
	branch := "single_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (id int primary key, value int, note varchar(32))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (1, 10, 'seed'), (2, 20, 'seed'), (3, 30, 'seed')", base))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` clone `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (4, 40, 'inserted'), (5, 50, 'inserted')", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set value = value + 90, note = 'updated' where id = 2", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where id = 3", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("replace into %s.%s", strings.ToLower(dbName), base))

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runMultiPKWithBase(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "multi_pk_base"
	branch := "multi_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (org_id int, event_id int, quantity int, status varchar(16), primary key (org_id, event_id))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (1, 1, 100, 'seed'), (1, 2, 200, 'seed'), (2, 1, 300, 'seed')", base))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` clone `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (3, 3, 900, 'inserted'), (2, 2, 400, 'inserted')", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set quantity = quantity + 5, status = 'updated' where org_id = 1 and event_id = 2", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where org_id = 2 and event_id = 1", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("replace into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, fmt.Sprintf("delete from %s.%s where (org_id,event_id)", strings.ToLower(dbName), base))

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runSinglePKNoBase(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "single_pk_nobranch_base"
	target := "single_pk_nobranch_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (id int primary key, label varchar(20), amount int)", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (id int primary key, label varchar(20), amount int)", target))

	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (1, 'alpha-new', 150), (3, 'gamma', 300)", target))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", target, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".csv", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	records := readDiffCSVFile(t, diffPath)
	expected := [][]string{
		{"1", "alpha-new", "150"},
		{"3", "gamma", "300"},
	}
	require.ElementsMatch(t, expected, records)

	applyCSVDiffRecords(t, ctx, db, dbName, base, records)
	assertTablesEqual(t, ctx, db, dbName, target, base)
}

func runMultiPKNoBase(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "multi_pk_nobranch_base"
	target := "multi_pk_nobranch_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (region int, device_id int, reading int, note varchar(24), primary key (region, device_id))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (region int, device_id int, reading int, note varchar(24), primary key (region, device_id))", target))

	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (1, 10, 55, 'updated'), (3, 30, 90, 'inserted')", target))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", target, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".csv", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	records := readDiffCSVFile(t, diffPath)
	expected := [][]string{
		{"1", "10", "55", "updated"},
		{"3", "30", "90", "inserted"},
	}
	require.ElementsMatch(t, expected, records)

	applyCSVDiffRecords(t, ctx, db, dbName, base, records)
	assertTablesEqual(t, ctx, db, dbName, target, base)
}

func runLargeCompositeDiff(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*150)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "composite_base"
	branch := "composite_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf(`
create table %s (
	org_id int,
	dept_id int,
	seq bigint,
	amount decimal(20,4),
	ratio double,
	memo varchar(64),
	created_at datetime,
	primary key (org_id, dept_id, seq)
)`, base))

	baseInsert := fmt.Sprintf(`
insert into %s
select
	((g.result %% 50) + 1) as org_id,
	((g.result %% 200) + 1) as dept_id,
	g.result as seq,
	cast(g.result * 1.5 as decimal(20,4)) as amount,
	g.result * 0.001 as ratio,
	concat('seed-', g.result %% 200) as memo,
	date_add('2024-01-01 00:00:00', interval g.result second) as created_at
from generate_series(1, 10000) as g`, base)
	execSQLDB(t, ctx, db, baseInsert)

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s clone %s", branch, base))

	newInserts := fmt.Sprintf(`
insert into %s
select
	((g.result %% 75) + 100) as org_id,
	((g.result %% 120) + 300) as dept_id,
	g.result as seq,
	cast(g.result * 2.25 as decimal(20,4)) as amount,
	g.result * 0.002 as ratio,
	concat('new-', g.result %% 500) as memo,
	date_add('2024-02-01 00:00:00', interval g.result second) as created_at
from generate_series(10001, 10800) as g`, branch)
	execSQLDB(t, ctx, db, newInserts)

	execSQLDB(t, ctx, db, fmt.Sprintf(
		"update %s set amount = amount + 77.7700, ratio = ratio * 1.05, memo = concat(memo, '-upd') where seq %% 91 = 0",
		branch,
	))

	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where seq %% 137 = 0", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("replace into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, fmt.Sprintf("delete from %s.%s", strings.ToLower(dbName), base))

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runSQLDiffHandlesNulls(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "sql_null_base"
	branch := "sql_null_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf(`
create table %s (
	id int primary key,
	qty int,
	label varchar(32),
	extra varchar(64),
	created_at datetime
)`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf(`
insert into %s values
	(1, 10, 'alpha', 'seed-row', '2024-01-01 00:00:00'),
	(2, null, 'beta', null, '2024-01-02 00:00:00'),
	(3, 30, null, 'only-extra', null)`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s clone %s", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set label = null, extra = null where id = 1", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set qty = 22, created_at = null where id = 2", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (4, null, null, 'brand-new', '2024-01-04 00:00:00')", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where id = 3", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, "null")

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runCSVLoadSimple(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*180)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "csv_massive_base"
	target := "csv_massive_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (a int primary key, b int)", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s like %s", target, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s select *, * from generate_series(1, %d) g", target, 1000*100))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", target, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".csv", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	loadDiffCSVIntoTable(t, ctx, db, base, diffPath)
	assertTablesEqual(t, ctx, db, dbName, target, base)
}

func runCSVLoadRichTypes(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*180)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "csv_rich_types_base"
	target := "csv_rich_types_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
	execSQLDB(t, ctx, db, fmt.Sprintf(`
create table %s (
	id int primary key,
	qty bigint,
	weight float,
	ratio double,
	price decimal(12,4),
	label varchar(32),
	metadata json,
	embedding vecf32(4),
	payload varbinary(16),
	notes text,
	flag bool
)`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s like %s", target, base))

	execSQLDB(t, ctx, db, fmt.Sprintf(`
insert into %s values
	(1, 100, 1.5, 0.99, 19.7500, 'alpha', '{"tier":"gold","attrs":[1,2,3]}', '[0.10, 0.20, 0.30, 0.40]', x'000102030405060708090a0b0c0d0e0f', 'vector-ready payload', true),
	(2, 200, -3.25, -11.2, 0.0000, 'beta', '{"tier":"silver","nested":{"text":"你好","vec":[1,2]}}', '[0.90, -0.10, 0.20, -0.30]', x'0a0b0c0d', 'json-"mixing"-quotes', false),
	(3, 0, 0.0, 10000.0, 12345.6789, 'gamma', null, null, null, null, true)`, target))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", target, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".csv", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	loadDiffCSVIntoTable(t, ctx, db, base, diffPath)
	assertTablesEqual(t, ctx, db, dbName, target, base)
}

func readSQLFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, data, "diff sql output is empty")
	return string(data)
}

func applyDiffStatements(t *testing.T, ctx context.Context, db *sql.DB, sqlContent string) {
	t.Helper()

	for _, stmt := range parseSQLStatements(sqlContent) {
		execSQLDB(t, ctx, db, stmt)
	}
}

func parseSQLStatements(content string) []string {
	lines := strings.Split(content, ";")
	stmts := make([]string, 0, len(lines))
	for _, line := range lines {
		stmt := strings.TrimSpace(line)
		if stmt == "" {
			continue
		}
		stmts = append(stmts, stmt)
	}
	return stmts
}

func execDiffAndFetchFile(t *testing.T, ctx context.Context, db *sql.DB, stmt string) string {
	t.Helper()

	rows, err := db.QueryContext(ctx, stmt)
	require.NoErrorf(t, err, "sql: %s", stmt)
	defer rows.Close()

	require.Truef(t, rows.Next(), "diff statement %s returned no rows", stmt)
	cols, err := rows.Columns()
	require.NoError(t, err)

	raw := make([][]byte, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}

	require.NoError(t, rows.Scan(dest...))

	filePath := string(raw[0])
	require.Falsef(t, rows.Next(), "unexpected extra rows for diff statement %s", stmt)
	require.NoErrorf(t, rows.Err(), "diff statement %s failed", stmt)
	require.NotEmpty(t, filePath, "diff output filepath is empty")
	require.FileExistsf(t, filePath, "diff output filepath does not exist: %s", filePath)
	return filePath
}

func readDiffCSVFile(t *testing.T, path string) [][]string {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	reader := csv.NewReader(f)
	records := make([][]string, 0, 4)
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if len(rec) == 0 {
			continue
		}
		records = append(records, rec)
	}
	require.NotEmpty(t, records, "diff csv output is empty")
	return records
}

func applyCSVDiffRecords(t *testing.T, ctx context.Context, db *sql.DB, schema, table string, records [][]string) {
	t.Helper()

	require.NotEmpty(t, records, "no csv records to apply")
	valueClauses := make([]string, len(records))
	for i, rec := range records {
		values := make([]string, len(rec))
		for j, field := range rec {
			values[j] = csvFieldToSQLLiteral(field)
		}
		valueClauses[i] = fmt.Sprintf("(%s)", strings.Join(values, ","))
	}
	stmt := fmt.Sprintf("replace into %s.%s values %s", schema, table, strings.Join(valueClauses, ","))
	execSQLDB(t, ctx, db, stmt)
}

func csvFieldToSQLLiteral(val string) string {
	if val == `\N` {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
}

func loadDiffCSVIntoTable(t *testing.T, ctx context.Context, db *sql.DB, table, csvPath string) {
	t.Helper()

	pathLiteral := strings.ReplaceAll(csvPath, "'", "''")
	stmt := fmt.Sprintf("load data infile '%s' into table %s fields terminated by ',' enclosed by '\"' escaped by '\\\\' lines terminated by '\\n'", pathLiteral, table)
	execSQLDB(t, ctx, db, stmt)
}

func assertTablesEqual(t *testing.T, ctx context.Context, db *sql.DB, schema, left, right string) {
	t.Helper()

	check := func(query string) {
		rows, err := db.QueryContext(ctx, query)
		require.NoErrorf(t, err, "sql: %s", query)
		require.NoErrorf(t, rows.Err(), "sql: %s", query)
		defer rows.Close()
		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		require.Equalf(t, 0, rowCount, "expected no rows for query %s", query)
	}

	check(fmt.Sprintf("select * from %s.%s except select * from %s.%s", schema, left, schema, right))
	check(fmt.Sprintf("select * from %s.%s except select * from %s.%s", schema, right, schema, left))
}

func execSQLDB(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	_, err := db.ExecContext(ctx, stmt)
	require.NoErrorf(t, err, "sql: %s", stmt)
}
