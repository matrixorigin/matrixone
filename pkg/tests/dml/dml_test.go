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

			t.Log("data branch create database populates metadata")
			runBranchDatabaseMetadata(t, ctx, sqlDB)

			t.Log("csv diff emits large range dataset that can be loaded back")
			runCSVLoadSimple(t, ctx, sqlDB)

			t.Log("csv diff covers rich data type payloads")
			runCSVLoadRichTypes(t, ctx, sqlDB)

			t.Log("diff output limit returns subset of full diff")
			runDiffOutputLimitSubset(t, ctx, sqlDB)

			t.Log("diff output limit without branch relationship returns subset of full diff")
			runDiffOutputLimitNoBase(t, ctx, sqlDB)

			t.Log("diff output limit with large base workload still returns subset of full diff")
			runDiffOutputLimitLargeBase(t, ctx, sqlDB)

			t.Log("diff output to stage and load via datalink")
			runDiffOutputToStage(t, ctx, sqlDB)
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

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
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

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
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

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s from %s", branch, base))

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

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s from %s", branch, base))
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

func runDiffOutputLimitSubset(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "limit_base"
	branch := "limit_branch"

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (id int primary key, val int, note varchar(16))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (1, 10, 'seed'), (2, 20, 'seed'), (3, 30, 'seed'), (4, 40, 'seed'), (5, 50, 'seed'), (6, 60, 'seed')", base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s from %s", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (7, 70, 'inserted'), (8, 80, 'inserted')", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set val = val + 100, note = 'updated' where id in (2,3)", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where id in (4,5)", branch))

	fullStmt := fmt.Sprintf("data branch diff %s against %s", branch, base)
	fullRows := fetchDiffRowsAsStrings(t, ctx, db, fullStmt)
	require.GreaterOrEqual(t, len(fullRows), 6)

	limit := 3
	limitStmt := fmt.Sprintf("data branch diff %s against %s output limit %d", branch, base, limit)
	limitedRows := fetchDiffRowsAsStrings(t, ctx, db, limitStmt)

	require.NotEmpty(t, limitedRows, "limited diff returned no rows")
	require.LessOrEqual(t, len(limitedRows), limit, "limited diff returned too many rows")

	fullSet := make(map[string]struct{}, len(fullRows))
	for _, row := range fullRows {
		fullSet[strings.Join(row, "||")] = struct{}{}
	}
	for _, row := range limitedRows {
		_, ok := fullSet[strings.Join(row, "||")]
		require.Truef(t, ok, "limited diff row not contained in full diff: %v", row)
	}
}

func runDiffOutputLimitNoBase(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "limit_nobranch_base"
	target := "limit_nobranch_target"

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (id int primary key, val int, note varchar(16))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (id int primary key, val int, note varchar(16))", target))

	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (1, 10, 'seed'), (2, 20, 'seed'), (3, 30, 'seed'), (4, 40, 'seed')", base))

	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (1, 110, 'updated'), (2, 20, 'seed'), (5, 500, 'added'), (6, 600, 'added')", target))

	fullStmt := fmt.Sprintf("data branch diff %s against %s", target, base)
	fullRows := fetchDiffRowsAsStrings(t, ctx, db, fullStmt)
	require.GreaterOrEqual(t, len(fullRows), 3)

	limit := 1
	limitStmt := fmt.Sprintf("data branch diff %s against %s output limit %d", target, base, limit)
	limitedRows := fetchDiffRowsAsStrings(t, ctx, db, limitStmt)

	require.NotEmpty(t, limitedRows, "limited diff returned no rows")
	require.LessOrEqual(t, len(limitedRows), limit, "limited diff returned too many rows")

	fullSet := make(map[string]struct{}, len(fullRows))
	for _, row := range fullRows {
		fullSet[strings.Join(row, "||")] = struct{}{}
	}
	for _, row := range limitedRows {
		_, ok := fullSet[strings.Join(row, "||")]
		require.Truef(t, ok, "limited diff row not contained in full diff: %v", row)
	}
}

func runDiffOutputLimitLargeBase(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*180)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "limit_large_t1"
	branch := "limit_large_t2"

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (a int primary key, b int, c time)", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s select *, *, '12:34:56' from generate_series(1, 8192*100)g", base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s from %s", branch, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set b = b + 1 where a between 1 and 10000", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set b = b + 2 where a between 10000 and 10001", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where a between 30000 and 100000", base))

	fullStmt := fmt.Sprintf("data branch diff %s against %s", branch, base)
	fullRows := fetchDiffRowsAsStrings(t, ctx, db, fullStmt)
	//require.Equal(t, 30, len(fullRows), fmt.Sprintf("full diff: %v", fullRows))
	fmt.Println("full diff:", len(fullRows))

	limitQuery := func(cnt int) {
		limitStmt := fmt.Sprintf("data branch diff %s against %s output limit %d", branch, base, cnt)
		limitedRows := fetchDiffRowsAsStrings(t, ctx, db, limitStmt)

		require.NotEmpty(t, limitedRows, "limited diff returned no rows")
		require.LessOrEqual(t, len(limitedRows), cnt, fmt.Sprintf("limited diff returned too many rows: %v", limitedRows))

		fullSet := make(map[string]struct{}, len(fullRows))
		for _, row := range fullRows {
			fullSet[strings.Join(row, "||")] = struct{}{}
		}
		for _, row := range limitedRows {
			_, ok := fullSet[strings.Join(row, "||")]
			require.Truef(t, ok, "limited diff row not contained in full diff: %v", row)
		}
	}

	limitQuery(len(fullRows) * 1 / 100)
	limitQuery(len(fullRows) * 20 / 100)
}

func runDiffOutputToStage(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "stage_base"
	branch := "stage_branch"

	stageDir := t.TempDir()
	stageName := "stage_local_" + strings.ToLower(testutils.GetDatabaseName(t))
	stageURL := fmt.Sprintf("file://%s", stageDir)

	execSQLDB(t, ctx, db, "set role moadmin")
	execSQLDB(t, ctx, db, fmt.Sprintf("create stage %s url = '%s'", stageName, stageURL))
	defer execSQLDB(t, ctx, db, fmt.Sprintf("drop stage if exists %s", stageName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s`.`%s` (id int primary key, val int)", dbName, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s`.`%s` values (1, 10), (2, 20), (3, 30)", dbName, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s.%s from %s.%s", dbName, branch, dbName, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s.%s values (4, 40)", dbName, branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update %s.%s set val = val + 5 where id = 2", dbName, branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s.%s where id = 3", dbName, branch))

	diffStmt := fmt.Sprintf("data branch diff %s.%s against %s.%s output file 'stage://%s/'", dbName, branch, dbName, base, stageName)
	rows, err := db.QueryContext(ctx, diffStmt)
	require.NoErrorf(t, err, "sql: %s", diffStmt)
	defer rows.Close()

	require.Truef(t, rows.Next(), "diff statement %s returned no rows", diffStmt)
	cols, err := rows.Columns()
	require.NoError(t, err)

	raw := make([][]byte, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	require.NoError(t, rows.Scan(dest...))
	require.NoErrorf(t, rows.Err(), "diff statement %s failed", diffStmt)
	require.Falsef(t, rows.Next(), "unexpected extra rows for diff statement %s", diffStmt)

	require.NotEmpty(t, raw, "diff statement returned no columns")
	stagePath := string(raw[0])
	require.NotEmpty(t, stagePath, "diff output stage path is empty")

	t.Logf("stage diff path: %s", stagePath)
	require.Equal(t, ".sql", filepath.Ext(stagePath))

	loadStmt := fmt.Sprintf("select load_file(cast('%s' as datalink))", stagePath)
	loadRows, err := db.QueryContext(ctx, loadStmt)
	require.NoErrorf(t, err, "sql: %s", loadStmt)
	defer loadRows.Close()

	require.Truef(t, loadRows.Next(), "load_file %s returned no rows", stagePath)
	var payload []byte
	require.NoError(t, loadRows.Scan(&payload))
	require.Falsef(t, loadRows.Next(), "load_file %s returned unexpected extra rows", stagePath)
	require.NoErrorf(t, loadRows.Err(), "load_file %s failed", stagePath)
	require.NotEmpty(t, payload, "stage diff payload is empty")

	sqlContent := strings.ToLower(string(payload))
	require.Contains(t, sqlContent, fmt.Sprintf("replace into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, sqlContent, fmt.Sprintf("delete from %s.%s", strings.ToLower(dbName), base))

	applyDiffStatements(t, ctx, db, string(payload))
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runBranchDatabaseMetadata(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	copyDB := dbName + "_copy"
	tables := []string{"tbl_one", "tbl_two"}

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", copyDB))
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s`.`%s` (id int primary key)", dbName, tables[0]))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s`.`%s` (id int primary key)", dbName, tables[1]))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create database `%s` from `%s`", copyDB, dbName))

	query := fmt.Sprintf("select relname from mo_catalog.mo_tables where rel_id in (select table_id from mo_catalog.mo_branch_metadata) and lower(reldatabase) = '%s'", strings.ToLower(copyDB))
	rows, err := db.QueryContext(ctx, query)
	require.NoErrorf(t, err, "sql: %s", query)
	defer rows.Close()

	branchedTables := make([]string, 0, len(tables))
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		branchedTables = append(branchedTables, name)
	}
	require.NoErrorf(t, rows.Err(), "sql: %s", query)
	t.Logf("branch metadata tables for %s: %v(sql=%s)", copyDB, branchedTables, query)

	for _, tb := range tables {
		require.Containsf(t, branchedTables, tb, "table %s not found in branch metadata", tb)
	}
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

func fetchDiffRowsAsStrings(t *testing.T, ctx context.Context, db *sql.DB, stmt string) [][]string {
	t.Helper()

	rows, err := db.QueryContext(ctx, stmt)
	require.NoErrorf(t, err, "sql: %s", stmt)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)

	result := make([][]string, 0, 8)
	for rows.Next() {
		raw := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		require.NoError(t, rows.Scan(dest...))

		row := make([]string, len(cols))
		for i, b := range raw {
			if b == nil {
				row[i] = "NULL"
				continue
			}
			row[i] = string(b)
		}
		result = append(result, row)
	}
	require.NoErrorf(t, rows.Err(), "sql: %s", stmt)
	require.NotEmpty(t, result, "diff statement returned no rows: %s", stmt)
	return result
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
