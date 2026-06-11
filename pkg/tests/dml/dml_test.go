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
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*420)
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

			t.Log("diff output splits updates into delete + insert (single pk)")
			runUpdateSplitDiffAsFile(t, ctx, sqlDB)

			t.Log("diff output splits updates into delete + insert (composite pk)")
			runCompositeUpdateSplitDiffAsFile(t, ctx, sqlDB)

			t.Log("diff output handles no-pk duplicates and null deletes")
			runNoPKDuplicateDiffAsFile(t, ctx, sqlDB)

			t.Log("diff output handles mixed types and string edge cases")
			runComplexTypeDiffAsFile(t, ctx, sqlDB)

			t.Log("data branch diff covers remaining column type families")
			runDataBranchColumnTypeMatrix(t, ctx, sqlDB)

			t.Log("data branch preserves and enforces column constraints")
			runDataBranchColumnConstraintMatrix(t, ctx, sqlDB)

			t.Log("data branch covers supported table type behavior")
			runDataBranchTableTypeMatrix(t, ctx, sqlDB)

			t.Log("data branch rejects invalid unhappy-path operations")
			runDataBranchUnhappyPathMatrix(t, ctx, sqlDB)

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

			t.Log("diff output summary validates complex snapshot and branch divergence scenarios")
			runDiffOutputSummaryComplex(t, ctx, sqlDB)

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
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))

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
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
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

	loadDiffCSVIntoTable(t, ctx, db, base, diffPath)
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

	loadDiffCSVIntoTable(t, ctx, db, base, diffPath)
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
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
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

	projectedFullStmt := fmt.Sprintf("data branch diff %s against %s columns (val, note)", branch, base)
	projectedFullRows := fetchDiffRowsAsStrings(t, ctx, db, projectedFullStmt)
	require.GreaterOrEqual(t, len(projectedFullRows), 6)
	for _, row := range projectedFullRows {
		require.Len(t, row, 4, "projected diff should only include table, flag, and requested columns")
	}

	projectedLimitStmt := fmt.Sprintf("data branch diff %s against %s columns (val, note) output limit %d", branch, base, limit)
	projectedLimitedRows := fetchDiffRowsAsStrings(t, ctx, db, projectedLimitStmt)
	require.NotEmpty(t, projectedLimitedRows, "projected limited diff returned no rows")
	require.LessOrEqual(t, len(projectedLimitedRows), limit, "projected limited diff returned too many rows")

	projectedFullSet := make(map[string]struct{}, len(projectedFullRows))
	for _, row := range projectedFullRows {
		projectedFullSet[strings.Join(row, "||")] = struct{}{}
	}
	for _, row := range projectedLimitedRows {
		require.Len(t, row, 4, "projected limited diff should only include table, flag, and requested columns")
		_, ok := projectedFullSet[strings.Join(row, "||")]
		require.Truef(t, ok, "projected limited diff row not contained in projected full diff: %v", row)
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

func runDiffOutputSummaryComplex(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*150)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	seed := "summary_seed"
	left := "summary_left"
	right := "summary_right"
	standaloneBase := "summary_standalone_base"
	standaloneTarget := "summary_standalone_target"

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	// Divergent branch scenario to verify both target/base columns can be non-zero per metric.
	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (id int primary key, val int)", seed))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)", seed))
	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s from %s", left, seed))
	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s from %s", right, seed))

	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set val = val + 100 where id in (1, 2)", left))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where id = 4", left))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (7, 70)", left))

	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set val = val + 200 where id in (2, 3)", right))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where id = 1", right))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (8, 80)", right))

	leftSummaryStmt := fmt.Sprintf("data branch diff %s against %s output summary", left, right)
	leftCountStmt := fmt.Sprintf("data branch diff %s against %s output count", left, right)
	leftSummary := fetchDiffSummaryMetrics(t, ctx, db, leftSummaryStmt)
	assertSummaryMetrics(t, leftSummary, [2]int64{1, 1}, [2]int64{1, 1}, [2]int64{2, 2})
	assertSummaryMatchesCount(t, leftSummary, fetchDiffCount(t, ctx, db, leftCountStmt))

	rightSummaryStmt := fmt.Sprintf("data branch diff %s against %s output summary", right, left)
	rightCountStmt := fmt.Sprintf("data branch diff %s against %s output count", right, left)
	rightSummary := fetchDiffSummaryMetrics(t, ctx, db, rightSummaryStmt)
	assertSummaryMetrics(t, rightSummary, [2]int64{1, 1}, [2]int64{1, 1}, [2]int64{2, 2})
	assertSummaryMatchesCount(t, rightSummary, fetchDiffCount(t, ctx, db, rightCountStmt))

	// Non-branch baseline to ensure summary/count consistency still holds without branch lineage.
	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (id int primary key, val int, note varchar(16))", standaloneBase))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (id int primary key, val int, note varchar(16))", standaloneTarget))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (1, 10, 'seed'), (2, 20, 'seed'), (3, 30, 'seed'), (4, 40, 'seed')", standaloneBase))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (1, 110, 'updated'), (2, 20, 'seed'), (5, 500, 'added'), (6, 600, 'added')", standaloneTarget))

	standaloneSummaryStmt := fmt.Sprintf("data branch diff %s against %s output summary", standaloneTarget, standaloneBase)
	standaloneCountStmt := fmt.Sprintf("data branch diff %s against %s output count", standaloneTarget, standaloneBase)
	standaloneSummary := fetchDiffSummaryMetrics(t, ctx, db, standaloneSummaryStmt)
	standaloneCount := fetchDiffCount(t, ctx, db, standaloneCountStmt)
	assertSummaryMatchesCount(t, standaloneSummary, standaloneCount)
	require.Greater(t, standaloneCount, int64(0), "standalone summary/count should report non-zero diff rows")
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
	require.Contains(t, sqlContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, sqlContent, fmt.Sprintf("delete from %s.%s", strings.ToLower(dbName), base))

	applyDiffStatements(t, ctx, db, string(payload))
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runUpdateSplitDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "split_pk_base"
	branch := "split_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (id int primary key, score int, note varchar(32))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (1, 10, 'seed'), (2, 20, 'seed'), (3, 30, 'seed')", base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set score = score + 9, note = 'changed' where id in (1,3)", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, fmt.Sprintf("delete from %s.%s where id in", strings.ToLower(dbName), base))
	require.NotContains(t, lowerContent, "update ")

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runCompositeUpdateSplitDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "split_comp_base"
	branch := "split_comp_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (org_id int, event_id int, qty int, note varchar(32), primary key (org_id, event_id))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (1, 1, 10, 'seed'), (1, 2, 20, 'seed'), (2, 1, 30, 'seed')", base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set qty = qty + 5, note = 'shifted' where org_id = 1 and event_id = 2", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set note = null where org_id = 2 and event_id = 1", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, fmt.Sprintf("delete from %s.%s where (org_id,event_id) in", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, "null")
	require.NotContains(t, lowerContent, "update ")

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runNoPKDuplicateDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "no_pk_base"
	branch := "no_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (id int, grp int, note varchar(32))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf(`insert into %s values
		(1, 10, 'dup'),
		(1, 10, 'dup'),
		(1, 10, 'dup'),
		(1, 10, 'dup'),
		(2, 20, null),
		(3, 30, 'keep'),
		(4, null, 'nil'),
		(5, 50, 'change')`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where id = 1 and grp = 10 and note = 'dup' limit 1", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where id = 1 and grp = 10 and note = 'dup' limit 1", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where id = 2 and grp = 20 and note is null", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set grp = 41 where id = 4 and grp is null", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set grp = 55, note = 'changed' where id = 5 and grp = 50 and note = 'change'", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (6, 60, 'added')", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, fmt.Sprintf("delete from %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, "limit 1")
	require.Contains(t, lowerContent, "is null")
	require.NotContains(t, lowerContent, "update ")

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runComplexTypeDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "complex_base"
	branch := "complex_branch"
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
	name varchar(32),
	note text,
	amount decimal(10,2),
	created_at datetime,
	active bool
)`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf(`insert into %s values
		(1, 'Alpha', 'Seed', 10.50, '2024-01-01 10:00:00', true),
		(2, 'alpha', '', 0.00, null, false),
		(3, 'MIX', 'case', 33.33, '2024-02-02 02:02:02', true),
		(4, 'keep', 'NULLABLE', null, '2024-03-03 03:03:03', true)`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set name = 'ALPHA', note = 'seed' where id = 1", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set note = 'O\\'Reilly', created_at = '2024-01-02 00:00:00', active = true where id = 2", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set note = '', amount = 40.00 where id = 3", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where id = 4", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (5, 'path\\\\dir', null, 99.99, null, false)", branch))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, fmt.Sprintf("delete from %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, "null")
	require.Contains(t, lowerContent, "''")
	require.NotContains(t, lowerContent, "update ")

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runDataBranchColumnTypeMatrix(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*150)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	base := "type_matrix_base"
	branch := "type_matrix_branch"
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
	b bit(10),
	u8 tinyint unsigned,
	u16 smallint unsigned,
	u32 int unsigned,
	u64 bigint unsigned,
	d256 decimal(65,30),
	y year,
	uid uuid,
	bin binary(4),
	vbin varbinary(8),
	e enum('red','blue','green'),
	bl blob,
	dl datalink,
	g geometry,
	vf vecf64(3)
)`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf(`
insert into %s values
	(1, b'101', 250, 65000, 4000000000, 9000000000000000000,
		cast('12345678901234567890123456789012345.123456789012345678901234567890' as decimal(65,30)),
		2024, '6d1b1f73-2dbf-11ed-940f-000c29847904', 'ab', x'01020304',
		'red', 'blob-base', 'file:///tmp/mo_branch_type_base.csv', 'POINT(1 1)', '[1.1,2.2,3.3]'),
	(2, b'010', 1, 2, 3, 4,
		cast('-0.000000000000000000000000000001' as decimal(65,30)),
		1999, 'ad9f809f-2dbd-11ed-940f-000c29847904', 'cd', x'05060708',
		'blue', 'blob-two', 'file:///tmp/mo_branch_type_two.csv', 'LINESTRING(0 0,1 1)', '[4.4,5.5,6.6]')`, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf(`
update %s set
	b = b'111',
	u8 = 251,
	u16 = 65001,
	u32 = 4000000001,
	u64 = 9000000000000000001,
	d256 = cast('42.000000000000000000000000000000' as decimal(65,30)),
	y = 2025,
	uid = '1b50c137-2dba-11ed-940f-000c29847904',
	bin = 'ef',
	vbin = x'090a0b0c',
	e = 'green',
	bl = 'blob-updated',
	dl = 'file:///tmp/mo_branch_type_updated.csv',
	g = 'POINT(2 2)',
	vf = '[7.7,8.8,9.9]'
where id = 1`, branch))
	execSQLDB(t, ctx, db, fmt.Sprintf(`
insert into %s values
	(3, b'001', 2, 3, 4, 5,
		cast('7.000000000000000000000000000000' as decimal(65,30)),
		2000, '3ddf7b28-2dba-11ed-940f-000c29847904', 'gh', x'0d0e0f10',
		'red', 'blob-new', 'file:///tmp/mo_branch_type_new.csv', 'POINT(3 3)', '[0.1,0.2,0.3]')`, branch))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where id = 2", branch))

	summary := fetchDiffSummaryMetrics(t, ctx, db, fmt.Sprintf("data branch diff %s against %s output summary", branch, base))
	assertSummaryMetrics(t, summary, [2]int64{1, 0}, [2]int64{1, 0}, [2]int64{1, 0})
	assertSummaryMatchesCount(t, summary, fetchDiffCount(t, ctx, db, fmt.Sprintf("data branch diff %s against %s output count", branch, base)))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", branch, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	sqlContent := readSQLFile(t, diffPath)
	lowerContent := strings.ToLower(sqlContent)
	require.Contains(t, lowerContent, fmt.Sprintf("insert into %s.%s", strings.ToLower(dbName), base))
	require.Contains(t, lowerContent, fmt.Sprintf("delete from %s.%s", strings.ToLower(dbName), base))

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runDataBranchColumnConstraintMatrix(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, "create table parent (id int primary key)")
	execSQLDB(t, ctx, db, "insert into parent values (1), (2), (3)")
	execSQLDB(t, ctx, db, `
create table child (
	id int auto_increment primary key,
	parent_id int not null,
	code varchar(20) not null default 'seed' comment 'branch code',
	qty decimal(10,2) default 0.00,
	constraint chk_qty CHECK (qty IS NULL OR qty >= 0),
	unique key uq_code (code),
	constraint fk_child_parent foreign key (parent_id) references parent(id)
) comment = 'branch constraint table'`)
	execSQLDB(t, ctx, db, "insert into child(parent_id, code, qty) values (1, 'base-a', 10.00), (2, 'base-b', 20.00)")

	execSQLDB(t, ctx, db, "data branch create table child_branch from child")
	execSQLDB(t, ctx, db, "update child_branch set qty = qty + 5 where code = 'base-a'")
	execSQLDB(t, ctx, db, "insert into child_branch(parent_id, qty) values (2, 77.75)")

	execSQLDBExpectError(t, ctx, db, "insert into child_branch(parent_id, code, qty) values (1, null, 1.00)")
	execSQLDBExpectError(t, ctx, db, "insert into child_branch(parent_id, code, qty) values (1, 'base-a', 1.00)")
	execSQLDBExpectError(t, ctx, db, "insert into child_branch(parent_id, code, qty) values (99, 'bad-fk', 1.00)")

	createSQL := strings.ToLower(fetchShowCreateTable(t, ctx, db, "child_branch"))
	require.Contains(t, createSQL, "branch code")
	require.Contains(t, createSQL, "branch constraint table")
	require.Contains(t, createSQL, "check")
	require.Contains(t, createSQL, "foreign key")

	execSQLDB(t, ctx, db, "data branch merge child_branch into child")
	require.Equal(t, int64(3), fetchSingleInt64(t, ctx, db, "select count(*) from child"))
	require.Equal(t, "seed", fetchSingleString(t, ctx, db, "select code from child where parent_id = 2 and qty = 77.75"))

	execSQLDB(t, ctx, db, "create table clustered_base (tenant int not null comment 'cluster tenant', seq int default 0, payload varchar(20)) cluster by (tenant, seq)")
	execSQLDB(t, ctx, db, "insert into clustered_base values (1, 1, 'base'), (1, 2, 'old'), (2, 1, 'keep')")
	execSQLDB(t, ctx, db, "data branch create table clustered_branch from clustered_base")
	execSQLDB(t, ctx, db, "update clustered_branch set payload = 'new' where tenant = 1 and seq = 2")
	execSQLDB(t, ctx, db, "insert into clustered_branch(tenant, payload) values (3, 'default-seq')")
	execSQLDB(t, ctx, db, "delete from clustered_branch where tenant = 2")

	clusterCreateSQL := strings.ToLower(fetchShowCreateTable(t, ctx, db, "clustered_branch"))
	require.Contains(t, clusterCreateSQL, "cluster by")
	require.Contains(t, clusterCreateSQL, "cluster tenant")

	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")
	diffStmt := fmt.Sprintf("data branch diff clustered_branch against clustered_base output file '%s'", diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	applyDiffStatements(t, ctx, db, readSQLFile(t, diffPath))
	assertTablesEqual(t, ctx, db, dbName, "clustered_branch", "clustered_base")
}

func runDataBranchTableTypeMatrix(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*150)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)
	copyDB := dbName + "_copy"

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", copyDB))
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, `
create table part_base (
	id int primary key,
	val int,
	note varchar(16)
) partition by range columns(id) (
	partition p0 values less than (10),
	partition p1 values less than (MAXVALUE)
)`)
	execSQLDB(t, ctx, db, "insert into part_base values (1, 10, 'p0'), (11, 110, 'p1'), (12, 120, 'p1')")
	execSQLDB(t, ctx, db, "data branch create table part_branch from part_base")
	execSQLDB(t, ctx, db, "update part_branch set val = val + 1 where id = 11")
	execSQLDB(t, ctx, db, "insert into part_branch values (2, 20, 'new-p0'), (21, 210, 'new-p1')")
	execSQLDB(t, ctx, db, "delete from part_branch where id = 12")

	partCreateSQL := strings.ToLower(fetchShowCreateTable(t, ctx, db, "part_branch"))
	require.Contains(t, partCreateSQL, "partition by range")

	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")
	diffStmt := fmt.Sprintf("data branch diff part_branch against part_base output file '%s'", diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".sql", filepath.Ext(diffPath))
	applyDiffStatements(t, ctx, db, readSQLFile(t, diffPath))
	assertTablesEqual(t, ctx, db, dbName, "part_branch", "part_base")

	execSQLDB(t, ctx, db, "create table view_base (id int primary key, val int)")
	execSQLDB(t, ctx, db, "insert into view_base values (1, 10), (2, 20), (3, 30)")
	execSQLDB(t, ctx, db, "create view v_active as select id, val from view_base where val >= 20")
	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create database `%s` from `%s`", copyDB, dbName))
	execSQLDB(t, ctx, db, "insert into view_base values (4, 40)")
	require.Equal(t, int64(2), fetchSingleInt64(t, ctx, db, fmt.Sprintf("select count(*) from `%s`.v_active", copyDB)))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s`.view_base values (5, 50)", copyDB))
	require.Equal(t, int64(3), fetchSingleInt64(t, ctx, db, fmt.Sprintf("select count(*) from `%s`.v_active", copyDB)))

	execSQLDB(t, ctx, db, "create temporary table temp_branch_base (id int primary key, val int)")
	execSQLDB(t, ctx, db, "insert into temp_branch_base values (1, 10)")
	if _, err := db.ExecContext(ctx, "data branch create table temp_branch_copy from temp_branch_base"); err == nil {
		require.Equal(t, int64(1), fetchSingleInt64(t, ctx, db, "select count(*) from temp_branch_copy"))
	}

	externalPath := filepath.Join(t.TempDir(), "external.csv")
	require.NoError(t, os.WriteFile(externalPath, []byte("1\n2\n"), 0o600))
	externalPathLiteral := strings.ReplaceAll(externalPath, "'", "''")
	execSQLDB(t, ctx, db, fmt.Sprintf("create external table ext_branch_base (id int) infile{\"filepath\"='%s'} fields terminated by ',' lines terminated by '\\n'", externalPathLiteral))
	execSQLDBExpectError(t, ctx, db, "data branch create table ext_branch_copy from ext_branch_base")

	execSQLDB(t, ctx, db, "use mo_catalog")
	execSQLDB(t, ctx, db, "drop table if exists cluster_branch_base")
	execSQLDB(t, ctx, db, "create cluster table cluster_branch_base(a int)")
	defer execSQLDB(t, ctx, db, "drop table if exists mo_catalog.cluster_branch_base")
	execSQLDB(t, ctx, db, "insert into cluster_branch_base values (1, 0), (2, 0)")
	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s`.cluster_branch_copy from mo_catalog.cluster_branch_base", dbName))
	require.Equal(t, int64(2), fetchSingleInt64(t, ctx, db, fmt.Sprintf("select count(*) from `%s`.cluster_branch_copy", dbName)))
}

func runDataBranchUnhappyPathMatrix(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	dbName := testutils.GetDatabaseName(t)

	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	defer func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}()
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

	execSQLDB(t, ctx, db, "create table base(a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1, 10), (2, 20)")
	execSQLDB(t, ctx, db, "create view v_base as select * from base")
	execSQLDBExpectError(t, ctx, db, "data branch create table v_branch from v_base")
	execSQLDBExpectError(t, ctx, db, "data branch create table missing_branch from missing_base")

	execSQLDB(t, ctx, db, "data branch create table br from base")
	execSQLDBExpectError(t, ctx, db, "data branch diff br against base columns (missing)")
	execSQLDBExpectError(t, ctx, db, "data branch diff missing_branch against base")
	execSQLDBExpectError(t, ctx, db, "data branch diff br against missing_base")
	diffDir := strings.ReplaceAll(t.TempDir(), "'", "''")
	execSQLDBExpectError(t, ctx, db, fmt.Sprintf("data branch diff br against base columns (a) output file '%s'", diffDir))
	execSQLDB(t, ctx, db, "alter table br add column c int default 0")
	execSQLDBExpectError(t, ctx, db, "data branch diff br against base")
	execSQLDBExpectError(t, ctx, db, "data branch merge br into base")

	execSQLDB(t, ctx, db, "create table no_pk(a int, b int)")
	execSQLDB(t, ctx, db, "insert into no_pk values (1, 10), (2, 20)")
	execSQLDB(t, ctx, db, "data branch create table no_pk_branch from no_pk")
	execSQLDBExpectError(t, ctx, db, "data branch pick no_pk_branch into no_pk keys(1)")

	execSQLDB(t, ctx, db, "create table single_pk(a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into single_pk values (1, 10), (2, 20)")
	execSQLDB(t, ctx, db, "data branch create table single_left from single_pk")
	execSQLDB(t, ctx, db, "data branch create table single_right from single_pk")
	execSQLDB(t, ctx, db, "insert into single_right values (3, 30)")
	execSQLDBExpectError(t, ctx, db, "data branch pick single_right into single_left")
	execSQLDBExpectError(t, ctx, db, "data branch pick single_right into single_left keys((3, 3))")
	execSQLDBExpectError(t, ctx, db, "data branch pick single_right into single_left keys(select cast(null as int))")
	execSQLDBExpectError(t, ctx, db, "data branch pick single_right into single_left keys(select a, b from single_right)")
	execSQLDB(t, ctx, db, fmt.Sprintf("create snapshot %s_single_sp for table `%s` single_left", dbName, dbName))
	execSQLDBExpectError(t, ctx, db, fmt.Sprintf("data branch pick single_right into single_left{snapshot='%s_single_sp'} keys(3)", dbName))
	execSQLDB(t, ctx, db, fmt.Sprintf("drop snapshot %s_single_sp", dbName))

	execSQLDB(t, ctx, db, "create table comp_pk(a int, b int, c varchar(20), primary key(a, b))")
	execSQLDB(t, ctx, db, "insert into comp_pk values (1, 1, 'base'), (2, 2, 'base')")
	execSQLDB(t, ctx, db, "data branch create table comp_left from comp_pk")
	execSQLDB(t, ctx, db, "data branch create table comp_right from comp_pk")
	execSQLDB(t, ctx, db, "insert into comp_right values (3, 3, 'new')")
	execSQLDBExpectError(t, ctx, db, "data branch pick comp_right into comp_left keys(3)")
	execSQLDBExpectError(t, ctx, db, "data branch pick comp_right into comp_left keys((3, 3, 3))")
	execSQLDBExpectError(t, ctx, db, "data branch pick single_right into single_left between snapshot missing_from and missing_to keys(3)")

	execSQLDB(t, ctx, db, "create table exists_base(a int primary key)")
	execSQLDB(t, ctx, db, "create table exists_target(a int primary key)")
	execSQLDBExpectError(t, ctx, db, "data branch create table exists_target from exists_base")
	execSQLDBExpectError(t, ctx, db, fmt.Sprintf("data branch create database `%s` from `%s`", dbName, dbName))

	execSQLDB(t, ctx, db, "create table uniq_base(a int primary key, u int unique)")
	execSQLDB(t, ctx, db, "insert into uniq_base values (1, 10), (2, 20)")
	execSQLDB(t, ctx, db, "data branch create table uniq_left from uniq_base")
	execSQLDB(t, ctx, db, "data branch create table uniq_right from uniq_base")
	execSQLDB(t, ctx, db, "insert into uniq_left values (3, 30)")
	execSQLDB(t, ctx, db, "insert into uniq_right values (4, 30)")
	execSQLDBExpectError(t, ctx, db, "data branch merge uniq_right into uniq_left when conflict accept")

	require.Equal(t, int64(2), fetchSingleInt64(t, ctx, db, "select count(*) from single_left"))
	require.Equal(t, int64(2), fetchSingleInt64(t, ctx, db, "select count(*) from comp_left"))
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

func fetchDiffSummaryMetrics(t *testing.T, ctx context.Context, db *sql.DB, stmt string) map[string][2]int64 {
	t.Helper()

	rows, err := db.QueryContext(ctx, stmt)
	require.NoErrorf(t, err, "sql: %s", stmt)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equalf(t, 3, len(cols), "summary result should have 3 columns: %s", stmt)

	result := make(map[string][2]int64, 3)
	for rows.Next() {
		var (
			metric string
			left   int64
			right  int64
		)
		require.NoError(t, rows.Scan(&metric, &left, &right))
		result[strings.ToUpper(metric)] = [2]int64{left, right}
	}
	require.NoErrorf(t, rows.Err(), "sql: %s", stmt)
	require.Lenf(t, result, 3, "summary should include 3 metrics: %s", stmt)
	require.Containsf(t, result, "INSERTED", "summary missing INSERTED: %s", stmt)
	require.Containsf(t, result, "DELETED", "summary missing DELETED: %s", stmt)
	require.Containsf(t, result, "UPDATED", "summary missing UPDATED: %s", stmt)
	return result
}

func fetchDiffCount(t *testing.T, ctx context.Context, db *sql.DB, stmt string) int64 {
	t.Helper()

	var cnt int64
	err := db.QueryRowContext(ctx, stmt).Scan(&cnt)
	require.NoErrorf(t, err, "sql: %s", stmt)
	return cnt
}

func assertSummaryMetrics(
	t *testing.T,
	summary map[string][2]int64,
	inserted [2]int64,
	deleted [2]int64,
	updated [2]int64,
) {
	t.Helper()

	require.Equal(t, inserted, summary["INSERTED"], "INSERTED metric mismatch")
	require.Equal(t, deleted, summary["DELETED"], "DELETED metric mismatch")
	require.Equal(t, updated, summary["UPDATED"], "UPDATED metric mismatch")
}

func assertSummaryMatchesCount(t *testing.T, summary map[string][2]int64, count int64) {
	t.Helper()

	total := int64(0)
	for _, metric := range summary {
		total += metric[0] + metric[1]
	}
	require.Equal(t, total, count, "summary total should match output count")
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

func fetchShowCreateTable(t *testing.T, ctx context.Context, db *sql.DB, table string) string {
	t.Helper()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("show create table %s", table))
	require.NoErrorf(t, err, "show create table %s", table)
	defer rows.Close()

	require.Truef(t, rows.Next(), "show create table %s returned no rows", table)
	var (
		tableName string
		createSQL string
	)
	require.NoError(t, rows.Scan(&tableName, &createSQL))
	require.Falsef(t, rows.Next(), "show create table %s returned unexpected extra rows", table)
	require.NoErrorf(t, rows.Err(), "show create table %s", table)
	require.NotEmpty(t, createSQL, "show create table %s returned empty create sql", table)
	return createSQL
}

func fetchSingleInt64(t *testing.T, ctx context.Context, db *sql.DB, stmt string) int64 {
	t.Helper()

	var value int64
	err := db.QueryRowContext(ctx, stmt).Scan(&value)
	require.NoErrorf(t, err, "sql: %s", stmt)
	return value
}

func fetchSingleString(t *testing.T, ctx context.Context, db *sql.DB, stmt string) string {
	t.Helper()

	var value string
	err := db.QueryRowContext(ctx, stmt).Scan(&value)
	require.NoErrorf(t, err, "sql: %s", stmt)
	return value
}

func execSQLDB(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	_, err := db.ExecContext(ctx, stmt)
	require.NoErrorf(t, err, "sql: %s", stmt)
}

func execSQLDBExpectError(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	_, err := db.ExecContext(ctx, stmt)
	require.Errorf(t, err, "expected error for sql: %s", stmt)
}
