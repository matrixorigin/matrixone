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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

func TestForcedMultiCNDeleteAndInsertIgnore(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		internalExec := testutils.GetSQLExecutor(cn)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		dbPrefix := testutils.GetDatabaseName(t)
		deleteDB := dbPrefix + "_delete"
		castDB := dbPrefix + "_cast"
		defer cleanupTestDatabases(t, db, deleteDB, castDB)
		execSQLDB(t, ctx, db, "create database `"+deleteDB+"`")
		execSQLDB(t, ctx, db, "create database `"+castDB+"`")

		const deleteTable = "forced_delete"
		deleteOpts := executor.Options{}.WithDatabase(deleteDB)
		for _, statement := range []string{
			"create table " + deleteTable + " (a varchar primary key, b varchar)",
			"insert into " + deleteTable + " values ('1','1'),('2','2'),('3','3'),('7','7'),('8','8')",
		} {
			res, execErr := internalExec.Exec(ctx, statement, deleteOpts)
			require.NoError(t, execErr)
			res.Close()
		}

		execSQLDB(t, ctx, db, "use `"+castDB+"`")
		execSQLDB(t, ctx, db, "set session sql_mode = 'STRICT_TRANS_TABLES'")
		execSQLDB(t, ctx, db, "create table forced_src (v int)")
		execSQLDB(t, ctx, db, "insert into forced_src values (31),(31),(31),(31)")
		execSQLDB(t, ctx, db, "create table forced_dst (b bit(4))")

		// Force only the operations under test. Applying this process-wide test
		// hook to fixture DDL would exercise an unrelated execution path and can
		// make setup contend with the test's frontend session.
		defer plan.SetForceScanOnMultiCN(false)
		plan.SetForceScanOnMultiCN(true)

		t.Run("delete and select retain exact rows", func(t *testing.T) {
			execSQLDB(t, ctx, db, "use `"+deleteDB+"`")
			planResult, planErr := testutils.QueryTextResult(ctx, db,
				"explain phyplan select * from "+deleteTable+" where a >= '7'")
			require.NoError(t, planErr)
			require.Contains(t, strings.ToUpper(planResult.ColumnName), "PHYPLAN ON MULTICN(")
			require.Contains(t, planResult.Text, "Magic: Remote",
				"the small fixture must still compile a real remote scan scope")

			selected, execErr := internalExec.Exec(ctx,
				"select a,b from "+deleteTable+" where a >= '7' order by a", deleteOpts)
			require.NoError(t, execErr)
			var selectedRows [][2]string
			for _, batch := range selected.Batches {
				as := executor.GetStringRows(batch.Vecs[0])
				bs := executor.GetStringRows(batch.Vecs[1])
				for i := range as {
					selectedRows = append(selectedRows, [2]string{as[i], bs[i]})
				}
			}
			selected.Close()
			require.Equal(t, [][2]string{{"7", "7"}, {"8", "8"}}, selectedRows)

			deleted, execErr := internalExec.Exec(ctx,
				"delete from "+deleteTable+" where a >= '7'", deleteOpts)
			require.NoError(t, execErr)
			require.Equal(t, uint64(2), deleted.AffectedRows)
			deleted.Close()

			remaining, execErr := internalExec.Exec(ctx,
				"select a from "+deleteTable+" order by a", deleteOpts)
			require.NoError(t, execErr)
			var remainingKeys []string
			for _, batch := range remaining.Batches {
				remainingKeys = append(remainingKeys, executor.GetStringRows(batch.Vecs[0])...)
			}
			remaining.Close()
			require.Equal(t, []string{"1", "2", "3"}, remainingKeys)
		})

		t.Run("insert ignore evaluates assignment cast remotely", func(t *testing.T) {
			execSQLDB(t, ctx, db, "use `"+castDB+"`")
			planResult, planErr := testutils.QueryTextResult(ctx, db,
				"explain phyplan select v from forced_src")
			require.NoError(t, planErr)
			require.Contains(t, strings.ToUpper(planResult.ColumnName), "PHYPLAN ON MULTICN(")
			require.Contains(t, planResult.Text, "Magic: Remote",
				"the insert source must be scanned by a real remote scope")

			execSQLDB(t, ctx, db, "insert ignore into forced_dst select v from forced_src")
			var count, min, max int
			err = db.QueryRowContext(ctx,
				"select count(*), min(b + 0), max(b + 0) from forced_dst").Scan(&count, &min, &max)
			require.NoError(t, err)
			require.Equal(t, 4, count)
			require.Equal(t, 15, min)
			require.Equal(t, 15, max)
		})
	})
}

func TestDataBranchDiffAsFile(t *testing.T) {
	embed.RunBaseClusterTests(t,
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
			sqlDB.SetMaxOpenConns(1)

			dbName := testutils.GetDatabaseName(t)
			defer cleanupTestDatabases(t, sqlDB, dbName)
			execSQLDB(t, ctx, sqlDB, fmt.Sprintf("create database `%s`", dbName))
			execSQLDB(t, ctx, sqlDB, fmt.Sprintf("use `%s`", dbName))

			t.Run("single_pk_with_base", func(t *testing.T) {
				runSinglePKWithBase(t, ctx, sqlDB, dbName)
			})
			t.Run("composite_pk_with_base", func(t *testing.T) {
				runMultiPKWithBase(t, ctx, sqlDB, dbName)
			})
			t.Run("single_pk_without_base", func(t *testing.T) {
				runSinglePKNoBase(t, ctx, sqlDB, dbName)
			})
			t.Run("composite_pk_without_base", func(t *testing.T) {
				runMultiPKNoBase(t, ctx, sqlDB, dbName)
			})
			t.Run("composite_multi_column_mutations", func(t *testing.T) {
				runCompositeDiffMultiColumn(t, ctx, sqlDB, dbName)
			})
			t.Run("single_pk_update_split", func(t *testing.T) {
				runUpdateSplitDiffAsFile(t, ctx, sqlDB, dbName)
			})
			t.Run("composite_pk_update_split", func(t *testing.T) {
				runCompositeUpdateSplitDiffAsFile(t, ctx, sqlDB, dbName)
			})
			t.Run("no_pk_duplicates_and_null_delete", func(t *testing.T) {
				runNoPKDuplicateDiffAsFile(t, ctx, sqlDB, dbName)
			})
			t.Run("complex_types_and_string_edges", func(t *testing.T) {
				runComplexTypeDiffAsFile(t, ctx, sqlDB, dbName)
			})
			t.Run("sql_null_values", func(t *testing.T) {
				runSQLDiffHandlesNulls(t, ctx, sqlDB, dbName)
			})
			t.Run("database_branch_metadata", func(t *testing.T) {
				runBranchDatabaseMetadata(t, ctx, sqlDB, dbName+"_metadata")
				execSQLDB(t, ctx, sqlDB, fmt.Sprintf("use `%s`", dbName))
			})
			t.Run("csv_multi_block_round_trip", func(t *testing.T) {
				runCSVLoadSimple(t, ctx, sqlDB, dbName)
			})
			t.Run("csv_rich_types_round_trip", func(t *testing.T) {
				runCSVLoadRichTypes(t, ctx, sqlDB, dbName)
			})
			t.Run("csv_user_diff_prefix_identifier", func(t *testing.T) {
				runCSVUserDiffPrefixIdentifier(t, ctx, sqlDB, dbName)
			})
			t.Run("output_limit_subset", func(t *testing.T) {
				runDiffOutputLimitSubset(t, ctx, sqlDB, dbName)
			})
			t.Run("output_limit_without_base", func(t *testing.T) {
				runDiffOutputLimitNoBase(t, ctx, sqlDB, dbName)
			})
			t.Run("output_limit_multi_block", func(t *testing.T) {
				runDiffOutputLimitMultiBlockBase(t, ctx, sqlDB, dbName)
			})
			t.Run("output_summary", func(t *testing.T) {
				runDiffOutputSummaryComplex(t, ctx, sqlDB, dbName)
			})
			t.Run("stage_round_trip", func(t *testing.T) {
				runDiffOutputToStage(t, ctx, sqlDB, dbName)
			})
			t.Run("update_apply_special_columns", func(t *testing.T) {
				runDataBranchUpdateApplySpecialColumns(t, ctx, sqlDB)
			})
		})
}

func runDataBranchUpdateApplySpecialColumns(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	t.Run("generated_primary_key", func(t *testing.T) {
		execSQLDB(t, ctx, db, "create table update_generated_base (a int, b int generated always as (a * 2) stored, payload int, primary key (b))")
		execSQLDB(t, ctx, db, "insert into update_generated_base(a, payload) values (1, 10)")
		execSQLDB(t, ctx, db, "data branch create table update_generated_branch from update_generated_base")
		execSQLDB(t, ctx, db, "update update_generated_branch set payload = 11 where b = 2")
		execSQLDB(t, ctx, db, "data branch merge update_generated_branch into update_generated_base when conflict accept")
		require.Equal(t, [][]string{{"1", "2", "11"}}, queryStringRows(t, ctx, db, "select a, b, payload from update_generated_base"))

		execSQLDB(t, ctx, db, "create table update_generated_pick_base (a int, b int generated always as (a * 2) stored, payload int, primary key (b))")
		execSQLDB(t, ctx, db, "insert into update_generated_pick_base(a, payload) values (1, 10)")
		execSQLDB(t, ctx, db, "data branch create table update_generated_pick_src from update_generated_pick_base")
		execSQLDB(t, ctx, db, "data branch create table update_generated_pick_dst from update_generated_pick_base")
		execSQLDB(t, ctx, db, "update update_generated_pick_src set payload = 11 where b = 2")
		execSQLDB(t, ctx, db, "data branch pick update_generated_pick_src into update_generated_pick_dst keys(2) when conflict accept")
		require.Equal(t, [][]string{{"1", "2", "11"}}, queryStringRows(t, ctx, db, "select a, b, payload from update_generated_pick_dst"))

		execSQLDB(t, ctx, db, "create table update_generated_special_base (a int, b int generated always as (a * 2) stored, v set('a','b','c'), primary key (b))")
		execSQLDB(t, ctx, db, "insert into update_generated_special_base(a, v) values (1, 'a')")
		execSQLDB(t, ctx, db, "data branch create table update_generated_special_branch from update_generated_special_base")
		execSQLDB(t, ctx, db, "update update_generated_special_branch set v = 'b,c' where b = 2")
		execSQLDB(t, ctx, db, "data branch merge update_generated_special_branch into update_generated_special_base when conflict accept")
		require.Equal(t, [][]string{{"1", "2", "b,c"}}, queryStringRows(t, ctx, db, "select a, b, cast(v as char) from update_generated_special_base"))

		execSQLDB(t, ctx, db, "create table update_generated_special_pick_base (a int, b int generated always as (a * 2) stored, v set('a','b','c'), primary key (b))")
		execSQLDB(t, ctx, db, "insert into update_generated_special_pick_base(a, v) values (1, 'a')")
		execSQLDB(t, ctx, db, "data branch create table update_generated_special_pick_src from update_generated_special_pick_base")
		execSQLDB(t, ctx, db, "data branch create table update_generated_special_pick_dst from update_generated_special_pick_base")
		execSQLDB(t, ctx, db, "update update_generated_special_pick_src set v = 'b,c' where b = 2")
		execSQLDB(t, ctx, db, "data branch pick update_generated_special_pick_src into update_generated_special_pick_dst keys(2) when conflict accept")
		require.Equal(t, [][]string{{"1", "2", "b,c"}}, queryStringRows(t, ctx, db, "select a, b, cast(v as char) from update_generated_special_pick_dst"))
	})

	t.Run("set", func(t *testing.T) {
		execSQLDB(t, ctx, db, "create table update_set_base (id int primary key, v set('a','b','c'))")
		execSQLDB(t, ctx, db, "insert into update_set_base values (1, 'a')")
		execSQLDB(t, ctx, db, "data branch create table update_set_branch from update_set_base")
		execSQLDB(t, ctx, db, "update update_set_branch set v = 'b,c' where id = 1")
		execSQLDB(t, ctx, db, "data branch merge update_set_branch into update_set_base when conflict accept")
		require.Equal(t, [][]string{{"1", "b,c"}}, queryStringRows(t, ctx, db, "select id, cast(v as char) from update_set_base"))

		execSQLDB(t, ctx, db, "create table update_set_fk_merge_base (id int primary key, v set('a','b','c'))")
		execSQLDB(t, ctx, db, "create table update_set_fk_merge_child (id int primary key, parent_id int, constraint fk_set_merge foreign key (parent_id) references update_set_fk_merge_base(id))")
		execSQLDB(t, ctx, db, "insert into update_set_fk_merge_base values (1, 'a')")
		execSQLDB(t, ctx, db, "insert into update_set_fk_merge_child values (1, 1)")
		execSQLDB(t, ctx, db, "data branch create table update_set_fk_merge_branch from update_set_fk_merge_base")
		execSQLDB(t, ctx, db, "update update_set_fk_merge_branch set v = 'b,c' where id = 1")
		execSQLDB(t, ctx, db, "data branch merge update_set_fk_merge_branch into update_set_fk_merge_base when conflict accept")
		require.Equal(t, [][]string{{"1", "b,c", "1"}}, queryStringRows(t, ctx, db, "select p.id, cast(p.v as char), c.parent_id from update_set_fk_merge_base p join update_set_fk_merge_child c on p.id = c.parent_id"))

		execSQLDB(t, ctx, db, "create table update_set_fk_pick_base (id int primary key, v set('a','b','c'))")
		execSQLDB(t, ctx, db, "create table update_set_fk_pick_child (id int primary key, parent_id int, constraint fk_set_pick foreign key (parent_id) references update_set_fk_pick_base(id))")
		execSQLDB(t, ctx, db, "insert into update_set_fk_pick_base values (1, 'a')")
		execSQLDB(t, ctx, db, "insert into update_set_fk_pick_child values (1, 1)")
		execSQLDB(t, ctx, db, "data branch create table update_set_fk_pick_src from update_set_fk_pick_base")
		execSQLDB(t, ctx, db, "update update_set_fk_pick_src set v = 'b,c' where id = 1")
		execSQLDB(t, ctx, db, "update update_set_fk_pick_base set v = 'c' where id = 1")
		execSQLDB(t, ctx, db, "data branch pick update_set_fk_pick_src into update_set_fk_pick_base keys(1) when conflict accept")
		require.Equal(t, [][]string{{"1", "b,c", "1"}}, queryStringRows(t, ctx, db, "select p.id, cast(p.v as char), c.parent_id from update_set_fk_pick_base p join update_set_fk_pick_child c on p.id = c.parent_id"))
	})

	t.Run("geometry32", func(t *testing.T) {
		execSQLDB(t, ctx, db, "create table update_geometry_base (id int primary key, g geometry32)")
		execSQLDB(t, ctx, db, "insert into update_geometry_base values (1, cast('POINT(1 1)' as geometry32))")
		execSQLDB(t, ctx, db, "data branch create table update_geometry_branch from update_geometry_base")
		execSQLDB(t, ctx, db, "update update_geometry_branch set g = cast('POINT(2 2)' as geometry32) where id = 1")
		execSQLDB(t, ctx, db, "data branch merge update_geometry_branch into update_geometry_base when conflict accept")
		require.Equal(t, [][]string{{"1", "POINT(2 2)"}}, queryStringRows(t, ctx, db, "select id, st_astext(g) from update_geometry_base"))
	})

	t.Run("indexed_enum", func(t *testing.T) {
		execSQLDB(t, ctx, db, "create table update_enum_unique_merge_base (id int primary key, status enum('new','paid','shipped'), unique key uk_status(status))")
		execSQLDB(t, ctx, db, "insert into update_enum_unique_merge_base values (1, 'new'), (2, 'paid')")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_merge_branch from update_enum_unique_merge_base")
		execSQLDB(t, ctx, db, "update update_enum_unique_merge_branch set status = 'shipped' where id = 1")
		execSQLDB(t, ctx, db, "data branch merge update_enum_unique_merge_branch into update_enum_unique_merge_base when conflict accept")
		require.Equal(t, [][]string{{"1", "shipped"}, {"2", "paid"}}, queryStringRows(t, ctx, db, "select id, cast(status as char) from update_enum_unique_merge_base order by id"))

		execSQLDB(t, ctx, db, "create table update_enum_unique_payload_base (id int primary key, payload varchar(32), status enum('new','paid'), unique key uk_status(status))")
		execSQLDB(t, ctx, db, "insert into update_enum_unique_payload_base values (1, 'one', 'new'), (2, 'two', 'paid')")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_payload_branch from update_enum_unique_payload_base")
		execSQLDB(t, ctx, db, "update update_enum_unique_payload_branch set payload = 'branch-one' where id = 1")
		execSQLDB(t, ctx, db, "data branch merge update_enum_unique_payload_branch into update_enum_unique_payload_base when conflict accept")
		require.Equal(t, [][]string{{"1", "branch-one", "new"}, {"2", "two", "paid"}}, queryStringRows(t, ctx, db, "select id, payload, cast(status as char) from update_enum_unique_payload_base order by id"))

		execSQLDB(t, ctx, db, "create table update_enum_unique_merge_duplicate_base (id int primary key, status enum('new','paid','shipped'), unique key uk_status(status))")
		execSQLDB(t, ctx, db, "insert into update_enum_unique_merge_duplicate_base values (1, 'new'), (2, 'paid')")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_merge_duplicate_branch from update_enum_unique_merge_duplicate_base")
		execSQLDB(t, ctx, db, "update update_enum_unique_merge_duplicate_branch set status = 'shipped' where id = 1")
		execSQLDB(t, ctx, db, "update update_enum_unique_merge_duplicate_base set status = 'shipped' where id = 2")
		_, err := db.ExecContext(ctx, "data branch merge update_enum_unique_merge_duplicate_branch into update_enum_unique_merge_duplicate_base when conflict accept")
		require.Error(t, err)
		require.Equal(t, [][]string{{"1", "new"}, {"2", "shipped"}}, queryStringRows(t, ctx, db, "select id, cast(status as char) from update_enum_unique_merge_duplicate_base order by id"))

		execSQLDB(t, ctx, db, "create table update_enum_unique_pick_base (id int primary key, status enum('new','paid','shipped'), unique key uk_status(status))")
		execSQLDB(t, ctx, db, "insert into update_enum_unique_pick_base values (1, 'new'), (2, 'paid')")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_pick_src from update_enum_unique_pick_base")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_pick_dst from update_enum_unique_pick_base")
		execSQLDB(t, ctx, db, "update update_enum_unique_pick_src set status = 'shipped' where id = 1")
		execSQLDB(t, ctx, db, "data branch pick update_enum_unique_pick_src into update_enum_unique_pick_dst keys(1) when conflict accept")
		require.Equal(t, [][]string{{"1", "shipped"}, {"2", "paid"}}, queryStringRows(t, ctx, db, "select id, cast(status as char) from update_enum_unique_pick_dst order by id"))

		execSQLDB(t, ctx, db, "create table update_enum_unique_pick_duplicate_base (id int primary key, status enum('new','paid','shipped'), unique key uk_status(status))")
		execSQLDB(t, ctx, db, "insert into update_enum_unique_pick_duplicate_base values (1, 'new'), (2, 'paid')")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_pick_duplicate_src from update_enum_unique_pick_duplicate_base")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_pick_duplicate_dst from update_enum_unique_pick_duplicate_base")
		execSQLDB(t, ctx, db, "update update_enum_unique_pick_duplicate_src set status = 'shipped' where id = 1")
		execSQLDB(t, ctx, db, "update update_enum_unique_pick_duplicate_dst set status = 'shipped' where id = 2")
		_, err = db.ExecContext(ctx, "data branch pick update_enum_unique_pick_duplicate_src into update_enum_unique_pick_duplicate_dst keys(1) when conflict accept")
		require.Error(t, err)
		require.Equal(t, [][]string{{"1", "new"}, {"2", "shipped"}}, queryStringRows(t, ctx, db, "select id, cast(status as char) from update_enum_unique_pick_duplicate_dst order by id"))
	})

	t.Run("indexed_enum_conflict_accept", func(t *testing.T) {
		execSQLDB(t, ctx, db, "create table update_enum_unique_conflict_merge_base (id int primary key, payload varchar(32), status enum('new','paid','shipped'), unique key uk_status(status))")
		execSQLDB(t, ctx, db, "insert into update_enum_unique_conflict_merge_base values (1, 'base-one', 'new'), (2, 'two', 'paid')")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_conflict_merge_src from update_enum_unique_conflict_merge_base")
		execSQLDB(t, ctx, db, "update update_enum_unique_conflict_merge_src set payload = 'source-one' where id = 1")
		execSQLDB(t, ctx, db, "update update_enum_unique_conflict_merge_base set status = 'shipped' where id = 1")
		execSQLDB(t, ctx, db, "data branch merge update_enum_unique_conflict_merge_src into update_enum_unique_conflict_merge_base when conflict accept")
		require.Equal(t,
			[][]string{{"1", "source-one", "new"}, {"2", "two", "paid"}},
			queryStringRows(t, ctx, db, "select id, payload, cast(status as char) from update_enum_unique_conflict_merge_base order by id"),
		)

		execSQLDB(t, ctx, db, "create table update_enum_unique_conflict_pick_base (id int primary key, payload varchar(32), status enum('new','paid','shipped'), unique key uk_status(status))")
		execSQLDB(t, ctx, db, "insert into update_enum_unique_conflict_pick_base values (1, 'base-one', 'new'), (2, 'two', 'paid')")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_conflict_pick_src from update_enum_unique_conflict_pick_base")
		execSQLDB(t, ctx, db, "data branch create table update_enum_unique_conflict_pick_dst from update_enum_unique_conflict_pick_base")
		execSQLDB(t, ctx, db, "update update_enum_unique_conflict_pick_src set payload = 'source-one' where id = 1")
		execSQLDB(t, ctx, db, "update update_enum_unique_conflict_pick_dst set status = 'shipped' where id = 1")
		execSQLDB(t, ctx, db, "data branch pick update_enum_unique_conflict_pick_src into update_enum_unique_conflict_pick_dst keys(1) when conflict accept")
		require.Equal(t,
			[][]string{{"1", "source-one", "new"}, {"2", "two", "paid"}},
			queryStringRows(t, ctx, db, "select id, payload, cast(status as char) from update_enum_unique_conflict_pick_dst order by id"),
		)
	})

	t.Run("mixed_schema_ordinary_assignment", func(t *testing.T) {
		execSQLDB(t, ctx, db, "create table update_mixed_merge_base (id int primary key, payload varchar(32), status enum('new','ready'))")
		execSQLDB(t, ctx, db, "insert into update_mixed_merge_base values (1, 'one', 'new'), (2, 'two', 'ready'), (3, 'three', 'new')")
		execSQLDB(t, ctx, db, "data branch create table update_mixed_merge_branch from update_mixed_merge_base")
		execSQLDB(t, ctx, db, "update update_mixed_merge_branch set payload = concat('branch-', payload) where id in (1, 2, 3)")
		execSQLDB(t, ctx, db, "data branch merge update_mixed_merge_branch into update_mixed_merge_base when conflict accept")
		require.Equal(t,
			[][]string{{"1", "branch-one", "new"}, {"2", "branch-two", "ready"}, {"3", "branch-three", "new"}},
			queryStringRows(t, ctx, db, "select id, payload, cast(status as char) from update_mixed_merge_base order by id"),
		)

		execSQLDB(t, ctx, db, "create table update_mixed_pick_base (id int primary key, payload varchar(32), status enum('new','ready'))")
		execSQLDB(t, ctx, db, "insert into update_mixed_pick_base values (1, 'one', 'new'), (2, 'two', 'ready'), (3, 'three', 'new')")
		execSQLDB(t, ctx, db, "data branch create table update_mixed_pick_src from update_mixed_pick_base")
		execSQLDB(t, ctx, db, "data branch create table update_mixed_pick_dst from update_mixed_pick_base")
		execSQLDB(t, ctx, db, "update update_mixed_pick_src set payload = concat('branch-', payload) where id in (1, 2, 3)")
		execSQLDB(t, ctx, db, "data branch pick update_mixed_pick_src into update_mixed_pick_dst keys(1, 2, 3) when conflict accept")
		require.Equal(t,
			[][]string{{"1", "branch-one", "new"}, {"2", "branch-two", "ready"}, {"3", "branch-three", "new"}},
			queryStringRows(t, ctx, db, "select id, payload, cast(status as char) from update_mixed_pick_dst order by id"),
		)
	})
}

func cleanupTestDatabases(t *testing.T, db *sql.DB, names ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, "use mo_catalog"); err != nil {
		t.Errorf("select cleanup database: %v", err)
	}
	for _, name := range names {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("drop database if exists `%s`", name)); err != nil {
			t.Errorf("drop cleanup database %s: %v", name, err)
		}
	}
}

func TestCloneCommitFailureRollbackKeepsSourceFiles(t *testing.T) {
	embed.RunBaseClusterTests(t,
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

			runCloneCommitFailureRollbackKeepsSourceFiles(t, ctx, sqlDB)
		})
}

func runCloneCommitFailureRollbackKeepsSourceFiles(t *testing.T, parentCtx context.Context, db *sql.DB) {
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
	execSQLDB(t, ctx, db, "create table src (id int primary key, value int, note varchar(32))")

	// Force the source insert and clone transaction to use CN object files, then
	// fail commit after workspace dump to exercise rollback with clone metadata alive.
	fault.Enable()
	defer fault.Disable()

	removeForceFlush, err := objectio.SimpleInject(objectio.FJ_CNWorkspaceForceFlush)
	require.NoError(t, err)
	defer removeForceFlush()

	execSQLDB(t, ctx, db, "insert into src select result, result * 10, concat('seed_', cast(result as char)) from generate_series(1,5000) g")
	require.Equal(t, 5000, queryRowCount(t, ctx, db, "select count(*) from src"))

	removeCommitFailure, err := objectio.SimpleInject(objectio.FJ_CNCommitAfterWorkspaceDumpFailed)
	require.NoError(t, err)
	defer removeCommitFailure()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	connClosed := false
	defer func() {
		if !connClosed {
			_ = conn.Close()
		}
	}()

	_, err = conn.ExecContext(ctx, fmt.Sprintf("use `%s`", dbName))
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "begin")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create table clone_t clone src")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "alter table clone_t add column added int default 7")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "commit")
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected commit failure after workspace dump")
	_ = conn.Close()
	connClosed = true

	removeCommitFailure()

	require.Equal(t, 0, queryRowCount(t, ctx, db,
		fmt.Sprintf("select count(*) from information_schema.tables where table_schema = '%s' and table_name = 'clone_t'", dbName)))
	require.Equal(t, 5000, queryRowCount(t, ctx, db, "select count(*) from src"))
	require.Equal(t, 50, queryRowCount(t, ctx, db, "select count(*) from src where id mod 100 = 0"))
}

func runSinglePKWithBase(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "single_pk_base"
	branch := "single_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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
	require.Contains(t, lowerContent, "insert into "+diffSQLTable(dbName, base))

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runMultiPKWithBase(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "multi_pk_base"
	branch := "multi_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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
	require.Contains(t, lowerContent, "insert into "+diffSQLTable(dbName, base))
	require.Contains(t, lowerContent, "delete from "+diffSQLTable(dbName, base)+" where "+diffSQLColumns("org_id", "event_id"))

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runSinglePKNoBase(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "single_pk_nobranch_base"
	target := "single_pk_nobranch_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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

func runMultiPKNoBase(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "multi_pk_nobranch_base"
	target := "multi_pk_nobranch_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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

func runCompositeDiffMultiColumn(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*150)
	defer cancel()

	base := "composite_base"
	branch := "composite_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")
	const (
		baseRows   = 2000
		insertRows = 200
	)

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
from generate_series(1, %d) as g`, base, baseRows)
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
from generate_series(%d, %d) as g`, branch, baseRows+1, baseRows+insertRows)
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
	require.Contains(t, lowerContent, "insert into "+diffSQLTable(dbName, base))
	require.Contains(t, lowerContent, "delete from "+diffSQLTable(dbName, base))

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runSQLDiffHandlesNulls(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	base := "sql_null_base"
	branch := "sql_null_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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

func runCSVLoadSimple(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*180)
	defer cancel()

	base := "csv_range_base"
	target := "csv_range_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")
	rowCount := int(objectio.BlockMaxRows) * 2

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (a int primary key, b int)", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s like %s", target, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s select *, * from generate_series(1, %d) g", target, rowCount))

	diffStmt := fmt.Sprintf("data branch diff %s against %s output file '%s'", target, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".csv", filepath.Ext(diffPath))
	require.True(t, strings.HasPrefix(diffPath, diffDir), "diff file %s not in dir %s", diffPath, diffDir)

	loadDiffCSVIntoTable(t, ctx, db, base, diffPath)
	assertTablesEqual(t, ctx, db, dbName, target, base)
}

func runCSVLoadRichTypes(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*180)
	defer cancel()

	base := "csv_rich_types_base"
	target := "csv_rich_types_target"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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

func runCSVUserDiffPrefixIdentifier(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "user_prefix_csv_base"
	target := "__mo_diff_orders"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` (id int primary key, value varchar(32))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` like `%s`", target, base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into `%s` values (1, 'first'), (2, 'second')", target))

	diffStmt := fmt.Sprintf("data branch diff `%s` against `%s` output file '%s'", target, base, diffLiteral)
	diffPath := execDiffAndFetchFile(t, ctx, db, diffStmt)
	require.Equal(t, ".csv", filepath.Ext(diffPath))

	records := readDiffCSVFile(t, diffPath)
	require.ElementsMatch(t, [][]string{{"1", "first"}, {"2", "second"}}, records)

	loadDiffCSVIntoTable(t, ctx, db, base, diffPath)
	assertTablesEqual(t, ctx, db, dbName, target, base)
}

func runDiffOutputLimitSubset(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "limit_base"
	branch := "limit_branch"

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
		require.Len(t, row, 5, "projected diff should include table, flag, primary key, and requested columns")
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
		require.Len(t, row, 5, "projected limited diff should include table, flag, primary key, and requested columns")
		_, ok := projectedFullSet[strings.Join(row, "||")]
		require.Truef(t, ok, "projected limited diff row not contained in projected full diff: %v", row)
	}
}

func runDiffOutputLimitNoBase(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "limit_nobranch_base"
	target := "limit_nobranch_target"

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

func runDiffOutputLimitMultiBlockBase(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*180)
	defer cancel()

	base := "limit_multiblock_base"
	branch := "limit_multiblock_branch"
	// Three full blocks plus a tail block are the minimum deterministic shape
	// needed by this case. Larger volumes repeat the same diff/limit paths.
	rowCount := int(objectio.BlockMaxRows)*3 + 100
	updateEnd := rowCount / 4
	branchUpdateStart := updateEnd
	branchUpdateEnd := updateEnd + 1
	deleteStart := rowCount / 2
	deleteEnd := rowCount * 3 / 4

	execSQLDB(t, ctx, db, fmt.Sprintf("create table %s (a int primary key, b int, c time)", base))
	execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s select *, *, '12:34:56' from generate_series(1, %d)g", base, rowCount))

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table %s from %s", branch, base))

	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set b = b + 1 where a between 1 and %d", base, updateEnd))
	execSQLDB(t, ctx, db, fmt.Sprintf("update %s set b = b + 2 where a between %d and %d", branch, branchUpdateStart, branchUpdateEnd))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where a between %d and %d", base, deleteStart, deleteEnd))

	fullStmt := fmt.Sprintf("data branch diff %s against %s", branch, base)
	fullRows := fetchDiffRowsAsStrings(t, ctx, db, fullStmt)
	//require.Equal(t, 30, len(fullRows), fmt.Sprintf("full diff: %v", fullRows))
	t.Logf("full diff: %d", len(fullRows))
	fullSet := make(map[string]struct{}, len(fullRows))
	for _, row := range fullRows {
		fullSet[strings.Join(row, "||")] = struct{}{}
	}

	limitQuery := func(cnt int) {
		limitStmt := fmt.Sprintf("data branch diff %s against %s output limit %d", branch, base, cnt)
		limitedRows := fetchDiffRowsAsStrings(t, ctx, db, limitStmt)

		require.NotEmpty(t, limitedRows, "limited diff returned no rows")
		require.LessOrEqual(t, len(limitedRows), cnt, fmt.Sprintf("limited diff returned too many rows: %v", limitedRows))

		for _, row := range limitedRows {
			_, ok := fullSet[strings.Join(row, "||")]
			require.Truef(t, ok, "limited diff row not contained in full diff: %v", row)
		}
	}

	limitQuery(len(fullRows) * 1 / 100)
	limitQuery(len(fullRows) * 20 / 100)
}

func runDiffOutputSummaryComplex(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*150)
	defer cancel()

	seed := "summary_seed"
	left := "summary_left"
	right := "summary_right"
	standaloneBase := "summary_standalone_base"
	standaloneTarget := "summary_standalone_target"

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

func runDiffOutputToStage(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	base := "stage_base"
	branch := "stage_branch"

	stageDir := t.TempDir()
	stageName := "stage_local_" + strings.ToLower(dbName)
	stageURL := fmt.Sprintf("file://%s", stageDir)

	execSQLDB(t, ctx, db, "set role moadmin")
	execSQLDB(t, ctx, db, fmt.Sprintf("create stage %s url = '%s'", stageName, stageURL))
	defer execSQLDB(t, ctx, db, fmt.Sprintf("drop stage if exists %s", stageName))

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
	require.Contains(t, sqlContent, "insert into "+diffSQLTable(dbName, base))
	require.Contains(t, sqlContent, "delete from "+diffSQLTable(dbName, base))

	applyDiffStatements(t, ctx, db, string(payload))
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runUpdateSplitDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "split_pk_base"
	branch := "split_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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
	baseTable := diffSQLTable(dbName, base)
	require.Contains(t, lowerContent, "update "+baseTable+" as branch_apply_base join "+diffSQLIdent(dbName)+".`__mo_diff_upd_")
	require.Contains(t, lowerContent,
		"branch_apply_base."+diffSQLIdent("id")+" = branch_apply_stage."+diffSQLIdent("branch_apply_key_0"))
	require.Contains(t, lowerContent,
		"set branch_apply_base."+diffSQLIdent("score")+" = branch_apply_stage."+diffSQLIdent("score")+
			",branch_apply_base."+diffSQLIdent("note")+" = branch_apply_stage."+diffSQLIdent("note"))
	require.NotContains(t, lowerContent, "update "+baseTable+" set")
	require.NotContains(t, lowerContent, "insert into "+baseTable)
	require.NotContains(t, lowerContent, "delete from "+baseTable)

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runCompositeUpdateSplitDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	base := "split_comp_base"
	branch := "split_comp_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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
	baseTable := diffSQLTable(dbName, base)
	require.Contains(t, lowerContent, "update "+baseTable+" as branch_apply_base join "+diffSQLIdent(dbName)+".`__mo_diff_upd_")
	require.Contains(t, lowerContent,
		"branch_apply_base."+diffSQLIdent("org_id")+" = branch_apply_stage."+diffSQLIdent("branch_apply_key_0")+
			" and branch_apply_base."+diffSQLIdent("event_id")+" = branch_apply_stage."+diffSQLIdent("branch_apply_key_1"))
	require.Contains(t, lowerContent,
		"set branch_apply_base."+diffSQLIdent("qty")+" = branch_apply_stage."+diffSQLIdent("qty")+
			",branch_apply_base."+diffSQLIdent("note")+" = branch_apply_stage."+diffSQLIdent("note"))
	require.NotContains(t, lowerContent, "update "+baseTable+" set")
	require.Contains(t, lowerContent, "null")
	require.NotContains(t, lowerContent, "insert into "+baseTable)
	require.NotContains(t, lowerContent, "delete from "+baseTable)

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runNoPKDuplicateDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	base := "no_pk_base"
	branch := "no_pk_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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
	require.Contains(t, lowerContent, "insert into "+diffSQLTable(dbName, base))
	require.Contains(t, lowerContent, "delete from "+diffSQLTable(dbName, base))
	require.Contains(t, lowerContent, "limit 1")
	require.Contains(t, lowerContent, "is null")
	require.NotContains(t, lowerContent, "update ")

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runComplexTypeDiffAsFile(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*120)
	defer cancel()

	base := "complex_base"
	branch := "complex_branch"
	diffDir := t.TempDir()
	diffLiteral := strings.ReplaceAll(diffDir, "'", "''")

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
	baseTable := diffSQLTable(dbName, base)
	require.Contains(t, lowerContent, "update "+baseTable+" as branch_apply_base join "+diffSQLIdent(dbName)+".`__mo_diff_upd_")
	require.Contains(t, lowerContent,
		"branch_apply_base."+diffSQLIdent("id")+" = branch_apply_stage."+diffSQLIdent("branch_apply_key_0"))
	require.Contains(t, lowerContent,
		"set branch_apply_base."+diffSQLIdent("name")+" = branch_apply_stage."+diffSQLIdent("name"))
	require.NotContains(t, lowerContent, "update "+baseTable+" set")
	require.Contains(t, lowerContent, "insert into "+baseTable)
	require.Contains(t, lowerContent, "delete from "+baseTable)
	require.Contains(t, lowerContent, "null")
	require.Contains(t, lowerContent, "''")

	applyDiffStatements(t, ctx, db, sqlContent)
	assertTablesEqual(t, ctx, db, dbName, branch, base)
}

func runBranchDatabaseMetadata(t *testing.T, parentCtx context.Context, db *sql.DB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(parentCtx, time.Second*90)
	defer cancel()

	copyDB := dbName + "_copy"
	tables := []string{"tbl_one", "tbl_two"}

	defer cleanupTestDatabases(t, db, copyDB, dbName)
	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))

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

func diffSQLIdent(name string) string {
	return "`" + strings.ReplaceAll(strings.ToLower(name), "`", "``") + "`"
}

func diffSQLTable(dbName, tableName string) string {
	return diffSQLIdent(dbName) + "." + diffSQLIdent(tableName)
}

func diffSQLColumns(cols ...string) string {
	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted = append(quoted, diffSQLIdent(col))
	}
	return "(" + strings.Join(quoted, ",") + ")"
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

func execSQLDB(t *testing.T, ctx context.Context, db *sql.DB, stmt string) {
	t.Helper()
	_, err := db.ExecContext(ctx, stmt)
	require.NoErrorf(t, err, "sql: %s", stmt)
}
