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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28308AffectedRowsExcludeForeignKeySideEffects(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		exec := func(statement string, args ...any) sql.Result {
			t.Helper()
			result, err := conn.ExecContext(ctx, statement, args...)
			require.NoError(t, err, statement)
			return result
		}
		assertAffected := func(statement string, expected int64, args ...any) {
			t.Helper()
			result := exec(statement, args...)
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, expected, affected, statement)

			var rowCount int64
			require.NoError(t, conn.QueryRowContext(ctx, "select row_count()").Scan(&rowCount))
			require.Equal(t, expected, rowCount, "ROW_COUNT() after %s", statement)
		}
		assertRows := func(statement string, expected [][]sql.NullInt64) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			rowCount := 0
			for rows.Next() {
				actual := make([]sql.NullInt64, len(expected[0]))
				scanArgs := make([]any, len(actual))
				for i := range actual {
					scanArgs[i] = &actual[i]
				}
				require.NoError(t, rows.Scan(scanArgs...), statement)
				require.Less(t, rowCount, len(expected), statement)
				require.Equal(t, expected[rowCount], actual, statement)
				rowCount++
			}
			require.NoError(t, rows.Err(), statement)
			require.NoError(t, rows.Close(), statement)
			require.Equal(t, len(expected), rowCount, statement)
		}

		exec("drop database if exists `" + dbName + "`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists `"+dbName+"`")
		}()
		exec("create database `" + dbName + "`")
		exec("use `" + dbName + "`")

		exec("create table cascade_parent (id int primary key, value int)")
		exec("create table cascade_child (id int primary key, parent_id int, value int, " +
			"foreign key (parent_id) references cascade_parent(id) on delete cascade)")
		exec("create table cascade_grandchild (id int primary key, child_id int, " +
			"foreign key (child_id) references cascade_child(id) on delete cascade)")
		exec("insert into cascade_parent values (1, 10), (2, 20)")
		exec("insert into cascade_child values (11, 1, 110), (22, 2, 220)")
		exec("insert into cascade_grandchild values (111, 11), (222, 22)")

		assertAffected("delete from cascade_parent where id = 1", 1)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from cascade_child where parent_id = 1").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from cascade_grandchild where child_id = 11").Scan(&count))
		require.Zero(t, count)

		deleteStmt, err := conn.PrepareContext(ctx, "delete from cascade_parent where id = ?")
		require.NoError(t, err)
		defer deleteStmt.Close()
		result, err := deleteStmt.ExecContext(ctx, 2)
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected, "prepared DELETE")
		var rowCount int64
		require.NoError(t, conn.QueryRowContext(ctx, "select row_count()").Scan(&rowCount))
		require.Equal(t, int64(1), rowCount, "ROW_COUNT() after prepared DELETE")
		exec("create table self_cascade (id int primary key, parent_id int, " +
			"foreign key (parent_id) references self_cascade(id) on delete cascade)")
		exec("insert into self_cascade values (1, null), (2, 1), (3, 2)")
		assertAffected("delete from self_cascade where id = 1", 1)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from self_cascade").Scan(&count))
		require.Zero(t, count)
		exec("insert into self_cascade values (1, null), (2, 1)")
		assertAffected("replace into self_cascade values (1, null)", 2)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from self_cascade").Scan(&count))
		require.Equal(t, 1, count, "REPLACE keeps the new root and cascades the old child")

		exec("create table self_set_null (id int primary key, parent_id int, " +
			"foreign key (parent_id) references self_set_null(id) on delete set null)")
		exec("insert into self_set_null values (1, null), (2, 1)")
		assertAffected("delete from self_set_null where id = 1", 1)
		var selfParentID sql.NullInt64
		require.NoError(t, conn.QueryRowContext(ctx, "select parent_id from self_set_null where id = 2").Scan(&selfParentID))
		require.False(t, selfParentID.Valid)

		exec("create table self_set_null_multi (id int primary key, parent_a int, parent_b int, " +
			"foreign key (parent_a) references self_set_null_multi(id) on delete set null, " +
			"foreign key (parent_b) references self_set_null_multi(id) on delete set null)")
		exec("insert into self_set_null_multi values (1, null, null), (2, 1, 1)")
		assertAffected("delete from self_set_null_multi where id = 1", 1)
		var parentA, parentB sql.NullInt64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select parent_a, parent_b from self_set_null_multi where id = 2").Scan(&parentA, &parentB))
		require.False(t, parentA.Valid)
		require.False(t, parentB.Valid)

		exec("create table self_fk_boundary_single (id int primary key, k int unique, p int, " +
			"foreign key (k) references self_fk_boundary_single(id) on delete set null, " +
			"foreign key (p) references self_fk_boundary_single(k) on update restrict)")
		exec("insert into self_fk_boundary_single values (1, null, null), (2, 1, null), (3, null, 1)")
		_, err = conn.ExecContext(ctx, "delete from self_fk_boundary_single where id = 1")
		require.ErrorContains(t, err, "foreign key constraint fails",
			"a sibling self-referencing ON UPDATE RESTRICT must reject the SET NULL action")
		assertRows("select id, k, p from self_fk_boundary_single order by id", [][]sql.NullInt64{
			{{Int64: 1, Valid: true}, {Valid: false}, {Valid: false}},
			{{Int64: 2, Valid: true}, {Int64: 1, Valid: true}, {Valid: false}},
			{{Int64: 3, Valid: true}, {Valid: false}, {Int64: 1, Valid: true}},
		})

		exec("create table self_fk_boundary_combined (id int primary key, k int unique, k2 int, p int, " +
			"foreign key (k) references self_fk_boundary_combined(id) on delete set null, " +
			"foreign key (k2) references self_fk_boundary_combined(id) on delete set null, " +
			"foreign key (p) references self_fk_boundary_combined(k) on update restrict)")
		exec("insert into self_fk_boundary_combined values " +
			"(1, null, null, null), (2, 1, 1, null), (3, null, null, 1)")
		_, err = conn.ExecContext(ctx, "delete from self_fk_boundary_combined where id = 1")
		require.ErrorContains(t, err, "foreign key constraint fails",
			"the combined SET NULL action must retain the sibling ON UPDATE RESTRICT")
		assertRows("select id, k, k2, p from self_fk_boundary_combined order by id", [][]sql.NullInt64{
			{{Int64: 1, Valid: true}, {Valid: false}, {Valid: false}, {Valid: false}},
			{{Int64: 2, Valid: true}, {Int64: 1, Valid: true}, {Int64: 1, Valid: true}, {Valid: false}},
			{{Int64: 3, Valid: true}, {Valid: false}, {Valid: false}, {Int64: 1, Valid: true}},
		})

		exec("create table null_parent (id int primary key)")
		exec("create table null_child (id int primary key, parent_id int, " +
			"foreign key (parent_id) references null_parent(id) on delete set null)")
		exec("insert into null_parent values (1)")
		exec("insert into null_child values (10, 1)")
		assertAffected("delete from null_parent where id = 1", 1)
		var parentID sql.NullInt64
		require.NoError(t, conn.QueryRowContext(ctx, "select parent_id from null_child where id = 10").Scan(&parentID))
		require.False(t, parentID.Valid)

		exec("create table multi_parent (id int primary key, uk int unique)")
		exec("create table multi_child (id int primary key, parent_id int, parent_uk int, " +
			"foreign key (parent_id) references multi_parent(id) on delete set null, " +
			"foreign key (parent_uk) references multi_parent(uk) on delete set null)")
		exec("insert into multi_parent values (1, 10)")
		exec("insert into multi_child values (11, 1, 10)")
		assertAffected("delete from multi_parent where id = 1", 1)
		var parentIDByID, parentIDByUnique sql.NullInt64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select parent_id, parent_uk from multi_child where id = 11").Scan(&parentIDByID, &parentIDByUnique))
		require.False(t, parentIDByID.Valid)
		require.False(t, parentIDByUnique.Valid)

		exec("create table replace_parent (id int primary key, value int)")
		exec("create table replace_child (id int primary key, parent_id int, " +
			"foreign key (parent_id) references replace_parent(id) on delete cascade)")
		exec("insert into replace_parent values (1, 10)")
		exec("insert into replace_child values (10, 1)")
		assertAffected("replace into replace_parent values (1, 20)", 2)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from replace_child").Scan(&count))
		require.Zero(t, count)
	})
}
