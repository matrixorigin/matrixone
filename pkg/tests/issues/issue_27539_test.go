// Copyright 2021 - 2026 Matrix Origin
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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue27539DeleteSetNullMaintainsSecondaryIndex(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			mustExec(t, cleanupCtx, conn, "use mo_catalog")
			mustExec(t, cleanupCtx, conn, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		createTables := func(t *testing.T, prefix string, indexed bool) (string, string, string) {
			t.Helper()
			parent := prefix + "_parent"
			child := prefix + "_child"
			indexDDL := ""
			if indexed {
				indexDDL = ", key idx_parent(parent_id, note)"
			}
			mustExec(t, ctx, conn, fmt.Sprintf(
				"create table %s(id int primary key, code varchar(20) unique)", parent))
			mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
				child_id int primary key, parent_id int, note varchar(30)%s,
				constraint fk_%s foreign key(parent_id) references %s(id) on delete set null)`,
				child, indexDDL, prefix, parent))
			mustExec(t, ctx, conn, fmt.Sprintf(
				"insert into %s values (100, 'p100'), (200, 'p200')", parent))
			mustExec(t, ctx, conn, fmt.Sprintf(
				"insert into %s values (1,100,'s100-a'), (2,200,'s200-a'), (3,200,'s200-b')", child))
			hiddenIndexTable := ""
			if indexed {
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(`select index_table_name
					from mo_catalog.mo_indexes where column_name = 'parent_id' and index_table_name != '' and table_id =
					(select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = '%s')
					order by ordinal_position limit 1`, child)).Scan(&hiddenIndexTable))
				require.NotEmpty(t, hiddenIndexTable)
				var hiddenCount int
				require.NoError(t, conn.QueryRowContext(ctx,
					fmt.Sprintf("select count(*) from `%s`", hiddenIndexTable)).Scan(&hiddenCount))
				require.Equal(t, 3, hiddenCount)
			}
			return parent, child, hiddenIndexTable
		}

		assertSetNullState := func(t *testing.T, child, hiddenIndexTable string) {
			t.Helper()
			var parentID sql.NullInt64
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select parent_id from %s where child_id = 1", child)).Scan(&parentID))
			require.False(t, parentID.Valid)

			var count int
			if hiddenIndexTable != "" {
				require.NoError(t, conn.QueryRowContext(ctx,
					fmt.Sprintf("select count(*) from `%s`", hiddenIndexTable)).Scan(&count))
				require.Equal(t, 3, count)
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s force index(idx_parent) where parent_id is null and note = 's100-a'", child)).Scan(&count))
				require.Equal(t, 1, count)
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s force index(idx_parent) where parent_id = 100 and note = 's100-a'", child)).Scan(&count))
				require.Zero(t, count)
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s force index(idx_parent) where parent_id = 200 and note in ('s200-a', 's200-b')", child)).Scan(&count))
				require.Equal(t, 2, count)
				return
			}
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s where parent_id = 100", child)).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s where parent_id = 200", child)).Scan(&count))
			require.Equal(t, 2, count)
		}

		assertOriginalState := func(t *testing.T, parent, child, hiddenIndexTable string) {
			t.Helper()
			var count int
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where id = 100", parent)).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s force index(idx_parent) where parent_id = 100 and note = 's100-a'", child)).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s force index(idx_parent) where parent_id is null and note = 's100-a'", child)).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from `%s`", hiddenIndexTable)).Scan(&count))
			require.Equal(t, 3, count)
		}

		testDelete := func(t *testing.T, prefix string, indexed, prepared bool) {
			t.Helper()
			parent, child, hiddenIndexTable := createTables(t, prefix, indexed)
			deleteSQL := fmt.Sprintf("delete from %s where id = ?", parent)
			if prepared {
				stmt, err := conn.PrepareContext(ctx, deleteSQL)
				require.NoError(t, err)
				defer stmt.Close()
				_, err = stmt.ExecContext(ctx, 100)
				require.NoError(t, err)
			} else {
				mustExec(t, ctx, conn, fmt.Sprintf("delete from %s where id = 100", parent))
			}

			assertSetNullState(t, child, hiddenIndexTable)
		}

		t.Run("indexed literal delete", func(t *testing.T) {
			testDelete(t, "indexed_literal", true, false)
		})
		t.Run("indexed prepared delete", func(t *testing.T) {
			testDelete(t, "indexed_prepared", true, true)
		})
		t.Run("no index control", func(t *testing.T) {
			testDelete(t, "no_index", false, false)
		})
		t.Run("rollback restores composite secondary index", func(t *testing.T) {
			parent, child, hiddenIndexTable := createTables(t, "rollback", true)
			mustExec(t, ctx, conn, "begin")
			mustExec(t, ctx, conn, fmt.Sprintf("delete from %s where id = 100", parent))
			assertSetNullState(t, child, hiddenIndexTable)
			mustExec(t, ctx, conn, "rollback")
			assertOriginalState(t, parent, child, hiddenIndexTable)
		})
		t.Run("failed delete preserves composite secondary index", func(t *testing.T) {
			parent, child, hiddenIndexTable := createTables(t, "failed_delete", true)
			blocker := "failed_delete_blocker"
			mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
				id int primary key, parent_id int,
				foreign key(parent_id) references %s(id) on delete restrict)`, blocker, parent))
			mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(1, 100)", blocker))
			_, err := conn.ExecContext(ctx, fmt.Sprintf("delete from %s where id = 100", parent))
			require.Error(t, err)
			assertOriginalState(t, parent, child, hiddenIndexTable)
		})
	})
}
