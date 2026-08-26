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

		testDelete := func(t *testing.T, prefix string, indexed, prepared bool) {
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

			var parentID sql.NullInt64
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select parent_id from %s where child_id = 1", child)).Scan(&parentID))
			require.False(t, parentID.Valid)

			var count int
			queryHint := ""
			if indexed {
				queryHint = " force index(idx_parent)"
				require.NoError(t, conn.QueryRowContext(ctx,
					fmt.Sprintf("select count(*) from `%s`", hiddenIndexTable)).Scan(&count))
				require.Equal(t, 2, count)
			}
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s%s where parent_id = 100", child, queryHint)).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s%s where parent_id = 200", child, queryHint)).Scan(&count))
			require.Equal(t, 2, count)
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
	})
}
