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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedNumericCastNullInPartitionSelectAndUpdate(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		database := fmt.Sprintf("fold_null_partition_%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, "create database "+database)
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "drop database if exists "+database)
		_, err = conn.ExecContext(ctx, "use "+database)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `create table t (
			id int not null primary key,
			value int not null default 0
		) partition by range (id) (
			partition p0 values less than (10),
			partition p1 values less than maxvalue
		)`)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into t(id) values (1),(2),(11)")
		require.NoError(t, err)

		selectStmt, err := conn.PrepareContext(ctx,
			"select id from t where id in (cast(? as signed), null) order by id")
		require.NoError(t, err)
		defer selectStmt.Close()
		queryIDs := func(arg any) []int {
			t.Helper()
			rows, queryErr := selectStmt.QueryContext(ctx, arg)
			require.NoError(t, queryErr)
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			return ids
		}
		require.Empty(t, queryIDs(nil), "NULL IN (NULL) must produce no selected partition rows")
		require.Equal(t, []int{2}, queryIDs(int64(2)))

		updateStmt, err := conn.PrepareContext(ctx,
			"update t set value=value+1 where id in (cast(? as signed), null)")
		require.NoError(t, err)
		defer updateStmt.Close()
		result, err := updateStmt.ExecContext(ctx, nil)
		require.NoError(t, err)
		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, rowsAffected, "NULL IN (NULL) must not update a partition row")
		result, err = updateStmt.ExecContext(ctx, int64(2))
		require.NoError(t, err)
		rowsAffected, err = result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, rowsAffected)

		rows, err := conn.QueryContext(ctx, "select id,value from t order by id")
		require.NoError(t, err)
		defer rows.Close()
		var values [][2]int
		for rows.Next() {
			var id, value int
			require.NoError(t, rows.Scan(&id, &value))
			values = append(values, [2]int{id, value})
		}
		require.NoError(t, rows.Err())
		require.Equal(t, [][2]int{{1, 0}, {2, 1}, {11, 0}}, values)
	})
}
