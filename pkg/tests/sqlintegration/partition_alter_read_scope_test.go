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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// The partition metadata SELECTs borrow ALTER's transaction after it has
// pending catalog writes. Those reads must not reorder the parent's writes
// and invalidate their own metadata scope on every retry.
func TestPartitionAlterBorrowedReadScopeSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "partition_alter_read_scope"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		for _, q := range []string{
			"create database " + schema,
			"create table " + schema + ".t(id int auto_increment primary key, v int) auto_id_cache=1 partition by hash(id) partitions 2",
			"insert into " + schema + ".t(v) values(10),(20)",
			"alter table " + schema + ".t auto_increment=100",
			"alter table " + schema + ".t add index ix(v)",
			"alter table " + schema + ".t drop index ix",
			"insert into " + schema + ".t(v) values(30)",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		rows, err := db.QueryContext(ctx, "select id,v from "+schema+".t order by id")
		require.NoError(t, err)
		defer rows.Close()
		var got [][2]int
		for rows.Next() {
			var id, v int
			require.NoError(t, rows.Scan(&id, &v))
			got = append(got, [2]int{id, v})
		}
		require.NoError(t, rows.Err())
		require.Equal(t, [][2]int{{1, 10}, {2, 20}, {100, 30}}, got)
	})
}
