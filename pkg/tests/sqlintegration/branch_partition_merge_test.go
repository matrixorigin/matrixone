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

package sqlintegration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func checkBranchMergePreservesUnchangedPartitionRows(t *testing.T, ctx context.Context, db *sql.DB) {
	f := rebaseSQLFixture{t, ctx, db, "nr_partition_merge_"}
	f.exec("create database nr_partition_merge")

	for _, tc := range []struct {
		name      string
		partition string
		sameTxn   bool
	}{
		{name: "plain"},
		{name: "partitioned", partition: " partition by range columns(id) (partition p0 values less than (10), partition p1 values less than (maxvalue))"},
		{name: "partitioned_same_txn", partition: " partition by range columns(id) (partition p0 values less than (10), partition p1 values less than (maxvalue))", sameTxn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			f := rebaseSQLFixture{t, ctx, conn, "nr_partition_merge_"}
			base := tc.name + "_base"
			branch := tc.name + "_branch"
			f.exec("create table nr_partition_merge." + base + "(id int primary key,v int)" + tc.partition)
			f.exec("insert into nr_partition_merge." + base + " values(1,10),(11,110),(12,120)")
			if tc.sameTxn {
				f.exec("begin")
				defer func() {
					cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					_, _ = conn.ExecContext(cleanup, "rollback")
				}()
			}
			f.exec("data branch create table nr_partition_merge." + branch + " from nr_partition_merge." + base)
			require.Equal(t, []string{"1:10", "11:110", "12:120"}, f.rows("nr_partition_merge", branch))
			if !tc.sameTxn {
				unchanged, err := conn.QueryContext(ctx, "data branch diff nr_partition_merge."+branch+" against nr_partition_merge."+base)
				require.NoError(t, err)
				defer unchanged.Close()
				require.False(t, unchanged.Next(), "a pure clone must have no changes")
				require.NoError(t, unchanged.Err())
				require.NoError(t, unchanged.Close())
			}

			f.exec("update nr_partition_merge." + branch + " set v=v+1 where id=11")
			f.exec("insert into nr_partition_merge." + branch + " values(2,20),(21,210)")
			f.exec("delete from nr_partition_merge." + branch + " where id=12")
			if tc.sameTxn {
				f.exec("commit")
			}
			require.Equal(t, []string{"1:10", "11:110", "12:120"}, f.rows("nr_partition_merge", base))
			require.Equal(t, []string{"1:10", "2:20", "11:111", "21:210"}, f.rows("nr_partition_merge", branch))

			rows, err := conn.QueryContext(ctx, "data branch diff nr_partition_merge."+branch+" against nr_partition_merge."+base)
			require.NoError(t, err)
			defer rows.Close()
			changes := make(map[int]string)
			for rows.Next() {
				var side, action string
				var id int
				var value sql.NullInt64
				require.NoError(t, rows.Scan(&side, &action, &id, &value))
				_, duplicate := changes[id]
				require.False(t, duplicate, "duplicate diff for id %d", id)
				changes[id] = action
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, map[int]string{2: "INSERT", 11: "UPDATE", 12: "DELETE", 21: "INSERT"}, changes)

			f.exec("data branch merge nr_partition_merge." + branch + " into nr_partition_merge." + base)
			require.Equal(t, []string{"1:10", "2:20", "11:111", "21:210"}, f.rows("nr_partition_merge", base))
		})
	}
}
