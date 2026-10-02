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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestPreparedPlansPreserveWorkspaceVisibility(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists prepared_stats_reuse")
		inTxn := false
		defer func() {
			if inTxn {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				_, err := conn.ExecContext(cleanup, "rollback")
				if err != nil {
					t.Errorf("rollback: %v", err)
				}
			}
		}()
		exec := func(q string) { _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database prepared_stats_reuse")
		exec("use prepared_stats_reuse")
		exec("create table t(id int primary key, v int)")
		values := make([]string, 128)
		for i := range values {
			values[i] = fmt.Sprintf("(%d,0)", i+1)
		}
		exec("insert into t values " + strings.Join(values, ","))
		read, err := conn.PrepareContext(ctx, "select v from t where id=?")
		require.NoError(t, err)
		defer read.Close()
		update, err := conn.PrepareContext(ctx, "update t set v=v+1 where id=?")
		require.NoError(t, err)
		defer update.Close()
		checkRead := func(want int) {
			var got int
			require.NoError(t, read.QueryRowContext(ctx, 1).Scan(&got))
			require.Equal(t, want, got)
		}
		exec("begin")
		inTxn = true
		checkRead(0)
		exec("commit")
		inTxn = false
		exec("begin")
		inTxn = true
		for i := 1; i <= 20; i++ {
			result, err := update.ExecContext(ctx, i)
			require.NoError(t, err)
			n, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
		checkRead(1)
		var updatedKeys int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from t where id<=20 and v=1").Scan(&updatedKeys))
		require.Equal(t, 20, updatedKeys, "each bound key must be updated exactly once")
		exec("rollback")
		inTxn = false
		var restored int
		require.NoError(t, read.QueryRowContext(ctx, 1).Scan(&restored))
		require.Zero(t, restored)
		var countRows int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from t").Scan(&countRows))
		require.Equal(t, 128, countRows)
		// Changing parameter domains still passes through specialization.
		for _, value := range []any{"1", float64(1), int64(1)} {
			require.NoError(t, read.QueryRowContext(ctx, value).Scan(&restored))
			require.Zero(t, restored)
		}
		require.ErrorIs(t, read.QueryRowContext(ctx, nil).Scan(&restored), sql.ErrNoRows)

		// The same prepared UPDATE must see a newly inserted key, then lose
		// that key after rollback. Frontend owner tests assert generation reuse.
		exec("begin")
		inTxn = true
		exec("insert into t values(129,7)")
		result, err := update.ExecContext(ctx, 129)
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		require.NoError(t, conn.QueryRowContext(ctx, "select v from t where id=129").Scan(&restored))
		require.Equal(t, 8, restored)
		exec("rollback")
		inTxn = false
		result, err = update.ExecContext(ctx, 129)
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, affected)

		exec("create table composite_t(a int, b int, v int, primary key(a,b))")
		exec("insert into composite_t select id,1,v from t")
		composite, err := conn.PrepareContext(ctx, "update composite_t set v=v+1 where a=? and b=?")
		require.NoError(t, err)
		defer composite.Close()
		exec("begin")
		inTxn = true
		for i := 1; i <= 20; i++ {
			result, err := composite.ExecContext(ctx, i, 1)
			require.NoError(t, err)
			n, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
		var sum int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(v) from composite_t").Scan(&countRows, &sum))
		require.Equal(t, 128, countRows)
		require.Equal(t, 20, sum)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from composite_t where a<=20 and b=1 and v=1").Scan(&updatedKeys))
		require.Equal(t, 20, updatedKeys, "a reused composite plan must bind every new key")
		exec("rollback")
		inTxn = false
		require.NoError(t, conn.QueryRowContext(ctx, "select sum(v) from composite_t").Scan(&sum))
		require.Zero(t, sum)
		// Real COM_QUERY dispatch sees growth; frontend owner tests distinguish
		// minor-drift reuse from material-growth rebuilding.
		exec("begin")
		inTxn = true
		for i := 129; i < 132; i++ {
			exec(fmt.Sprintf("insert into t values(%d,%d)", i, i-128))
			require.NoError(t, conn.QueryRowContext(ctx, "select v from t where id=1").Scan(&restored))
			require.Zero(t, restored)
			require.NoError(t, conn.QueryRowContext(ctx, "select sum(v) from t where id>=1").Scan(&sum))
			require.Equal(t, (i-128)*(i-127)/2, sum)
		}
		exec("rollback")
		inTxn = false
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(v) from t").Scan(&countRows, &sum))
		require.Equal(t, 128, countRows)
		require.Zero(t, sum)
		// Small NewOrder/Delivery-like mix: matching integer domains permit
		// prepared reuse, while every statement sees current transaction writes.
		exec("create table orders_t(w bigint,id bigint,v bigint,primary key(w,id))")
		exec("insert into orders_t values(1,1,10),(1,2,20),(1,3,30),(1,4,40),(2,1,100)")
		var ordersTableID uint64
		require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='prepared_stats_reuse' and relname='orders_t'").Scan(&ordersTableID))
		lockedRows := func() int {
			count := 0
			lockservice.GetLockServiceByServiceID(cn.ServiceID()).IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
				if tableID == ordersTableID && !lock.IsRangeLock() && lock.GetLockMode() == lockpb.LockMode_Exclusive {
					count += len(keys)
				}
				return true
			})
			return count
		}
		prepare := func(query string) *sql.Stmt {
			t.Helper()
			stmt, err := conn.PrepareContext(ctx, query)
			require.NoError(t, err)
			return stmt
		}
		locked := prepare("select v from orders_t where w=? and id=? for update")
		defer locked.Close()
		change := prepare("update orders_t set v=v+1 where w=? and id=?")
		defer change.Close()
		rangeSum := prepare("select sum(v) from orders_t where w=?")
		defer rangeSum.Close()
		oldest := prepare("select id from orders_t where w=? order by id")
		defer oldest.Close()
		remove := prepare("delete from orders_t where w=? and id=?")
		defer remove.Close()
		for _, finish := range []string{"rollback", "commit"} {
			exec("begin")
			inTxn = true
			for id := 1; id <= 4; id++ {
				require.NoError(t, locked.QueryRowContext(ctx, 1, id).Scan(&restored))
				require.Equal(t, id*10, restored)
				require.Equal(t, id, lockedRows(), "reused locking read must lock each newly bound key before UPDATE")
				result, err := change.ExecContext(ctx, 1, id)
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), affected)
				require.NoError(t, locked.QueryRowContext(ctx, 1, id).Scan(&restored))
				require.Equal(t, id*10+1, restored)
				require.NoError(t, rangeSum.QueryRowContext(ctx, 1).Scan(&sum))
				require.Equal(t, 100+id, sum)
			}
			exec("insert into orders_t values(1,5,50)")
			require.NoError(t, locked.QueryRowContext(ctx, 1, 5).Scan(&restored))
			require.Equal(t, 50, restored)
			for id := 1; id <= 2; id++ {
				readIDs := func() []int {
					rows, err := oldest.QueryContext(ctx, 1)
					require.NoError(t, err)
					defer rows.Close()
					var ids []int
					for rows.Next() {
						var got int
						require.NoError(t, rows.Scan(&got))
						ids = append(ids, got)
					}
					require.NoError(t, rows.Err())
					return ids
				}
				require.Equal(t, []int{1, 2, 3, 4, 5}[id-1:], readIDs())
				result, err := remove.ExecContext(ctx, 1, id)
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), affected)
				require.ErrorIs(t, locked.QueryRowContext(ctx, 1, id).Scan(&restored), sql.ErrNoRows)
			}
			require.NoError(t, rangeSum.QueryRowContext(ctx, 1).Scan(&sum))
			require.Equal(t, 122, sum)
			exec(finish)
			inTxn = false
			wantSum, wantCount := 122, 3
			if finish == "rollback" {
				wantSum, wantCount = 100, 4
			}
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(v) from orders_t where w=1").Scan(&countRows, &sum))
			require.Equal(t, wantCount, countRows)
			require.Equal(t, wantSum, sum)
		}
		require.NoError(t, conn.QueryRowContext(ctx, "select v from orders_t where w=2 and id=1").Scan(&restored))
		require.Equal(t, 100, restored, "composite-key prefix must isolate the other warehouse")
		exec("create table string_t(id varchar(8) primary key,v int)")
		exec("insert into string_t values('1',0),('01',0)")
		stringUpdate, err := conn.PrepareContext(ctx, "update string_t set v=v+1 where id=?")
		require.NoError(t, err)
		defer stringUpdate.Close()
		for _, tc := range []struct {
			value    any
			affected int64
		}{{"1", 1}, {int64(1), 2}, {"1", 1}} {
			result, err := stringUpdate.ExecContext(ctx, tc.value)
			require.NoError(t, err)
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, tc.affected, affected)
		}
		require.NoError(t, conn.QueryRowContext(ctx, "select sum(v) from string_t").Scan(&sum))
		require.Equal(t, 4, sum)
		require.NoError(t, conn.QueryRowContext(ctx, "select v from string_t where id='01'").Scan(&restored))
		require.Equal(t, 1, restored)
		require.NoError(t, conn.QueryRowContext(ctx, "select v from string_t where id='1'").Scan(&restored))
		require.Equal(t, 3, restored)

	})
}
