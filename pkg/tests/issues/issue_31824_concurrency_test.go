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

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue31824RepeatedIgnoreConcurrentCNs(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		dbs := make([]*sql.DB, 2)
		for i := range dbs {
			cn, err := cluster.GetCNService(i)
			require.NoError(t, err)
			dbs[i], err = sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer dbs[i].Close()
		}
		database := testutils.GetDatabaseName(t)
		_, err := dbs[0].ExecContext(ctx, "create database `"+database+"`")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, cleanupErr := dbs[0].ExecContext(cleanupCtx, "drop database if exists `"+database+"`")
			if cleanupErr != nil {
				t.Errorf("drop test database: %v", cleanupErr)
			}
		}()
		syncAuthenticatedClusterCommit(t, ctx, cluster)

		for _, tc := range []struct {
			name      string
			mode      pbtxn.TxnMode
			isolation pbtxn.TxnIsolation
		}{
			{"pessimistic RC", pbtxn.TxnMode_Pessimistic, pbtxn.TxnIsolation_RC},
			{"optimistic SI", pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_SI},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for i := range dbs {
					cn, getErr := cluster.GetCNService(i)
					require.NoError(t, getErr)
					rt := moruntime.ServiceRuntime(cn.ServiceID())
					oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
					oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
					rt.SetGlobalVariables(moruntime.TxnMode, tc.mode)
					rt.SetGlobalVariables(moruntime.TxnIsolation, tc.isolation)
					defer func() {
						if hadMode {
							rt.SetGlobalVariables(moruntime.TxnMode, oldMode)
						} else {
							rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
						}
						if hadIsolation {
							rt.SetGlobalVariables(moruntime.TxnIsolation, oldIsolation)
						} else {
							rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
						}
					}()
				}
				table := "`" + database + "`.t"
				_, err := dbs[0].ExecContext(ctx, "drop table if exists "+table)
				require.NoError(t, err)
				_, err = dbs[0].ExecContext(ctx, "create table "+table+"(id int primary key,u int unique,v int not null)")
				require.NoError(t, err)
				_, err = dbs[0].ExecContext(ctx, "insert into "+table+" values(1,10,0),(2,20,0)")
				require.NoError(t, err)
				syncAuthenticatedClusterCommit(t, ctx, cluster)

				txs := make([]*sql.Tx, 2)
				for i := range txs {
					txs[i], err = dbs[i].BeginTx(ctx, nil)
					require.NoError(t, err)
					defer txs[i].Rollback()
				}
				type result struct {
					row                  int
					affected             int64
					updateErr, commitErr error
				}
				ready := make(chan struct{}, 2)
				start := make(chan struct{})
				results := make(chan result, 2)
				for i, tx := range txs {
					go func(row int, tx *sql.Tx) {
						ready <- struct{}{}
						<-start
						r, updateErr := tx.ExecContext(ctx, fmt.Sprintf(
							"update ignore %s x join %s y on x.id=%d and y.id=x.id set x.u=500,y.v=y.v+1", table, table, row))
						var affected int64
						if updateErr == nil {
							affected, updateErr = r.RowsAffected()
						}
						commitErr := tx.Commit()
						results <- result{row, affected, updateErr, commitErr}
					}(i+1, tx)
				}
				<-ready
				<-ready
				close(start)
				outcomes := make([]result, 0, 2)
				for range 2 {
					select {
					case outcome := <-results:
						outcomes = append(outcomes, outcome)
					case <-ctx.Done():
						t.Fatal("concurrent UPDATE IGNORE did not finish", ctx.Err())
					}
				}
				syncAuthenticatedClusterCommit(t, ctx, cluster)
				var owners int
				require.NoError(t, dbs[1].QueryRowContext(ctx, "select count(*) from "+table+" force index(u) where u=500").Scan(&owners))
				require.Equal(t, 1, owners)
				rows, err := dbs[1].QueryContext(ctx, "select id,u,v from "+table+" order by id")
				require.NoError(t, err)
				defer rows.Close()
				var values [2][3]int
				for i := 0; rows.Next(); i++ {
					require.Less(t, i, len(values))
					require.NoError(t, rows.Scan(&values[i][0], &values[i][1], &values[i][2]))
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, 1, values[0][0])
				require.Equal(t, 2, values[1][0])
				for _, outcome := range outcomes {
					t.Logf("row=%d affected=%d update=%v commit=%v", outcome.row, outcome.affected, outcome.updateErr, outcome.commitErr)
					if tc.mode == pbtxn.TxnMode_Pessimistic {
						require.NoError(t, outcome.updateErr)
						require.NoError(t, outcome.commitErr)
						require.Contains(t, []int64{1, 2}, outcome.affected)
						require.Equal(t, 1, values[outcome.row-1][2], "valid sibling update must survive a rejected unique assignment")
					} else if outcome.updateErr == nil && outcome.commitErr == nil {
						require.Equal(t, 1, values[outcome.row-1][2])
					} else {
						conflictErr := outcome.updateErr
						if conflictErr == nil {
							conflictErr = outcome.commitErr
						}
						var mysqlErr *mysql.MySQLError
						require.True(t, errors.As(conflictErr, &mysqlErr), "expected a transaction conflict, got %v", conflictErr)
						require.Equal(t, moerr.ErrTxnWWConflict, mysqlErr.Number)
						require.Equal(t, 0, values[outcome.row-1][2], "failed transaction must not publish its sibling update")
					}
				}
			})
		}
	})
}

func TestIssue31824RepeatedIgnoreLockRefresh(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		dbs := make([]*sql.DB, 2)
		for i := range dbs {
			cn, e := c.GetCNService(i)
			require.NoError(t, e)
			dbs[i], e = sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, e)
			defer dbs[i].Close()
		}
		name := strings.ToLower(testutils.GetDatabaseName(t))
		table := "`" + name + "`.t"
		_, e := dbs[0].ExecContext(ctx, "create database `"+name+"`")
		require.NoError(t, e)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			_, err := dbs[0].ExecContext(cleanup, "drop database if exists `"+name+"`")
			require.NoError(t, err)
		}()
		_, e = dbs[0].ExecContext(ctx, "create table "+table+"(id int primary key,u int,v int,unique key uk_u(u))")
		require.NoError(t, e)
		syncAuthenticatedClusterCommit(t, ctx, c)
		holder, e := dbs[0].Conn(ctx)
		require.NoError(t, e)
		defer holder.Close()
		writer, e := dbs[1].Conn(ctx)
		require.NoError(t, e)
		defer writer.Close()
		var tableID, indexID uint64
		var indexName string
		require.NoError(t, dbs[0].QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", name).Scan(&tableID))
		require.NoError(t, dbs[0].QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=? and name='uk_u' limit 1", tableID).Scan(&indexName))
		require.NoError(t, dbs[0].QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", name, indexName).Scan(&indexID))
		type result struct {
			affected int64
			err      error
		}
		for _, tc := range []struct {
			name, initial, holderSQL string
			affected                 int64
			expected                 string
		}{
			{"key_created_while_waiting", "(1,10,0),(2,20,0)", "update " + table + " set u=500 where id=1", 1, "1:500:0,2:20:1"},
			{"key_vacated_while_waiting", "(1,500,0),(2,20,0)", "update " + table + " set u=600 where id=1", 2, "1:600:0,2:500:1"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, e := dbs[0].ExecContext(ctx, "delete from "+table+" where id in (1,2)")
				require.NoError(t, e)
				_, e = dbs[0].ExecContext(ctx, "insert into "+table+" values "+tc.initial)
				require.NoError(t, e)
				syncAuthenticatedClusterCommit(t, ctx, c)
				_, e = holder.ExecContext(ctx, "begin")
				require.NoError(t, e)
				_, e = holder.ExecContext(ctx, tc.holderSQL)
				require.NoError(t, e)
				wctx, wcancel := context.WithCancel(ctx)
				done := make(chan result, 1)
				drained := false
				go func() {
					r, err := writer.ExecContext(wctx, "update ignore "+table+" x join "+table+" y on x.id=2 and y.id=2 set x.u=500,y.v=y.v+1")
					var n int64
					if err == nil {
						n, err = r.RowsAffected()
					}
					done <- result{n, err}
				}()
				defer func() {
					wcancel()
					cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = holder.ExecContext(cleanup, "rollback")
					if !drained {
						select {
						case <-done:
						case <-cleanup.Done():
							t.Error("contending update did not stop")
						}
					}
				}()
				require.Eventually(t, func() bool { return issue29903LockWaiters(c, indexID) > 0 }, 30*time.Second, 10*time.Millisecond, "writer did not wait for the proposed unique key")
				_, e = holder.ExecContext(ctx, "commit")
				require.NoError(t, e)
				var out result
				select {
				case out = <-done:
					drained = true
				case <-ctx.Done():
					t.Fatal("contending update timed out")
				}
				require.NoError(t, out.err)
				require.Equal(t, tc.affected, out.affected)
				syncAuthenticatedClusterCommit(t, ctx, c)
				var got string
				require.NoError(t, dbs[1].QueryRowContext(ctx, "select group_concat(concat_ws(':',id,u,v) order by id) from "+table).Scan(&got))
				require.Equal(t, tc.expected, got)
				var n int
				require.NoError(t, dbs[1].QueryRowContext(ctx, "select count(*) from "+table+" force index(uk_u) where u=500").Scan(&n))
				require.Equal(t, 1, n)
			})
		}
	})
}
