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
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue28246ConcurrentNextval(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(5)
		name := testutils.GetDatabaseName(t)
		_, err = db.ExecContext(ctx, "create database `"+name+"`")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			_, err := db.ExecContext(cleanupCtx, "drop database `"+name+"`")
			require.NoError(t, err)
		}()
		for _, workers := range []int{2, 4} {
			t.Run(fmt.Sprintf("%d sessions", workers), func(t *testing.T) {
				sequence := fmt.Sprintf("s%d", workers)
				_, err := db.ExecContext(ctx, "create sequence `"+name+"`.`"+sequence+"` increment 1 start with 1 no cycle")
				require.NoError(t, err)
				connections := make([]*sql.Conn, workers)
				for i := range connections {
					connections[i], err = db.Conn(ctx)
					require.NoError(t, err)
					defer connections[i].Close()
					_, err = connections[i].ExecContext(ctx, "use `"+name+"`")
					require.NoError(t, err)
				}
				type allocation struct {
					value int
					err   error
				}
				results := make(chan allocation, workers)
				start := make(chan struct{})
				for _, conn := range connections {
					go func(conn *sql.Conn) {
						<-start
						var result allocation
						result.err = conn.QueryRowContext(ctx, "select nextval('"+sequence+"')").Scan(&result.value)
						results <- result
					}(conn)
				}
				close(start)
				values := make([]int, 0, workers)
				errors := make([]error, 0, workers)
				for range connections {
					result := <-results
					errors = append(errors, result.err)
					values = append(values, result.value)
				}
				for _, err := range errors {
					require.NoError(t, err)
				}
				sort.Ints(values)
				for i, value := range values {
					require.Equal(t, i+1, value)
				}
				var persisted int
				require.NoError(t, db.QueryRowContext(ctx, "select last_seq_num from `"+name+"`.`"+sequence+"`").Scan(&persisted))
				require.Equal(t, workers, persisted)
				// Rollback must release the sequence lock without consuming a value.
				tx, err := connections[0].BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.QueryRowContext(ctx, "select nextval('"+sequence+"')").Scan(&persisted))
				require.Equal(t, workers+1, persisted)
				require.NoError(t, tx.Rollback())
				require.NoError(t, connections[1].QueryRowContext(ctx, "select nextval('"+sequence+"')").Scan(&persisted))
				require.Equal(t, workers+1, persisted)
			})
		}

		lockService := lockservice.GetLockServiceByServiceID(cn.ServiceID())
		require.NotNil(t, lockService)
		cnImpl, ok := cn.RawService().(cnservice.Service)
		require.True(t, ok)
		sqlExecutor := cnImpl.GetSQLExecutor()
		engine := cnImpl.GetEngine()
		runContention := func(t *testing.T, timeoutWaiter bool) {
			t.Helper()
			sequence := fmt.Sprintf("wait_%t", timeoutWaiter)
			_, err := db.ExecContext(ctx, "create sequence `"+name+"`.`"+sequence+"` increment 1 start with 1 no cycle")
			require.NoError(t, err)
			var sequenceTableID uint64
			require.Eventually(t, func() bool {
				lookupCtx := context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))
				err := sqlExecutor.ExecTxn(lookupCtx, func(txn executor.TxnExecutor) error {
					database, err := engine.Database(lookupCtx, strings.ToLower(name), txn.Txn())
					if err != nil {
						return err
					}
					relation, err := database.Relation(lookupCtx, sequence, nil)
					if err != nil {
						return err
					}
					sequenceTableID = relation.GetTableID(lookupCtx)
					return nil
				}, executor.Options{}.WithAccountID(0))
				return err == nil && sequenceTableID != 0
			}, 15*time.Second, 10*time.Millisecond, "sequence metadata did not become visible")

			holder, err := db.Conn(ctx)
			require.NoError(t, err)
			defer holder.Close()
			waiter, err := db.Conn(ctx)
			require.NoError(t, err)
			defer waiter.Close()
			for _, conn := range []*sql.Conn{holder, waiter} {
				_, err = conn.ExecContext(ctx, "use `"+name+"`")
				require.NoError(t, err)
			}
			if timeoutWaiter {
				// A server-side lock timeout is deterministic here.  Unlike a
				// client context cancellation, it waits for the frontend to
				// finish the statement and rollback before the holder is released.
				_, err = waiter.ExecContext(ctx, "set session lock_wait_timeout = 1")
				require.NoError(t, err)
			}

			holderTx, err := holder.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer holderTx.Rollback()
			var held int
			require.NoError(t, holderTx.QueryRowContext(ctx, "select nextval('"+sequence+"')").Scan(&held))
			require.Equal(t, 1, held)

			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			type allocation struct {
				value int
				err   error
			}
			waitDone := make(chan allocation, 1)
			go func() {
				var result allocation
				result.err = waiter.QueryRowContext(waitCtx, "select nextval('"+sequence+"')").Scan(&result.value)
				waitDone <- result
			}()

			// The lock service is the synchronization point: committing after the
			// waiter is visible proves this is an actual row-lock wait, not merely
			// two queries released by a scheduling barrier.
			require.Eventually(t, func() bool {
				// A waiter observed in the lock service is the synchronization
				// point.  The contention test owns all application connections in
				// this embedded cluster, so no scheduling-only sleep can satisfy
				// this predicate: NEXTVAL must have reached the row-lock queue.
				var foundWaiter bool
				lockService.IterLocks(func(lockedTableID uint64, _ [][]byte, lock lockservice.Lock) bool {
					if lockedTableID != sequenceTableID {
						return true
					}
					lock.IterWaiters(func(_ pblock.WaitTxn) bool {
						foundWaiter = true
						return false
					})
					return !foundWaiter
				})
				return foundWaiter
			}, 15*time.Second, 10*time.Millisecond, "NEXTVAL did not enter the sequence row-lock wait queue")

			if timeoutWaiter {
				select {
				case result := <-waitDone:
					require.Error(t, result.err)
					require.ErrorContains(t, result.err, "Lock wait timeout exceeded")
				case <-time.After(15 * time.Second):
					t.Fatal("timed-out NEXTVAL did not return")
				}
				require.NoError(t, holderTx.Rollback())
				var usable int
				require.NoError(t, waiter.QueryRowContext(ctx, "select 1").Scan(&usable))
				require.Equal(t, 1, usable, "waiter connection must remain usable after lock timeout")
				fresh, err := db.Conn(ctx)
				require.NoError(t, err)
				defer fresh.Close()
				_, err = fresh.ExecContext(ctx, "use `"+name+"`")
				require.NoError(t, err)
				var next int
				require.NoError(t, fresh.QueryRowContext(ctx, "select nextval('"+sequence+"')").Scan(&next))
				require.Equal(t, 1, next, "a timed-out waiter must not consume the value")
				return
			}

			require.NoError(t, holderTx.Commit())
			result := <-waitDone
			require.NoError(t, result.err)
			require.Equal(t, 2, result.value)
			var next int
			require.NoError(t, waiter.QueryRowContext(ctx, "select nextval('"+sequence+"')").Scan(&next))
			require.Equal(t, 3, next, "the waiter connection must remain usable after lock handoff")
		}

		t.Run("controlled lock handoff", func(t *testing.T) { runContention(t, false) })
		t.Run("lock wait timeout", func(t *testing.T) { runContention(t, true) })
	})
}
