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
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/stretchr/testify/require"
)

func TestMoLocksListsAllWaiters(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		execSQLRequire(t, ctx, db, "create database lock_waiter_view")
		defer cleanupSQLIntegration(t, cn, "drop database lock_waiter_view")
		execSQLRequire(t, ctx, db, "create table lock_waiter_view.t(id int primary key, v int)")
		execSQLRequire(t, ctx, db, "insert into lock_waiter_view.t values (1,10)")
		var tableID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='lock_waiter_view' and relname='t'").Scan(&tableID))
		tableFilter := fmt.Sprintf(" where table_id='%d'", tableID)

		// Reuse the canonical single-CN fixture: omission happens while the owner
		// builds its diagnostic RPC response, independent of the waiters' CNs.
		// Repeat on the same table to prove the observation does not consume or
		// retain queue entries and that normal lock handoff still completes.
		for round := 0; round < 2; round++ {
			func() {
				holder, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer holder.Rollback()
				const query = "select v from lock_waiter_view.t where id=1 for update"
				var value int
				require.NoError(t, holder.QueryRowContext(ctx, query).Scan(&value))
				require.Equal(t, 10, value)

				waitCtx, stop := context.WithCancel(ctx)
				var workers sync.WaitGroup
				defer func() {
					stop()
					_ = holder.Rollback()
					workers.Wait()
				}()
				type result struct {
					value       int
					queryErr    error
					rollbackErr error
				}
				done := make(chan result, 2)
				for i := 0; i < 2; i++ {
					waiter, err := db.BeginTx(waitCtx, nil)
					require.NoError(t, err)
					workers.Go(func() {
						var r result
						r.queryErr = waiter.QueryRowContext(waitCtx, query).Scan(&r.value)
						r.rollbackErr = waiter.Rollback()
						done <- r
					})
				}

				// Use the independent transaction view as the arrival barrier, not
				// the lock view being tested. Match the exact table and encoded key.
				waiting := func() map[string]string {
					arrivalCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					defer cancel()
					ticker := time.NewTicker(10 * time.Millisecond)
					defer ticker.Stop()
					for {
						waiting := func() map[string]string {
							rows, err := db.QueryContext(arrivalCtx, "select txn_id, lock_content from mo_transactions() as t"+tableFilter+" and lock_mode='Exclusive'")
							require.NoError(t, err)
							defer rows.Close()
							waiting := make(map[string]string)
							for rows.Next() {
								var id, key string
								require.NoError(t, rows.Scan(&id, &key))
								waiting[id] = key
							}
							require.NoError(t, rows.Err())
							return waiting
						}()
						if len(waiting) == 2 {
							return waiting
						}
						select {
						case <-ticker.C:
						case <-arrivalCtx.Done():
							t.Fatalf("both SQL transactions must be waiting: %v", arrivalCtx.Err())
						}
					}
				}()
				var key string
				for _, current := range waiting {
					require.NotEmpty(t, current)
					if key == "" {
						key = current
					}
					require.Equal(t, key, current, "both transactions must contend on the same key")
				}
				encoded, err := hex.DecodeString(key)
				require.NoError(t, err)
				// A transaction records its intent before enqueueing. Wait on the
				// underlying queue, not IterWaiters, to close that observation race.
				require.NoError(t, lockservice.WaitWaiters(lockservice.GetLockServiceByServiceID(cn.ServiceID()), 0, tableID, encoded, 2))
				for _, view := range []string{"mo_locks()", "mo_catalog.mo_locks"} {
					func() {
						rows, err := db.QueryContext(ctx, "select txn_id, lock_wait, lock_content from "+view+" as l"+tableFilter+" and lock_wait<>''")
						require.NoError(t, err)
						defer rows.Close()
						got := make(map[string]string)
						var holderID string
						count := 0
						for rows.Next() {
							var owner, waiter, key string
							require.NoError(t, rows.Scan(&owner, &waiter, &key))
							require.NotEmpty(t, owner)
							if holderID == "" {
								holderID = owner
							}
							require.Equal(t, holderID, owner)
							got[waiter] = key
							count++
						}
						require.NoError(t, rows.Err())
						require.Equal(t, 2, count, "%s round %d must report each waiter exactly once", view, round)
						require.Equal(t, waiting, got, "%s must retain both waiter identities and keys", view)
					}()
				}

				require.NoError(t, holder.Rollback())
				for i := 0; i < 2; i++ {
					select {
					case r := <-done:
						require.NoError(t, r.queryErr)
						require.NoError(t, r.rollbackErr)
						require.Equal(t, 10, r.value)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				var remaining int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_locks() as l"+tableFilter).Scan(&remaining))
				require.Zero(t, remaining, "finished transactions must release their locks")
			}()
		}
	})
}
