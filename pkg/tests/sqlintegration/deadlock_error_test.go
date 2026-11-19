// Copyright 2024 Matrix Origin
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
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDeadlockMySQLRetryIdentity(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err = db.ExecContext(ctx, "create database deadlock_retry_identity")
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database deadlock_retry_identity")
		_, err = db.ExecContext(ctx, "create table deadlock_retry_identity.t(id int primary key, v int)")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "insert into deadlock_retry_identity.t values (1,10),(2,20)")
		require.NoError(t, err)
		// Repeat on the same table and connections to prove locks and transaction
		// state are reusable after a victim rollback and whole-transaction retry.
		conns := make([]*sql.Conn, 2)
		for i := range conns {
			conns[i], err = db.Conn(ctx)
			require.NoError(t, err)
			defer conns[i].Close()
		}
		for round := 0; round < 2; round++ {
			func() {
				txns := make([]*sql.Tx, 2)
				for i := range txns {
					txns[i], err = conns[i].BeginTx(ctx, nil)
					require.NoError(t, err)
					defer txns[i].Rollback()
					// Distinct primary keys ensure both owners hold a row before either
					// requests the other. Launch order cannot avoid the resulting cycle.
					_, err = txns[i].ExecContext(ctx, "update deadlock_retry_identity.t set v=v+1 where id=?", i+1)
					require.NoError(t, err)
				}
				type result struct {
					actor int
					err   error
				}
				done := make(chan result, 2)
				waitCtx, stop := context.WithCancel(ctx)
				var workers sync.WaitGroup
				defer func() { stop(); workers.Wait() }()
				for i, txn := range txns {
					workers.Go(func() {
						var value int
						err := txn.QueryRowContext(waitCtx, "select v from deadlock_retry_identity.t where id=? for update", 2-i).Scan(&value)
						done <- result{i, err}
					})
				}
				victim := -1
				for range txns {
					r := <-done
					if r.err != nil {
						require.Equal(t, -1, victim, "only one transaction must be aborted")
						victim = r.actor
						var serverErr *mysql.MySQLError
						require.ErrorAs(t, r.err, &serverErr)
						require.Equal(t, uint16(1213), serverErr.Number)
						require.Equal(t, "40001", string(serverErr.SQLState[:]))
					}
				}
				require.NotEqual(t, -1, victim, "cycle must produce a deadlock victim")
				require.NoError(t, txns[1-victim].Commit())
				require.NoError(t, txns[victim].Rollback())
				for i := range conns {
					var value int
					require.NoError(t, conns[i].QueryRowContext(ctx, "select v from deadlock_retry_identity.t where id=?", i+1).Scan(&value))
					want := 10*(i+1) + round
					if i != victim {
						want++
					}
					require.Equal(t, want, value, "victim's earlier update must roll back")
				}
				retry, err := conns[victim].BeginTx(ctx, nil)
				require.NoError(t, err)
				defer retry.Rollback()
				_, err = retry.ExecContext(ctx, "update deadlock_retry_identity.t set v=v+1 where id=?", victim+1)
				require.NoError(t, err)
				require.NoError(t, retry.Commit())
			}()
		}
		var sum int
		require.NoError(t, db.QueryRowContext(ctx, "select sum(v) from deadlock_retry_identity.t").Scan(&sum))
		require.Equal(t, 34, sum)
	})
}
