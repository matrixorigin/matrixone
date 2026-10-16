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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue30245LockingReadRetriesBeforeLimit(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		open := func(index int) *sql.DB {
			cn, err := c.GetCNService(index)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			return db
		}
		db := open(0)
		defer db.Close()
		remoteDB := open(1)
		defer remoteDB.Close()
		const database = "issue_30245_locking_read"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, db, "drop database "+database)
		}()
		const table = database + ".t"
		execSQLRequire(t, ctx, db, "create table "+table+"(id varchar(16) primary key, c int)")
		var tableID uint64
		require.NoError(t, db.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", database).Scan(&tableID))

		for index, tc := range []struct {
			name         string
			suffix       string
			prepared     bool
			sessionLimit bool
			shared       bool
			rollback     bool
			remote       bool
			repeatable   bool
			update       bool
		}{
			{name: "without limit"},
			{name: "limit", suffix: "limit 1"},
			{name: "offset", suffix: "limit 1 offset 0"},
			{name: "prepared reuse", suffix: "limit 1", prepared: true},
			{name: "session limit", sessionLimit: true},
			{name: "shared lock", suffix: "limit 1", shared: true},
			{name: "holder rollback", suffix: "limit 1", rollback: true},
			{name: "remote owner", suffix: "limit 1", remote: true},
			{name: "RR without limit", repeatable: true},
			{name: "RR with limit", suffix: "limit 1", repeatable: true},
			{name: "update after lock wait", update: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				readerDB := db
				if tc.remote {
					readerDB = remoteDB
				}
				reader, err := readerDB.Conn(ctx)
				require.NoError(t, err)
				defer reader.Close()
				var savedIsolation string
				var savedLimit uint64
				require.NoError(t, reader.QueryRowContext(ctx,
					"select @@transaction_isolation, @@sql_select_limit").Scan(&savedIsolation, &savedLimit))
				defer func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cleanupCancel()
					_, err := reader.ExecContext(cleanupCtx, fmt.Sprintf(
						"set session transaction_isolation='%s', sql_select_limit=%d", savedIsolation, savedLimit))
					require.NoError(t, err)
				}()
				isolation := "READ-COMMITTED"
				if tc.repeatable {
					isolation = "REPEATABLE-READ"
				}
				selectLimit := ^uint64(0)
				if tc.sessionLimit {
					selectLimit = 1
				}
				_, err = reader.ExecContext(ctx, fmt.Sprintf(
					"set session transaction_isolation='%s', sql_select_limit=%d", isolation, selectLimit))
				require.NoError(t, err)
				q := "select c from " + table + " where id=? " + tc.suffix + " for update"
				if tc.shared {
					q = strings.Replace(q, "for update", "for share", 1)
				}
				if tc.update {
					q = "update " + table + " set c=c+1 where id=?"
				}
				var prepared *sql.Stmt
				trials := 1
				if tc.prepared {
					prepared, err = reader.PrepareContext(ctx, q)
					require.NoError(t, err)
					defer prepared.Close()
					trials = 2
				}
				for trial := range trials {
					func() {
						queryCtx, queryCancel := context.WithTimeout(ctx, 20*time.Second)
						defer queryCancel()
						id := fmt.Sprintf("r%d_%d", index, trial)
						execSQLRequire(t, queryCtx, db, "insert into "+table+" values('"+id+"',0)")
						holder, err := db.BeginTx(queryCtx, nil)
						require.NoError(t, err)
						defer holder.Rollback()
						var old int
						require.NoError(t, holder.QueryRowContext(queryCtx,
							"select c from "+table+" where id=? for update", id).Scan(&old))
						require.Zero(t, old)
						_, err = holder.ExecContext(queryCtx, "update "+table+" set c=1 where id=?", id)
						require.NoError(t, err)
						readTxn, err := reader.BeginTx(queryCtx, nil)
						require.NoError(t, err)
						defer readTxn.Rollback()
						type outcome struct {
							value int
							err   error
						}
						done := make(chan outcome, 1)
						go func() {
							var result outcome
							textQuery := strings.Replace(q, "?", "'"+id+"'", 1)
							if tc.update {
								updated, err := readTxn.ExecContext(queryCtx, textQuery)
								result.err = err
								if err == nil {
									var affected int64
									affected, result.err = updated.RowsAffected()
									result.value = int(affected)
								}
							} else if prepared != nil {
								func() {
									stmt := readTxn.StmtContext(queryCtx, prepared)
									defer stmt.Close()
									result.err = stmt.QueryRowContext(queryCtx, id).Scan(&result.value)
								}()
							} else {
								result.err = readTxn.QueryRowContext(queryCtx, textQuery).Scan(&result.value)
							}
							done <- result
						}()
						received := false
						defer func() {
							if !received {
								_ = holder.Rollback()
								queryCancel()
								<-done
							}
						}()
						// Synchronize on a real waiter, not a delay before COMMIT.
						ticker := time.NewTicker(5 * time.Millisecond)
						defer ticker.Stop()
						for issue27947WaiterCount(c, tableID) == 0 {
							select {
							case result := <-done:
								received = true
								t.Fatalf("query returned before holder release: %+v", result)
							case <-queryCtx.Done():
								t.Fatal(queryCtx.Err())
							case <-ticker.C:
							}
						}
						if tc.rollback {
							require.NoError(t, holder.Rollback())
						} else {
							require.NoError(t, holder.Commit())
						}
						var result outcome
						select {
						case result = <-done:
							received = true
						case <-queryCtx.Done():
							t.Fatal(queryCtx.Err())
						}
						if tc.repeatable {
							var conflict *mysql.MySQLError
							require.ErrorAs(t, result.err, &conflict)
							require.Equal(t, uint16(20619), conflict.Number)
							return
						}
						require.NoError(t, result.err)
						want := 1
						if tc.rollback {
							want = 0
						}
						require.Equal(t, want, result.value)
						require.NoError(t, readTxn.Commit())
						if tc.update {
							var value int
							require.NoError(t, db.QueryRowContext(queryCtx,
								"select c from "+table+" where id=?", id).Scan(&value))
							require.Equal(t, 2, value)
						}
					}()
				}
			})
		}
	})
}
