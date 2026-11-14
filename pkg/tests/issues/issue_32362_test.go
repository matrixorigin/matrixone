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

//go:build linux

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

var issue32362DialID atomic.Uint64

func issue32362Conn(t *testing.T, ctx context.Context, addr string) (*sql.Conn, *net.TCPConn) {
	t.Helper()
	dialName := fmt.Sprintf("issue32362_%d", issue32362DialID.Add(1))
	captured := make(chan *net.TCPConn, 1)
	mysql.RegisterDialContext(dialName, func(ctx context.Context, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err == nil {
			select {
			case captured <- conn.(*net.TCPConn):
			default:
			}
		}
		return conn, err
	})
	t.Cleanup(func() { mysql.DeregisterDialContext(dialName) })
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(%s)/", dialName, addr))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	select {
	case raw := <-captured:
		return conn, raw
	case <-ctx.Done():
		t.Fatal("connection dial did not produce a TCP socket")
		return nil, nil
	}
}

func issue32362QueryVisible(ctx context.Context, db *sql.DB, marker string) (bool, error) {
	rows, err := db.QueryContext(ctx, "show full processlist")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return false, err
	}
	values := make([]sql.RawBytes, len(columns))
	dest := make([]any, len(values))
	for i := range values {
		dest[i] = &values[i]
	}
	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			return false, err
		}
		for _, value := range values {
			if strings.Contains(string(value), marker) {
				return true, nil
			}
		}
	}
	return false, rows.Err()
}

func TestIssue32362TerminalConnectionCancellation(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		addr := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		admin, err := sql.Open("mysql", "dump:111@tcp("+addr+")/")
		require.NoError(t, err)
		defer admin.Close()
		require.NoError(t, admin.PingContext(ctx))
		const dbName = "issue32362_disconnect"
		_, err = admin.ExecContext(ctx, "create database "+dbName)
		require.NoError(t, err)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
			defer done()
			_, _ = admin.ExecContext(cleanup, "drop database if exists "+dbName)
		}()
		_, err = admin.ExecContext(ctx, "create table "+dbName+".t (id int primary key, v int)")
		require.NoError(t, err)
		var tableID uint64
		require.NoError(t, admin.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", dbName).Scan(&tableID))
		_, err = admin.ExecContext(ctx, "insert into "+dbName+".t values (1,0)")
		require.NoError(t, err)
		visible := func(t *testing.T, marker string) {
			t.Helper()
			require.Eventually(t, func() bool {
				found, queryErr := issue32362QueryVisible(ctx, admin, marker)
				return queryErr == nil && found
			}, 10*time.Second, 50*time.Millisecond, "statement must be running before the connection changes")
		}

		for _, mode := range []struct {
			name  string
			reset bool
		}{
			{name: "reset rolls back and releases the lock", reset: true},
			{name: "FIN rolls back and releases the lock"},
		} {
			t.Run(mode.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				holder, raw := issue32362Conn(t, ctx, addr)
				var holderID uint32
				require.NoError(t, holder.QueryRowContext(ctx, "select connection_id()").Scan(&holderID))
				needKill := true
				t.Cleanup(func() {
					if !needKill {
						return
					}
					cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
					defer done()
					_, _ = admin.ExecContext(cleanup, fmt.Sprintf("kill query %d", holderID))
				})
				_, err = holder.ExecContext(ctx, "begin")
				require.NoError(t, err)
				_, err = holder.ExecContext(ctx, "update "+dbName+".t set v=1 where id=1")
				require.NoError(t, err)
				holderDone := make(chan error, 1)
				holderStarted := time.Now()
				go func() {
					var sleep, marker int
					holderDone <- holder.QueryRowContext(ctx, "select sleep(60),323620993").Scan(&sleep, &marker)
				}()
				visible(t, "323620993")
				waiter, _ := issue32362Conn(t, ctx, addr)
				_, err = waiter.ExecContext(ctx, "begin")
				require.NoError(t, err)
				type result struct {
					value int
					err   error
				}
				waiterDone := make(chan result, 1)
				go func() {
					var r result
					r.err = waiter.QueryRowContext(ctx, "select v from "+dbName+".t where id=1 and 323620994=323620994 for update").Scan(&r.value)
					waiterDone <- r
				}()
				visible(t, "323620994")
				// A running statement may still be planning. Observe a queued lock
				// waiter before disconnecting the holder so this exercises wakeup.
				waitDeadline := time.Now().Add(10 * time.Second)
				for {
					var queued int
					require.NoError(t, admin.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_locks where table_id=? and lock_wait<>''", fmt.Sprint(tableID)).Scan(&queued))
					if queued > 0 {
						break
					}
					if time.Now().After(waitDeadline) {
						t.Fatal("waiter did not enter the table's lock queue")
					}
					time.Sleep(50 * time.Millisecond)
				}
				select {
				case r := <-waiterDone:
					t.Fatalf("waiter finished before disconnect: %+v", r)
				default:
				}
				if mode.reset {
					require.NoError(t, raw.SetLinger(0))
				}
				require.NoError(t, raw.Close())
				beforeLegacy := time.NewTimer(time.Until(holderStarted.Add(25 * time.Second)))
				defer beforeLegacy.Stop()
				select {
				case err = <-holderDone:
					require.Error(t, err)
				case <-time.After(8 * time.Second):
					t.Fatal("disconnected client did not terminate")
				}
				select {
				case r := <-waiterDone:
					require.NoError(t, r.err)
					require.Equal(t, 0, r.value, "the disconnected transaction must roll back")
					needKill = false
				case <-beforeLegacy.C:
					t.Fatal("disconnected connection retained its transaction lock until the legacy grace")
				}
				_, err = waiter.ExecContext(ctx, "rollback")
				require.NoError(t, err)
				var durable int
				require.NoError(t, admin.QueryRowContext(ctx, "select v from "+dbName+".t where id=1").Scan(&durable))
				require.Equal(t, 0, durable, "an independent session must observe the rolled-back value")
			})
		}

		t.Run("write half close cancels the active request", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			conn, raw := issue32362Conn(t, ctx, addr)
			var connID uint32
			require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
			needKill := true
			t.Cleanup(func() {
				if !needKill {
					return
				}
				cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
				defer done()
				_, _ = admin.ExecContext(cleanup, fmt.Sprintf("kill query %d", connID))
			})
			done := make(chan error, 1)
			go func() {
				var sleep, marker int
				done <- conn.QueryRowContext(ctx, "select sleep(60),323620995").Scan(&sleep, &marker)
			}()
			visible(t, "323620995")
			require.NoError(t, raw.CloseWrite())
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("half-closed client was not canceled before the legacy grace")
			}
			require.Eventually(t, func() bool {
				found, queryErr := issue32362QueryVisible(ctx, admin, "323620995")
				return queryErr == nil && !found
			}, 10*time.Second, 50*time.Millisecond, "cancellation must remove the server-side query")
			needKill = false
		})

		t.Run("connected client survives scanning and reuses the session", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			conn, _ := issue32362Conn(t, ctx, addr)
			type result struct {
				sleep, marker int
				err           error
			}
			done := make(chan result, 1)
			go func() {
				var r result
				r.err = conn.QueryRowContext(ctx, "select sleep(6),323620996").Scan(&r.sleep, &r.marker)
				done <- r
			}()
			visible(t, "323620996")
			select {
			case r := <-done:
				require.NoError(t, r.err)
				require.Equal(t, 0, r.sleep)
				require.Equal(t, 323620996, r.marker)
			case <-time.After(15 * time.Second):
				t.Fatal("connected client did not receive its result")
			}
			var value int
			require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&value))
			require.Equal(t, 1, value)
		})
	})
}
