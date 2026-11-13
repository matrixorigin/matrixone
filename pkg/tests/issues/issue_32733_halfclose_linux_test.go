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
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func issue32733Visible(ctx context.Context, db *sql.DB, marker string) (bool, error) {
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

func TestIssue32733MainHalfCloseReturnsSQLResult(t *testing.T) {
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

		const network = "issue32733_half_close_main"
		captured := make(chan *net.TCPConn, 1)
		mysql.RegisterDialContext(network, func(ctx context.Context, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if err == nil {
				select {
				case captured <- conn.(*net.TCPConn):
				default:
				}
			}
			return conn, err
		})
		defer mysql.DeregisterDialContext(network)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(%s)/", network, addr))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { cancel(); _ = conn.Close() }()
		var connID uint32
		require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
		var raw *net.TCPConn
		select {
		case raw = <-captured:
		case <-ctx.Done():
			t.Fatal("no client TCP connection")
		}
		type result struct {
			sleep, marker int
			err           error
		}
		done := make(chan result, 1)
		finished := make(chan struct{})
		defer func() {
			cancel()
			_ = raw.Close()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Error("query worker did not exit")
			}
		}()
		go func() {
			defer close(finished)
			var result result
			result.err = conn.QueryRowContext(ctx, "select sleep(4),32733011").Scan(&result.sleep, &result.marker)
			done <- result
		}()
		require.Eventually(t, func() bool {
			visible, err := issue32733Visible(ctx, admin, "32733011")
			return err == nil && visible
		}, 10*time.Second, 50*time.Millisecond, "query must be active before half-close")
		require.NoError(t, raw.CloseWrite())
		select {
		case r := <-done:
			require.NoError(t, r.err, "half-closed client must receive the SQL result")
			require.Equal(t, 0, r.sleep)
			require.Equal(t, 32733011, r.marker)
			require.Eventually(t, func() bool {
				var count int
				err := admin.QueryRowContext(ctx, fmt.Sprintf("select count(*) from processlist() a where conn_id = %d", connID)).Scan(&count)
				return err == nil && count == 0
			}, 10*time.Second, 50*time.Millisecond, "session must be removed after EOF")
		case <-time.After(15 * time.Second):
			t.Fatal("half-closed client did not receive the SQL result")
		}

		// A graceful full close carries the same TCP FIN. The accepted work must
		// finish, commit, and release its session even though no reply is read.
		const fullDB = "issue32733_full_fin"
		_, err = admin.ExecContext(ctx, "create database "+fullDB)
		require.NoError(t, err)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
			defer done()
			_, _ = admin.ExecContext(cleanup, "drop database if exists "+fullDB)
		}()
		_, err = admin.ExecContext(ctx, "create table "+fullDB+".t (id int primary key, v int)")
		require.NoError(t, err)
		fullCtx, fullCancel := context.WithCancel(ctx)
		defer fullCancel()
		fullDBConn, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(%s)/", network, addr))
		require.NoError(t, err)
		defer fullDBConn.Close()
		fullConn, err := fullDBConn.Conn(fullCtx)
		require.NoError(t, err)
		defer fullConn.Close()
		var fullID uint32
		require.NoError(t, fullConn.QueryRowContext(fullCtx, "select connection_id()").Scan(&fullID))
		var fullRaw *net.TCPConn
		select {
		case fullRaw = <-captured:
		case <-fullCtx.Done():
			t.Fatal("full-close client did not produce a TCP connection")
		}
		fullDone := make(chan error, 1)
		fullFinished := make(chan struct{})
		defer func() {
			fullCancel()
			_ = fullRaw.Close()
			select {
			case <-fullFinished:
			case <-time.After(5 * time.Second):
				t.Error("closed client's worker did not exit")
			}
		}()
		go func() {
			defer close(fullFinished)
			_, err := fullConn.ExecContext(fullCtx, "insert into "+fullDB+".t values (1, sleep(4) + 0 * 32733012)")
			fullDone <- err
		}()
		require.Eventually(t, func() bool {
			visible, err := issue32733Visible(ctx, admin, "32733012")
			return err == nil && visible
		}, 10*time.Second, 50*time.Millisecond, "insert must be active before full close")
		require.NoError(t, fullRaw.Close())
		select {
		case err := <-fullDone:
			require.Error(t, err, "the fully closed client cannot receive the result")
		case <-time.After(10 * time.Second):
			t.Fatal("fully closed client did not return")
		}
		require.Eventually(t, func() bool {
			var count int
			err := admin.QueryRowContext(ctx, "select count(*) from "+fullDB+".t where id = 1 and v = 0").Scan(&count)
			return err == nil && count == 1
		}, 15*time.Second, 50*time.Millisecond, "accepted work must complete after a graceful FIN")
		require.Eventually(t, func() bool {
			var count int
			err := admin.QueryRowContext(ctx, fmt.Sprintf("select count(*) from processlist() a where conn_id = %d", fullID)).Scan(&count)
			return err == nil && count == 0
		}, 10*time.Second, 50*time.Millisecond, "closed session must be removed")
	})
}
