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
	"net"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

var issue32733UnixDialID atomic.Uint64

func TestIssue32733UnixFullCloseCancelsAndRollsBack(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		unixPath := cn.GetServiceConfig().CN.Frontend.UnixSocketAddress
		require.NotEmpty(t, unixPath)
		addr := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		admin, err := sql.Open("mysql", "dump:111@tcp("+addr+")/")
		require.NoError(t, err)
		defer admin.Close()
		require.NoError(t, admin.PingContext(ctx))
		const dbName = "issue32733_unix_close"
		_, err = admin.ExecContext(ctx, "create database "+dbName)
		require.NoError(t, err)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
			defer done()
			_, _ = admin.ExecContext(cleanup, "drop database if exists "+dbName)
		}()
		_, err = admin.ExecContext(ctx, "create table "+dbName+".t (id int primary key, v int)")
		require.NoError(t, err)
		_, err = admin.ExecContext(ctx, "insert into "+dbName+".t values (1,0)")
		require.NoError(t, err)

		network := fmt.Sprintf("issue32733_unix_%d", issue32733UnixDialID.Add(1))
		captured := make(chan *net.UnixConn, 1)
		mysql.RegisterDialContext(network, func(ctx context.Context, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "unix", address)
			if err == nil {
				select {
				case captured <- conn.(*net.UnixConn):
				default:
				}
			}
			return conn, err
		})
		defer mysql.DeregisterDialContext(network)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(%s)/", network, unixPath))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		var raw *net.UnixConn
		select {
		case raw = <-captured:
		case <-ctx.Done():
			t.Fatal("Unix client did not connect")
		}
		var connID uint32
		require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
		needKill := true
		defer func() {
			if needKill {
				cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
				defer done()
				_, _ = admin.ExecContext(cleanup, fmt.Sprintf("kill query %d", connID))
			}
		}()
		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "update "+dbName+".t set v = 1 where id = 1")
		require.NoError(t, err)
		done := make(chan error, 1)
		finished := make(chan struct{})
		defer func() {
			cancel()
			_ = raw.Close()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Error("Unix client query worker did not exit")
			}
		}()
		go func() {
			defer close(finished)
			var sleep, marker int
			done <- conn.QueryRowContext(ctx, "select sleep(60),32733013").Scan(&sleep, &marker)
		}()
		require.Eventually(t, func() bool {
			visible, err := issue32733Visible(ctx, admin, "32733013")
			return err == nil && visible
		}, 10*time.Second, 50*time.Millisecond, "query must run before the Unix client disconnects")
		require.NoError(t, raw.Close())
		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("closed Unix client did not return")
		}
		require.Eventually(t, func() bool {
			var count int
			err := admin.QueryRowContext(ctx, fmt.Sprintf("select count(*) from processlist() a where conn_id = %d", connID)).Scan(&count)
			return err == nil && count == 0
		}, 20*time.Second, 50*time.Millisecond, "the disconnected Unix session must be removed promptly")
		var value int
		require.NoError(t, admin.QueryRowContext(ctx, "select v from "+dbName+".t where id = 1").Scan(&value))
		require.Equal(t, 0, value, "the disconnected transaction must roll back")
		needKill = false
	})
}
