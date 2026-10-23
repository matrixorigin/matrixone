// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestClientDisconnectCancelsQueryAndRollsBack(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		addr := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		observer, err := sql.Open("mysql", "dump:111@tcp("+addr+")/")
		require.NoError(t, err)
		defer observer.Close()
		execSQLRequire(t, ctx, observer, "create database disconnect_cancel_test")
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			_, err := observer.ExecContext(cleanupCtx, "drop database if exists disconnect_cancel_test")
			require.NoError(t, err)
		}()
		execSQLRequire(t, ctx, observer, "create table disconnect_cancel_test.t (id int primary key, v int)")
		execSQLRequire(t, ctx, observer, "insert into disconnect_cancel_test.t values (1, 10)")

		for _, mode := range []string{"fin", "reset"} {
			t.Run(mode, func(t *testing.T) {
				runClientDisconnect(t, ctx, observer, addr, mode)
			})
		}
		var slept int
		require.NoError(t, observer.QueryRowContext(ctx, "select sleep(2)").Scan(&slept))
		require.Zero(t, slept, "monitor ticks must not cancel a healthy long-running query")
		var one int
		require.NoError(t, observer.QueryRowContext(ctx, "select 1").Scan(&one))
		require.Equal(t, 1, one, "a healthy connection must remain usable")
	})
}

func runClientDisconnect(t *testing.T, ctx context.Context, observer *sql.DB, addr, mode string) {
	t.Helper()
	// A dedicated dialer exposes the transport: sql.Conn.Close sends/queues a
	// protocol close and cannot reproduce a client dying during execution.
	network := "disconnect-" + mode
	var connected atomic.Pointer[net.TCPConn]
	mysql.RegisterDialContext(network, func(ctx context.Context, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err == nil {
			connected.Store(conn.(*net.TCPConn))
		}
		return conn, err
	})
	defer mysql.DeregisterDialContext(network)
	db, err := sql.Open("mysql", "dump:111@"+network+"("+addr+")/disconnect_cancel_test")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	transport := connected.Load()
	require.NotNil(t, transport)
	defer transport.Close()
	var id uint64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&id))
	// Only a failure cleanup uses KILL; the regression oracle below must pass
	// without it. This also prevents a pre-fix run retaining the long sleep.
	serverGone := false
	defer func() {
		if serverGone {
			return
		}
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, _ = observer.ExecContext(cleanupCtx, fmt.Sprintf("kill connection %d", id))
	}()
	_, err = conn.ExecContext(ctx, "begin")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "update t set v=20 where id=1")
	require.NoError(t, err)

	queryCtx, cancelQuery := context.WithCancel(ctx)
	defer cancelQuery()
	done := make(chan error, 1)
	go func() {
		var result int
		done <- conn.QueryRowContext(queryCtx, "select sleep(600)").Scan(&result)
	}()
	defer func() {
		cancelQuery()
		_ = transport.Close()
		select {
		case queryErr := <-done:
			require.Error(t, queryErr, "the disconnected client must not receive a successful result")
		case <-time.After(10 * time.Second):
			t.Error("client query goroutine did not exit")
		}
	}()
	waitForDisconnectSession(t, ctx, observer, id, true)
	if mode == "reset" {
		require.NoError(t, transport.SetLinger(0))
	}
	require.NoError(t, transport.Close())
	waitForDisconnectSession(t, ctx, observer, id, false)
	serverGone = true

	// A new transaction must acquire the released row lock and see rollback,
	// rather than the uncommitted value left by the dead connection.
	lockCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	txn, err := observer.BeginTx(lockCtx, nil)
	require.NoError(t, err)
	defer txn.Rollback()
	var value int
	require.NoError(t, txn.QueryRowContext(lockCtx,
		"select v from disconnect_cancel_test.t where id=1 for update").Scan(&value))
	require.Equal(t, 10, value)
	require.NoError(t, txn.Commit())
}

func waitForDisconnectSession(t *testing.T, ctx context.Context, db *sql.DB, id uint64, running bool) {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	query := "select count(*) from processlist() p where conn_id = ?"
	if running {
		query += " and info like '%sleep(600)%'"
	}
	for {
		var count int
		require.NoError(t, db.QueryRowContext(pollCtx, query, id).Scan(&count))
		if (running && count == 1) || (!running && count == 0) {
			return
		}
		select {
		case <-pollCtx.Done():
			t.Fatalf("connection %d did not reach running=%v: %v", id, running, pollCtx.Err())
		case <-ticker.C:
		}
	}
}
