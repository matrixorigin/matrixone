// Copyright 2026 Matrix Origin
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

package isolated

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// Restart owns a separate cluster; never restart the SQL package's shared CN.
// All replay statements are predetermined substrate probes, not native Rebase.
func TestRebaseTransactionRestart(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	open := func() *sql.DB {
		t.Helper()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.NoError(t, waitSystemBootstrap(ctx, db))
		return db
	}
	db := open()
	exec := func(query string) {
		t.Helper()
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	for _, table := range []string{"a", "b"} {
		if table == "a" {
			exec("create database rebase_p1_restart_upstream")
		}
		exec("create table rebase_p1_restart_upstream." + table + "(id int primary key,v int)")
		exec("insert into rebase_p1_restart_upstream." + table + " values(1,10),(2,20)")
	}
	exec("create snapshot rebase_p1_restart_u for database rebase_p1_restart_upstream")
	begin := func(name string) *sql.Conn {
		t.Helper()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_ = conn.Close()
		})
		for _, query := range []string{
			"set session transaction isolation level read committed", "begin",
			"data branch create database " + name + " from rebase_p1_restart_upstream{snapshot='rebase_p1_restart_u'}",
			"update " + name + ".a set v=11 where id=1",
			"delete from " + name + ".b where id=2",
		} {
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		return conn
	}
	ids := func(conn *sql.Conn, name string) [2]uint64 {
		t.Helper()
		var out [2]uint64
		for i, table := range []string{"a", "b"} {
			require.NoError(t, conn.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase=? and relname=?", name, table).Scan(&out[i]))
		}
		return out
	}

	failed := begin("rebase_p1_restart_failed")
	failedIDs := ids(failed, "rebase_p1_restart_failed")
	func() {
		wasEnabled := fault.Status()
		fault.Enable()
		defer func() {
			_, err := fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNCommitAfterWorkspaceDumpFailed)
			require.NoError(t, err)
			if !wasEnabled {
				fault.Disable()
			}
		}()
		require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CNCommitAfterWorkspaceDumpFailed,
			":::", "echo", 0, "rebase p1 injected commit failure", false))
		_, err := failed.ExecContext(ctx, "commit")
		require.ErrorContains(t, err, "rebase p1 injected commit failure")
	}()
	_, err = failed.ExecContext(ctx, "rollback")
	require.NoError(t, err)
	require.NoError(t, failed.Close())

	committed := begin("rebase_p1_restart_committed")
	committedIDs := ids(committed, "rebase_p1_restart_committed")
	_, err = committed.ExecContext(ctx, "commit")
	require.NoError(t, err)
	require.NoError(t, committed.Close())

	aborted := begin("rebase_p1_restart_disconnected")
	abortedIDs := ids(aborted, "rebase_p1_restart_disconnected")
	// Force a real client disconnect, not a returned-to-pool connection. Restart
	// must never recover its unacknowledged/uncommitted clone as committed data.
	require.ErrorIs(t, aborted.Raw(func(any) error { return driver.ErrBadConn }), driver.ErrBadConn)
	require.NoError(t, db.Close())
	require.NoError(t, cluster.Stop())
	require.NoError(t, cluster.Start())
	db = open()
	for _, name := range []string{"rebase_p1_restart_failed", "rebase_p1_restart_disconnected"} {
		var n int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_database where account_id=0 and datname=?", name).Scan(&n))
		require.Zero(t, n)
	}
	for _, tid := range append(failedIDs[:], abortedIDs[:]...) {
		var n int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_branch_metadata where table_id=?", tid).Scan(&n))
		require.Zero(t, n)
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname=?", fmt.Sprintf("__mo_branch_%d", tid)).Scan(&n))
		require.Zero(t, n)
	}
	exec("drop snapshot rebase_p1_restart_u")
	for i, table := range []string{"a", "b"} {
		var value int
		require.NoError(t, db.QueryRowContext(ctx,
			"select v from rebase_p1_restart_committed."+table+" where id=1").Scan(&value))
		want := 10
		if table == "a" {
			want = 11
		}
		require.Equal(t, want, value)
		var ts int64
		require.NoError(t, db.QueryRowContext(ctx,
			"select clone_ts from mo_catalog.mo_branch_metadata where table_id=?", committedIDs[i]).Scan(&ts))
		var pins int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname=? and ts=?", fmt.Sprintf("__mo_branch_%d", committedIDs[i]), ts).Scan(&pins))
		require.Equal(t, 1, pins)
		exec("update rebase_p1_restart_upstream." + table + " set v=99 where id=1")
		require.NoError(t, db.QueryRowContext(ctx,
			fmt.Sprintf("select v from rebase_p1_restart_upstream.%s{MO_TS=%d} where id=1", table, ts)).Scan(&value))
		require.Equal(t, 10, value)
	}
}
