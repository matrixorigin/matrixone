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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestRebaseTransactionCancellation(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists rebase_p1_cancel_target", "drop database if exists rebase_p1_cancel_source")
		exec := func(query string) {
			t.Helper()
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		count := func(query string) int {
			t.Helper()
			var n int
			require.NoError(t, db.QueryRowContext(ctx, query).Scan(&n))
			return n
		}
		exec("create database rebase_p1_cancel_source")
		var parents [2]uint64
		for i, table := range []string{"a", "b"} {
			exec("create table rebase_p1_cancel_source." + table + "(id int primary key,v int)")
			exec("insert into rebase_p1_cancel_source." + table + " values(1,10)")
			require.NoError(t, db.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='rebase_p1_cancel_source' and relname=?", table).Scan(&parents[i]))
		}
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		var connectionID uint32
		require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
		// Without a user snapshot, the first exclusive mo_snapshots grant is
		// the child protection pin: table cloning and an edge already happened.
		pacer := newAdmissionPacer(t, "mo_snapshots#0#Exclusive", true)
		queryCtx, queryCancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			_, err := conn.ExecContext(queryCtx, "data branch create database rebase_p1_cancel_target from rebase_p1_cancel_source")
			done <- err
		}()
		joined := false
		defer func() {
			// Cancel on the server BEFORE releasing the hook. A client context
			// cancellation alone need not reach a handler parked in this hook.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("kill query %d", connectionID))
			queryCancel()
			pacer.releaseOnce.Do(func() { close(pacer.release) })
			if !joined {
				select {
				case <-done:
				case <-cleanupCtx.Done():
					t.Error("cancelled clone did not terminate")
				}
			}
		}()
		pacer.waitFired(t, "private clone before its protection pin")
		require.Zero(t, count("select count(*) from mo_catalog.mo_database where account_id=0 and datname='rebase_p1_cancel_target'"), "uncommitted destination must be invisible to another session")
		// A real committed source writer runs while the private clone owner is
		// paused. It must not require taking over or ending the clone owner.
		exec("update rebase_p1_cancel_source.a set v=99 where id=1")
		exec(fmt.Sprintf("kill query %d", connectionID))
		pacer.resume(t)
		select {
		case err := <-done:
			joined = true
			require.Error(t, err)
		case <-ctx.Done():
			t.Fatal("cancelled clone did not return")
		}
		require.Zero(t, count("select count(*) from mo_catalog.mo_database where account_id=0 and datname='rebase_p1_cancel_target'"))
		require.Zero(t, count(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where p_table_id in (%d,%d)", parents[0], parents[1])))
		require.Zero(t, count("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='rebase_p1_cancel_source'"))
		require.Equal(t, 99, count("select v from rebase_p1_cancel_source.a where id=1"))
		require.Equal(t, 10, count("select v from rebase_p1_cancel_source.b where id=1"))
		// The restored hook must not intercept synchronous teardown.
		pacer.restore()
	})
}
