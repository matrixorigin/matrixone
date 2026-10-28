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

package isolated

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// DROP's internal index-task DELETE must abort with the whole transaction
// when reconnect invalidates the subscription used by its PK check.
func TestDropDatabaseReconnectRollback(t *testing.T) {
	start := time.Now()
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithCNCount(1))
	if cluster != nil {
		var dataDir string
		cluster.ForeachServices(func(service embed.ServiceOperator) bool {
			dataDir = service.GetServiceConfig().DataDir
			return false
		})
		t.Cleanup(func() {
			if err := cluster.Close(); err != nil {
				t.Errorf("close owned cluster: %v", err)
				return
			}
			if dataDir != "" {
				require.NoError(t, os.RemoveAll(dataDir))
			}
		})
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	t.Logf("cluster startup: %s", time.Since(start))
	cnOp, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn := cnOp.RawService().(cnservice.Service)
	push := cn.GetEngine().(*disttae.Engine).PushClient()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=10s&readTimeout=30s&writeTimeout=10s", cnOp.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()

	for _, tc := range []struct {
		name     string
		explicit bool
	}{{"autocommit", false}, {"explicit", true}} {
		if !t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			exec := func(sqlText string) {
				t.Helper()
				_, err := db.ExecContext(ctx, sqlText)
				require.NoError(t, err, sqlText)
			}
			target := "issue31542_target_" + tc.name
			guard := "issue31542_guard_" + tc.name
			exec("create database " + target)
			exec("create table " + target + ".retained(id int primary key)")
			exec("insert into " + target + ".retained values (10)")
			if tc.explicit {
				exec("create database " + guard)
				exec("create table " + guard + ".t(id int primary key)")
			}
			var catalogID, targetID uint64
			require.NoError(t, db.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_index_update'").Scan(&catalogID))
			require.NoError(t, db.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase=? and relname='retained'", target).Scan(&targetID))
			require.NoError(t, idxcron.RegisterUpdate(defines.AttachAccountId(ctx, 0), cnOp.ServiceID(), nil,
				targetID, target, "retained", "probe", "whitebox", "{}"))
			jobCount := func() int {
				t.Helper()
				var count int
				require.NoError(t, db.QueryRowContext(ctx,
					"select count(*) from mo_catalog.mo_index_update where account_id=0 and db_name=?", target).Scan(&count))
				return count
			}
			require.Equal(t, 1, jobCount())
			writer, err := db.Conn(ctx)
			require.NoError(t, err)
			defer writer.Close()
			_, err = writer.ExecContext(ctx, "set mo_rollback_txn_on_error=0")
			require.NoError(t, err)
			_, err = writer.ExecContext(ctx, "set session transaction isolation level read committed")
			require.NoError(t, err)
			var connectionID uint64
			require.NoError(t, writer.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
			var tx *sql.Tx
			if tc.explicit {
				tx, err = writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
				require.NoError(t, err)
				defer tx.Rollback()
				_, err = tx.ExecContext(ctx, "create table "+guard+".staged(id int primary key)")
				require.NoError(t, err)
				_, err = tx.ExecContext(ctx, "insert into "+guard+".t values (2)")
				require.NoError(t, err)
			}

			locked := make(chan []byte, 1)
			releaseLock, reconnecting, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce, resumeOnce, reconnectOnce sync.Once
			unblockLock := func() { releaseOnce.Do(func() { close(releaseLock) }) }
			unblockReconnect := func() { resumeOnce.Do(func() { close(resume) }) }
			done := make(chan error, 1)
			started, joined := false, false
			testingCtx := moruntime.MustGetTestingContext(cnOp.ServiceID())
			defer func() {
				unblockLock()
				unblockReconnect()
				cancel()
				if started && !joined {
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("DROP did not terminate during cleanup")
					}
				}
				testingCtx.SetAdjustLockResultFunc(nil)
				testingCtx.SetBeforeLockFunc(nil)
				push.SetReconnectHandler(nil)
			}()
			push.SetReconnectHandler(func() {
				reconnectOnce.Do(func() { close(reconnecting) })
				select {
				case <-resume:
				case <-ctx.Done():
				}
			})
			testingCtx.SetBeforeLockFunc(func([]byte, uint64) {})
			var armed atomic.Bool
			var frontier timestamp.Timestamp
			testingCtx.SetAdjustLockResultFunc(func(txnID []byte, tableID uint64, result *pblock.Result) {
				if tableID != catalogID || !armed.CompareAndSwap(false, true) {
					return
				}
				locked <- bytes.Clone(txnID)
				select {
				case <-releaseLock:
					// Simulate a conflict at an already applied frontier; the
					// internal DELETE and reconnect are real.
					result.HasConflict, result.HasPrevCommit = true, true
					result.Timestamp = frontier
				case <-ctx.Done():
				}
			})
			started = true
			go func() {
				if tx != nil {
					_, err := tx.ExecContext(ctx, "drop database "+target)
					done <- err
					return
				}
				_, err := writer.ExecContext(ctx, "drop database "+target)
				done <- err
			}()
			var owner []byte
			select {
			case owner = <-locked:
			case err := <-done:
				joined = true
				t.Fatalf("DROP ended before the catalog lock: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var actor string
			require.NoError(t, db.QueryRowContext(ctx, "select txn_id from mo_catalog.mo_sessions where conn_id=?", connectionID).Scan(&actor))
			require.Equal(t, fmt.Sprintf("%x", owner), actor, "the catalog lock belongs to the DROP session")
			var snapshot timestamp.Timestamp
			found := false
			cn.GetTxnClient().IterTxns(func(info client.TxnOverview) bool {
				if bytes.Equal(info.Meta.ID, owner) {
					found = true
					require.True(t, info.Meta.IsRCIsolation())
					require.True(t, info.Meta.IsPessimistic())
					snapshot = info.Meta.SnapshotTS
					return false
				}
				return true
			})
			require.True(t, found, "the DROP transaction is active")
			applied, err := cn.GetTxnClient().WaitLogTailAppliedAt(ctx, snapshot)
			require.NoError(t, err)
			frontier = applied.Prev()
			require.False(t, frontier.IsEmpty())
			require.True(t, snapshot.LessEq(frontier))
			require.NoError(t, push.Disconnect())
			select {
			case <-reconnecting:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.False(t, push.IsSubscriberReady())
			unblockLock()
			select {
			case err = <-done:
				joined = true
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var serverErr *mysql.MySQLError
			require.ErrorAs(t, err, &serverErr)
			require.Equal(t, moerr.ErrRetryForCNRollingRestart, serverErr.Number)
			unblockReconnect()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for !push.IsSubscriberReady() {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if tx != nil {
				require.NoError(t, tx.Commit(), "a subsequent COMMIT must not publish aborted work")
			}
			var row, count int
			require.NoError(t, db.QueryRowContext(ctx, "select id from "+target+".retained").Scan(&row))
			require.Equal(t, 10, row)
			require.Equal(t, 1, jobCount())
			if tc.explicit {
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+guard+".t where id=2").Scan(&count))
				require.Zero(t, count)
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname='staged'", guard).Scan(&count))
				require.Zero(t, count)
				retry, err := writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
				require.NoError(t, err)
				defer retry.Rollback()
				for _, statement := range []string{"create table " + guard + ".staged(id int primary key)", "insert into " + guard + ".t values (2)", "drop database " + target} {
					_, err = retry.ExecContext(ctx, statement)
					require.NoError(t, err, statement)
				}
				require.NoError(t, retry.Commit())
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+guard+".t where id=2").Scan(&count))
				require.Equal(t, 1, count)
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname='staged'", guard).Scan(&count))
				require.Equal(t, 1, count)
				exec("drop database " + guard)
			} else {
				_, err = writer.ExecContext(ctx, "drop database "+target)
				require.NoError(t, err)
			}
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database where datname=?", target).Scan(&count))
			require.Zero(t, count)
			require.Zero(t, jobCount())
		}) {
			return
		}
	}
}
