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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// A reconnect invalidates the subscription generation used by a PK check.
// Exercise that error through the frontend after earlier DDL/DML, then retry
// the entire transaction. The owned cluster isolates the reconnect callback
// and admission interruption from other SQL tests.
func TestPKCheckReconnectRollsBackAndFreshTxnSucceeds(t *testing.T) {
	started := time.Now()
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() {
			closing := time.Now()
			require.NoError(t, cluster.Close())
			t.Logf("cluster teardown: %s", time.Since(closing))
		})
	}
	require.NoError(t, err)
	t.Logf("cluster startup: %s", time.Since(started))
	bodyStarted := time.Now()
	defer func() { t.Logf("SQL and recovery: %s", time.Since(bodyStarted)) }()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cnOp, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn := cnOp.RawService().(cnservice.Service)
	pushClient := cn.GetEngine().(*disttae.Engine).PushClient()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=10s&readTimeout=30s&writeTimeout=10s", cnOp.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, waitSystemBootstrap(ctx, db))
	execSQLRequire(t, ctx, db, "create database reconnect_guard")
	execSQLRequire(t, ctx, db, "create table reconnect_guard.t(id int primary key)")
	execSQLRequire(t, ctx, db, "insert into reconnect_guard.t values (1)")
	execSQLRequire(t, ctx, db, "create database reconnect_target")
	execSQLRequire(t, ctx, db, "create table reconnect_target.retained(id int primary key)")
	execSQLRequire(t, ctx, db, "insert into reconnect_target.retained values (10)")
	var tableID uint64
	require.NoError(t, db.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where reldatabase='reconnect_guard' and relname='t'").Scan(&tableID))
	writer, err := db.Conn(ctx)
	require.NoError(t, err)
	defer writer.Close()
	_, err = writer.ExecContext(ctx, "set mo_rollback_txn_on_error=0")
	require.NoError(t, err)
	txn, err := writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	defer txn.Rollback()
	_, err = txn.ExecContext(ctx, "insert into reconnect_guard.t values (2)")
	require.NoError(t, err)
	_, err = txn.ExecContext(ctx, "drop database reconnect_target")
	require.NoError(t, err)

	locked := make(chan []byte, 1)
	releaseLock, reconnecting, resumeReconnect := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseLockOnce, resumeOnce, reconnectOnce sync.Once
	unblockLock := func() { releaseLockOnce.Do(func() { close(releaseLock) }) }
	unblockReconnect := func() { resumeOnce.Do(func() { close(resumeReconnect) }) }
	defer unblockLock()
	defer unblockReconnect()
	pushClient.SetReconnectHandler(func() {
		reconnectOnce.Do(func() { close(reconnecting) })
		select {
		case <-resumeReconnect:
		case <-ctx.Done():
		}
	})
	testingCtx := moruntime.MustGetTestingContext(cnOp.ServiceID())
	before, adjust := testingCtx.GetBeforeLockFunc(), testingCtx.GetAdjustLockResultFunc()
	defer testingCtx.SetBeforeLockFunc(before)
	defer testingCtx.SetAdjustLockResultFunc(adjust)
	testingCtx.SetBeforeLockFunc(func([]byte, uint64) {})
	var armed atomic.Bool
	var frontier timestamp.Timestamp
	testingCtx.SetAdjustLockResultFunc(func(txnID []byte, id uint64, result *pblock.Result) {
		if id != tableID || !armed.CompareAndSwap(false, true) {
			return
		}
		locked <- bytes.Clone(txnID)
		select {
		case <-releaseLock:
			// Model a granted conflicting lock with a frontier already applied
			// before reconnect. Only the subscription-generation check may fail.
			result.HasConflict, result.HasPrevCommit = true, true
			result.Timestamp = frontier
		case <-ctx.Done():
		}
	})
	done := make(chan error, 1)
	joined := false
	defer func() {
		unblockLock()
		unblockReconnect()
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("DELETE did not terminate during cleanup")
			}
		}
	}()
	go func() {
		_, err := txn.ExecContext(ctx, "delete from reconnect_guard.t where id=1")
		done <- err
	}()
	var owner []byte
	select {
	case owner = <-locked:
	case err := <-done:
		joined = true
		t.Fatalf("DELETE ended before the lock barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var snapshot timestamp.Timestamp
	found := false
	cn.GetTxnClient().IterTxns(func(tx client.TxnOverview) bool {
		if bytes.Equal(tx.Meta.ID, owner) {
			found = true
			require.True(t, tx.Meta.IsRCIsolation())
			snapshot = tx.Meta.SnapshotTS
			return false
		}
		return true
	})
	require.True(t, found, "the parked DELETE must belong to an active transaction")
	// RC chooses the latest applied timestamp's successor. A TN read barrier
	// need not cover that snapshot, so wait for the parked transaction's actual
	// snapshot before choosing the already-applied conflicting lock frontier.
	_, err = cn.GetTxnClient().WaitLogTailAppliedAt(ctx, snapshot)
	require.NoError(t, err)
	frontier = pushClient.LatestLogtailAppliedTime()
	require.True(t, snapshot.LessEq(frontier))
	require.NoError(t, pushClient.Disconnect())
	select {
	case <-reconnecting:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, pushClient.IsSubscriberReady())
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
	require.NoError(t, checkLogtailResumed(ctx, cn))
	// COMMIT after the error must not publish the earlier insert or drop.
	// An observer alone could miss an incorrectly still-active transaction.
	require.NoError(t, txn.Commit())
	var rows, retained int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from reconnect_guard.t where id=2").Scan(&rows))
	require.Zero(t, rows)
	require.NoError(t, db.QueryRowContext(ctx, "select id from reconnect_target.retained").Scan(&retained))
	require.Equal(t, 10, retained)
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from reconnect_guard.t where id=1").Scan(&rows))
	require.Equal(t, 1, rows)

	retry, err := writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	defer retry.Rollback()
	for _, statement := range []string{
		"insert into reconnect_guard.t values (2)",
		"drop database reconnect_target",
		"delete from reconnect_guard.t where id=1",
	} {
		_, err = retry.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	require.NoError(t, retry.Commit())
	require.NoError(t, db.QueryRowContext(ctx, "select count(*), coalesce(min(id), -1) from reconnect_guard.t").Scan(&rows, &retained))
	require.Equal(t, 1, rows)
	require.Equal(t, 2, retained)
	require.NoError(t, db.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_database where datname='reconnect_target'").Scan(&rows))
	require.Zero(t, rows)
	execSQLRequire(t, ctx, db, "drop database reconnect_guard")
}
