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
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestIssue28012And28013AcceptedCommitDuringStandaloneShutdown keeps an
// accepted transaction after WaitWalAndTail and before on1PCApply, then starts
// a real embedded standalone shutdown. The shutdown must not outrun the
// accepted commit; after restart the multi-row statement is wholly visible or
// wholly absent, and the restarted cluster accepts a new transaction.
func TestIssue28012And28013AcceptedCommitDuringStandaloneShutdown(t *testing.T) {
	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}

	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), objectio.FJ_CommitWait)
		_, _ = fault.RemoveFaultPoint(context.Background(), embed.StandaloneLifecycleClose)
		if !closed {
			require.NoError(t, cluster.Close())
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db := openIssue28012And28013DB(t, cluster)
	defer db.Close()
	dbName := fmt.Sprintf("issue_28012_28013_%d", time.Now().UnixNano())
	mustExecIssue28012And28013(t, ctx, db, "create database "+dbName)
	mustExecIssue28012And28013(t, ctx, db, "create table "+dbName+".t (id int primary key, v int)")

	commitWaiters := "issue28012_28013_commit_waiters"
	closeWaiters := "issue28012_28013_close_waiters"
	commitRelease := "issue28012_28013_commit_release"
	closeRelease := "issue28012_28013_close_release"
	for _, point := range []string{commitWaiters, closeWaiters, commitRelease, closeRelease} {
		defer fault.RemoveFaultPoint(context.Background(), point)
	}
	require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CommitWait, "1:1::", "WAIT", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, commitWaiters, ":::", "GETWAITERS", 0, objectio.FJ_CommitWait, false))
	require.NoError(t, fault.AddFaultPoint(ctx, commitRelease, ":::", "NOTIFYALL", 0, objectio.FJ_CommitWait, false))
	require.NoError(t, fault.AddFaultPoint(ctx, embed.StandaloneLifecycleClose, "1:1::", "WAIT", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, closeWaiters, ":::", "GETWAITERS", 0, embed.StandaloneLifecycleClose, false))
	require.NoError(t, fault.AddFaultPoint(ctx, closeRelease, ":::", "NOTIFYALL", 0, embed.StandaloneLifecycleClose, false))

	commitResult := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(ctx, "insert into "+dbName+".t values (1, 10), (2, 20), (3, 30)")
		commitResult <- err
	}()
	waitIssue28012And28013FaultWaiters(t, commitWaiters, 1, 30*time.Second)

	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- cluster.Close() }()
	waitIssue28012And28013FaultWaiters(t, closeWaiters, 1, 30*time.Second)
	_, _, ok := fault.TriggerFault(closeRelease)
	require.True(t, ok)

	// The close path has now been admitted, but the accepted transaction still
	// owns its WAL/apply dependencies. Returning before the terminal result
	// would recreate the shutdown ordering bug.
	select {
	case err := <-shutdownResult:
		require.FailNow(t, "standalone shutdown returned before accepted commit completed", "error: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	_, _, ok = fault.TriggerFault(commitRelease)
	require.True(t, ok)
	var commitErr error
	select {
	case commitErr = <-commitResult:
	case <-time.After(30 * time.Second):
		t.Fatal("accepted commit did not reach a terminal result after WAL barrier release")
	}
	select {
	case err := <-shutdownResult:
		require.NoError(t, err)
		closed = true
	case <-time.After(30 * time.Second):
		t.Fatal("standalone shutdown did not finish after accepted commit completed")
	}
	require.NoError(t, db.Close())

	for _, point := range []string{objectio.FJ_CommitWait, embed.StandaloneLifecycleClose, commitWaiters, closeWaiters, commitRelease, closeRelease} {
		_, _ = fault.RemoveFaultPoint(context.Background(), point)
	}
	require.NoError(t, cluster.Start())
	closed = false

	restartedDB := openIssue28012And28013DB(t, cluster)
	defer restartedDB.Close()
	var count, total int
	require.NoError(t, restartedDB.QueryRowContext(ctx, "select count(*), coalesce(sum(v), 0) from "+dbName+".t").Scan(&count, &total))
	if commitErr == nil {
		require.Equal(t, []int{3, 60}, []int{count, total}, "successful commit must be wholly durable after restart")
	} else {
		require.Contains(t, []int{0, 3}, count, "failed client result must not leave a partial statement")
		require.Contains(t, []int{0, 60}, total, "failed client result must not leave partial values")
	}
	mustExecIssue28012And28013(t, ctx, restartedDB, "insert into "+dbName+".t values (4, 40)")
	var finalCount int
	require.NoError(t, restartedDB.QueryRowContext(ctx, "select count(*) from "+dbName+".t").Scan(&finalCount))
	require.Contains(t, []int{1, 4}, finalCount, "restarted standalone cluster must accept a new transaction")
}

func waitIssue28012And28013FaultWaiters(t *testing.T, probe string, want int64, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		waiters, _, ok := fault.TriggerFault(probe)
		return ok && waiters == want
	}, timeout, 10*time.Millisecond, "fault point %s did not reach %d waiters", probe, want)
}

func openIssue28012And28013DB(t *testing.T, cluster embed.Cluster) *sql.DB {
	t.Helper()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func mustExecIssue28012And28013(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "statement failed: %s", statement)
}
