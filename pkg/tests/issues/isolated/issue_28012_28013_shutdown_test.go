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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestIssue28012And28013AcceptedCommitDuringStandaloneShutdown keeps an
// accepted transaction after WaitWalAndTail and before on1PCApply, then starts
// TN shutdown inside a real embedded standalone cluster. TN shutdown must not
// outrun the accepted commit; after the complete standalone shutdown and
// restart, the multi-row statement is wholly visible or wholly absent, and the
// restarted cluster accepts a new transaction.
func TestIssue28012And28013AcceptedCommitDuringStandaloneShutdown(t *testing.T) {
	faultEnabledHere := fault.Enable()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	commitWaiters := "issue28012_28013_commit_waiters"
	drainWaiters := "issue28012_28013_drain_waiters"
	commitRelease := "issue28012_28013_commit_release"
	drainRelease := "issue28012_28013_drain_release"
	faultPoints := []string{
		objectio.FJ_CommitWait,
		rpc.FJ_TxnServerDrainWithActiveHandler,
		objectio.FJ_CommitWaitTargetTenant,
		commitWaiters,
		drainWaiters,
		commitRelease,
		drainRelease,
	}
	var (
		cluster    embed.Cluster
		dbs        []*sql.DB
		goroutines sync.WaitGroup
	)
	t.Cleanup(func() {
		// Remove WAIT points first: removing a WAIT wakes every registered
		// waiter, so both the accepted commit and shutdown can make progress
		// even when an assertion above calls FailNow.
		for _, point := range faultPoints {
			_, _ = fault.RemoveFaultPoint(context.Background(), point)
		}
		cancel()

		cleanupErr := waitIssue28012And28013Goroutines(&goroutines, 30*time.Second)
		for _, db := range dbs {
			cleanupErr = errors.Join(cleanupErr, db.Close())
		}
		if cluster != nil {
			cleanupErr = errors.Join(cleanupErr, cluster.Close())
		}
		if faultEnabledHere {
			fault.Disable()
		}
		require.NoError(t, cleanupErr)
	})

	var err error
	cluster, err = embed.StartTestCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	sysDB := openIssue28012And28013DB(t, cluster, "dump:111")
	dbs = append(dbs, sysDB)
	tn := issue28012And28013TNService(t, cluster)
	accountName := fmt.Sprintf("issue28012_%d", time.Now().UnixNano())
	mustExecIssue28012And28013(t, ctx, sysDB, fmt.Sprintf(
		"create account `%s` admin_name 'root' identified by '111'", accountName))
	var accountID uint32
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", accountName).Scan(&accountID))
	db := openIssue28012And28013DB(t, cluster, accountName+"#root#accountadmin:111")
	dbs = append(dbs, db)
	dbName := fmt.Sprintf("issue_28012_28013_%d", time.Now().UnixNano())
	mustExecIssue28012And28013(t, ctx, db, "create database "+dbName)
	txn, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = txn.ExecContext(ctx, "create table "+dbName+".t (id int primary key, v int)")
	require.NoError(t, err)
	_, err = txn.ExecContext(ctx, "insert into "+dbName+".t values (1, 10), (2, 20), (3, 30)")
	require.NoError(t, err)

	require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CommitWaitTargetTenant, ":::", "ECHO", int64(accountID), "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CommitWait, "1:1::", "WAIT", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, commitWaiters, ":::", "GETWAITERS", 0, objectio.FJ_CommitWait, false))
	require.NoError(t, fault.AddFaultPoint(ctx, commitRelease, ":::", "NOTIFYALL", 0, objectio.FJ_CommitWait, false))
	require.NoError(t, fault.AddFaultPoint(ctx, rpc.FJ_TxnServerDrainWithActiveHandler, "1:1::", "WAIT", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, drainWaiters, ":::", "GETWAITERS", 0, rpc.FJ_TxnServerDrainWithActiveHandler, false))
	require.NoError(t, fault.AddFaultPoint(ctx, drainRelease, ":::", "NOTIFYALL", 0, rpc.FJ_TxnServerDrainWithActiveHandler, false))

	commitResult := make(chan error, 1)
	goroutines.Add(1)
	go func() {
		defer goroutines.Done()
		commitResult <- txn.Commit()
	}()
	waitIssue28012And28013FaultWaiters(t, commitWaiters, 1, 30*time.Second)

	tnShutdownResult := make(chan error, 1)
	goroutines.Add(1)
	go func() {
		defer goroutines.Done()
		tnShutdownResult <- tn.Close()
	}()
	// This boundary is reached from the TN transaction server only after
	// quiesce has started and while the accepted commit is still counted as an
	// active handler. It deterministically proves the commit overlaps TN drain.
	waitIssue28012And28013FaultWaiters(t, drainWaiters, 1, 5*time.Second)

	_, _, ok := fault.TriggerFault(commitRelease)
	require.True(t, ok)
	var commitErr error
	select {
	case commitErr = <-commitResult:
	case <-time.After(30 * time.Second):
		t.Fatal("accepted commit did not reach a terminal result after WAL barrier release")
	}
	_, _, ok = fault.TriggerFault(drainRelease)
	require.True(t, ok)
	select {
	case err := <-tnShutdownResult:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("TN shutdown did not finish after accepted commit completed")
	}
	require.NoError(t, db.Close())
	require.NoError(t, cluster.Close())

	for _, point := range faultPoints {
		_, _ = fault.RemoveFaultPoint(context.Background(), point)
	}
	require.NoError(t, cluster.Start())

	restartedDB := openIssue28012And28013DB(t, cluster, accountName+"#root#accountadmin:111")
	dbs = append(dbs, restartedDB)
	var count, total int
	queryErr := restartedDB.QueryRowContext(ctx,
		"select count(*), coalesce(sum(v), 0) from "+dbName+".t").Scan(&count, &total)
	if commitErr == nil {
		require.NoError(t, queryErr)
		require.Equal(t, []int{3, 60}, []int{count, total}, "successful commit must be wholly durable after restart")
	} else if queryErr != nil {
		var mysqlErr *mysql.MySQLError
		require.True(t, errors.As(queryErr, &mysqlErr) && mysqlErr.Number == 1146,
			"unknown commit may leave the transaction wholly absent, got %v", queryErr)
	} else {
		require.Equal(t, []int{3, 60}, []int{count, total},
			"unknown commit may be wholly durable but must not be partial")
	}
	mustExecIssue28012And28013(t, ctx, restartedDB,
		"create table if not exists "+dbName+".t (id int primary key, v int)")
	mustExecIssue28012And28013(t, ctx, restartedDB, "insert into "+dbName+".t values (4, 40)")
	var finalCount int
	require.NoError(t, restartedDB.QueryRowContext(ctx, "select count(*) from "+dbName+".t").Scan(&finalCount))
	require.Contains(t, []int{1, 4}, finalCount, "restarted standalone cluster must accept a new transaction")
}

func issue28012And28013TNService(t *testing.T, cluster embed.Cluster) tnservice.Service {
	t.Helper()
	var service tnservice.Service
	cluster.ForeachServices(func(operator embed.ServiceOperator) bool {
		if operator.ServiceType() != metadata.ServiceType_TN {
			return true
		}
		raw := operator.RawService()
		var ok bool
		service, ok = raw.(tnservice.Service)
		require.True(t, ok, "embedded TN operator returned %T", raw)
		return false
	})
	require.NotNil(t, service, "embedded cluster has no TN service")
	return service
}

func waitIssue28012And28013Goroutines(goroutines *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		goroutines.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("issue 28012/28013 goroutines did not exit within %s", timeout)
	}
}

func waitIssue28012And28013FaultWaiters(t *testing.T, probe string, want int64, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		waiters, _, ok := fault.TriggerFault(probe)
		return ok && waiters == want
	}, timeout, 10*time.Millisecond, "fault point %s did not reach %d waiters", probe, want)
}

func openIssue28012And28013DB(t *testing.T, cluster embed.Cluster, credentials string) *sql.DB {
	t.Helper()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(127.0.0.1:%d)/", credentials, cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func mustExecIssue28012And28013(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "statement failed: %s", statement)
}
