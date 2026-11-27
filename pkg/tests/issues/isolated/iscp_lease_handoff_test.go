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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestISCPManagedLeaseHandoff(t *testing.T) {
	path, err := os.MkdirTemp("", "managed-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(path)) })
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(path), embed.WithCNCount(2),
		embed.WithPreStart(func(s embed.ServiceOperator) {
			if s.ServiceType() == metadata.ServiceType_CN {
				s.Adjust(func(cfg *embed.ServiceConfig) {
					cfg.CN.TaskRunner.FetchInterval.Duration = 500 * time.Millisecond
					cfg.CN.TaskRunner.HeartbeatInterval.Duration = time.Hour
					cfg.CN.TaskRunner.HeartbeatTimeout.Duration = 5 * time.Minute
				})
			}
		}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	first, err := cluster.GetCNService(0)
	require.NoError(t, err)
	second, err := cluster.GetCNService(1)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=45s&writeTimeout=45s", first.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// Keep the scenario timeout independent of waiting for cluster admission.
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var oldOwner string
	var daemonID uint64
	var oldRuntime *iscp.ISCPTaskExecutor
	require.Eventually(t, func() bool {
		if conn.QueryRowContext(ctx, "select task_id,task_runner from mo_task.sys_daemon_task where task_type='ISCP' and task_runner is not null").Scan(&daemonID, &oldOwner) != nil {
			return false
		}
		var ok bool
		oldRuntime, ok = iscp.GetExecutorRuntime(oldOwner)
		return ok && oldRuntime.IsRunning()
	}, 90*time.Second, 100*time.Millisecond)
	nextOwner := first.ServiceID()
	if nextOwner == oldOwner {
		nextOwner = second.ServiceID()
	}
	require.NotEqual(t, oldOwner, nextOwner)
	ts, ready := first.RawService().(cnservice.Service).GetTaskService()
	require.True(t, ready)
	run := func(q string) { t.Helper(); _, e := conn.ExecContext(ctx, q); require.NoError(t, e, q) }
	run("create database managed_iscp_handoff")
	run("use managed_iscp_handoff")
	run("set experimental_hnsw_index=1")
	run("create table h(a int,id bigint generated always as(a*2) stored,v vecf32(3),primary key(id))")
	run("insert into h(a,v) values(1,'[1,2,3]'),(2,'[9,9,9]')")
	run("create index hv using hnsw on h(v) op_type 'vector_l2_ops'")
	query := "select table_id,job_id,job_state,job_status from mo_catalog.mo_iscp_log where drop_at is null and job_name='index_hv' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='managed_iscp_handoff' and relname='h')"
	read := func() (uint64, uint64, int8, iscp.JobStatus, error) {
		var tableID, jobID uint64
		var state int8
		var status iscp.JobStatus
		var raw string
		e := db.QueryRowContext(ctx, query).Scan(&tableID, &jobID, &state, &raw)
		if e == nil {
			e = json.Unmarshal([]byte(raw), &status)
		}
		return tableID, jobID, state, status, e
	}
	require.Eventually(t, func() bool {
		_, _, s, st, e := read()
		return e == nil && s == iscp.ISCPJobState_Completed && st.ErrorCode == 0
	}, 60*time.Second, 100*time.Millisecond)
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	key := objectio.FJ_ISCPCancelLongHnswBeforeSave + ":hv"
	require.NoError(t, fault.AddFaultPoint(ctx, key, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, "managed-waiters", ":::", "getwaiters", 0, key, false))
	submitKey := objectio.FJ_ISCPCancelAfterSubmit
	require.NoError(t, fault.AddFaultPoint(ctx, submitKey, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, "managed-submit-waiters", ":::", "getwaiters", 0, submitKey, false))
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), key)
		_, _ = fault.RemoveFaultPoint(context.Background(), "managed-waiters")
		_, _ = fault.RemoveFaultPoint(context.Background(), submitKey)
		_, _ = fault.RemoveFaultPoint(context.Background(), "managed-submit-waiters")
	})
	run("insert into h(a,v) values(3,'[0,0,0]')")
	require.Eventually(t, func() bool { n, _, ok := fault.TriggerFault("managed-waiters"); return ok && n == 1 }, 60*time.Second, 100*time.Millisecond, "old consumer must wait")
	require.Eventually(t, func() bool { n, _, ok := fault.TriggerFault("managed-submit-waiters"); return ok && n == 1 }, 10*time.Second, 100*time.Millisecond, "old scheduler must be held after submission")
	tableID, jobID, state, oldStatus, e := read()
	require.NoError(t, e)
	require.Equal(t, iscp.ISCPJobState_Running, state)
	daemon, err := ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, daemonID))
	require.NoError(t, err)
	require.Len(t, daemon, 1)
	// Simulate an expired lease; the normal taskservice claim moves it to the
	// other CN while the old consumer and scheduler are held at known barriers.
	daemon[0].LastHeartbeat = time.Now().Add(-10 * time.Minute)
	updated, err := ts.UpdateDaemonTask(ctx, daemon, taskservice.WithTaskRunnerCond(taskservice.EQ, oldOwner), taskservice.WithTaskStatusCond(task.TaskStatus_Running))
	require.NoError(t, err)
	require.Equal(t, 1, updated)
	require.Eventually(t, func() bool {
		var owner string
		if db.QueryRowContext(ctx, fmt.Sprintf("select task_runner from mo_task.sys_daemon_task where task_id=%d", daemonID)).Scan(&owner) != nil || owner != nextOwner {
			return false
		}
		rt, ok := iscp.GetExecutorRuntime(nextOwner)
		if !ok || !rt.IsRunning() {
			return false
		}
		n, _, ok := fault.TriggerFault("managed-waiters")
		if !ok || n != 2 {
			return false
		}
		_, _, s, st, e := read()
		return e == nil && s == iscp.ISCPJobState_Running && st.LSN > oldStatus.LSN
	}, 90*time.Second, 100*time.Millisecond, "successor must claim and run while old consumer remains held")
	require.NoError(t, oldRuntime.CancelAndDrainJobConsumer(ctx, 0, tableID, "index_hv", jobID))
	_, err = fault.RemoveFaultPoint(ctx, submitKey)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, ok := iscp.GetExecutorRuntime(oldOwner)
		n, _, w := fault.TriggerFault("managed-waiters")
		return !ok && w && n == 1
	}, 30*time.Second, 100*time.Millisecond, "old recovery must stop without disturbing successor")
	_, _, state, _, err = read()
	require.NoError(t, err)
	require.Equal(t, iscp.ISCPJobState_Running, state)

	// The other recovery entry is a delayed startup from the former owner.
	// It must reject the same persisted lease while the successor stays held.
	oldCN := first
	if oldCN.ServiceID() != oldOwner {
		oldCN = second
	}
	svc := oldCN.RawService().(cnservice.Service)
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	stale, err := iscp.NewISCPTaskExecutor(ctx, svc.GetEngine(), svc.GetTxnClient(), oldOwner, daemonID,
		&iscp.ISCPExecutorOption{GCInterval: time.Hour, SyncTaskInterval: time.Hour, FlushWatermarkInterval: time.Hour, RetryTimes: 1}, mp)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stale.Cancel() })
	err = stale.Start()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask), "stale startup must reject lost ownership: %v", err)
	_, _, state, _, err = read()
	require.NoError(t, err)
	require.Equal(t, iscp.ISCPJobState_Running, state)

	_, err = fault.RemoveFaultPoint(ctx, key)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, _, s, st, e := read()
		return e == nil && s == iscp.ISCPJobState_Completed && st.ErrorCode == 0
	}, 60*time.Second, 100*time.Millisecond)
	var nearest int64
	require.NoError(t, conn.QueryRowContext(ctx, "select id from h order by l2_distance(v,'[0,0,0]') limit 1").Scan(&nearest))
	require.Equal(t, int64(6), nearest)
	verifyOptimisticLeaseConflict(t, ctx, first, second, conn, ts)
}

// Optimistic task storage can commit despite the repair's pessimistic row
// lock. The repair must then fail its own commit instead of publishing jobs.
func verifyOptimisticLeaseConflict(t *testing.T, ctx context.Context, first, second embed.ServiceOperator, conn *sql.Conn, ts taskservice.TaskService) {
	t.Helper()
	dbSecond, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=45s&writeTimeout=45s", second.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbSecond.Close()) })
	_, err = conn.ExecContext(ctx, "create table lease_jobs(id bigint primary key, state int, lsn int, watermark varchar(32))")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "insert into lease_jobs values(1,2,7,'old')")
	require.NoError(t, err)

	svc := first.RawService().(cnservice.Service)
	eng, tc := svc.GetEngine(), svc.GetTxnClient()
	sysCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	begin := func(isolation pbtxn.TxnIsolation) (client.TxnOperator, func()) {
		t.Helper()
		op, beginErr := tc.New(sysCtx, eng.LatestLogtailAppliedTime(), client.WithTxnMode(pbtxn.TxnMode_Pessimistic), client.WithTxnIsolation(isolation))
		active := op != nil
		t.Cleanup(func() {
			if active {
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(sysCtx), 15*time.Second)
				defer cancel()
				require.NoError(t, op.Rollback(cleanupCtx))
			}
		})
		require.NoError(t, beginErr)
		require.NotNil(t, op)
		require.NoError(t, eng.New(sysCtx, op))
		return op, func() { active = false }
	}
	run := func(op client.TxnOperator, sql string) (uint64, error) {
		result, runErr := iscp.ExecWithResult(sysCtx, sql, first.ServiceID(), op)
		if runErr != nil {
			return 0, runErr
		}
		defer result.Close()
		return result.AffectedRows, nil
	}

	setup, finishSetup := begin(pbtxn.TxnIsolation_RC)
	_, err = run(setup, `insert into mo_task.sys_daemon_task (task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,account_id,account,task_type,task_runner,task_status,create_at,update_at,details) select 'lease-transaction-test',task_metadata_executor,task_metadata_context,task_metadata_option,account_id,account,'Unknown','old-cn',4,now(),now(),details from mo_task.sys_daemon_task where task_type='ISCP' limit 1`)
	require.NoError(t, err)
	require.NoError(t, setup.Commit(sysCtx))
	finishSetup()
	var taskID uint64
	require.NoError(t, conn.QueryRowContext(ctx, "select task_id from mo_task.sys_daemon_task where task_metadata_id='lease-transaction-test'").Scan(&taskID))
	tasks, err := ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, taskID))
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// Keep the repair snapshot fixed before its first job write. A heartbeat
	// that commits during the scan must fail the attempt closed; the retry can
	// then start from the heartbeat's latest snapshot.
	repair, finishRepair := begin(pbtxn.TxnIsolation_SI)
	require.NoError(t, repair.GetWorkspace().AdvanceSnapshot(sysCtx, timestamp.Timestamp{}))
	affected, err := run(repair, "update managed_iscp_handoff.lease_jobs set state=3 where id=1")
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	marker := time.Now().In(time.Local).Truncate(time.Second)
	for i := 1; i <= 2; i++ {
		tasks[0].LastHeartbeat = marker.Add(time.Duration(i) * time.Second)
		count, heartbeatErr := ts.GetStorage().HeartbeatDaemonTask(ctx, tasks)
		require.NoError(t, heartbeatErr)
		require.Equal(t, 1, count)
	}
	var visibilityErr error
	require.Eventually(t, func() bool {
		var count int
		visibilityErr = conn.QueryRowContext(ctx, "select count(*) from mo_task.sys_daemon_task where task_id=? and last_heartbeat=?", taskID, tasks[0].LastHeartbeat.Format("2006-01-02 15:04:05")).Scan(&count)
		return visibilityErr != nil || count == 1
	}, 30*time.Second, 100*time.Millisecond, "final heartbeat must be visible on the repair CN")
	require.NoError(t, visibilityErr)
	guard := fmt.Sprintf("update mo_task.sys_daemon_task set task_runner=task_runner where task_id=%d and task_type='Unknown' and task_runner='old-cn' and task_status=4", taskID)
	affected, err = run(repair, guard)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	commitErr := repair.Commit(sysCtx)
	finishRepair()
	require.True(t, moerr.IsMoErrCode(commitErr, moerr.ErrTxnWWConflict), "heartbeat after the fixed snapshot must invalidate this attempt: %v", commitErr)
	var state int
	var owner string
	require.NoError(t, conn.QueryRowContext(ctx, "select state from lease_jobs where id=1").Scan(&state))
	require.Equal(t, 2, state, "failed attempt must not publish its job update")
	require.NoError(t, conn.QueryRowContext(ctx, "select task_runner from mo_task.sys_daemon_task where task_id=?", taskID).Scan(&owner))
	require.Equal(t, "old-cn", owner)

	// A fresh repair transaction includes that heartbeat and can make progress.
	repair, finishRepair = begin(pbtxn.TxnIsolation_SI)
	require.NoError(t, repair.GetWorkspace().AdvanceSnapshot(sysCtx, timestamp.Timestamp{}))
	affected, err = run(repair, "update managed_iscp_handoff.lease_jobs set state=3 where id=1")
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	affected, err = run(repair, guard)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	require.NoError(t, repair.Commit(sysCtx))
	finishRepair()
	require.NoError(t, conn.QueryRowContext(ctx, "select state from lease_jobs where id=1").Scan(&state))
	require.Equal(t, 3, state, "retry on the fresh snapshot must apply the repair")
	_, err = conn.ExecContext(ctx, "update lease_jobs set state=2,lsn=7,watermark='old' where id=1")
	require.NoError(t, err)

	// ISCP job writers use pessimistic transactions. A concurrent update to a
	// row being repaired must serialize behind the repair. Keep this separate
	// from lease takeover so either fence cannot mask the other.
	repair, finishRepair = begin(pbtxn.TxnIsolation_SI)
	require.NoError(t, repair.GetWorkspace().AdvanceSnapshot(sysCtx, timestamp.Timestamp{}))
	affected, err = run(repair, "update managed_iscp_handoff.lease_jobs set state=3 where id=1")
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)

	// Task storage may connect to either CN. Save both runtime settings so this
	// test can exercise pessimistic job writes and an optimistic lease takeover.
	restoreTxnModes := make([]func(), 0, 2)
	for _, id := range []string{first.ServiceID(), second.ServiceID()} {
		rt := moruntime.ServiceRuntime(id)
		oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
		oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
		restoreTxnModes = append(restoreTxnModes, func() {
			if !hadMode {
				oldMode = pbtxn.TxnMode_Pessimistic
			}
			if !hadIsolation {
				oldIsolation = pbtxn.TxnIsolation_RC
			}
			rt.SetGlobalVariables(moruntime.TxnMode, oldMode)
			rt.SetGlobalVariables(moruntime.TxnIsolation, oldIsolation)
		})
	}
	defer func() {
		for _, restore := range restoreTxnModes {
			restore()
		}
	}()
	setTxnModes := func(mode pbtxn.TxnMode, isolation pbtxn.TxnIsolation) {
		for _, id := range []string{first.ServiceID(), second.ServiceID()} {
			rt := moruntime.ServiceRuntime(id)
			rt.SetGlobalVariables(moruntime.TxnMode, mode)
			rt.SetGlobalVariables(moruntime.TxnIsolation, isolation)
		}
	}
	setTxnModes(pbtxn.TxnMode_Pessimistic, pbtxn.TxnIsolation_RC)
	var leaseJobsTableID uint64
	require.NoError(t, conn.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where reldatabase='managed_iscp_handoff' and relname='lease_jobs'",
	).Scan(&leaseJobsTableID))
	jobWriteDone := make(chan error, 1)
	go func() {
		_, writeErr := dbSecond.ExecContext(ctx, "update managed_iscp_handoff.lease_jobs set lsn=8,watermark='successor' where id=1")
		jobWriteDone <- writeErr
	}()
	var lockWaitErr error
	require.Eventually(t, func() bool {
		var waiting int
		lockWaitErr = conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_locks where table_id=? and lock_wait<>''", fmt.Sprint(leaseJobsTableID),
		).Scan(&waiting)
		return lockWaitErr != nil || waiting > 0
	}, 10*time.Second, 50*time.Millisecond, "pessimistic writer must reach the row-lock wait queue")
	require.NoError(t, lockWaitErr)
	select {
	case writeErr := <-jobWriteDone:
		require.NoError(t, writeErr)
		t.Fatal("pessimistic job update must wait for the repair row lock")
	default:
	}
	affected, err = run(repair, guard)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	require.NoError(t, repair.Commit(sysCtx))
	finishRepair()
	require.NoError(t, <-jobWriteDone)
	var lsn int
	var watermark string
	require.NoError(t, conn.QueryRowContext(ctx, "select state,lsn,watermark from lease_jobs where id=1").Scan(&state, &lsn, &watermark))
	require.NoError(t, conn.QueryRowContext(ctx, "select task_runner from mo_task.sys_daemon_task where task_id=?", taskID).Scan(&owner))
	require.Equal(t, 3, state, "repair must commit before the waiting job writer")
	require.Equal(t, 8, lsn, "waiting writer's LSN must be preserved")
	require.Equal(t, "successor", watermark, "waiting writer's watermark must be preserved")
	require.Equal(t, "old-cn", owner, "this scenario must not depend on lease takeover")

	// In a separate transaction, an optimistic takeover alone must invalidate
	// the repair commit while leaving the job row unchanged.
	_, err = conn.ExecContext(ctx, "update lease_jobs set state=2,lsn=7,watermark='old' where id=1")
	require.NoError(t, err)
	setTxnModes(pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_SI)
	repair, finishRepair = begin(pbtxn.TxnIsolation_SI)
	require.NoError(t, repair.GetWorkspace().AdvanceSnapshot(sysCtx, timestamp.Timestamp{}))
	affected, err = run(repair, "update managed_iscp_handoff.lease_jobs set state=3 where id=1")
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	affected, err = run(repair, guard)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	tasks[0].TaskRunner = "new-cn"
	updated, err := ts.UpdateDaemonTask(ctx, tasks,
		taskservice.WithTaskRunnerCond(taskservice.EQ, "old-cn"),
		taskservice.WithTaskStatusCond(task.TaskStatus_Error))
	require.NoError(t, err)
	require.Equal(t, 1, updated)
	var readErr error
	require.Eventually(t, func() bool {
		readErr = conn.QueryRowContext(ctx, "select task_runner from mo_task.sys_daemon_task where task_id=?", taskID).Scan(&owner)
		return readErr != nil || owner == "new-cn"
	}, 30*time.Second, 100*time.Millisecond, "optimistic takeover must be visible before stale repair commits")
	require.NoError(t, readErr)
	beforeCommit := repair.SnapshotTS()
	err = repair.Commit(sysCtx)
	finishRepair()
	commitErr = err
	require.NoError(t, conn.QueryRowContext(ctx, "select state,lsn,watermark from lease_jobs where id=1").Scan(&state, &lsn, &watermark))
	require.NoError(t, conn.QueryRowContext(ctx, "select task_runner from mo_task.sys_daemon_task where task_id=?", taskID).Scan(&owner))
	var count int
	require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_task.sys_daemon_task where task_id=?", taskID).Scan(&count))
	t.Logf("stale repair commit: %v; durable job state: %d; lsn: %d; watermark: %s; durable owner: %s; row count: %d; snapshot before/after commit: %v/%v", commitErr, state, lsn, watermark, owner, count, beforeCommit, repair.SnapshotTS())
	require.True(t, moerr.IsMoErrCode(commitErr, moerr.ErrTxnWWConflict), "stale repair must fail with a write conflict: %v", commitErr)
	require.Equal(t, 2, state, "a failed repair must not publish its job update")
	require.Equal(t, 7, lsn, "lease-only conflict must leave the job LSN unchanged")
	require.Equal(t, "old", watermark, "lease-only conflict must leave the job watermark unchanged")
	require.Equal(t, 1, count)
	require.Equal(t, "new-cn", owner, "stale repair must not overwrite the successor")
	verifyISCPRepairDoesNotUndoOptimisticUnregister(t, ctx, first, second, conn)
}

// The repair transaction must not restore an active catalog row after a DDL
// transaction retires it. DDL passes its caller-owned transaction to
// UnregisterJob, so exercise an optimistic RC writer against the production
// repair ordering on two CNs.
func verifyISCPRepairDoesNotUndoOptimisticUnregister(t *testing.T, ctx context.Context, first, second embed.ServiceOperator, conn *sql.Conn) {
	t.Helper()
	var accountID uint32
	var tableID, jobID, taskID uint64
	var owner string
	var watermark, statusJSON string
	require.NoError(t, conn.QueryRowContext(ctx,
		"select task_id, task_runner from mo_task.sys_daemon_task where task_type='ISCP' and task_runner is not null limit 1",
	).Scan(&taskID, &owner))
	require.True(t, owner == first.ServiceID() || owner == second.ServiceID(), "unexpected ISCP owner %q", owner)

	ownerService := first
	if second.ServiceID() == owner {
		ownerService = second
	}
	ownerRuntime, ok := iscp.GetExecutorRuntime(owner)
	require.True(t, ok)
	t.Cleanup(func() { _ = ownerRuntime.Cancel() })
	require.NoError(t, ownerRuntime.Pause())
	require.NoError(t, conn.QueryRowContext(ctx,
		"select account_id, table_id, job_id, watermark, job_status from mo_catalog.mo_iscp_log where job_name='index_hv' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='managed_iscp_handoff' and relname='h')",
	).Scan(&accountID, &tableID, &jobID, &watermark, &statusJSON))
	repairService := ownerService.RawService().(cnservice.Service)
	repairEngine, repairClient := repairService.GetEngine(), repairService.GetTxnClient()
	ownerDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=45s&writeTimeout=45s", ownerService.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownerDB.Close()) })
	ownerConn, err := ownerDB.Conn(ctx)
	require.NoError(t, err)
	defer ownerConn.Close()
	sysCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	oldStatus := &iscp.JobStatus{}
	if statusJSON != "" {
		var unmarshalErr error
		unmarshalErr = json.Unmarshal([]byte(statusJSON), oldStatus)
		require.NoError(t, unmarshalErr)
	}
	newLSN := oldStatus.LSN + 1
	status := *oldStatus
	status.LSN = newLSN
	require.NoError(t, iscp.FlushJobStatusOnIterationState(
		sysCtx, owner, repairEngine, repairClient, accountID, tableID,
		[]string{"index_hv"}, []uint64{jobID}, []uint64{newLSN}, []*iscp.JobStatus{&status},
		types.StringToTS(watermark), iscp.ISCPJobState_Running, []uint64{oldStatus.LSN},
	))
	require.Eventually(t, func() bool {
		var current int8
		return ownerConn.QueryRowContext(ctx,
			"select job_state from mo_catalog.mo_iscp_log where account_id=? and table_id=? and job_name='index_hv' and job_id=?",
			accountID, tableID, jobID,
		).Scan(&current) == nil && current == iscp.ISCPJobState_Running
	}, 30*time.Second, 100*time.Millisecond, "repair CN must observe the running fixture before its snapshot starts")
	minSnapshot := repairEngine.LatestLogtailAppliedTime()
	if latestCommit := repairClient.GetLatestCommitTS(); latestCommit.Greater(minSnapshot) {
		minSnapshot = latestCommit
	}
	repair, err := repairClient.New(sysCtx, minSnapshot,
		client.WithTxnMode(pbtxn.TxnMode_Pessimistic),
		client.WithTxnIsolation(pbtxn.TxnIsolation_SI),
	)
	require.NoError(t, err)
	require.NoError(t, repairEngine.New(sysCtx, repair))
	repairFinished := false
	defer func() {
		if repairFinished {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(sysCtx), 15*time.Second)
		defer cancel()
		_ = repair.Rollback(cleanup)
	}()
	require.NoError(t, repair.GetWorkspace().AdvanceSnapshot(sysCtx, timestamp.Timestamp{}))
	repairJobSQL := fmt.Sprintf("update mo_catalog.mo_iscp_log set job_state=%d where account_id=%d and table_id=%d and job_name=%s and job_id=%d and job_state in (%d,%d)",
		iscp.ISCPJobState_Completed, accountID, tableID, sqlquote.String("index_hv"), jobID,
		iscp.ISCPJobState_Pending, iscp.ISCPJobState_Running)
	result, err := iscp.ExecWithResult(sysCtx, repairJobSQL, owner, repair)
	require.NoError(t, err)
	require.GreaterOrEqual(t, result.AffectedRows, uint64(1), "repair fixture must actually update an active job")
	result.Close()

	unregisterService := first
	if ownerService.ServiceID() == first.ServiceID() {
		unregisterService = second
	}
	unregisterCN := unregisterService.RawService().(cnservice.Service)
	unregisterEngine, unregisterClient := unregisterCN.GetEngine(), unregisterCN.GetTxnClient()
	unregister, err := unregisterClient.New(sysCtx, unregisterEngine.LatestLogtailAppliedTime(),
		client.WithTxnMode(pbtxn.TxnMode_Optimistic),
		client.WithTxnIsolation(pbtxn.TxnIsolation_RC),
	)
	require.NoError(t, err)
	require.NoError(t, unregisterEngine.New(sysCtx, unregister))
	_, err = iscp.UnregisterJob(sysCtx, unregisterService.ServiceID(), unregister, &iscp.JobID{
		DBName: "managed_iscp_handoff", TableName: "h", JobName: "index_hv",
	})
	require.NoError(t, err)
	require.NoError(t, unregister.Commit(sysCtx))

	var visibilityErr error
	require.Eventually(t, func() bool {
		var count int
		visibilityErr = ownerConn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_iscp_log where account_id=? and table_id=? and job_name='index_hv' and job_id=? and drop_at is not null",
			accountID, tableID, jobID,
		).Scan(&count)
		return visibilityErr != nil || count == 1
	}, 30*time.Second, 100*time.Millisecond, "optimistic DDL retirement must reach the repair CN")
	require.NoError(t, visibilityErr)

	// A concurrent optimistic DDL retirement after the fixed snapshot must
	// conflict with repair rather than be hidden by a late RC/SI refresh.
	guard := fmt.Sprintf("update mo_task.sys_daemon_task set task_runner=task_runner where task_id=%d and task_metadata_executor=%d and task_type='ISCP' and task_runner=%s and task_status in (%d,%d,%d,%d,%d)",
		taskID, task.TaskCode_ISCPExecutor, sqlquote.String(owner), task.TaskStatus_Running, task.TaskStatus_PauseRequested,
		task.TaskStatus_ResumeRequested, task.TaskStatus_CancelRequested, task.TaskStatus_RestartRequested)
	result, err = iscp.ExecWithResult(sysCtx, guard, owner, repair)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.AffectedRows)
	result.Close()
	commitErr := repair.Commit(sysCtx)
	repairFinished = true
	require.True(t, moerr.IsMoErrCode(commitErr, moerr.ErrTxnWWConflict), "repair must fail closed on concurrent retirement: %v", commitErr)

	var dropped bool
	var jobState int8
	require.NoError(t, ownerConn.QueryRowContext(ctx,
		"select drop_at is not null, job_state from mo_catalog.mo_iscp_log where account_id=? and table_id=? and job_name='index_hv' and job_id=?",
		accountID, tableID, jobID,
	).Scan(&dropped, &jobState))
	require.True(t, dropped, "repair must not restore the retired job")
	require.Equal(t, iscp.ISCPJobState_Running, jobState, "failed repair must not publish its state update")

	// Once a fresh repair sees the committed retirement, it may succeed but
	// must leave the historical job row untouched.
	require.NoError(t, ownerRuntime.Resume())
	require.NoError(t, ownerRuntime.Pause())
	require.NoError(t, ownerConn.QueryRowContext(ctx,
		"select drop_at is not null, job_state from mo_catalog.mo_iscp_log where account_id=? and table_id=? and job_name='index_hv' and job_id=?",
		accountID, tableID, jobID,
	).Scan(&dropped, &jobState))
	require.True(t, dropped)
	require.Equal(t, iscp.ISCPJobState_Running, jobState, "subsequent repair must skip retired history")
}
