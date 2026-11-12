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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pbtask "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retryTransitionFaultStore struct {
	taskservice.TaskStorage
	lostAck  bool
	attempts atomic.Int32
}

func (*retryTransitionFaultStore) Close() error { return nil }

func (s *retryTransitionFaultStore) TransitionSQLTaskRun(ctx context.Context, request taskservice.SQLTaskRetryRequest) (taskservice.SQLTaskRetryTransition, error) {
	if s.attempts.Add(1) == 1 {
		if !s.lostAck {
			return taskservice.SQLTaskRetryTransition{}, errors.New("injected pre-commit failure")
		}
		result, err := s.TaskStorage.TransitionSQLTaskRun(ctx, request)
		if err == nil {
			return result, errors.New("injected lost acknowledgement after commit")
		}
		return result, err
	}
	return s.TaskStorage.TransitionSQLTaskRun(ctx, request)
}

type retryAdmissionGuard struct {
	service    taskservice.TaskService
	definition taskservice.SQLTask
	calls      int
	release    func()
}

type guardedRetryExecutor struct {
	ie.InternalExecutor
	guard *retryAdmissionGuard
}

func (e *guardedRetryExecutor) ExecWithStatus(ctx context.Context, statement string, opts ie.SessionOverrideOptions) (ie.InternalExecStatus, error) {
	e.guard.calls++
	status, err := e.InternalExecutor.(ie.InternalExecutorWithStatus).ExecWithStatus(ctx, statement, opts)
	if err != nil {
		return status, err
	}
	if e.guard.calls == 1 {
		_, e.guard.release, err = taskservice.BeginSQLTaskManualAdmission(ctx, e.guard.service,
			e.guard.definition.AccountID, e.guard.definition.TaskName)
	} else if e.guard.calls == 3 {
		e.guard.release()
	}
	return status, err
}

func TestSQLTaskRunBarrierAcrossDropAndRecreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithTestAdmissionContext(ctx))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	peer, err := cluster.GetCNService(1)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	root := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	defer root.Close()
	require.NoError(t, waitSystemBootstrap(ctx, root))
	require.NoError(t, waitSQLTaskReady(ctx, root))
	execSQLRequire(t, ctx, root, "create database task_ownership")
	db := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/task_ownership", port))
	defer db.Close()
	peerDB := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/task_ownership", peer.GetServiceConfig().CN.Frontend.Port))
	defer peerDB.Close()
	db.SetMaxOpenConns(4)
	execSQLRequire(t, ctx, db, "create table sink(v int)")
	execSQLRequire(t, ctx, db, "create table rollback_ids(id bigint primary key auto_increment)")
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	firstID, err := tx.ExecContext(ctx, "insert into rollback_ids values (null)")
	require.NoError(t, err)
	rolledBackID, err := firstID.LastInsertId()
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	secondID, err := db.ExecContext(ctx, "insert into rollback_ids values (null)")
	require.NoError(t, err)
	committedID, err := secondID.LastInsertId()
	require.NoError(t, err)
	require.NotEqual(t, rolledBackID, committedID, "an ambiguous admission must not alias its successor's run ID")

	// Hold a user lock in a separate session to make the old task's SQL stay
	// in flight until the test explicitly releases it.
	gate, err := db.Conn(ctx)
	require.NoError(t, err)
	defer gate.Close()
	var locked int
	require.NoError(t, gate.QueryRowContext(ctx, "select get_lock('task_ownership_gate', 0)").Scan(&locked))
	require.Equal(t, 1, locked)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_, _ = gate.ExecContext(cleanupCtx, "select release_lock('task_ownership_gate')")
	}()

	execSQLRequire(t, ctx, db, "create task audit_task as begin insert into sink select get_lock('task_ownership_gate', 30); end")
	oldDone := make(chan error, 1)
	go func() { _, err := db.ExecContext(ctx, "execute task audit_task"); oldDone <- err }()
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='audit_task' and status='RUNNING'").Scan(&n)
		return err == nil && n == 1
	}, 20*time.Second, 50*time.Millisecond)
	// Operators need the actual owner to decide whether FORCE is safe.
	var runner string
	var oldRunID uint64
	require.NoError(t, db.QueryRowContext(ctx, "select run_id, runner_cn from mo_task.sql_task_run where task_name='audit_task' and status='RUNNING'").Scan(&oldRunID, &runner))
	require.NotEmpty(t, runner)
	type runIdentity struct {
		taskID, account  uint64
		name, trigger    string
		scheduled, start sql.NullString
		attempt          int
		runner           string
	}
	readIdentity := func(id uint64) runIdentity {
		var value runIdentity
		err := db.QueryRowContext(ctx, "select task_id,account_id,task_name,scheduled_at,started_at,trigger_type,attempt_number,runner_cn from mo_task.sql_task_run where run_id=?", id).
			Scan(&value.taskID, &value.account, &value.name, &value.scheduled, &value.start, &value.trigger, &value.attempt, &value.runner)
		require.NoError(t, err)
		return value
	}
	assertSuccess := func(id uint64, wantAffected int64) {
		var completedStatus string
		var finished, errorMessage sql.NullString
		var duration float64
		var affected int64
		var errorCode, gateResult int
		require.NoError(t, db.QueryRowContext(ctx, "select status,finished_at,duration_seconds,rows_affected,error_code,error_message,gate_result from mo_task.sql_task_run where run_id=?", id).
			Scan(&completedStatus, &finished, &duration, &affected, &errorCode, &errorMessage, &gateResult))
		require.Equal(t, "SUCCESS", completedStatus)
		require.True(t, finished.Valid)
		require.Positive(t, duration)
		require.Equal(t, wantAffected, affected)
		require.Zero(t, errorCode)
		require.False(t, errorMessage.Valid)
		require.Equal(t, 1, gateResult)
	}
	admittedIdentity := readIdentity(oldRunID)
	shown, err := db.QueryContext(ctx, "show task runs for audit_task limit 1")
	require.NoError(t, err)
	defer shown.Close()
	columns, err := shown.Columns()
	require.NoError(t, err)
	require.Equal(t, []string{"run_id", "task_name", "trigger_type", "status", "started_at", "finished_at", "duration", "attempt", "rows_affected", "error_message", "runner_cn"}, columns)
	require.True(t, shown.Next())
	values := make([]sql.RawBytes, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	require.NoError(t, shown.Scan(dest...))
	require.Equal(t, "audit_task", string(values[1]))
	require.Equal(t, "RUNNING", string(values[3]))
	require.Equal(t, runner, string(values[10]))
	require.False(t, shown.Next())
	require.NoError(t, shown.Err())
	require.NoError(t, shown.Close())
	execSQLRequire(t, ctx, peerDB, "drop task audit_task")
	execSQLRequire(t, ctx, peerDB, "create task audit_task as begin insert into sink values (7); end")
	_, err = peerDB.ExecContext(ctx, "execute task audit_task")
	require.Error(t, err)
	require.Contains(t, err.Error(), "already running")
	select {
	case err := <-oldDone:
		t.Fatalf("old execution returned before overlap was checked: %v", err)
	default:
	}
	var rows int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from sink").Scan(&rows))
	require.Zero(t, rows)
	_, err = gate.ExecContext(ctx, "select release_lock('task_ownership_gate')")
	require.NoError(t, err)
	require.NoError(t, <-oldDone)
	require.Equal(t, admittedIdentity, readIdentity(oldRunID), "completion must preserve the admitted execution identity")
	assertSuccess(oldRunID, 1)
	execSQLRequire(t, ctx, peerDB, "execute task audit_task")
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from sink").Scan(&rows))
	require.Equal(t, 2, rows)

	// Start the same task on both CNs together. One admission must fail while
	// the other is held in its body, rather than letting both reach the body.
	require.NoError(t, gate.QueryRowContext(ctx, "select get_lock('task_race_gate', 0)").Scan(&locked))
	require.Equal(t, 1, locked)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_, _ = gate.ExecContext(cleanupCtx, "select release_lock('task_race_gate')")
	}()
	execSQLRequire(t, ctx, db, "create task concurrent_task as begin insert into sink select get_lock('task_race_gate', 30); end")
	startRace := make(chan struct{})
	raceResults := make(chan error, 2)
	for _, client := range []*sql.DB{db, peerDB} {
		go func() {
			<-startRace
			_, err := client.ExecContext(ctx, "execute task concurrent_task")
			raceResults <- err
		}()
	}
	close(startRace)
	select {
	case err := <-raceResults:
		require.ErrorContains(t, err, "already running")
	case <-time.After(15 * time.Second):
		t.Fatal("one concurrent admission must be rejected while the owner is held")
	}
	_, err = gate.ExecContext(ctx, "select release_lock('task_race_gate')")
	require.NoError(t, err)
	select {
	case err := <-raceResults:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("the admitted task did not complete after its gate was released")
	}
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from sink").Scan(&rows))
	require.Equal(t, 3, rows)

	// FORCE is an explicit operator action, not proof that the old SQL has
	// stopped. Keep it blocked to exercise a late completion after recovery.
	require.NoError(t, gate.QueryRowContext(ctx, "select get_lock('task_force_gate', 0)").Scan(&locked))
	require.Equal(t, 1, locked)
	execSQLRequire(t, ctx, db, "create task force_task as begin insert into sink select get_lock('task_force_gate', 30); end")
	forceDone := make(chan error, 1)
	go func() { _, err := db.ExecContext(ctx, "execute task force_task"); forceDone <- err }()
	var forceRunID uint64
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(ctx, "select run_id from mo_task.sql_task_run where task_name='force_task' and status='RUNNING'").Scan(&forceRunID)
		return err == nil && forceRunID != 0
	}, 20*time.Second, 50*time.Millisecond)
	execSQLRequire(t, ctx, db, fmt.Sprintf("alter task force_task reset runs %d force", forceRunID))
	var forceStatus string
	var enabled int
	require.NoError(t, db.QueryRowContext(ctx, "select status from mo_task.sql_task_run where run_id=?", forceRunID).Scan(&forceStatus))
	require.Equal(t, "UNKNOWN", forceStatus)
	require.NoError(t, db.QueryRowContext(ctx, "select enabled from mo_task.sql_task where task_name='force_task'").Scan(&enabled))
	require.Zero(t, enabled)
	_, err = gate.ExecContext(ctx, "select release_lock('task_force_gate')")
	require.NoError(t, err)
	require.NoError(t, <-forceDone)
	require.NoError(t, db.QueryRowContext(ctx, "select status from mo_task.sql_task_run where run_id=?", forceRunID).Scan(&forceStatus))
	require.Equal(t, "UNKNOWN", forceStatus)
	execSQLRequire(t, ctx, db, "alter task force_task resume")
	execSQLRequire(t, ctx, db, fmt.Sprintf("alter task force_task reset runs %d force", forceRunID))
	require.NoError(t, db.QueryRowContext(ctx, "select enabled from mo_task.sql_task where task_name='force_task'").Scan(&enabled))
	require.Equal(t, 1, enabled)

	// The same recovery also completes only the exact scheduled async claim.
	require.NoError(t, gate.QueryRowContext(ctx, "select get_lock('scheduled_force_gate', 0)").Scan(&locked))
	require.Equal(t, 1, locked)
	execSQLRequire(t, ctx, db, `create task scheduled_force schedule '*/1 * * * * *' timezone 'UTC' as begin
		insert into sink select get_lock('scheduled_force_gate', 30);
	end`)
	var scheduledRunID, scheduledAsyncID uint64
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(ctx, "select run_id from mo_task.sql_task_run where task_name='scheduled_force' and status='RUNNING' order by run_id limit 1").Scan(&scheduledRunID)
		if err != nil {
			return false
		}
		err = db.QueryRowContext(ctx, "select task_id from mo_task.sys_async_task where sql_task_run_id=?", scheduledRunID).Scan(&scheduledAsyncID)
		return err == nil && scheduledAsyncID != 0
	}, 30*time.Second, 50*time.Millisecond)
	execSQLRequire(t, ctx, db, fmt.Sprintf("alter task scheduled_force reset runs %d force", scheduledRunID))
	var asyncStatus int
	require.NoError(t, db.QueryRowContext(ctx, "select task_status from mo_task.sys_async_task where task_id=?", scheduledAsyncID).Scan(&asyncStatus))
	require.Equal(t, 2, asyncStatus)
	_, err = gate.ExecContext(ctx, "select release_lock('scheduled_force_gate')")
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(ctx, "select status from mo_task.sql_task_run where run_id=?", scheduledRunID).Scan(&forceStatus))
	require.Equal(t, "UNKNOWN", forceStatus)

	// A concurrent cron trigger can change the observed generation while
	// SUSPEND is in flight. Retry only the explicit, safe CAS-conflict response.
	suspendTask := func(name string) {
		t.Helper()
		suspendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for {
			_, err := db.ExecContext(suspendCtx, "alter task "+name+" suspend")
			if err == nil {
				return
			}
			var sqlErr *mysql.MySQLError
			if !errors.As(err, &sqlErr) || sqlErr.Number != moerr.ErrInvalidState ||
				sqlErr.Message != "invalid state sql task changed or was removed during alter; retry the operation" {
				t.Fatalf("suspend task %s: %v", name, err)
			}
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-suspendCtx.Done():
				timer.Stop()
				t.Fatalf("suspend task %s: %v; last conflict: %v", name, suspendCtx.Err(), err)
			case <-timer.C:
			}
		}
	}

	require.NoError(t, gate.QueryRowContext(ctx, "select get_lock('scheduled_success_gate', 0)").Scan(&locked))
	require.Equal(t, 1, locked)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_, _ = gate.ExecContext(cleanupCtx, "select release_lock('scheduled_success_gate')")
	}()
	execSQLRequire(t, ctx, db, "create task scheduled_success schedule '*/1 * * * * *' timezone 'UTC' as begin select get_lock('scheduled_success_gate', 120); end")
	var successRunID uint64
	require.Eventually(t, func() bool {
		return db.QueryRowContext(ctx, "select run_id from mo_task.sql_task_run where task_name='scheduled_success' and status='RUNNING' order by run_id limit 1").Scan(&successRunID) == nil
	}, 30*time.Second, 50*time.Millisecond)
	successIdentity := readIdentity(successRunID)
	require.Equal(t, "scheduled_success", successIdentity.name)
	require.Equal(t, "SCHEDULED", successIdentity.trigger)
	require.True(t, successIdentity.scheduled.Valid)
	require.True(t, successIdentity.start.Valid)
	require.NotEmpty(t, successIdentity.runner)
	suspendTask("scheduled_success")
	require.NoError(t, gate.QueryRowContext(ctx, "select release_lock('scheduled_success_gate')").Scan(&locked))
	require.Equal(t, 1, locked)
	require.Eventually(t, func() bool {
		var status string
		return db.QueryRowContext(ctx, "select status from mo_task.sql_task_run where run_id=?", successRunID).Scan(&status) == nil && status == "SUCCESS"
	}, 30*time.Second, 50*time.Millisecond)
	require.Equal(t, successIdentity, readIdentity(successRunID))
	assertSuccess(successRunID, 1)

	// Use the registered runner: a nil executor result can also mean deferred.
	// The other legacy firings cannot be allocated individually, so only batch
	// deletion can remove them, including the second 128-row batch.
	execSQLRequire(t, ctx, db, "create table stale_sink(v int)")
	execSQLRequire(t, ctx, db, "create task stale_cleanup as begin insert into stale_sink values (1); end")
	execSQLRequire(t, ctx, db, "alter task stale_cleanup suspend")
	const cleanupLabel = "sql-task-cleanup-test"
	func() {
		hakeeper, err := logservice.NewCNHAKeeperClient(ctx, cn.GetServiceConfig().CN.UUID, cn.GetServiceConfig().HAKeeperClient)
		require.NoError(t, err)
		defer func() { require.NoError(t, hakeeper.Close()) }()
		state, err := hakeeper.GetClusterState(ctx)
		require.NoError(t, err)
		require.Len(t, state.CNState.Stores, 2)
		require.Contains(t, state.CNState.Stores, successIdentity.runner)
		for _, store := range state.CNState.Stores {
			require.NotContains(t, store.Labels, cleanupLabel)
		}
	}()
	taskService, ok := cn.RawService().(testutil.TaskServiceGetter).GetTaskService()
	require.True(t, ok)
	storage := taskService.GetStorage()
	definitions, err := storage.QuerySQLTask(ctx, taskservice.WithAccountID(taskservice.EQ, 0), taskservice.WithTaskName(taskservice.EQ, "stale_cleanup"))
	require.NoError(t, err)
	require.Len(t, definitions, 1)
	definition := definitions[0]
	spec := &pbtask.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName, AccountId: definition.AccountID,
		TriggerType: taskservice.SQLTaskTriggerScheduled, TriggerCount: 1,
		DatabaseName: definition.DatabaseName, SQLBody: definition.SQLBody}
	parent := fmt.Sprintf("sql-task:%d", definition.TaskID)
	firings := make([]pbtask.AsyncTask, 130)
	for i := range firings {
		metadata := taskservice.BuildSQLTaskMetadata(spec)
		metadata.ID = fmt.Sprintf("%s:%d", parent, i+1)
		metadata.Options.Labels = map[string]string{cleanupLabel: "unavailable"}
		firings[i] = pbtask.AsyncTask{Metadata: metadata, ParentTaskID: parent, Status: pbtask.TaskStatus_Created}
	}
	firings[0].Status, firings[0].TaskRunner, firings[0].Epoch = pbtask.TaskStatus_Running, successIdentity.runner, 1
	firings[0].Metadata.Options.Labels = nil
	added, err := storage.AddAsyncTask(ctx, firings...)
	require.NoError(t, err)
	require.Equal(t, len(firings), added)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		children, err := storage.QueryAsyncTask(ctx, taskservice.WithTaskParentTaskIDCond(taskservice.EQ, parent))
		require.NoError(c, err)
		require.LessOrEqual(c, len(children), 1, "the batch must delete the other legacy firings")
		for _, child := range children {
			require.Equal(c, pbtask.TaskStatus_Completed, child.Status, "stale unclaimed firing remains after batch cleanup")
		}
	}, 20*time.Second, 50*time.Millisecond)
	var staleEffects, staleRuns int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from stale_sink").Scan(&staleEffects))
	require.Zero(t, staleEffects, "a stale firing must not execute its SQL body")
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_id=?", definition.TaskID).Scan(&staleRuns))
	require.Zero(t, staleRuns, "a stale firing must not acquire a run")

	// Each statement commits independently: the first attempt increments the
	// counter, then hits a duplicate key. The configured retry must run, retain
	// exclusive ownership, and record both attempts.
	execSQLRequire(t, ctx, db, "create table retry_counter(n int)")
	execSQLRequire(t, ctx, db, "insert into retry_counter values (0)")
	execSQLRequire(t, ctx, db, "create table retry_sink(v int primary key)")
	execSQLRequire(t, ctx, db, "insert into retry_sink values (0)")
	execSQLRequire(t, ctx, db, `create task retry_once retry 1 as begin
		update retry_counter set n=n+1;
		insert into retry_sink select case when n=1 then 0 else n end from retry_counter;
	end`)
	execSQLRequire(t, ctx, db, "execute task retry_once")
	var counter, failed, succeeded int
	require.NoError(t, db.QueryRowContext(ctx, "select n from retry_counter").Scan(&counter))
	require.Equal(t, 2, counter)
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from retry_sink where v=2").Scan(&rows))
	require.Equal(t, 1, rows)
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='retry_once' and status='FAILED'").Scan(&failed))
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='retry_once' and status='SUCCESS'").Scan(&succeeded))
	require.Equal(t, 1, failed)
	require.Equal(t, 1, succeeded)

	for _, lostAck := range []bool{false, true} {
		label := "precommit"
		if lostAck {
			label = "lostack"
		}
		t.Run("retry_"+label, func(t *testing.T) {
			counterTable, sinkTable, taskName := "counter_"+label, "sink_"+label, "retry_"+label
			execSQLRequire(t, ctx, db, "create table "+counterTable+"(n int)")
			execSQLRequire(t, ctx, db, "insert into "+counterTable+" values (0)")
			execSQLRequire(t, ctx, db, "create table "+sinkTable+"(v int primary key)")
			execSQLRequire(t, ctx, db, "insert into "+sinkTable+" values (0)")
			execSQLRequire(t, ctx, db, fmt.Sprintf(`create task %s retry 1 as begin
				update %s set n=n+1;
				insert into %s select case when n=1 then 0 else n end from %s;
			end`, taskName, counterTable, sinkTable, counterTable))
			definitions, err := storage.QuerySQLTask(ctx, taskservice.WithAccountID(taskservice.EQ, 0), taskservice.WithTaskName(taskservice.EQ, taskName))
			require.NoError(t, err)
			require.Len(t, definitions, 1)
			definition := definitions[0]
			fault := &retryTransitionFaultStore{TaskStorage: storage, lostAck: lostAck}
			owner := taskservice.NewTaskService(runtime.DefaultRuntime(), fault)
			t.Cleanup(func() { require.NoError(t, owner.Close()) })
			guard := &retryAdmissionGuard{service: owner, definition: definition, release: func() {}}
			t.Cleanup(func() { guard.release() })
			cnID := cn.GetServiceConfig().CN.UUID
			executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor {
				return &guardedRetryExecutor{InternalExecutor: frontend.NewInternalExecutor(cnID), guard: guard}
			}, owner, cnID)
			spec := &pbtask.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName, AccountId: definition.AccountID,
				DatabaseName: definition.DatabaseName, SQLBody: definition.SQLBody, RetryLimit: uint32(definition.RetryLimit),
				Creator: definition.Creator, CreatorUserId: definition.CreatorUserID, CreatorRoleId: definition.CreatorRoleID,
				TriggerType: taskservice.SQLTaskTriggerManual, ScheduledAt: time.Now().UnixMilli()}
			require.NoError(t, executor.ExecuteContext(ctx, spec, true))
			require.Equal(t, 4, guard.calls)
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select n from "+counterTable).Scan(&count))
			require.Equal(t, 2, count)
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+sinkTable+" where v=2").Scan(&count))
			require.Equal(t, 1, count)
			runs, err := storage.QuerySQLTaskRun(ctx, taskservice.WithTaskIDCond(taskservice.EQ, definition.TaskID))
			require.NoError(t, err)
			require.Len(t, runs, 2)
			require.Equal(t, taskservice.SQLTaskStatusSuccess, runs[0].Status)
			require.Equal(t, taskservice.SQLTaskStatusFailed, runs[1].Status)
		})
	}

	execSQLRequire(t, ctx, db, "create table scheduled_counter(n int)")
	execSQLRequire(t, ctx, db, "insert into scheduled_counter values (0)")
	execSQLRequire(t, ctx, db, "create table scheduled_sink(v int primary key)")
	execSQLRequire(t, ctx, db, "insert into scheduled_sink values (0)")
	require.NoError(t, gate.QueryRowContext(ctx, "select get_lock('scheduled_retry_gate', 0)").Scan(&locked))
	require.Equal(t, 1, locked)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_, _ = gate.ExecContext(cleanupCtx, "select release_lock('scheduled_retry_gate')")
	}()
	execSQLRequire(t, ctx, db, `create task scheduled_retry schedule '*/1 * * * * *' timezone 'UTC' retry 1 as begin
		select get_lock('scheduled_retry_gate', 120);
		update scheduled_counter set n=n+1;
		insert into scheduled_sink select case when n=1 then 0 else n end from scheduled_counter;
	end`)
	var firstRunID uint64
	require.Eventually(t, func() bool {
		return db.QueryRowContext(ctx, "select run_id from mo_task.sql_task_run where task_name='scheduled_retry' and status='RUNNING' and attempt_number=1").Scan(&firstRunID) == nil
	}, 30*time.Second, 50*time.Millisecond, "the scheduled firing must start")
	suspendTask("scheduled_retry")
	var status string
	var attempt int
	require.NoError(t, db.QueryRowContext(ctx, "select status, attempt_number from mo_task.sql_task_run where run_id=?", firstRunID).Scan(&status, &attempt))
	require.Equal(t, "RUNNING", status)
	require.Equal(t, 1, attempt)
	require.NoError(t, db.QueryRowContext(ctx, "select n from scheduled_counter").Scan(&counter))
	require.Zero(t, counter, "the first firing must remain behind the gate")
	require.NoError(t, gate.QueryRowContext(ctx, "select release_lock('scheduled_retry_gate')").Scan(&locked))
	require.Equal(t, 1, locked)
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='scheduled_retry' and status='SUCCESS' and attempt_number=2").Scan(&n)
		return err == nil && n == 1
	}, 30*time.Second, 50*time.Millisecond, "the scheduled retry must complete")
	result, err := db.QueryContext(ctx, "select run_id, status, attempt_number from mo_task.sql_task_run where task_name='scheduled_retry' order by attempt_number")
	require.NoError(t, err)
	defer result.Close()
	var runID uint64
	require.True(t, result.Next())
	require.NoError(t, result.Scan(&runID, &status, &attempt))
	require.Equal(t, firstRunID, runID)
	require.Equal(t, "FAILED", status)
	require.Equal(t, 1, attempt)
	require.True(t, result.Next())
	require.NoError(t, result.Scan(&runID, &status, &attempt))
	require.NotEqual(t, firstRunID, runID)
	require.Equal(t, "SUCCESS", status)
	require.Equal(t, 2, attempt)
	require.False(t, result.Next(), "only the captured firing and its retry should execute")
	require.NoError(t, result.Err())
	require.NoError(t, db.QueryRowContext(ctx, "select n from scheduled_counter").Scan(&counter))
	require.Equal(t, 2, counter)
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from scheduled_sink where v=2").Scan(&rows))
	require.Equal(t, 1, rows)
}
