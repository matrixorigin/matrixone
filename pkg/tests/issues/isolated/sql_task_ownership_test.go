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

	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtask "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

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

	require.NoError(t, gate.QueryRowContext(ctx, "select get_lock('scheduled_success_gate', 0)").Scan(&locked))
	require.Equal(t, 1, locked)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_, _ = gate.ExecContext(cleanupCtx, "select release_lock('scheduled_success_gate')")
	}()
	execSQLRequire(t, ctx, db, "create task scheduled_success schedule '*/1 * * * * *' timezone 'UTC' as begin select get_lock('scheduled_success_gate', 30); end")
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
	_, err = gate.ExecContext(ctx, "select release_lock('scheduled_success_gate')")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var status string
		return db.QueryRowContext(ctx, "select status from mo_task.sql_task_run where run_id=?", successRunID).Scan(&status) == nil && status == "SUCCESS"
	}, 30*time.Second, 50*time.Millisecond)
	require.Equal(t, successIdentity, readIdentity(successRunID))
	assertSuccess(successRunID, 1)

	// Exercise the actual task-store stale batch through the executor. Legacy
	// firings are unclaimed and must not enter their SQL body after upgrade.
	execSQLRequire(t, ctx, db, "create task stale_cleanup as begin select 1; end")
	execSQLRequire(t, ctx, db, "alter task stale_cleanup suspend")
	taskService, ok := cn.RawService().(testutil.TaskServiceGetter).GetTaskService()
	require.True(t, ok)
	storage := taskService.GetStorage()
	definitions, err := storage.QuerySQLTask(ctx, taskservice.WithAccountID(taskservice.EQ, 0), taskservice.WithTaskName(taskservice.EQ, "stale_cleanup"))
	require.NoError(t, err)
	require.Len(t, definitions, 1)
	definition := definitions[0]
	spec := &pbtask.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName, AccountId: definition.AccountID,
		TriggerType: taskservice.SQLTaskTriggerScheduled, TriggerCount: 1, SQLBody: "select 1"}
	parent := fmt.Sprintf("sql-task:%d", definition.TaskID)
	firings := make([]pbtask.AsyncTask, 130)
	for i := range firings {
		metadata := taskservice.BuildSQLTaskMetadata(spec)
		metadata.ID = fmt.Sprintf("%s:%d", parent, i+1)
		firings[i] = pbtask.AsyncTask{Metadata: metadata, ParentTaskID: parent, Status: pbtask.TaskStatus_Running, TaskRunner: "stale-probe", Epoch: 1}
	}
	added, err := storage.AddAsyncTask(ctx, firings...)
	require.NoError(t, err)
	require.Equal(t, len(firings), added)
	children, err := storage.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, firings[0].Metadata.ID))
	require.NoError(t, err)
	require.Len(t, children, 1)
	executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor {
		t.Fatal("a stale firing must not execute SQL")
		return nil
	}, taskService, "stale-probe")
	require.NoError(t, executor.TaskExecutor()(ctx, &children[0]))
	children, err = storage.QueryAsyncTask(ctx, taskservice.WithTaskParentTaskIDCond(taskservice.EQ, parent))
	require.NoError(t, err)
	require.LessOrEqual(t, len(children), 1, "the batch must delete the other legacy firings")
	for _, child := range children {
		require.Equal(t, pbtask.TaskStatus_Completed, child.Status, "stale unclaimed firing remains after batch cleanup")
	}

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

	execSQLRequire(t, ctx, db, "create table scheduled_counter(n int)")
	execSQLRequire(t, ctx, db, "insert into scheduled_counter values (0)")
	execSQLRequire(t, ctx, db, "create table scheduled_sink(v int primary key)")
	execSQLRequire(t, ctx, db, "insert into scheduled_sink values (0)")
	execSQLRequire(t, ctx, db, `create task scheduled_retry schedule '*/1 * * * * *' timezone 'UTC' retry 1 as begin
		update scheduled_counter set n=n+1;
		insert into scheduled_sink select case when n=1 then 0 else n end from scheduled_counter;
	end`)
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='scheduled_retry' and status='SUCCESS'").Scan(&n)
		return err == nil && n > 0
	}, 30*time.Second, 50*time.Millisecond, "the scheduled retry must complete")
	execSQLRequire(t, ctx, db, "alter task scheduled_retry suspend")
	result, err := db.QueryContext(ctx, "select status, attempt_number from mo_task.sql_task_run where task_name='scheduled_retry' order by run_id limit 2")
	require.NoError(t, err)
	defer result.Close()
	var status string
	var attempt int
	require.True(t, result.Next())
	require.NoError(t, result.Scan(&status, &attempt))
	require.Equal(t, "FAILED", status)
	require.Equal(t, 1, attempt)
	require.True(t, result.Next())
	require.NoError(t, result.Scan(&status, &attempt))
	require.Equal(t, "SUCCESS", status)
	require.Equal(t, 2, attempt)
	require.NoError(t, result.Err())
}
