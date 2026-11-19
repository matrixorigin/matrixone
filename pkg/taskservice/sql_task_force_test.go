// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package taskservice

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestForceRecoverSQLTaskCommitUnknownPreservesCause(t *testing.T) {
	store, mock := newMockStorage(t)
	lostAck := moerr.NewTxnUnknown(context.Background(), "force-txn")
	mock.ExpectQuery("select task_id, task_name from sql_task_run where run_id=? and account_id=? and task_name=?").
		WithArgs(uint64(9), uint32(1), "owned").WillReturnRows(sqlmock.NewRows([]string{"task_id", "task_name"}).AddRow(7, "owned"))
	mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("select account_id from mo_catalog.mo_account where account_id=? for update").
		WithArgs(uint32(1)).WillReturnRows(sqlmock.NewRows([]string{"account_id"}).AddRow(1))
	mock.ExpectQuery("select task_id, trigger_count, schedule_generation from sql_task where account_id=? and task_name=? for update").
		WithArgs(uint32(1), "owned").WillReturnRows(sqlmock.NewRows([]string{"task_id", "trigger_count", "schedule_generation"}).AddRow(7, 1, 1))
	mock.ExpectQuery("select task_id, task_status from sys_async_task where task_parent_id=? and sql_task_run_id=? limit 2 for update").
		WithArgs("sql-task:7", uint64(9)).WillReturnRows(sqlmock.NewRows([]string{"task_id", "task_status"}))
	mock.ExpectQuery(selectSQLTaskRun + " AND run_id=? for update").WithArgs(uint64(9)).
		WillReturnRows(sqlmock.NewRows(sqlTaskRunRows).AddRow(9, 7, "owned", 1, nil, nil, nil, 0, SQLTaskStatusRunning, SQLTaskTriggerScheduled, 1, 0, 0, nil, 1, "old-cn"))
	mock.ExpectExec("update sql_task set enabled=0, trigger_count=?, schedule_generation=?, updated_at=? where task_id=? and account_id=?").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update sql_task_run set status=?, finished_at=?, error_message=? where run_id=? and status=?").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("COMMIT").WillReturnError(lostAck)
	mock.ExpectClose()

	changed, err := store.forceRecoverSQLTaskRun(context.Background(), 1, "owned", 9, "admin")
	require.False(t, changed)
	require.ErrorIs(t, err, lostAck)
	require.ErrorContains(t, err, "recovery may have committed")
	var identity *moerr.Error
	require.ErrorAs(t, err, &identity)
	require.Equal(t, moerr.ErrTxnUnknown, identity.ErrorCode())
	require.NoError(t, store.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestForceRecoverSQLTaskRun(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := factory(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			definition := newTestSQLTask("owned", 1)
			definition.TriggerCount = 1
			mustAddTestSQLTask(t, store, 1, definition)
			definition = mustGetTestSQLTask(t, store, 1)[0]
			firing := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1,
				ParentTaskID: fmt.Sprintf("sql-task:%d", definition.TaskID),
				Metadata:     BuildSQLTaskMetadata(&task.SQLTaskContext{TaskId: definition.TaskID, TriggerCount: 1})}
			mustAddTestAsyncTask(t, store, 1, firing)
			firing = mustGetTestAsyncTask(t, store, 1)[0]
			claim := &SQLTaskClaim{ID: firing.ID, Runner: firing.TaskRunner, Epoch: firing.Epoch, ParentTaskID: firing.ParentTaskID, MetadataID: firing.Metadata.ID}
			run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
			run.TriggerType = SQLTaskTriggerScheduled
			run.ErrorMessage = "original reason"
			admission := testSQLTaskAdmission(claim, 1)
			var err error
			run.RunID, err = store.AcquireSQLTaskRun(ctx, definition, run, admission)
			require.NoError(t, err)
			_, err = ForceRecoverSQLTaskRun(ctx, store, 2, definition.TaskName, run.RunID, "admin")
			require.ErrorIs(t, err, ErrSQLTaskNotFound)

			changed, err := ForceRecoverSQLTaskRun(ctx, store, 1, definition.TaskName, run.RunID, "admin")
			require.NoError(t, err)
			require.True(t, changed)
			current := mustGetTestSQLTask(t, store, 1)[0]
			require.False(t, current.Enabled)
			require.Equal(t, uint64(2), current.ScheduleGeneration)
			require.Equal(t, current.ScheduleGeneration, current.TriggerCount)
			got := mustGetTestSQLTaskRun(t, store, 1)[0]
			require.Equal(t, SQLTaskStatusUnknown, got.Status)
			require.Contains(t, got.ErrorMessage, "original reason")
			require.Contains(t, got.ErrorMessage, `FORCE by "admin"`)
			require.Contains(t, got.ErrorMessage, "SQL may still be running")
			finished := mustGetTestAsyncTask(t, store, 1)[0]
			require.Equal(t, task.TaskStatus_Completed, finished.Status)
			require.Equal(t, task.ResultCode_Failed, finished.ExecuteResult.Code)

			// A late owner receipt cannot overwrite UNKNOWN, and replaying FORCE
			// cannot suspend the task a second time or mutate a successor run.
			run.Status, run.FinishedAt = SQLTaskStatusSuccess, time.Now()
			resolved, err := store.PublishSQLTaskReceipt(ctx, SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: run, Claim: *claim})
			require.NoError(t, err)
			require.True(t, resolved)
			resumed := current
			resumed.Enabled = true
			n, err := store.AlterSQLTask(ctx, current, resumed, SQLTaskResume)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			successor := newTestSQLTaskRun(current.TaskID, current.TaskName, SQLTaskStatusRunning)
			successor.RunID, err = store.AcquireSQLTaskRun(ctx, current, successor, testSQLTaskAdmission(nil, 0))
			require.NoError(t, err)
			changed, err = ForceRecoverSQLTaskRun(ctx, store, 1, definition.TaskName, run.RunID, "admin")
			require.NoError(t, err)
			require.False(t, changed)
			require.True(t, mustGetTestSQLTask(t, store, 1)[0].Enabled)
			require.Equal(t, SQLTaskStatusUnknown, mustGetTestSQLTaskRun(t, store, 1, WithSQLTaskRunIDCond(EQ, run.RunID))[0].Status)
			require.Equal(t, SQLTaskStatusRunning, mustGetTestSQLTaskRun(t, store, 1, WithSQLTaskRunIDCond(EQ, successor.RunID))[0].Status)
		})
	}
}

func TestForceRecoverRecreatedOrDroppedTask(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("recreated=%t", recreate), func(t *testing.T) {
			ctx := context.Background()
			store := NewMemTaskStorage()
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			mustAddTestSQLTask(t, store, 1, newTestSQLTask("owned", 1))
			old := mustGetTestSQLTask(t, store, 1)[0]
			run := newTestSQLTaskRun(old.TaskID, old.TaskName, SQLTaskStatusRunning)
			id, err := store.AcquireSQLTaskRun(ctx, old, run, testSQLTaskAdmission(nil, 0))
			require.NoError(t, err)
			_, err = store.DeleteSQLTask(ctx, WithTaskIDCond(EQ, old.TaskID))
			require.NoError(t, err)
			if recreate {
				mustAddTestSQLTask(t, store, 1, newTestSQLTask("owned", 1))
			}
			changed, err := ForceRecoverSQLTaskRun(ctx, store, 1, "owned", id, "admin")
			require.NoError(t, err)
			require.True(t, changed)
			require.Equal(t, SQLTaskStatusUnknown, mustGetTestSQLTaskRun(t, store, 1)[0].Status)
			if recreate {
				current := mustGetTestSQLTask(t, store, 1)[0]
				require.NotEqual(t, old.TaskID, current.TaskID)
				require.False(t, current.Enabled)
			}
		})
	}
}
