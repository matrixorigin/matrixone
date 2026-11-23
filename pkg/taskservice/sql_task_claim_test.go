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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskAdmissionRequiresOwnershipAndCredit(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			store := factory(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			definition := newTestSQLTask("owned", 1)
			mustAddTestSQLTask(t, store, 1, definition)
			definition = mustGetTestSQLTask(t, store, 1)[0]
			run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
			_, err := store.AcquireSQLTaskRun(context.Background(), definition, run, nil)
			require.Error(t, err)
			_, err = store.AcquireSQLTaskRun(context.Background(), definition, run, &SQLTaskRunAdmission{})
			require.Error(t, err)
			run.TriggerType = SQLTaskTriggerScheduled
			_, err = store.AcquireSQLTaskRun(context.Background(), definition, run, testSQLTaskAdmission(nil, 0))
			require.ErrorIs(t, err, ErrSQLTaskStaleFiring)
			run.TriggerType = SQLTaskTriggerManual
			for _, mutate := range []func(*SQLTaskRun){
				func(value *SQLTaskRun) { value.TaskID++ },
				func(value *SQLTaskRun) { value.AccountID++ },
				func(value *SQLTaskRun) { value.TaskName += "-different" },
			} {
				mismatch := run
				mutate(&mismatch)
				reserved := false
				_, err = store.AcquireSQLTaskRun(context.Background(), definition, mismatch, &SQLTaskRunAdmission{Reserve: func(uint32, string) bool {
					reserved = true
					return true
				}})
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.False(t, reserved)
			}
			mustGetTestSQLTaskRun(t, store, 0)
		})
	}
}

func TestSQLTaskScheduledAdmissionClaimsExactRun(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := factory(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			definition := newTestSQLTask("owned", 1)
			definition.ScheduleGeneration, definition.TriggerCount = 2, 3
			mustAddTestSQLTask(t, store, 1, definition)
			definition = mustGetTestSQLTask(t, store, 1)[0]

			newFiring := func(sequence uint64, legacy bool) task.AsyncTask {
				spec := &task.SQLTaskContext{TaskId: definition.TaskID, TriggerCount: sequence}
				value := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
				if legacy {
					value.Metadata.ID = fmt.Sprintf("sql-task:%d:%d", definition.TaskID, sequence)
				}
				value.ParentTaskID = fmt.Sprintf("sql-task:%d", definition.TaskID)
				value.Status = task.TaskStatus_Running
				value.TaskRunner = "cn1"
				value.Epoch = 1
				mustAddTestAsyncTask(t, store, 1, value)
				return mustGetTestAsyncTask(t, store, 1, WithTaskMetadataId(EQ, value.Metadata.ID))[0]
			}
			claimFor := func(value task.AsyncTask) *SQLTaskClaim {
				return &SQLTaskClaim{ID: value.ID, Runner: value.TaskRunner, Epoch: value.Epoch, ParentTaskID: value.ParentTaskID, MetadataID: value.Metadata.ID}
			}
			run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
			run.TriggerType = SQLTaskTriggerScheduled
			stale := newFiring(1, false)
			reserved := 0
			admission := testSQLTaskAdmission(claimFor(stale), 1)
			admission.Reserve = func(account uint32, name string) bool {
				reserved++
				require.Equal(t, definition.AccountID, account)
				require.Equal(t, definition.TaskName, name)
				return true
			}
			_, err := store.AcquireSQLTaskRun(ctx, definition, run, admission)
			require.ErrorIs(t, err, ErrSQLTaskStaleFiring)
			require.Zero(t, reserved)
			mustGetTestSQLTaskRun(t, store, 0)

			// Released 4.2 payloads contain a valid nonzero trigger count, but
			// their old metadata ID must not acquire ownership after upgrade.
			legacy := newFiring(2, true)
			admission.AsyncTaskID, admission.RunnerCN, admission.Epoch = legacy.ID, legacy.TaskRunner, legacy.Epoch
			admission.ParentTaskID, admission.MetadataID, admission.TriggerSequence = legacy.ParentTaskID, legacy.Metadata.ID, 2
			_, err = store.AcquireSQLTaskRun(ctx, definition, run, admission)
			require.ErrorIs(t, err, ErrSQLTaskStaleFiring)
			require.Zero(t, reserved)
			mustGetTestSQLTaskRun(t, store, 0)

			value := newFiring(2, false)
			admission.AsyncTaskID, admission.RunnerCN, admission.Epoch = value.ID, value.TaskRunner, value.Epoch
			admission.ParentTaskID, admission.MetadataID, admission.TriggerSequence = value.ParentTaskID, value.Metadata.ID, 2
			runID, err := store.AcquireSQLTaskRun(ctx, definition, run, admission)
			require.NoError(t, err)
			require.NotZero(t, runID)
			require.Equal(t, 1, reserved)
			runs := mustGetTestSQLTaskRun(t, store, 1)
			require.Equal(t, runID, runs[0].RunID)

			value.LastHeartbeat++
			mustUpdateTestAsyncTask(t, store, 1, []task.AsyncTask{value}, WithTaskIDCond(EQ, value.ID))
			_, err = store.AcquireSQLTaskRun(ctx, definition, run, admission)
			require.ErrorIs(t, err, ErrSQLTaskClaimed, "heartbeat must not discard the ownership pointer")
			admission.Epoch++
			_, err = store.AcquireSQLTaskRun(ctx, definition, run, admission)
			require.ErrorIs(t, err, ErrSQLTaskClaimed, "a later epoch cannot take over a claimed run")
			require.Equal(t, 1, reserved)
		})
	}
}

func TestSQLTaskRunnerExcludesClaimedBeforeLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage().(*memTaskStorage)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	claimed := task.AsyncTask{Metadata: task.TaskMetadata{ID: "claimed", Executor: task.TaskCode_SQLTask}}
	other := task.AsyncTask{Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}}
	unclaimed := task.AsyncTask{Metadata: task.TaskMetadata{ID: "unclaimed", Executor: task.TaskCode_SQLTask}}
	mustAddTestAsyncTask(t, store, 3, claimed, other, unclaimed)
	all := mustGetTestAsyncTask(t, store, 3)
	store.Lock()
	store.sqlTaskClaims[all[0].ID] = 99
	store.Unlock()
	fetched, err := store.QueryAsyncTask(ctx, withSQLTaskUnclaimed(), WithLimitCond(1))
	require.NoError(t, err)
	require.Len(t, fetched, 1)
	require.Equal(t, "other", fetched[0].Metadata.ID)
	fetched, err = store.QueryAsyncTask(ctx, withSQLTaskUnclaimed(), WithLimitCond(2))
	require.NoError(t, err)
	require.Len(t, fetched, 2)
	require.Equal(t, "unclaimed", fetched[1].Metadata.ID)

	mysqlStore, mock := newMockStorage(t)
	query := selectAsyncTask + " AND " + (sqlTaskUnclaimedCond{}).sql() + " order by task_id limit 1"
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{
		"id", "metadata", "executor", "context", "option", "parent", "status", "runner", "epoch", "heartbeat", "code", "error", "created", "completed",
	}))
	_, err = mysqlStore.QueryAsyncTask(ctx, withSQLTaskUnclaimed(), WithLimitCond(1))
	require.NoError(t, err)
	mock.ExpectClose()
	require.NoError(t, mysqlStore.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSQLTaskReceiptPublishesOnlyItsExactRun(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage().(*memTaskStorage)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	definition := newTestSQLTask("owned", 1)
	definition.TriggerCount = 1
	mustAddTestSQLTask(t, store, 1, definition)
	definition = mustGetTestSQLTask(t, store, 1)[0]
	value := task.AsyncTask{
		Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1,
		ParentTaskID: fmt.Sprintf("sql-task:%d", definition.TaskID),
		Metadata:     BuildSQLTaskMetadata(&task.SQLTaskContext{TaskId: definition.TaskID, TriggerCount: 1}),
	}
	mustAddTestAsyncTask(t, store, 1, value)
	value = mustGetTestAsyncTask(t, store, 1)[0]
	claim := SQLTaskClaim{ID: value.ID, Epoch: value.Epoch, Runner: value.TaskRunner, ParentTaskID: value.ParentTaskID, MetadataID: value.Metadata.ID}
	run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
	run.TriggerType = SQLTaskTriggerScheduled
	run.StartedAt = time.Now()
	admission := testSQLTaskAdmission(&claim, 1)
	id, err := store.AcquireSQLTaskRun(ctx, definition, run, admission)
	require.NoError(t, err)
	run.RunID, run.Status, run.FinishedAt, run.RowsAffected = id, SQLTaskStatusSuccess, time.Now(), 4
	receipt := SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: run, Claim: claim, EndAt: run.FinishedAt.UnixMilli()}

	// The owner's pointer remains authoritative even after allocation changes.
	value.Epoch++
	value.TaskRunner = "cn2"
	mustUpdateTestAsyncTask(t, store, 1, []task.AsyncTask{value}, WithTaskIDCond(EQ, value.ID))
	resolved, err := store.PublishSQLTaskReceipt(ctx, receipt)
	require.NoError(t, err)
	require.True(t, resolved)
	got := mustGetTestSQLTaskRun(t, store, 1)[0]
	require.Equal(t, SQLTaskStatusSuccess, got.Status)
	require.EqualValues(t, 4, got.RowsAffected)
	completed := mustGetTestAsyncTask(t, store, 1)[0]
	require.Equal(t, task.TaskStatus_Completed, completed.Status)
	require.Equal(t, task.ResultCode_Success, completed.ExecuteResult.Code)

	// A lost acknowledgement can be retried without rewriting the outcome.
	resolved, err = store.PublishSQLTaskReceipt(ctx, receipt)
	require.NoError(t, err)
	require.True(t, resolved)
	conflicting := receipt
	conflicting.Run.RowsAffected++
	resolved, err = store.PublishSQLTaskReceipt(ctx, conflicting)
	require.ErrorIs(t, err, errSQLTaskReceiptConflict)
	require.False(t, resolved)
	require.EqualValues(t, 4, mustGetTestSQLTaskRun(t, store, 1)[0].RowsAffected)
}

func TestSQLTaskNoClaimCannotFinishClaimedRun(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage().(*memTaskStorage)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	value := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 3, Metadata: task.TaskMetadata{ID: "firing", Executor: task.TaskCode_SQLTask}}
	mustAddTestAsyncTask(t, store, 1, value)
	value = mustGetTestAsyncTask(t, store, 1)[0]
	store.Lock()
	store.sqlTaskClaims[value.ID] = 88
	store.Unlock()
	err := store.FinishSQLTaskUnclaimed(ctx, SQLTaskClaim{ID: value.ID, Runner: "cn1", Epoch: 3}, task.ExecuteResult{}, time.Now().UnixMilli())
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_Running, mustGetTestAsyncTask(t, store, 1)[0].Status)
}

func TestSQLTaskNoClaimCannotFinishAnotherTaskType(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage().(*memTaskStorage)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	value := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 3, Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}}
	mustAddTestAsyncTask(t, store, 1, value)
	value = mustGetTestAsyncTask(t, store, 1)[0]
	claim := SQLTaskClaim{ID: value.ID, Runner: value.TaskRunner, Epoch: value.Epoch}
	require.NoError(t, store.FinishSQLTaskUnclaimed(ctx, claim, task.ExecuteResult{}, time.Now().UnixMilli()))
	resolved, err := store.PublishSQLTaskReceipt(ctx, SQLTaskReceiptRequest{Kind: SQLTaskReceiptAdmission, Run: SQLTaskRun{RunID: 999}, Claim: claim})
	require.NoError(t, err)
	require.True(t, resolved)
	require.Equal(t, task.TaskStatus_Running, mustGetTestAsyncTask(t, store, 1)[0].Status)
}

func TestSQLTaskMalformedMultibyteFiringCanFinish(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage().(*memTaskStorage)
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	firing := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1,
		ParentTaskID: strings.Repeat("界", 50), Metadata: task.TaskMetadata{ID: strings.Repeat("字", 50), Executor: task.TaskCode_SQLTask, Context: []byte{0xff}}}
	mustAddTestAsyncTask(t, store, 1, firing)
	firing = mustGetTestAsyncTask(t, store, 1)[0]
	disposition := &sqlTaskRunnerDisposition{}
	ctx = context.WithValue(ctx, sqlTaskDispositionContextKey{}, disposition)
	executor := NewSQLTaskExecutor(nil, service, "cn1").TaskExecutor()
	require.NoError(t, executor(ctx, &firing))
	require.True(t, disposition.handled)
	require.False(t, disposition.deferred)
	completed := mustGetTestAsyncTask(t, store, 1)[0]
	require.Equal(t, task.TaskStatus_Completed, completed.Status)
	require.Equal(t, task.ResultCode_Failed, completed.ExecuteResult.Code)
}

func TestSQLTaskNoClaimAndAdmissionAreMutuallyExclusive(t *testing.T) {
	for name, factory := range storages {
		for _, claimFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/claim_first=%t", name, claimFirst), func(t *testing.T) {
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
				claim := SQLTaskClaim{ID: firing.ID, Runner: firing.TaskRunner, Epoch: firing.Epoch, ParentTaskID: firing.ParentTaskID, MetadataID: firing.Metadata.ID}
				run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
				run.TriggerType = SQLTaskTriggerScheduled
				admit := func() error {
					_, err := store.AcquireSQLTaskRun(ctx, definition, run, testSQLTaskAdmission(&claim, 1))
					return err
				}
				finish := func() error {
					return store.FinishSQLTaskUnclaimed(ctx, claim, task.ExecuteResult{Code: task.ResultCode_Success}, time.Now().UnixMilli())
				}
				if claimFirst {
					require.NoError(t, admit())
					require.NoError(t, finish())
					require.Equal(t, task.TaskStatus_Running, mustGetTestAsyncTask(t, store, 1)[0].Status)
					mustGetTestSQLTaskRun(t, store, 1)
				} else {
					require.NoError(t, finish())
					require.ErrorIs(t, admit(), ErrSQLTaskStaleFiring)
					require.Equal(t, task.TaskStatus_Completed, mustGetTestAsyncTask(t, store, 1)[0].Status)
					mustGetTestSQLTaskRun(t, store, 0)
				}
			})
		}
	}
}

func TestSQLTaskMySQLAdmissionClaimsRunAtomically(t *testing.T) {
	for _, loseCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_commit_response=%t", loseCommit), func(t *testing.T) {
			store, mock := newMockStorage(t)
			definition := newTestSQLTask("owned", 1)
			definition.TaskID, definition.ScheduleGeneration, definition.TriggerCount = 7, 2, 3
			value := task.AsyncTask{
				ID: 9, Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 4,
				ParentTaskID: "sql-task:7", Metadata: BuildSQLTaskMetadata(&task.SQLTaskContext{TaskId: 7, TriggerCount: 2}),
			}
			run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
			run.TriggerType = SQLTaskTriggerScheduled
			reserved := 0
			admission := testSQLTaskAdmission(&SQLTaskClaim{ID: value.ID, Runner: value.TaskRunner, Epoch: value.Epoch, ParentTaskID: value.ParentTaskID, MetadataID: value.Metadata.ID}, 2)
			admission.Reserve = func(account uint32, name string) bool {
				reserved++
				return account == definition.AccountID && name == definition.TaskName
			}
			mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(lockOwnedSQLTaskAdmission).
				WithArgs(definition.TaskID, definition.AccountID, definition.TaskName).
				WillReturnRows(sqlmock.NewRows([]string{"task_id", "account_id", "task_name", "enabled", "schedule_generation", "trigger_count"}).
					AddRow(definition.TaskID, definition.AccountID, definition.TaskName, 1, 2, 3))
			mock.ExpectQuery(lockSQLTaskAsyncClaim).WithArgs(value.ID).
				WillReturnRows(sqlmock.NewRows([]string{"id", "status", "runner", "epoch", "parent", "metadata", "executor", "run"}).
					AddRow(value.ID, int32(value.Status), value.TaskRunner, value.Epoch, value.ParentTaskID, value.Metadata.ID, int32(value.Metadata.Executor), 0))
			mock.ExpectQuery(selectRunningSQLTaskByName).
				WithArgs(definition.AccountID, definition.TaskName, SQLTaskStatusRunning).
				WillReturnRows(sqlmock.NewRows([]string{"run_id"}))
			mock.ExpectExec(insertSQLTaskRun).WillReturnResult(sqlmock.NewResult(42, 1))
			mock.ExpectExec(setSQLTaskAsyncClaim).WithArgs(uint64(42), value.ID).WillReturnResult(sqlmock.NewResult(0, 1))
			commitErr := errors.New("commit acknowledgement lost")
			if loseCommit {
				mock.ExpectExec("COMMIT").WillReturnError(commitErr)
				mock.ExpectClose()
			} else {
				mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			runID, err := store.AcquireSQLTaskRun(context.Background(), definition, run, admission)
			require.Equal(t, uint64(42), runID)
			require.Equal(t, 1, reserved)
			if loseCommit {
				require.ErrorIs(t, err, commitErr)
			} else {
				require.NoError(t, err)
				mock.ExpectClose()
			}
			require.NoError(t, store.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSQLTaskMySQLAdmissionRejectsNoncanonicalIdentity(t *testing.T) {
	store, mock := newMockStorage(t)
	definition := newTestSQLTask("owned", 1)
	definition.TaskID = 7
	run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
	bad := run
	bad.TaskName = "other"
	_, err := store.AcquireSQLTaskRun(context.Background(), definition, bad, testSQLTaskAdmission(nil, 0))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))

	// Simulate a case-insensitive SQL comparison returning a differently
	// spelled catalog name. Do not admit a run under a noncanonical identity.
	mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(lockOwnedSQLTaskAdmission).WithArgs(definition.TaskID, definition.AccountID, definition.TaskName).
		WillReturnRows(sqlmock.NewRows([]string{"id", "account", "name", "enabled", "generation", "count"}).AddRow(7, 1, "OWNED", 1, 1, 0))
	mock.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))
	_, err = store.AcquireSQLTaskRun(context.Background(), definition, run, testSQLTaskAdmission(nil, 0))
	require.ErrorIs(t, err, ErrSQLTaskNotFound)
	mock.ExpectClose()
	require.NoError(t, store.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}
