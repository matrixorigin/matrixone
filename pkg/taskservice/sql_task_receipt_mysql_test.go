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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskMySQLReceiptCommitAndReconcile(t *testing.T) {
	finished := time.Date(2026, 11, 6, 3, 0, 10, 0, time.UTC)
	run := SQLTaskRun{
		RunID: 42, TaskID: 7, TaskName: "owned", AccountID: 1, Status: SQLTaskStatusSuccess,
		RunnerCN: "cn1", AttemptNumber: 1, TriggerType: SQLTaskTriggerScheduled,
		StartedAt: finished.Add(-time.Second), FinishedAt: finished, DurationSeconds: 1,
		RowsAffected: 4, GateResult: true,
	}
	receipt := SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: run, EndAt: finished.UnixMilli(), Claim: SQLTaskClaim{ID: 9, Runner: "cn1", Epoch: 4}}
	for _, loseCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_commit_response=%t", loseCommit), func(t *testing.T) {
			store, mock := newMockStorage(t)
			mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(lockSQLTaskReceiptAsync).WithArgs(receipt.Claim.ID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "runner", "epoch", "run"}).AddRow(int32(task.TaskStatus_Running), "cn2", 5, run.RunID))
			mock.ExpectExec(completeSQLTaskRun+" and task_id=? and account_id=?").
				WithArgs(run.FinishedAt, run.DurationSeconds, run.Status, run.RowsAffected, run.ErrorCode, nil, 1,
					run.RunID, SQLTaskStatusRunning, run.RunnerCN, run.AttemptNumber, run.TaskID, run.AccountID).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(completeSQLTaskAsync).
				WithArgs(task.TaskStatus_Completed, task.ResultCode_Success, nil, receipt.EndAt, receipt.Claim.ID, task.TaskStatus_Running, run.RunID).
				WillReturnResult(sqlmock.NewResult(0, 1))
			commitErr := errors.New("commit acknowledgement lost")
			if loseCommit {
				mock.ExpectExec("COMMIT").WillReturnError(commitErr)
				mock.ExpectClose()
			} else {
				mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			resolved, err := store.PublishSQLTaskReceipt(context.Background(), receipt)
			if loseCommit {
				require.ErrorIs(t, err, commitErr)
				require.False(t, resolved)
			} else {
				require.NoError(t, err)
				require.True(t, resolved)
				mock.ExpectClose()
			}
			require.NoError(t, store.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("retry confirms existing result", func(t *testing.T) {
		store, mock := newMockStorage(t)
		mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(lockSQLTaskReceiptAsync).WithArgs(receipt.Claim.ID).
			WillReturnRows(sqlmock.NewRows([]string{"status", "runner", "epoch", "run"}).AddRow(int32(task.TaskStatus_Completed), "cn2", 5, run.RunID))
		mock.ExpectExec(completeSQLTaskRun + " and task_id=? and account_id=?").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(selectSQLTaskRun + " AND run_id=? for update").WithArgs(run.RunID).
			WillReturnRows(sqlmock.NewRows([]string{
				"run_id", "task_id", "task_name", "account_id", "scheduled_at", "started_at", "finished_at", "duration_seconds",
				"status", "trigger_type", "attempt_number", "rows_affected", "error_code", "error_message", "gate_result", "runner_cn",
			}).AddRow(run.RunID, run.TaskID, run.TaskName, run.AccountID, nil, run.StartedAt, run.FinishedAt, run.DurationSeconds,
				run.Status, run.TriggerType, run.AttemptNumber, run.RowsAffected, run.ErrorCode, nil, 1, run.RunnerCN))
		mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
		resolved, err := store.PublishSQLTaskReceipt(context.Background(), receipt)
		require.NoError(t, err)
		require.True(t, resolved)
		mock.ExpectClose()
		require.NoError(t, store.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
