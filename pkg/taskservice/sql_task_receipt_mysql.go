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
	"database/sql"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

const (
	lockSQLTaskReceiptAsync  = "select task_status, task_runner, task_epoch, sql_task_run_id from sys_async_task where task_id=? for update"
	completeSQLTaskAsync     = "update sys_async_task set task_status=?, result_code=?, error_msg=?, end_at=? where task_id=? and task_status=? and sql_task_run_id=?"
	completeSQLTaskUnclaimed = "update sys_async_task set task_status=?, result_code=?, error_msg=?, end_at=? where task_id=? and task_status=? and task_runner=? and task_epoch=? and sql_task_run_id=0 and task_metadata_executor=?"
	completeSQLTaskRun       = "update sql_task_run set finished_at=?,duration_seconds=?,status=?,rows_affected=?,error_code=?,error_message=?,gate_result=? where run_id=? and status=? and runner_cn=? and attempt_number=?"
	markSQLTaskUnknown       = "update sql_task_run set error_code=?, error_message=?, gate_result=? where run_id=? and task_id=? and account_id=? and status=? and runner_cn=? and attempt_number=?"
)

type sqlTaskReceiptAsyncState struct {
	status  sql.NullInt64
	runner  sql.NullString
	epoch   sql.NullInt64
	pointer uint64
}

func (m *mysqlTaskStorage) PublishSQLTaskReceipt(ctx context.Context, request SQLTaskReceiptRequest) (bool, error) {
	receipt := &request
	if receipt.Kind != SQLTaskReceiptTerminal && receipt.Kind != SQLTaskReceiptUnknown && receipt.Kind != SQLTaskReceiptAdmission && receipt.Kind != SQLTaskReceiptRetryAdmission {
		return false, ErrNotReady
	}
	resolved := false
	_, err := runSQLTaskMetadataTransaction(ctx, m.db, func(ctx context.Context, conn *sql.Conn) error {
		if receipt.Kind == SQLTaskReceiptAdmission || receipt.Kind == SQLTaskReceiptRetryAdmission {
			// Serialize with the original admission before deciding that an
			// absent run means its COMMIT did not persist.
			var taskID uint64
			err := conn.QueryRowContext(ctx,
				"select task_id from sql_task where task_id=? and account_id=? and task_name=? for update",
				receipt.Run.TaskID, receipt.Run.AccountID, receipt.Run.TaskName).Scan(&taskID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		var async sqlTaskReceiptAsyncState
		asyncExists := false
		if receipt.Claim.ID != 0 {
			err := conn.QueryRowContext(ctx, lockSQLTaskReceiptAsync, receipt.Claim.ID).
				Scan(&async.status, &async.runner, &async.epoch, &async.pointer)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			asyncExists = err == nil
		}
		if receipt.Kind == SQLTaskReceiptRetryAdmission {
			previous, previousExists, err := lockSQLTaskRunReceipt(ctx, conn, receipt.Run.RunID)
			if err != nil {
				return err
			}
			_, nextExists, err := lockSQLTaskRunReceipt(ctx, conn, receipt.NextRun.RunID)
			if err != nil {
				return err
			}
			selected := *receipt
			selected.Kind = SQLTaskReceiptTerminal
			if nextExists {
				if !previousExists || !sameSQLTaskReceiptResult(previous, receipt.Run) {
					return errSQLTaskReceiptConflict
				}
				selected.Run = receipt.NextRun
			}
			receipt = &selected
		}
		result := sqlTaskReceiptAsyncResult(receipt)
		if receipt.Kind == SQLTaskReceiptAdmission {
			_, exists, err := lockSQLTaskRunReceipt(ctx, conn, receipt.Run.RunID)
			if err != nil {
				return err
			}
			if !exists {
				if asyncExists && async.status.Int64 == int64(task.TaskStatus_Running) &&
					async.runner.String == receipt.Claim.Runner && async.epoch.Int64 == int64(receipt.Claim.Epoch) && async.pointer == 0 {
					updated, err := conn.ExecContext(ctx, completeSQLTaskUnclaimed,
						task.TaskStatus_Completed, result.Code, nullString(result.Error), receipt.EndAt,
						receipt.Claim.ID, task.TaskStatus_Running, receipt.Claim.Runner, receipt.Claim.Epoch, task.TaskCode_SQLTask)
					if err != nil {
						return err
					}
					count, err := updated.RowsAffected()
					if err != nil {
						return err
					}
					if count > 1 {
						return errSQLTaskReceiptConflict
					}
				}
				resolved = true
				return nil
			}
		}

		run := receipt.Run
		var updated sql.Result
		var err error
		if receipt.Kind == SQLTaskReceiptUnknown {
			updated, err = conn.ExecContext(ctx, markSQLTaskUnknown,
				run.ErrorCode, nullString(run.ErrorMessage), sqlTaskEnabledValue(run.GateResult),
				run.RunID, run.TaskID, run.AccountID, SQLTaskStatusRunning, run.RunnerCN, run.AttemptNumber)
		} else {
			updated, err = execOwnedSQLTaskRunResult(ctx, conn, run)
		}
		if err != nil {
			return err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			var same, superseded bool
			same, superseded, err = reconcileSQLTaskRunReceipt(ctx, conn, receipt)
			if err != nil {
				return err
			}
			if superseded {
				resolved = true
				return nil
			}
			if !same {
				return errSQLTaskReceiptConflict
			}
		} else if count != 1 {
			return errSQLTaskReceiptConflict
		}
		if asyncExists && async.status.Int64 == int64(task.TaskStatus_Running) && async.pointer == run.RunID {
			updated, err := conn.ExecContext(ctx, completeSQLTaskAsync,
				task.TaskStatus_Completed, result.Code, nullString(result.Error), receipt.EndAt,
				receipt.Claim.ID, task.TaskStatus_Running, run.RunID)
			if err != nil {
				return err
			}
			count, err := updated.RowsAffected()
			if err != nil {
				return err
			}
			if count != 1 {
				return errSQLTaskReceiptConflict
			}
		}
		resolved = true
		return nil
	})
	return resolved && err == nil, err
}

func execOwnedSQLTaskRunResult(ctx context.Context, conn *sql.Conn, run SQLTaskRun) (sql.Result, error) {
	return conn.ExecContext(ctx, completeSQLTaskRun+" and task_id=? and account_id=?",
		nullTime(run.FinishedAt), run.DurationSeconds, run.Status, run.RowsAffected, run.ErrorCode,
		nullString(run.ErrorMessage), sqlTaskEnabledValue(run.GateResult),
		run.RunID, SQLTaskStatusRunning, run.RunnerCN, run.AttemptNumber, run.TaskID, run.AccountID)
}

// Re-read the exact run when an UPDATE affected no rows. In particular, a
// lost COMMIT acknowledgement must not be mistaken for a fresh success.
func reconcileSQLTaskRunReceipt(ctx context.Context, conn *sql.Conn, receipt *SQLTaskReceiptRequest) (same, superseded bool, err error) {
	existing, exists, err := lockSQLTaskRunReceipt(ctx, conn, receipt.Run.RunID)
	if err != nil {
		return false, false, err
	}
	if !exists {
		var account uint32
		err := conn.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_id=?", receipt.Run.AccountID).Scan(&account)
		if errors.Is(err, sql.ErrNoRows) {
			return false, true, nil
		}
		if err != nil {
			return false, false, err
		}
		return false, false, nil
	}
	if existing.TaskID != receipt.Run.TaskID || existing.AccountID != receipt.Run.AccountID || existing.TaskName != receipt.Run.TaskName {
		return false, false, nil
	}
	if existing.Status == SQLTaskStatusUnknown {
		return false, true, nil
	}
	if receipt.Kind == SQLTaskReceiptUnknown {
		return existing.Status == SQLTaskStatusRunning && existing.RunnerCN == receipt.Run.RunnerCN &&
			existing.AttemptNumber == receipt.Run.AttemptNumber && existing.ErrorCode == receipt.Run.ErrorCode &&
			existing.ErrorMessage == receipt.Run.ErrorMessage && existing.GateResult == receipt.Run.GateResult, false, nil
	}
	return sameSQLTaskReceiptResult(existing, receipt.Run), false, nil
}

func lockSQLTaskRunReceipt(ctx context.Context, conn *sql.Conn, runID uint64) (SQLTaskRun, bool, error) {
	rows, err := conn.QueryContext(ctx, selectSQLTaskRun+" AND run_id=? for update", runID)
	if err != nil {
		return SQLTaskRun{}, false, err
	}
	defer rows.Close()
	runs, err := scanSQLTaskRuns(rows)
	closeErr := rows.Close()
	if err != nil {
		return SQLTaskRun{}, false, err
	}
	if closeErr != nil {
		return SQLTaskRun{}, false, closeErr
	}
	if len(runs) == 0 {
		return SQLTaskRun{}, false, nil
	}
	return runs[0], true, nil
}
