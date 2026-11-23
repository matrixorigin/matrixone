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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

func (m *mysqlTaskStorage) TransitionSQLTaskRun(ctx context.Context, retry SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	retry = snapshotSQLTaskRetry(retry)
	var nextID uint64
	continued := false
	commitAttempted, err := runSQLTaskMetadataTransaction(ctx, m.db, func(ctx context.Context, conn *sql.Conn) error {
		var definition SQLTask
		var enabled uint8
		err := conn.QueryRowContext(ctx, lockOwnedSQLTaskAdmission,
			retry.Previous.TaskID, retry.Previous.AccountID, retry.Previous.TaskName,
		).Scan(&definition.TaskID, &definition.AccountID, &definition.TaskName, &enabled, &definition.ScheduleGeneration, &definition.TriggerCount)
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		definition.Enabled = enabled != 0
		var state sqlTaskReceiptAsyncState
		var async task.AsyncTask
		if retry.Claim != nil {
			err = conn.QueryRowContext(ctx, lockSQLTaskReceiptAsync, retry.Claim.ID).
				Scan(&state.status, &state.runner, &state.epoch, &state.pointer)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil {
				async.ID, async.Status, async.TaskRunner, async.Epoch = retry.Claim.ID, task.TaskStatus(state.status.Int64), state.runner.String, uint32(state.epoch.Int64)
			}
		}
		continued = canRetrySQLTask(definition, exists, retry, async, state.pointer)
		updated, err := execOwnedSQLTaskRunResult(ctx, conn, retry.Previous)
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
		if !continued {
			if async.ID != 0 && async.Status == task.TaskStatus_Running && state.pointer == retry.Previous.RunID {
				result := sqlTaskReceiptAsyncResult(&SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: retry.Previous})
				updated, err = conn.ExecContext(ctx, completeSQLTaskAsync, task.TaskStatus_Completed, result.Code, nullString(result.Error),
					retry.Previous.FinishedAt.UnixMilli(), async.ID, task.TaskStatus_Running, retry.Previous.RunID)
				if err != nil {
					return err
				}
				count, err = updated.RowsAffected()
				if err != nil {
					return err
				}
				if count != 1 {
					return errSQLTaskReceiptConflict
				}
			}
			return nil
		}
		next := retry.Next
		inserted, err := conn.ExecContext(ctx, insertSQLTaskRun,
			next.TaskID, next.TaskName, next.AccountID, nullTime(next.ScheduledAt), nullTime(next.StartedAt),
			nullTime(next.FinishedAt), next.DurationSeconds, next.Status, next.TriggerType, next.AttemptNumber,
			next.RowsAffected, next.ErrorCode, nullString(next.ErrorMessage), sqlTaskEnabledValue(next.GateResult), next.RunnerCN)
		if err != nil {
			return err
		}
		id, err := inserted.LastInsertId()
		if err != nil {
			return err
		}
		if id <= 0 {
			return moerr.NewInternalErrorNoCtx("SQL task retry did not return a run ID")
		}
		nextID = uint64(id)
		if retry.Claim != nil {
			updated, err = conn.ExecContext(ctx,
				"update sys_async_task set sql_task_run_id=? where task_id=? and sql_task_run_id=? and task_status=? and task_runner=? and task_epoch=?",
				nextID, retry.Claim.ID, retry.Previous.RunID, task.TaskStatus_Running, retry.Claim.Runner, retry.Claim.Epoch)
			if err != nil {
				return err
			}
			count, err = updated.RowsAffected()
			if err != nil {
				return err
			}
			if count != 1 {
				return errSQLTaskReceiptConflict
			}
		}
		return nil
	})
	if err != nil && !commitAttempted {
		return SQLTaskRetryTransition{}, err
	}
	return SQLTaskRetryTransition{RunID: nextID, Continued: continued, CommitAttempted: commitAttempted}, err
}

func transitionSQLTaskRun(ctx context.Context, store TaskStorage, retry SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	return store.TransitionSQLTaskRun(ctx, snapshotSQLTaskRetry(retry))
}

// retrySQLTaskTransition keeps the business retry with its live executor. A
// metadata COMMIT must be reconciled before the next SQL attempt can start.
func retrySQLTaskTransition(ctx context.Context, store TaskStorage, retry SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var result SQLTaskRetryTransition
	var lastErr error
	canceled := func(cause error) error {
		if lastErr == nil {
			return cause
		}
		if errors.Is(lastErr, cause) {
			return lastErr
		}
		return errors.Join(lastErr, cause)
	}
	reconcile := false
	backoff := 20 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return result, canceled(err)
		}
		if reconcile {
			var resolution SQLTaskRetryResolution
			resolution, lastErr = store.ReconcileSQLTaskRetry(ctx, snapshotSQLTaskRetry(retry), result.RunID)
			if lastErr == nil {
				switch resolution {
				case SQLTaskRetryRolledBack:
					result, reconcile = SQLTaskRetryTransition{}, false
				case SQLTaskRetryAdmitted:
					result.Continued = true
					return result, nil
				case SQLTaskRetryStopped:
					return SQLTaskRetryTransition{CommitAttempted: true}, nil
				default:
					return result, errSQLTaskReceiptConflict
				}
			}
		} else {
			result, lastErr = transitionSQLTaskRun(ctx, store, retry)
			if lastErr == nil {
				return result, nil
			}
			if result.CommitAttempted {
				// Even a normally terminal error must be reconciled if
				// COMMIT was attempted.
				reconcile = true
				continue
			}
		}
		if expectedSQLTaskMetadataError(lastErr) {
			return result, lastErr
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return result, canceled(ctx.Err())
		case <-timer.C:
		}
		backoff = min(backoff*2, 250*time.Millisecond)
	}
}

// ReconcileSQLTaskRetry serializes with the uncertain transition before
// deciding whether the original executor owns the tentative successor.
func (m *mysqlTaskStorage) ReconcileSQLTaskRetry(ctx context.Context, retry SQLTaskRetryRequest, nextID uint64) (SQLTaskRetryResolution, error) {
	retry = snapshotSQLTaskRetry(retry)
	var resolution SQLTaskRetryResolution
	_, err := runSQLTaskMetadataTransaction(ctx, m.db, func(ctx context.Context, conn *sql.Conn) error {
		var taskID uint64
		err := conn.QueryRowContext(ctx,
			"select task_id from sql_task where task_id=? and account_id=? and task_name=? for update",
			retry.Previous.TaskID, retry.Previous.AccountID, retry.Previous.TaskName).Scan(&taskID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var async task.AsyncTask
		var pointer uint64
		if retry.Claim != nil {
			var state sqlTaskReceiptAsyncState
			err = conn.QueryRowContext(ctx, lockSQLTaskReceiptAsync, retry.Claim.ID).
				Scan(&state.status, &state.runner, &state.epoch, &state.pointer)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil {
				async.ID, async.Status, async.TaskRunner, async.Epoch = retry.Claim.ID, task.TaskStatus(state.status.Int64), state.runner.String, uint32(state.epoch.Int64)
				pointer = state.pointer
			}
		}
		previous, previousExists, err := lockSQLTaskRunReceipt(ctx, conn, retry.Previous.RunID)
		if err != nil {
			return err
		}
		var next SQLTaskRun
		var nextExists bool
		if nextID != 0 {
			next, nextExists, err = lockSQLTaskRunReceipt(ctx, conn, nextID)
			if err != nil {
				return err
			}
		}
		resolution, err = resolveSQLTaskRetry(retry, nextID, previous, previousExists, next, nextExists, async, pointer)
		return err
	})
	return resolution, err
}

func sameSQLTaskRetryOwner(actual, expected SQLTaskRun) bool {
	return actual.RunID == expected.RunID && actual.TaskID == expected.TaskID &&
		actual.AccountID == expected.AccountID && actual.TaskName == expected.TaskName &&
		actual.RunnerCN == expected.RunnerCN && actual.AttemptNumber == expected.AttemptNumber &&
		actual.TriggerType == expected.TriggerType
}

func resolveSQLTaskRetry(retry SQLTaskRetryRequest, nextID uint64, previous SQLTaskRun, previousExists bool,
	next SQLTaskRun, nextExists bool, async task.AsyncTask, pointer uint64) (SQLTaskRetryResolution, error) {
	if !previousExists || !sameSQLTaskRetryOwner(previous, retry.Previous) {
		return 0, errSQLTaskReceiptConflict
	}
	if previous.Status == SQLTaskStatusUnknown {
		return SQLTaskRetryStopped, nil
	}
	if previous.Status == SQLTaskStatusRunning && !nextExists {
		return SQLTaskRetryRolledBack, nil
	}
	if !sameSQLTaskReceiptResult(previous, retry.Previous) {
		return 0, errSQLTaskReceiptConflict
	}
	if nextID == 0 {
		if retry.Claim != nil && async.Status == task.TaskStatus_Running && pointer == previous.RunID {
			return 0, errSQLTaskReceiptConflict
		}
		return SQLTaskRetryStopped, nil
	}
	expected := retry.Next
	expected.RunID = nextID
	if !nextExists || !sameSQLTaskRetryOwner(next, expected) {
		return 0, errSQLTaskReceiptConflict
	}
	if next.Status == SQLTaskStatusUnknown {
		return SQLTaskRetryStopped, nil
	}
	if next.Status != SQLTaskStatusRunning {
		return 0, errSQLTaskReceiptConflict
	}
	if retry.Claim != nil && (async.ID != retry.Claim.ID || async.Status != task.TaskStatus_Running ||
		async.TaskRunner != retry.Claim.Runner || async.Epoch != retry.Claim.Epoch || pointer != nextID) {
		return 0, errSQLTaskReceiptConflict
	}
	return SQLTaskRetryAdmitted, nil
}

func canRetrySQLTask(definition SQLTask, exists bool, retry SQLTaskRetryRequest, async task.AsyncTask, pointer uint64) bool {
	if !exists || definition.TaskID != retry.Previous.TaskID || definition.AccountID != retry.Previous.AccountID ||
		definition.TaskName != retry.Previous.TaskName {
		return false
	}
	if retry.Claim == nil {
		return true
	}
	claim := retry.Claim
	// Schedule changes invalidate unclaimed firings, not the remaining retry
	// attempts of an execution that already owns this async row.
	return async.ID == claim.ID && async.Status == task.TaskStatus_Running && async.TaskRunner == claim.Runner &&
		async.Epoch == claim.Epoch && pointer == retry.Previous.RunID
}
