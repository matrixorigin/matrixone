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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

type sqlTaskRetry struct {
	previous SQLTaskRun
	next     SQLTaskRun
	claim    *sqlTaskAsyncClaim
}

func (m *mysqlTaskStorage) transitionSQLTaskRun(ctx context.Context, retry sqlTaskRetry) (uint64, bool, error) {
	var nextID uint64
	continued := false
	commitAttempted, err := runSQLTaskMetadataTransaction(ctx, m.db, func(ctx context.Context, conn *sql.Conn) error {
		var definition SQLTask
		var enabled uint8
		err := conn.QueryRowContext(ctx, lockOwnedSQLTaskAdmission,
			retry.previous.TaskID, retry.previous.AccountID, retry.previous.TaskName,
		).Scan(&definition.TaskID, &definition.AccountID, &definition.TaskName, &enabled, &definition.ScheduleGeneration, &definition.TriggerCount)
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		definition.Enabled = enabled != 0
		var state sqlTaskReceiptAsyncState
		var async task.AsyncTask
		if retry.claim != nil {
			err = conn.QueryRowContext(ctx, lockSQLTaskReceiptAsync, retry.claim.id).
				Scan(&state.status, &state.runner, &state.epoch, &state.pointer)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil {
				async.ID, async.Status, async.TaskRunner, async.Epoch = retry.claim.id, task.TaskStatus(state.status.Int64), state.runner.String, uint32(state.epoch.Int64)
			}
		}
		continued = canRetrySQLTask(definition, exists, retry, async, state.pointer)
		updated, err := execOwnedSQLTaskRunResult(ctx, conn, retry.previous)
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
			if async.ID != 0 && async.Status == task.TaskStatus_Running && state.pointer == retry.previous.RunID {
				result := sqlTaskReceiptAsyncResult(&sqlTaskReceipt{kind: sqlTaskReceiptTerminal, run: retry.previous})
				updated, err = conn.ExecContext(ctx, completeSQLTaskAsync, task.TaskStatus_Completed, result.Code, nullString(result.Error),
					retry.previous.FinishedAt.UnixMilli(), async.ID, task.TaskStatus_Running, retry.previous.RunID)
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
		next := retry.next
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
		if retry.claim != nil {
			updated, err = conn.ExecContext(ctx,
				"update sys_async_task set sql_task_run_id=? where task_id=? and sql_task_run_id=? and task_status=? and task_runner=? and task_epoch=?",
				nextID, retry.claim.id, retry.previous.RunID, task.TaskStatus_Running, retry.claim.runner, retry.claim.epoch)
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
		return 0, false, err
	}
	return nextID, continued, err
}

func transitionSQLTaskRun(ctx context.Context, store TaskStorage, retry sqlTaskRetry) (uint64, bool, error) {
	return store.TransitionSQLTaskRun(ctx, retry.request())
}

func canRetrySQLTask(definition SQLTask, exists bool, retry sqlTaskRetry, async task.AsyncTask, pointer uint64) bool {
	if !exists || definition.TaskID != retry.previous.TaskID || definition.AccountID != retry.previous.AccountID ||
		definition.TaskName != retry.previous.TaskName {
		return false
	}
	if retry.claim == nil {
		return true
	}
	claim := retry.claim
	// Schedule changes invalidate unclaimed firings, not the remaining retry
	// attempts of an execution that already owns this async row.
	return async.ID == claim.id && async.Status == task.TaskStatus_Running && async.TaskRunner == claim.runner &&
		async.Epoch == claim.epoch && pointer == retry.previous.RunID
}
