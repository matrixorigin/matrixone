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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
)

// ForceRecoverSQLTaskRun releases one unprovable run. It does not stop SQL or
// undo effects; callers must isolate the old executor before invoking it.
func ForceRecoverSQLTaskRun(ctx context.Context, store TaskStorage, account uint32, name string, runID uint64, actor string) (bool, error) {
	if runID == 0 || name == "" {
		return false, moerr.NewInvalidInputNoCtx("task name and run ID are required")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return forceRecoverSQLTaskRun(ctx, store, account, name, runID, actor)
}

func forceRecoverSQLTaskRun(ctx context.Context, store TaskStorage, account uint32, name string, runID uint64, actor string) (bool, error) {
	return store.ForceRecoverSQLTaskRun(ctx, account, name, runID, actor)
}

func forceSQLTaskReason(previous, actor string, now time.Time) string {
	reason := fmt.Sprintf("FORCE by %q at %s: execution outcome unknown; SQL may still be running", util.Abbreviate(actor, 128), now.UTC().Format(time.RFC3339))
	if previous != "" {
		reason += "; previous: " + util.Abbreviate(previous, 768)
	}
	return reason
}

func (s *memTaskStorage) forceRecoverSQLTaskRun(ctx context.Context, account uint32, name string, runID uint64, actor string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.Lock()
	defer s.Unlock()
	run, ok := s.sqlTaskRuns[runID]
	if !ok || run.AccountID != account || run.TaskName != name {
		return false, ErrSQLTaskNotFound
	}
	if run.Status != SQLTaskStatusRunning {
		return false, nil
	}
	parent := fmt.Sprintf("sql-task:%d", run.TaskID)
	var asyncID uint64
	for id, async := range s.asyncTasks {
		if async.ParentTaskID == parent && s.sqlTaskClaims[id] == runID {
			if asyncID != 0 {
				return false, errSQLTaskReceiptConflict
			}
			asyncID = id
		}
	}
	now := time.Now().UTC()
	if id, ok := s.sqlTaskIndexes[sqlTaskIndexKey(account, run.TaskName)]; ok {
		definition := s.sqlTasks[id]
		sequence, err := nextSQLTaskInvalidationSequence(definition)
		if err != nil {
			return false, err
		}
		definition.Enabled, definition.UpdatedAt = false, now
		definition.TriggerCount, definition.ScheduleGeneration = sequence, sequence
		s.sqlTasks[id] = definition
	}
	run.Status, run.FinishedAt = SQLTaskStatusUnknown, now
	run.ErrorMessage = forceSQLTaskReason(run.ErrorMessage, actor, now)
	s.sqlTaskRuns[runID] = run
	if asyncID != 0 {
		async := s.asyncTasks[asyncID]
		if async.Status == task.TaskStatus_Running {
			result := task.ExecuteResult{Code: task.ResultCode_Failed, Error: util.Abbreviate(run.ErrorMessage, 997)}
			async.Status, async.ExecuteResult, async.CompletedAt = task.TaskStatus_Completed, &result, now.UnixMilli()
			s.asyncTasks[asyncID] = async
		}
	}
	return true, nil
}

func (m *mysqlTaskStorage) forceRecoverSQLTaskRun(ctx context.Context, account uint32, name string, runID uint64, actor string) (bool, error) {
	// This lookup is only a hint for lock ordering. Every identity is checked
	// again under the transaction's locks.
	var taskID uint64
	var storedName string
	err := m.db.QueryRowContext(ctx, "select task_id, task_name from sql_task_run where run_id=? and account_id=? and task_name=?", runID, account, name).Scan(&taskID, &storedName)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrSQLTaskNotFound
	}
	if err != nil {
		return false, err
	}
	changed := false
	commitAttempted, err := runSQLTaskMetadataTransaction(ctx, m.db, func(ctx context.Context, conn *sql.Conn) error {
		var lockedAccount uint32
		err := conn.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_id=? for update", account).Scan(&lockedAccount)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSQLTaskNotFound
		}
		if err != nil {
			return err
		}
		var definition SQLTask
		err = conn.QueryRowContext(ctx, "select task_id, trigger_count, schedule_generation from sql_task where account_id=? and task_name=? for update", account, storedName).
			Scan(&definition.TaskID, &definition.TriggerCount, &definition.ScheduleGeneration)
		definitionExists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// The pointer belongs to at most one async task. Lock it before the run,
		// matching the order used by completion and retry.
		rows, err := conn.QueryContext(ctx, "select task_id, task_status from sys_async_task where task_parent_id=? and sql_task_run_id=? limit 2 for update", fmt.Sprintf("sql-task:%d", taskID), runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		var asyncID uint64
		var asyncStatus sql.NullInt64
		for rows.Next() {
			if asyncID != 0 {
				return errSQLTaskReceiptConflict
			}
			if err := rows.Scan(&asyncID, &asyncStatus); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		run, exists, err := lockSQLTaskRunReceipt(ctx, conn, runID)
		if err != nil {
			return err
		}
		if !exists || run.AccountID != account || run.TaskID != taskID || run.TaskName != storedName {
			return ErrSQLTaskNotFound
		}
		if run.Status != SQLTaskStatusRunning {
			return nil
		}
		now := time.Now().UTC()
		if definitionExists {
			sequence, err := nextSQLTaskInvalidationSequence(definition)
			if err != nil {
				return err
			}
			if _, err = conn.ExecContext(ctx, "update sql_task set enabled=0, trigger_count=?, schedule_generation=?, updated_at=? where task_id=? and account_id=?", sequence, sequence, now, definition.TaskID, account); err != nil {
				return err
			}
		}
		reason := forceSQLTaskReason(run.ErrorMessage, actor, now)
		result, err := conn.ExecContext(ctx, "update sql_task_run set status=?, finished_at=?, error_message=? where run_id=? and status=?", SQLTaskStatusUnknown, now, reason, runID, SQLTaskStatusRunning)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return errSQLTaskReceiptConflict
		}
		if asyncID != 0 && asyncStatus.Int64 == int64(task.TaskStatus_Running) {
			result, err = conn.ExecContext(ctx, completeSQLTaskAsync, task.TaskStatus_Completed, task.ResultCode_Failed,
				util.Abbreviate(reason, 997), now.UnixMilli(), asyncID, task.TaskStatus_Running, runID)
			if err != nil {
				return err
			}
			count, err = result.RowsAffected()
			if err != nil {
				return err
			}
			if count != 1 {
				return errSQLTaskReceiptConflict
			}
		}
		changed = true
		return nil
	})
	if err != nil && commitAttempted {
		return false, errutil.Wrap(err, "SQL task recovery may have committed; check the run or repeat the same recovery command")
	}
	return changed && err == nil, err
}
