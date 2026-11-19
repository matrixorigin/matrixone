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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

const sqlTaskStaleBatchSize = 128

func (m *mysqlTaskStorage) FinishSQLTaskUnclaimed(ctx context.Context, claim SQLTaskClaim, result task.ExecuteResult, endAt int64) error {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return err
	}
	discard := true
	defer func() { closeSQLTaskMetadataConn(conn, discard) }()
	updated, err := conn.ExecContext(ctx, completeSQLTaskUnclaimed,
		task.TaskStatus_Completed, result.Code, nullString(util.Abbreviate(result.Error, 997)), endAt,
		claim.ID, task.TaskStatus_Running, claim.Runner, claim.Epoch, task.TaskCode_SQLTask)
	if err != nil {
		return err
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return err
	}
	discard = false
	if count > 1 {
		return errSQLTaskReceiptConflict
	}
	return nil // zero means it was completed, removed, reassigned, or claimed
}

func (s *memTaskStorage) FinishSQLTaskUnclaimed(ctx context.Context, claim SQLTaskClaim, result task.ExecuteResult, endAt int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	async, exists := s.asyncTasks[claim.ID]
	if !exists || async.Metadata.Executor != task.TaskCode_SQLTask || async.Status != task.TaskStatus_Running || async.TaskRunner != claim.Runner ||
		async.Epoch != claim.Epoch || s.sqlTaskClaims[async.ID] != 0 {
		return nil
	}
	result.Error = util.Abbreviate(result.Error, 997)
	async.Status, async.ExecuteResult, async.CompletedAt = task.TaskStatus_Completed, &result, endAt
	s.asyncTasks[async.ID] = async
	return nil
}

func deleteStaleSQLTaskBatch(ctx context.Context, store TaskStorage, taskID uint64) (int, error) {
	return store.DeleteStaleSQLTaskBatch(ctx, taskID)
}

func (m *mysqlTaskStorage) deleteStaleSQLTaskBatch(ctx context.Context, taskID uint64) (int, error) {
	deleted := 0
	_, err := runSQLTaskMetadataTransaction(ctx, m.db, func(ctx context.Context, conn *sql.Conn) error {
		var generation uint64
		err := conn.QueryRowContext(ctx, "select schedule_generation from sql_task where task_id=? for update", taskID).Scan(&generation)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = validateSQLTaskTriggerSequence(generation); err != nil {
			return err
		}
		parent := fmt.Sprintf("sql-task:%d", taskID)
		prefix := fmt.Sprintf("sqltask2:%d:", taskID)
		result, err := conn.ExecContext(ctx,
			"delete from sys_async_task where task_parent_id=? and task_metadata_executor=? and sql_task_run_id=0 and task_status in (?,?) and (task_metadata_id like ? or (task_metadata_id>=? and task_metadata_id<? and length(task_metadata_id)=? and substring(task_metadata_id,?) regexp '^[0-9]{20}$')) limit 128",
			parent, task.TaskCode_SQLTask, task.TaskStatus_Created, task.TaskStatus_Running,
			parent+":%", prefix+"00000000000000000000", fmt.Sprintf("%s%020d", prefix, generation), len(prefix)+20, len(prefix)+1)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count < 0 || count > sqlTaskStaleBatchSize {
			return errSQLTaskReceiptConflict
		}
		deleted = int(count)
		return nil
	})
	return deleted, err
}

func (s *memTaskStorage) deleteStaleSQLTaskBatch(ctx context.Context, taskID uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.Lock()
	defer s.Unlock()
	definition, exists := s.sqlTasks[taskID]
	if !exists {
		return 0, nil
	}
	if err := validateSQLTaskTriggerSequence(definition.ScheduleGeneration); err != nil {
		return 0, err
	}
	parent := fmt.Sprintf("sql-task:%d", taskID)
	prefix := fmt.Sprintf("sqltask2:%d:", taskID)
	lower, upper := prefix+"00000000000000000000", fmt.Sprintf("%s%020d", prefix, definition.ScheduleGeneration)
	deleted := 0
	for id, async := range s.asyncTasks {
		metadata := async.Metadata.ID
		legacy := strings.HasPrefix(metadata, parent+":")
		oldSequence := len(metadata) == len(prefix)+20 && metadata >= lower && metadata < upper
		for i := len(prefix); oldSequence && i < len(metadata); i++ {
			oldSequence = metadata[i] >= '0' && metadata[i] <= '9'
		}
		if async.ParentTaskID != parent || async.Metadata.Executor != task.TaskCode_SQLTask || s.sqlTaskClaims[id] != 0 ||
			(async.Status != task.TaskStatus_Created && async.Status != task.TaskStatus_Running) || (!legacy && !oldSequence) {
			continue
		}
		delete(s.asyncTasks, id)
		delete(s.asyncTaskIndexes, metadata)
		delete(s.sqlTaskClaims, id)
		deleted++
		if deleted == sqlTaskStaleBatchSize {
			break
		}
	}
	return deleted, nil
}
