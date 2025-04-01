// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"database/sql"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

const (
	CdcRunning   = "running"
	CdcPaused    = "paused"
	CdcFailed    = "failed"
	maxErrMsgLen = 256
)

var queryTable = func(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	callback func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
	var rows *sql.Rows
	var err error
	rows, err = tx.QueryContext(ctx, query)
	if err != nil {
		return false, err
	}
	if rows.Err() != nil {
		return false, rows.Err()
	}
	defer func() {
		_ = rows.Close()
	}()

	var ret bool
	for rows.Next() {
		ret, err = callback(ctx, rows)
		if err != nil {
			return false, err
		}
		if ret {
			return true, nil
		}
	}
	return false, nil
}

var initAesKeyByInternalExecutor = func(
	ctx context.Context,
	exec *CDCTaskExecutor,
	accountId uint32,
) error {
	return exec.initAesKeyByInternalExecutor(ctx, accountId)
}

func updateCdc(ctx context.Context, ses *Session, st tree.Statement) (err error) {
	var targetTaskStatus task.TaskStatus
	var taskName string

	conds := make([]taskservice.Condition, 0)
	appendCond := func(cond ...taskservice.Condition) {
		conds = append(conds, cond...)
	}
	accountId := ses.GetTenantInfo().GetTenantID()

	switch stmt := st.(type) {
	case *tree.DropCDC:
		targetTaskStatus = task.TaskStatus_CancelRequested
		if stmt.Option == nil {
			return moerr.NewInternalErrorf(ctx, "invalid cdc option")
		}
		if stmt.Option.All {
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			taskName = stmt.Option.TaskName.String()
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.PauseCDC:
		targetTaskStatus = task.TaskStatus_PauseRequested
		if stmt.Option == nil {
			return moerr.NewInternalErrorf(ctx, "invalid cdc option")
		}
		if stmt.Option.All {
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			taskName = stmt.Option.TaskName.String()
			appendCond(taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.RestartCDC:
		targetTaskStatus = task.TaskStatus_RestartRequested
		taskName = stmt.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalErrorf(ctx, "invalid task name")
		}
		appendCond(
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	case *tree.ResumeCDC:
		targetTaskStatus = task.TaskStatus_ResumeRequested
		taskName = stmt.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalErrorf(ctx, "invalid task name")
		}
		appendCond(
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	}

	return runUpdateCdcTask(ctx, targetTaskStatus, uint64(accountId), taskName, ses.GetService(), conds...)
}

func runUpdateCdcTask(
	ctx context.Context,
	targetTaskStatus task.TaskStatus,
	accountId uint64,
	taskName string,
	service string,
	conds ...taskservice.Condition) (err error) {
	ts := getPu(service).TaskService
	if ts == nil {
		return nil
	}
	taskCollector := func(
		ctx context.Context,
		targetStatus task.TaskStatus,
		tasks map[taskservice.CDCTaskKey]struct{},
		tx taskservice.SqlExecutor,
	) (int, error) {
		return collectTasksToUpdate(
			ctx,
			targetStatus,
			tasks,
			tx,
			accountId,
			taskName,
		)
	}
	_, err = ts.UpdateCDCTask(ctx,
		targetTaskStatus,
		taskCollector,
		conds...,
	)
	return err
}

func collectTasksToUpdate(
	ctx context.Context,
	targetStatus task.TaskStatus,
	tasks map[taskservice.CDCTaskKey]struct{},
	tx taskservice.SqlExecutor,
	accountId uint64,
	taskName string,
) (int, error) {
	var taskId string
	var affectedCdcRow int
	var cnt int64

	query := cdc.CDCSQLBuilder.GetTaskIdSQL(accountId, taskName)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	defer func() {
		_ = rows.Close()
	}()

	empty := true
	for rows.Next() {
		empty = false
		if err = rows.Scan(&taskId); err != nil {
			return 0, err
		}
		key := taskservice.CDCTaskKey{AccountId: accountId, TaskId: taskId}
		tasks[key] = struct{}{}
	}

	if taskName != "" && empty {
		return 0, moerr.NewInternalErrorf(ctx, "no cdc task found, task name: %s", taskName)
	}

	var prepare *sql.Stmt
	//step2: update or cancel cdc task
	if targetStatus != task.TaskStatus_CancelRequested {
		//Update cdc task
		//updating mo_cdc_task table
		updateSql := cdc.CDCSQLBuilder.UpdateTaskStateSQL(accountId, taskName)
		prepare, err = tx.PrepareContext(ctx, updateSql)
		if err != nil {
			return 0, err
		}
		defer func() {
			_ = prepare.Close()
		}()

		if prepare != nil {
			//execute update cdc status in mo_cdc_task
			var targetCdcStatus string
			if targetStatus == task.TaskStatus_PauseRequested {
				targetCdcStatus = CdcPaused
			} else {
				targetCdcStatus = CdcRunning
			}
			res, err := prepare.ExecContext(ctx, targetCdcStatus)
			if err != nil {
				return 0, err
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return 0, err
			}
			affectedCdcRow += int(affected)
		}

		if targetStatus == task.TaskStatus_RestartRequested {
			//delete mo_cdc_watermark
			cnt, err = deleteWatermark(ctx, tx, tasks)
			if err != nil {
				return 0, err
			}
			affectedCdcRow += int(cnt)
		}
	} else {
		//Cancel cdc task
		//deleting mo_cdc_task
		deleteSql := cdc.CDCSQLBuilder.DeleteTaskSQL(accountId, taskName)
		cnt, err = ExecuteAndGetRowsAffected(ctx, tx, deleteSql)
		if err != nil {
			return 0, err
		}
		affectedCdcRow += int(cnt)

		//delete mo_cdc_watermark
		cnt, err = deleteWatermark(ctx, tx, tasks)
		if err != nil {
			return 0, err
		}
		affectedCdcRow += int(cnt)
	}
	return affectedCdcRow, nil
}

func deleteWatermark(ctx context.Context, tx taskservice.SqlExecutor, taskKeyMap map[taskservice.CDCTaskKey]struct{}) (int64, error) {
	tCount := int64(0)
	cnt := int64(0)
	var err error
	//deleting mo_cdc_watermark belongs to cancelled cdc task
	for tInfo := range taskKeyMap {
		deleteSql2 := cdc.CDCSQLBuilder.DeleteWatermarkSQL(tInfo.AccountId, tInfo.TaskId)
		cnt, err = ExecuteAndGetRowsAffected(ctx, tx, deleteSql2)
		if err != nil {
			return 0, err
		}
		tCount += cnt
	}
	return tCount, nil
}
