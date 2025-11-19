// Copyright 2023 Matrix Origin
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
	"fmt"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

type CDCCreateTaskRequest = tree.CreateCDC
type CDCShowTaskRequest = tree.ShowCDC

// All handle functions
// 1. handleCreateCdc: create a cdc task
// 2. handleDropCdc: drop a cdc task
// 3. handlePauseCdc: pause a cdc task
// 4. handleResumeCdc: resume a cdc task
// 5. handleRestartCdc: restart a cdc task
// 6. handleShowCDCTaskRequest: show a cdc task
// 7. handleShowCdc: show a cdc task

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return handleCreateCDCTaskRequest(execCtx.reqCtx, ses, create)
}

func handleDropCdc(ses *Session, execCtx *ExecCtx, st *tree.DropCDC) error {
	return handleUpdateCDCTaskRequest(execCtx.reqCtx, ses, st)
}

func handlePauseCdc(ses *Session, execCtx *ExecCtx, st *tree.PauseCDC) error {
	return handleUpdateCDCTaskRequest(execCtx.reqCtx, ses, st)
}

func handleResumeCdc(ses *Session, execCtx *ExecCtx, st *tree.ResumeCDC) error {
	return handleUpdateCDCTaskRequest(execCtx.reqCtx, ses, st)
}

func handleRestartCdc(ses *Session, execCtx *ExecCtx, st *tree.RestartCDC) error {
	return handleUpdateCDCTaskRequest(execCtx.reqCtx, ses, st)
}

func handleShowCdc(
	ses *Session,
	execCtx *ExecCtx,
	st *tree.ShowCDC,
) (err error) {
	dao := NewCDCDao(ses)
	return dao.ShowTasks(execCtx.reqCtx, st)
}

func handleCreateCDCTaskRequest(
	ctx context.Context,
	ses *Session,
	req *CDCCreateTaskRequest,
) (err error) {
	dao := NewCDCDao(ses)

	err = dao.CreateTask(ctx, req)
	return
}

func handleUpdateCDCTaskRequest(
	ctx context.Context,
	ses *Session,
	req tree.Statement,
) (err error) {
	var (
		targetTaskStatus task.TaskStatus
		// task name may be empty
		taskName  string
		accountId = ses.GetTenantInfo().GetTenantID()
		conds     = make([]taskservice.Condition, 0)
	)

	var (
		operation string
	)

	switch updateReq := req.(type) {
	case *tree.DropCDC:
		if updateReq.Option == nil {
			return moerr.NewInternalError(ctx, "invalid drop cdc option")
		}
		targetTaskStatus = task.TaskStatus_CancelRequested
		operation = "drop"
		conds = append(
			conds,
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
		if !updateReq.Option.All {
			taskName = updateReq.Option.TaskName.String()
			conds = append(
				conds,
				taskservice.WithTaskName(taskservice.EQ, taskName),
			)
		}
	case *tree.PauseCDC:
		if updateReq.Option == nil {
			return moerr.NewInternalError(ctx, "invalid pause cdc option")
		}
		targetTaskStatus = task.TaskStatus_PauseRequested
		operation = "pause"
		conds = append(
			conds,
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
		if !updateReq.Option.All {
			taskName = updateReq.Option.TaskName.String()
			conds = append(
				conds,
				taskservice.WithTaskName(taskservice.EQ, taskName),
			)
		}
	case *tree.ResumeCDC:
		targetTaskStatus = task.TaskStatus_ResumeRequested
		operation = "resume"
		taskName = updateReq.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalError(ctx, "invalid resume cdc task name")
		}
		conds = append(
			conds,
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			taskservice.WithTaskName(taskservice.EQ, taskName),
		)
	case *tree.RestartCDC:
		targetTaskStatus = task.TaskStatus_RestartRequested
		operation = "restart"
		taskName = updateReq.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalError(ctx, "invalid restart cdc task name")
		}
		conds = append(
			conds,
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			taskservice.WithTaskName(taskservice.EQ, taskName),
		)
	default:
		return moerr.NewInternalErrorf(
			ctx,
			"invalid cdc task request: %s",
			req.String(),
		)
	}

	logutil.Info(
		"cdc.task.request",
		zap.String("statement-type", fmt.Sprintf("%T", req)),
		zap.String("operation", operation),
		zap.String("target-status", targetTaskStatus.String()),
		zap.String("task-name", taskName),
		zap.Uint32("account-id", accountId),
	)

	return doUpdateCDCTask(
		ctx,
		targetTaskStatus,
		uint64(accountId),
		taskName,
		ses.GetService(),
		conds...,
	)
}

func doUpdateCDCTask(
	ctx context.Context,
	targetTaskStatus task.TaskStatus,
	accountId uint64,
	taskName string,
	service string,
	conds ...taskservice.Condition,
) (err error) {
	ts := getPu(service).TaskService
	if ts == nil {
		return nil
	}
	_, err = ts.UpdateCDCTask(ctx,
		targetTaskStatus,
		func(
			ctx context.Context,
			targetStatus task.TaskStatus,
			keys map[taskservice.CDCTaskKey]struct{},
			tx taskservice.SqlExecutor,
		) (int, error) {
			return onPreUpdateCDCTasks(
				ctx,
				targetStatus,
				keys,
				tx,
				accountId,
				taskName,
			)
		},
		conds...,
	)
	return
}

func onPreUpdateCDCTasks(
	ctx context.Context,
	targetTaskStatus task.TaskStatus,
	keys map[taskservice.CDCTaskKey]struct{},
	tx taskservice.SqlExecutor,
	accountId uint64,
	taskName string,
) (affectedCdcRow int, err error) {
	var (
		cnt int64
		dao = NewCDCDao(nil, WithSQLExecutor(tx))
	)
	if cnt, err = dao.GetTaskKeys(
		ctx,
		accountId,
		taskName,
		keys,
	); err != nil {
		return
	}
	affectedCdcRow = int(cnt)

	//Cancel cdc task
	if targetTaskStatus == task.TaskStatus_CancelRequested {
		var deleteRes DeleteCDCArtifactsResult
		if deleteRes, err = dao.DeleteTaskAndWatermark(ctx, accountId, taskName, keys); err != nil {
			return
		}
		affectedCdcRow += int(deleteRes.TaskRows + deleteRes.WatermarkRows)
		return
	}

	logutil.Debug(
		"cdc.handle.update_task",
		zap.Uint64("account-id", accountId),
		zap.String("task-name", taskName),
		zap.Int("key-count", len(keys)),
		zap.String("target-status", targetTaskStatus.String()),
	)

	//step2: update or cancel cdc task
	var targetCDCStatus string
	if targetTaskStatus == task.TaskStatus_PauseRequested {
		targetCDCStatus = cdc.CDCState_Paused
	} else {
		targetCDCStatus = cdc.CDCState_Running
	}

	if cnt, err = dao.PrepareUpdateTask(
		ctx,
		accountId,
		taskName,
		targetCDCStatus,
	); err != nil {
		return
	}

	affectedCdcRow += int(cnt)

	return
}
