// Copyright 2021 - 2023 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

func handlePauseDaemonTask(ctx context.Context, ses *Session, st *tree.PauseDaemonTask) error {
	ts := getPu(ses.GetService()).GetTaskService()
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskIDCond(taskservice.EQ, st.TaskID),
		taskservice.WithAccountID(taskservice.EQ, accountID),
	)
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewErrTaskNotFound(ctx, st.TaskID)
	}
	// Already in paused status.
	if daemonControlSatisfied(tasks[0].TaskStatus, task.TaskStatus_PauseRequested) {
		return nil
	} else if tasks[0].TaskStatus != task.TaskStatus_Running {
		return moerr.NewInternalErrorf(ctx,
			"task can not be paused only if it is in %s statue, now it is %s",
			task.TaskStatus_Running,
			tasks[0].TaskStatus)
	}
	return requestDaemonTaskStatus(ctx, ts, tasks[0], task.TaskStatus_PauseRequested)
}

func handleCancelDaemonTask(ctx context.Context, ses *Session, taskID uint64) error {
	ts := getPu(ses.GetService()).GetTaskService()
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskIDCond(taskservice.EQ, taskID),
		taskservice.WithAccountID(taskservice.EQ, accountID),
	)
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewErrTaskNotFound(ctx, taskID)
	}
	// Already in canceled status.
	if daemonControlSatisfied(tasks[0].TaskStatus, task.TaskStatus_CancelRequested) {
		return nil
	} else if tasks[0].TaskStatus == task.TaskStatus_Error {
		return moerr.NewInternalErrorf(ctx,
			"task can not be canceled because it is in %s state", task.TaskStatus_Error)
	}
	return requestDaemonTaskStatus(ctx, ts, tasks[0], task.TaskStatus_CancelRequested)
}

func handleResumeDaemonTask(ctx context.Context, ses *Session, st *tree.ResumeDaemonTask) error {
	ts := getPu(ses.GetService()).GetTaskService()
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskIDCond(taskservice.EQ, st.TaskID),
		taskservice.WithAccountID(taskservice.EQ, accountID),
	)
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewErrTaskNotFound(ctx, st.TaskID)
	}
	// Already in canceled status.
	if daemonControlSatisfied(tasks[0].TaskStatus, task.TaskStatus_ResumeRequested) {
		return nil
	} else if tasks[0].TaskStatus != task.TaskStatus_Paused {
		return moerr.NewInternalErrorf(ctx,
			"task can be resumed only if it is in %s state, now it is %s",
			task.TaskStatus_Paused,
			tasks[0].TaskStatus)
	}
	return requestDaemonTaskStatus(ctx, ts, tasks[0], task.TaskStatus_ResumeRequested)
}

func requestDaemonTaskStatus(ctx context.Context, ts taskservice.TaskService, observed task.DaemonTask, status task.TaskStatus) error {
	c, err := ts.RequestDaemonTaskStatus(ctx, observed, status)
	if err != nil {
		return err
	}
	if c == 1 {
		return nil
	}
	if c != 0 {
		return moerr.NewInternalErrorf(ctx, "daemon task %d status request matched %d rows", observed.ID, c)
	}
	current, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskIDCond(taskservice.EQ, observed.ID),
		taskservice.WithAccountID(taskservice.EQ, observed.AccountID),
	)
	if err != nil {
		return err
	}
	if len(current) != 1 {
		return moerr.NewErrTaskNotFound(ctx, observed.ID)
	}
	if current[0].Metadata.ID == observed.Metadata.ID && daemonControlSatisfied(current[0].TaskStatus, status) {
		return nil
	}
	return moerr.NewTxnWriteConflictf(ctx, "daemon task %d changed during control request", observed.ID)
}

func daemonControlSatisfied(current, requested task.TaskStatus) bool {
	switch requested {
	case task.TaskStatus_PauseRequested:
		return current == requested || current == task.TaskStatus_Paused
	case task.TaskStatus_CancelRequested:
		return current == requested || current == task.TaskStatus_Canceled
	case task.TaskStatus_ResumeRequested:
		return current == requested || current == task.TaskStatus_Running
	default:
		return false
	}
}
