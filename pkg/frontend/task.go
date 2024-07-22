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
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

func handlePauseDaemonTask(ctx context.Context, ses *Session, st *tree.PauseDaemonTask) error {
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskIDCond(taskservice.EQ, st.TaskID),
		taskservice.WithAccountID(taskservice.EQ, ses.accountId),
	)
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewErrTaskNotFound(ctx, st.TaskID)
	}
	// Already in paused status.
	if tasks[0].TaskStatus == task.TaskStatus_Paused || tasks[0].TaskStatus == task.TaskStatus_PauseRequested {
		return nil
	} else if tasks[0].TaskStatus != task.TaskStatus_Running {
		return moerr.NewInternalError(ctx,
			"task can not be paused only if it is in %s statue, now it is %s",
			task.TaskStatus_Running,
			tasks[0].TaskStatus)
	}
	tasks[0].TaskStatus = task.TaskStatus_PauseRequested
	c, err := ts.UpdateDaemonTask(ctx, tasks)
	if err != nil {
		return err
	}
	if c != 1 {
		return moerr.NewErrTaskNotFound(ctx, st.TaskID)
	}
	return nil
}

func handleCancelDaemonTask(ctx context.Context, ses *Session, taskID uint64) error {
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskIDCond(taskservice.EQ, taskID),
		taskservice.WithAccountID(taskservice.EQ, ses.accountId),
	)
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewErrTaskNotFound(ctx, taskID)
	}
	// Already in canceled status.
	if tasks[0].TaskStatus == task.TaskStatus_Canceled || tasks[0].TaskStatus == task.TaskStatus_CancelRequested {
		return nil
	} else if tasks[0].TaskStatus == task.TaskStatus_Error {
		return moerr.NewInternalError(ctx,
			"task can not be canceled because it is in %s state", task.TaskStatus_Error)
	}
	tasks[0].TaskStatus = task.TaskStatus_CancelRequested
	c, err := ts.UpdateDaemonTask(ctx, tasks)
	if err != nil {
		return err
	}
	if c != 1 {
		return moerr.NewErrTaskNotFound(ctx, taskID)
	}
	return nil
}

func handleResumeDaemonTask(ctx context.Context, ses *Session, st *tree.ResumeDaemonTask) error {
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskIDCond(taskservice.EQ, st.TaskID),
		taskservice.WithAccountID(taskservice.EQ, ses.accountId),
	)
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewErrTaskNotFound(ctx, st.TaskID)
	}
	// Already in canceled status.
	if tasks[0].TaskStatus == task.TaskStatus_Running || tasks[0].TaskStatus == task.TaskStatus_ResumeRequested {
		return nil
	} else if tasks[0].TaskStatus != task.TaskStatus_Paused {
		return moerr.NewInternalError(ctx,
			"task can be resumed only if it is in %s state, now it is %s",
			task.TaskStatus_Paused,
			tasks[0].TaskStatus)
	}
	tasks[0].TaskStatus = task.TaskStatus_ResumeRequested
	c, err := ts.UpdateDaemonTask(ctx, tasks)
	if err != nil {
		return err
	}
	if c != 1 {
		return moerr.NewErrTaskNotFound(ctx, st.TaskID)
	}
	return nil
}
