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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

func AddISCPTaskIfNotExists(
	ctx context.Context,
	ts taskservice.TaskService,
) error {
	tasks, err := ts.QueryDaemonTask(
		ctx,
		taskservice.WithTaskExecutorCond(
			taskservice.EQ,
			task.TaskCode_ISCPExecutor,
		),
	)
	if err != nil {
		return err
	}
	if len(tasks) > 0 {
		return nil
	}

	taskID := uuid.Must(uuid.NewV7())
	metadata := task.TaskMetadata{
		ID:       taskID.String(),
		Executor: task.TaskCode_ISCPExecutor,
		Options: task.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
	details := &task.Details{
		AccountID: sysAccountID,
		Account:   sysAccountName,
		Details: &task.Details_ISCP{
			ISCP: &task.ISCPDetails{
				TaskName: "iscp",
				TaskId:   taskID.String(),
			},
		},
	}
	return ts.CreateDaemonTask(ctx, metadata, details)
}
