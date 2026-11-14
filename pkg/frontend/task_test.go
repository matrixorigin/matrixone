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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

type taskServiceAfterQuery struct {
	taskservice.TaskService
	afterQuery func()
}

func (s *taskServiceAfterQuery) QueryDaemonTask(ctx context.Context, conds ...taskservice.Condition) ([]task.DaemonTask, error) {
	tasks, err := s.TaskService.QueryDaemonTask(ctx, conds...)
	if err == nil && s.afterQuery != nil {
		s.afterQuery()
	}
	return tasks, err
}

func TestDaemonTaskControlPreservesConcurrentLease(t *testing.T) {
	for _, tc := range []struct {
		name       string
		before     task.TaskStatus
		concurrent task.TaskStatus
		after      task.TaskStatus
		wantErr    bool
		call       func(context.Context, *Session) error
	}{
		{"pause", task.TaskStatus_Running, task.TaskStatus_Running, task.TaskStatus_PauseRequested, false, func(ctx context.Context, ses *Session) error {
			return handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: 1})
		}},
		{"cancel", task.TaskStatus_Running, task.TaskStatus_Running, task.TaskStatus_CancelRequested, false, func(ctx context.Context, ses *Session) error {
			return handleCancelDaemonTask(ctx, ses, 1)
		}},
		{"resume", task.TaskStatus_Paused, task.TaskStatus_Paused, task.TaskStatus_ResumeRequested, false, func(ctx context.Context, ses *Session) error {
			return handleResumeDaemonTask(ctx, ses, &tree.ResumeDaemonTask{TaskID: 1})
		}},
		{"pause-superseded", task.TaskStatus_Running, task.TaskStatus_CancelRequested, task.TaskStatus_CancelRequested, true, func(ctx context.Context, ses *Session) error {
			return handlePauseDaemonTask(ctx, ses, &tree.PauseDaemonTask{TaskID: 1})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := taskservice.NewMemTaskStorage()
			service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			original := task.DaemonTask{ID: 1, Metadata: task.TaskMetadata{ID: tc.name}, AccountID: 1,
				TaskStatus: tc.before, TaskRunner: "old-cn", LastHeartbeat: time.Unix(1, 0)}
			n, err := store.AddDaemonTask(ctx, original)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			newHeartbeat := time.Unix(2, 0)
			wrapped := &taskServiceAfterQuery{TaskService: service, afterQuery: func() {
				newer := original
				newer.TaskRunner = "new-cn"
				newer.LastHeartbeat = newHeartbeat
				newer.TaskStatus = tc.concurrent
				updated, updateErr := service.UpdateDaemonTask(ctx, []task.DaemonTask{newer})
				require.NoError(t, updateErr)
				require.Equal(t, 1, updated)
			}}
			serviceName := "task-control-" + tc.name
			InitServerLevelVars(serviceName)
			pu := &config.ParameterUnit{SV: &config.FrontendParameters{}}
			pu.SetTaskService(wrapped)
			setPu(serviceName, pu)
			ses := &Session{feSessionImpl: feSessionImpl{service: serviceName, accountId: 1}}
			err = tc.call(ctx, ses)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			got, err := service.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, 1))
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, tc.after, got[0].TaskStatus)
			require.Equal(t, "new-cn", got[0].TaskRunner)
			require.Equal(t, newHeartbeat, got[0].LastHeartbeat)
		})
	}
}

// TestHandlePauseDaemonTask_TaskServiceNil tests the error case in handlePauseDaemonTask function
// when the task service is nil
func TestHandlePauseDaemonTask_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service:   "test-service",
			accountId: 1,
		},
	}

	// Create a PauseDaemonTask statement
	pauseStmt := &tree.PauseDaemonTask{
		TaskID: 123,
	}

	// Test the handlePauseDaemonTask function
	err := handlePauseDaemonTask(context.Background(), session, pauseStmt)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}

// TestHandleCancelDaemonTask_TaskServiceNil tests the error case in handleCancelDaemonTask function
// when the task service is nil
func TestHandleCancelDaemonTask_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service:   "test-service",
			accountId: 1,
		},
	}

	// Test the handleCancelDaemonTask function
	err := handleCancelDaemonTask(context.Background(), session, 123)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}

// TestHandleResumeDaemonTask_TaskServiceNil tests the error case in handleResumeDaemonTask function
// when the task service is nil
func TestHandleResumeDaemonTask_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service:   "test-service",
			accountId: 1,
		},
	}

	// Create a ResumeDaemonTask statement
	resumeStmt := &tree.ResumeDaemonTask{
		TaskID: 123,
	}

	// Test the handleResumeDaemonTask function
	err := handleResumeDaemonTask(context.Background(), session, resumeStmt)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}
