// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

var (
	// ErrInvalidTask task does not belong to the current task runner
	ErrInvalidTask = moerr.New(moerr.INVALID_STATE, "task does not belong to the current task runner")
)

// Condition options for query tasks
type Condition func(*conditions)

// Op condition op
type Op int

var (
	// EQ ==
	EQ = Op(1)
	// GT >
	GT = Op(2)
)

type conditions struct {
	limit int

	taskIDOp Op
	taskID   uint64

	taskRunnerOp Op
	taskRunner   string
}

// WithLimit set query result limit
func WithLimit(limit int) Condition {
	return func(qo *conditions) {
		qo.limit = limit
	}
}

// WithTaskID set task id condition
func WithTaskID(op Op, value uint64) Condition {
	return func(qo *conditions) {
		qo.taskID = value
		qo.taskIDOp = op
	}
}

// WithTaskRunner set task runner condition
func WithTaskRunner(op Op, value string) Condition {
	return func(qo *conditions) {
		qo.taskRunner = value
		qo.taskRunnerOp = op
	}
}

// TaskService Asynchronous Task Service, which provides scheduling execution and management of
// asynchronous tasks. CN, DN, HAKeeper, LogService will all hold this service.
type TaskService interface {
	// Create Creates an asynchronous task that executes a single time, this method is idempotent, the
	// same task is not created repeatedly based on multiple calls.
	Create(context.Context, task.TaskMetadata) error
	// CreateBatch is similar to Create, but with a batch task list
	CreateBatch(context.Context, []task.TaskMetadata) error
	// CreateCronTask is similar to Create, but create a task that runs periodically, with the period
	// described using a Cron expression.
	CreateCronTask(ctx context.Context, task task.TaskMetadata, cronExpr string) error
	// Complete task completed.
	Complete(ctx context.Context, taskRunner string, task task.Task) error
	// Heartbeat sending a heartbeat tells the scheduler that the specified task is running normally.
	// If the scheduler does not receive the heartbeat for a long time, it will reassign the task executor
	// to execute the task. Returning `ErrInvalidTask` means that the Task has been reassigned or has
	// ended, and the Task execution needs to be terminated immediately。
	Heartbeat(ctx context.Context, taskRunner string, tasks ...task.Task) error
	// QueryTask query tasks by conditions
	QueryTask(context.Context, ...Condition) ([]task.Task, error)
	// QueryCronTask returns all cron task metadata
	QueryCronTask(context.Context) ([]task.CronTask, error)
}

// TaskExecutor which is responsible for the execution logic of a specific Task, and the function exits to
// represent the completion of the task execution. In the process of task execution task may be interrupted
// at any time, so the implementation needs to frequently check the state of the Context, in the
// Context.Done(), as soon as possible to exit. Epoch is 1 means the task is executed for the first time,
// otherwise it means that the task is rescheduled, the task may completed or not.
type TaskExecutor func(ctx context.Context, task task.Task) error

// TaskRunner each runner can execute multiple task concurrently
type TaskRunner interface {
	// ID returns the TaskRunner ID
	ID() string
	// Start start the runner, after runner starts it will start to periodically load the tasks assigned to
	// the current executor, as well as periodically send heartbeats.
	Start() error
	// Stop stop the runner, all running tasks will be terminated
	Stop() error
	// Parallelism maximum number of concurrently executing Tasks
	Parallelism() int
	// RegisterExectuor register the task executor
	RegisterExectuor(code int, executor TaskExecutor)
}

// TaskStorage task storage
type TaskStorage interface {
	// Add add tasks and returns number of successful added
	Add(context.Context, ...task.Task) (int, error)
	// Update update tasks and returns number of successful updated
	Update(context.Context, []task.Task, ...Condition) (int, error)
	// Delete delete tasks and returns number of successful deleted
	Delete(context.Context, ...Condition) (int, error)
	// Query query tasks by conditions
	Query(context.Context, ...Condition) ([]task.Task, error)

	// AddCronTask add cron task and returns number of successful added
	AddCronTask(context.Context, ...task.CronTask) (int, error)
	// QueryCronTask query all cron tasks
	QueryCronTask(context.Context) ([]task.CronTask, error)

	// UpdateCronTask crontask generates tasks periodically, and this update
	// needs to be in a transaction. Update cron task and insert a new task.
	UpdateCronTask(context.Context, task.CronTask, task.Task) (int, error)
}
