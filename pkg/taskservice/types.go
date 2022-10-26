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

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

// Condition options for query tasks
type Condition func(*conditions)

// Op condition op
type Op int

var (
	// EQ record == condition
	EQ = Op(1)
	// GT record > condition
	GT = Op(2)
	// GE record >= condition
	GE = Op(3)
	// LT record < condition
	LT = Op(4)
	// LE record <= condition
	LE = Op(5)

	OpName = map[Op]string{
		EQ: "=",
		GT: ">",
		GE: ">=",
		LT: "<",
		LE: "<=",
	}
)

type conditions struct {
	limit int

	hasTaskIDCond bool
	taskIDOp      Op
	taskID        uint64

	hasTaskRunnerCond bool
	taskRunnerOp      Op
	taskRunner        string

	hasTaskStatusCond bool
	taskStatusOp      Op
	taskStatus        task.TaskStatus

	hasTaskEpochCond bool
	taskEpochOp      Op
	taskEpoch        uint32

	hasTaskParentIDCond bool
	taskParentTaskIDOp  Op
	taskParentTaskID    string

	hasTaskExecutorCond bool
	taskExecutorOp      Op
	taskExecutor        uint32

	orderByDesc bool
}

// WithTaskIDDesc set query with order by task id desc
func WithTaskIDDesc() Condition {
	return func(qo *conditions) {
		qo.orderByDesc = true
	}
}

// WithTaskExecutorCond set task executor condition
func WithTaskExecutorCond(op Op, value uint32) Condition {
	return func(qo *conditions) {
		qo.hasTaskExecutorCond = true
		qo.taskExecutorOp = op
		qo.taskExecutor = value
	}
}

// WithLimitCond set query result limit
func WithLimitCond(limit int) Condition {
	return func(qo *conditions) {
		qo.limit = limit
	}
}

// WithTaskIDCond set task id condition
func WithTaskIDCond(op Op, value uint64) Condition {
	return func(qo *conditions) {
		qo.hasTaskIDCond = true
		qo.taskIDOp = op
		qo.taskID = value
	}
}

// WithTaskRunnerCond set task runner condition
func WithTaskRunnerCond(op Op, value string) Condition {
	return func(qo *conditions) {
		qo.hasTaskRunnerCond = true
		qo.taskRunner = value
		qo.taskRunnerOp = op
	}
}

// WithTaskStatusCond set status condition
func WithTaskStatusCond(op Op, value task.TaskStatus) Condition {
	return func(qo *conditions) {
		qo.hasTaskStatusCond = true
		qo.taskStatus = value
		qo.taskStatusOp = op
	}
}

// WithTaskEpochCond set task epoch condition
func WithTaskEpochCond(op Op, value uint32) Condition {
	return func(qo *conditions) {
		qo.hasTaskEpochCond = true
		qo.taskEpoch = value
		qo.taskEpochOp = op
	}
}

// WithTaskParentTaskIDCond set task ParentTaskID condition
func WithTaskParentTaskIDCond(op Op, value string) Condition {
	return func(qo *conditions) {
		qo.hasTaskParentIDCond = true
		qo.taskParentTaskID = value
		qo.taskParentTaskIDOp = op
	}
}

// TaskService Asynchronous Task Service, which provides scheduling execution and management of
// asynchronous tasks. CN, DN, HAKeeper, LogService will all hold this service.
type TaskService interface {
	// Close close the task service
	Close() error

	// Create Creates an asynchronous task that executes a single time, this method is idempotent, the
	// same task is not created repeatedly based on multiple calls.
	Create(context.Context, task.TaskMetadata) error
	// CreateBatch is similar to Create, but with a batch task list
	CreateBatch(context.Context, []task.TaskMetadata) error
	// CreateCronTask is similar to Create, but create a task that runs periodically, with the period
	// described using a Cron expression.
	CreateCronTask(ctx context.Context, task task.TaskMetadata, cronExpr string) error
	// Allocate allocate task runner fot spec task.
	Allocate(ctx context.Context, value task.Task, taskRunner string) error
	// Complete task completed. The result used to indicate whether the execution succeeded or failed
	Complete(ctx context.Context, taskRunner string, task task.Task, result task.ExecuteResult) error
	// Heartbeat sending a heartbeat tells the scheduler that the specified task is running normally.
	// If the scheduler does not receive the heartbeat for a long time, it will reassign the task executor
	// to execute the task. Returning `ErrInvalidTask` means that the Task has been reassigned or has
	// ended, and the Task execution needs to be terminated immediately。
	Heartbeat(ctx context.Context, task task.Task) error
	// QueryTask query tasks by conditions
	QueryTask(context.Context, ...Condition) ([]task.Task, error)
	// QueryCronTask returns all cron task metadata
	QueryCronTask(context.Context) ([]task.CronTask, error)

	// StartScheduleCronTask start schedule cron tasks. A timer will be started to pull the latest CronTask
	// from the TaskStore at regular intervals, and a timer will be maintained in memory for all Cron's to be
	// triggered at regular intervals.
	StartScheduleCronTask()
	// StopScheduleCronTask stop schedule cron tasks.
	StopScheduleCronTask()

	// GetStorage returns the task storage
	GetStorage() TaskStorage
}

// TaskExecutor which is responsible for the execution logic of a specific Task, and the function exits to
// represent the completion of the task execution. In the process of task execution task may be interrupted
// at any time, so the implementation needs to frequently check the state of the Context, in the
// Context.Done(), as soon as possible to exit. Epoch is 1 means the task is executed for the first time,
// otherwise it means that the task is rescheduled, the task may be completed or not.
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
	// RegisterExecutor register the task executor
	RegisterExecutor(code uint32, executor TaskExecutor)
}

// TaskStorage task storage
type TaskStorage interface {
	// Close close the task storage
	Close() error

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
	// This update must be transactional and needs to be done conditionally
	// using CronTask.TriggerTimes and the task.Metadata.ID field.
	UpdateCronTask(context.Context, task.CronTask, task.Task) (int, error)
}

// TaskServiceHolder create and hold the task service in the cn, dn and log node. Create
// the TaskService from the heartbeat's CreateTaskService schedule command.
type TaskServiceHolder interface {
	// Close close the holder
	Close() error
	// Get returns the taskservice
	Get() (TaskService, bool)
	// Create create the taskservice
	Create(command logservicepb.CreateTaskService) error
}

type TaskStorageFactory interface {
	Create(address string) (TaskStorage, error)
}
