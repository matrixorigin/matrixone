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
	"fmt"
	"strings"
	"time"

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"golang.org/x/exp/constraints"
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
	// IN record in condition
	IN = Op(6)
	// LIKE record LIKE condition
	LIKE = Op(7)

	OpName = map[Op]string{
		EQ:   "=",
		GT:   ">",
		GE:   ">=",
		LT:   "<",
		LE:   "<=",
		IN:   "IN",
		LIKE: "LIKE",
	}
)

type condition interface {
	eval(v any) bool
	sql() string
}

type limitCond struct {
	limit int
}

func (c *limitCond) eval(v any) bool {
	limit, ok := v.(int)
	if !ok {
		return false
	}
	return c.limit > 0 && limit >= c.limit
}

func (c *limitCond) sql() string {
	return fmt.Sprintf(" limit %d", c.limit)
}

type taskIDCond struct {
	op     Op
	taskID uint64
}

func (c *taskIDCond) eval(v any) bool {
	taskID, ok := v.(uint64)
	if !ok {
		return false
	}
	return compare(c.op, taskID, c.taskID)
}

func (c *taskIDCond) sql() string {
	return fmt.Sprintf("task_id%s%d", OpName[c.op], c.taskID)
}

type taskRunnerCond struct {
	op         Op
	taskRunner string
}

func (c *taskRunnerCond) eval(v any) bool {
	runner, ok := v.(string)
	if !ok {
		return false
	}
	return compare(c.op, runner, c.taskRunner)
}

func (c *taskRunnerCond) sql() string {
	return fmt.Sprintf("task_runner%s'%s'", OpName[c.op], c.taskRunner)
}

type taskStatusCond struct {
	op         Op
	taskStatus []task.TaskStatus
}

func (c *taskStatusCond) eval(v any) bool {
	status, ok := v.(task.TaskStatus)
	if !ok {
		return false
	}
	for _, t := range c.taskStatus {
		if compare(EQ, status, t) {
			return true
		}
	}
	return false
}

func (c *taskStatusCond) sql() string {
	expr := strings.Trim(strings.Join(strings.Fields(fmt.Sprintf("%d", c.taskStatus)), ","), "[]")
	// task_status in (1,2,3)
	return fmt.Sprintf("task_status %s (%s)", OpName[c.op], expr)
}

type taskEpochCond struct {
	op        Op
	taskEpoch uint32
}

func (c *taskEpochCond) eval(v any) bool {
	epoch, ok := v.(uint32)
	if !ok {
		return false
	}
	return compare(c.op, epoch, c.taskEpoch)
}

func (c *taskEpochCond) sql() string {
	return fmt.Sprintf("task_epoch%s%d", OpName[c.op], c.taskEpoch)
}

type taskParentTaskIDCond struct {
	op               Op
	taskParentTaskID string
}

func (c *taskParentTaskIDCond) eval(v any) bool {
	taskID, ok := v.(string)
	if !ok {
		return false
	}
	return compare(c.op, taskID, c.taskParentTaskID)
}

func (c *taskParentTaskIDCond) sql() string {
	return fmt.Sprintf("task_parent_id%s'%s'", OpName[c.op], c.taskParentTaskID)
}

type taskExecutorCond struct {
	op           Op
	taskExecutor task.TaskCode
}

func (c *taskExecutorCond) eval(v any) bool {
	executor, ok := v.(task.TaskCode)
	if !ok {
		return false
	}
	return compare(c.op, executor, c.taskExecutor)
}

func (c *taskExecutorCond) sql() string {
	return fmt.Sprintf("task_metadata_executor%s%d", OpName[c.op], c.taskExecutor)
}

type taskTypeCond struct {
	op       Op
	taskType string
}

func (c *taskTypeCond) eval(v any) bool {
	type_, ok := v.(string)
	if !ok {
		return false
	}
	return compare(c.op, type_, c.taskType)
}

func (c *taskTypeCond) sql() string {
	return fmt.Sprintf("task_type%s'%s'", OpName[c.op], c.taskType)
}

type orderByDescCond struct{}

func (c *orderByDescCond) eval(_ any) bool {
	return true
}

func (c *orderByDescCond) sql() string {
	return " order by task_id desc"
}

type accountIDCond struct {
	op        Op
	accountID uint32
}

func (c *accountIDCond) eval(v any) bool {
	aid, ok := v.(uint32)
	if !ok {
		return false
	}
	return compare(c.op, aid, c.accountID)
}

func (c *accountIDCond) sql() string {
	return fmt.Sprintf("account_id%s%d", OpName[c.op], c.accountID)
}

type accountCond struct {
	op      Op
	account string
}

func (c *accountCond) eval(v any) bool {
	acc, ok := v.(string)
	if !ok {
		return false
	}
	return compare(c.op, acc, c.account)
}

func (c *accountCond) sql() string {
	return fmt.Sprintf("account%s%s", OpName[c.op], c.account)
}

type lastHeartbeatCond struct {
	op Op
	hb int64
}

func (c *lastHeartbeatCond) eval(v any) bool {
	hb, ok := v.(int64)
	if !ok {
		return false
	}
	return compare(c.op, hb, c.hb)
}

func (c *lastHeartbeatCond) sql() string {
	if c.op == LT || c.op == LE {
		return fmt.Sprintf("(last_heartbeat%s'%s' or last_heartbeat is NULL)",
			OpName[c.op],
			time.Unix(c.hb/1e9, c.hb%1e9).Format("2006-01-02 15:04:05"))
	} else {
		return fmt.Sprintf("last_heartbeat%s'%s'", OpName[c.op],
			time.Unix(c.hb/1e9, c.hb%1e9).Format("2006-01-02 15:04:05"))
	}
}

type cronTaskIDCond struct {
	op         Op
	cronTaskID uint64
}

func (c *cronTaskIDCond) eval(v any) bool {
	taskID, ok := v.(uint64)
	if !ok {
		return false
	}
	return compare(c.op, taskID, c.cronTaskID)
}

func (c *cronTaskIDCond) sql() string {
	return fmt.Sprintf("cron_task_id%s%d", OpName[c.op], c.cronTaskID)
}

type taskMetadataIDCond struct {
	op             Op
	taskMetadataID string
}

func (c *taskMetadataIDCond) eval(v any) bool {
	return false
}

func (c *taskMetadataIDCond) sql() string {
	return fmt.Sprintf("task_metadata_id%s'%s'", OpName[c.op], c.taskMetadataID)
}

func compare[T constraints.Ordered](op Op, a T, b T) bool {
	switch op {
	case EQ:
		return a == b
	case GT:
		return a > b
	case GE:
		return a >= b
	case LT:
		return a < b
	case LE:
		return a <= b
	default:
		return false
	}
}

type condCode uint32

const (
	CondLimit condCode = iota
	CondTaskID
	CondTaskRunner
	CondTaskStatus
	CondTaskEpoch
	CondTaskParentTaskID
	CondTaskExecutor
	CondTaskType
	CondOrderByDesc
	CondAccountID
	CondAccount
	CondLastHeartbeat
	CondCronTaskId
	CondTaskMetadataId
)

var (
	whereConditionCodes = map[condCode]struct{}{
		CondTaskID:           {},
		CondTaskRunner:       {},
		CondTaskStatus:       {},
		CondTaskEpoch:        {},
		CondTaskParentTaskID: {},
		CondTaskExecutor:     {},
		CondCronTaskId:       {},
		CondTaskMetadataId:   {},
	}
	daemonWhereConditionCodes = map[condCode]struct{}{
		CondTaskID:        {},
		CondTaskRunner:    {},
		CondTaskStatus:    {},
		CondTaskType:      {},
		CondAccountID:     {},
		CondAccount:       {},
		CondLastHeartbeat: {},
	}
)

type conditions map[condCode]condition

func newConditions(conds ...Condition) *conditions {
	c := &conditions{}
	for _, cond := range conds {
		cond(c)
	}
	return c
}

// WithTaskIDDesc set query with order by task id desc
func WithTaskIDDesc() Condition {
	return func(c *conditions) {
		(*c)[CondOrderByDesc] = &orderByDescCond{}
	}
}

// WithTaskExecutorCond set task executor condition
func WithTaskExecutorCond(op Op, value task.TaskCode) Condition {
	return func(c *conditions) {
		(*c)[CondTaskExecutor] = &taskExecutorCond{op: op, taskExecutor: value}
	}
}

// WithLimitCond set query result limit
func WithLimitCond(limit int) Condition {
	return func(c *conditions) {
		(*c)[CondLimit] = &limitCond{limit: limit}
	}
}

// WithTaskIDCond set task id condition
func WithTaskIDCond(op Op, value uint64) Condition {
	return func(c *conditions) {
		(*c)[CondTaskID] = &taskIDCond{op: op, taskID: value}
	}
}

// WithTaskRunnerCond set task runner condition
func WithTaskRunnerCond(op Op, value string) Condition {
	return func(c *conditions) {
		(*c)[CondTaskRunner] = &taskRunnerCond{op: op, taskRunner: value}
	}
}

// WithTaskStatusCond set status condition
func WithTaskStatusCond(value ...task.TaskStatus) Condition {
	op := IN
	return func(c *conditions) {
		(*c)[CondTaskStatus] = &taskStatusCond{op: op, taskStatus: value}
	}
}

// WithTaskEpochCond set task epoch condition
func WithTaskEpochCond(op Op, value uint32) Condition {
	return func(c *conditions) {
		(*c)[CondTaskEpoch] = &taskEpochCond{op: op, taskEpoch: value}
	}
}

// WithTaskParentTaskIDCond set task ParentTaskID condition
func WithTaskParentTaskIDCond(op Op, value string) Condition {
	return func(c *conditions) {
		(*c)[CondTaskParentTaskID] = &taskParentTaskIDCond{op: op, taskParentTaskID: value}
	}
}

// WithTaskType set task type condition.
func WithTaskType(op Op, value string) Condition {
	return func(c *conditions) {
		(*c)[CondTaskType] = &taskTypeCond{op: op, taskType: value}
	}
}

// WithAccountID set task account ID condition.
func WithAccountID(op Op, value uint32) Condition {
	return func(c *conditions) {
		(*c)[CondAccountID] = &accountIDCond{op: op, accountID: value}
	}
}

// WithAccount set task account condition.
func WithAccount(op Op, value string) Condition {
	return func(c *conditions) {
		(*c)[CondAccount] = &accountCond{op: op, account: value}
	}
}

// WithLastHeartbeat set last heartbeat condition.
func WithLastHeartbeat(op Op, value int64) Condition {
	return func(c *conditions) {
		(*c)[CondLastHeartbeat] = &lastHeartbeatCond{op: op, hb: value}
	}
}

func WithCronTaskId(op Op, value uint64) Condition {
	return func(c *conditions) {
		(*c)[CondCronTaskId] = &cronTaskIDCond{op: op, cronTaskID: value}
	}
}

func WithTaskMetadataId(op Op, value string) Condition {
	return func(c *conditions) {
		(*c)[CondTaskMetadataId] = &taskMetadataIDCond{op: op, taskMetadataID: value}
	}
}

// TaskService Asynchronous Task Service, which provides scheduling execution and management of
// asynchronous tasks. CN, DN, HAKeeper, LogService will all hold this service.
type TaskService interface {
	// Close close the task service
	Close() error

	// CreateAsyncTask Creates an asynchronous task that executes a single time, this method is idempotent, the
	// same task is not created repeatedly based on multiple calls.
	CreateAsyncTask(context.Context, task.TaskMetadata) error
	// CreateBatch is similar to Create, but with a batch task list
	CreateBatch(context.Context, []task.TaskMetadata) error
	// CreateCronTask is similar to Create, but create a task that runs periodically, with the period
	// described using a Cron expression.
	CreateCronTask(ctx context.Context, task task.TaskMetadata, cronExpr string) error
	// Allocate allocate task runner fot spec task.
	Allocate(ctx context.Context, value task.AsyncTask, taskRunner string) error
	// Complete task completed. The result used to indicate whether the execution succeeded or failed
	Complete(ctx context.Context, taskRunner string, task task.AsyncTask, result task.ExecuteResult) error
	// Heartbeat sending a heartbeat tells the scheduler that the specified task is running normally.
	// If the scheduler does not receive the heartbeat for a long time, it will reassign the task executor
	// to execute the task. Returning `ErrInvalidTask` means that the Task has been reassigned or has
	// ended, and the Task execution needs to be terminated immediately。
	Heartbeat(ctx context.Context, task task.AsyncTask) error
	// QueryAsyncTask query tasks by conditions
	QueryAsyncTask(context.Context, ...Condition) ([]task.AsyncTask, error)
	// QueryCronTask query cron tasks by conditions
	QueryCronTask(context.Context, ...Condition) ([]task.CronTask, error)

	// CreateDaemonTask creates a daemon task that will run in background for long time.
	CreateDaemonTask(ctx context.Context, value task.TaskMetadata, details *task.Details) error
	// QueryDaemonTask returns all daemon tasks which match the conditions.
	QueryDaemonTask(ctx context.Context, conds ...Condition) ([]task.DaemonTask, error)
	// UpdateDaemonTask updates the daemon task record.
	UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, cond ...Condition) (int, error)
	// HeartbeatDaemonTask sends heartbeat to daemon task.
	HeartbeatDaemonTask(ctx context.Context, task task.DaemonTask) error

	// StartScheduleCronTask start schedule cron tasks. A timer will be started to pull the latest CronTask
	// from the TaskStore at regular intervals, and a timer will be maintained in memory for all Cron's to be
	// triggered at regular intervals.
	StartScheduleCronTask()
	// StopScheduleCronTask stop schedule cron tasks.
	StopScheduleCronTask()

	// GetStorage returns the task storage
	GetStorage() TaskStorage
}

// TaskExecutor which is responsible for the execution logic of a specific Task, and the function exists to
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
	RegisterExecutor(code task.TaskCode, executor TaskExecutor)
	// GetExecutor returns the task executor
	GetExecutor(code task.TaskCode) TaskExecutor
	// Attach attaches the active go-routine to the daemon task.
	Attach(ctx context.Context, taskID uint64, routine ActiveRoutine) error
}

// TaskStorage task storage
type TaskStorage interface {
	// Close close the task storage
	Close() error

	// AddAsyncTask adds async tasks and returns number of successful added
	AddAsyncTask(context.Context, ...task.AsyncTask) (int, error)
	// UpdateAsyncTask updates async tasks and returns number of successful updated
	UpdateAsyncTask(context.Context, []task.AsyncTask, ...Condition) (int, error)
	// DeleteAsyncTask deletes tasks and returns number of successful deleted
	DeleteAsyncTask(context.Context, ...Condition) (int, error)
	// QueryAsyncTask queries tasks by conditions
	QueryAsyncTask(context.Context, ...Condition) ([]task.AsyncTask, error)

	// AddCronTask add cron task and returns number of successful added
	AddCronTask(context.Context, ...task.CronTask) (int, error)
	// QueryCronTask query all cron tasks
	QueryCronTask(context.Context, ...Condition) ([]task.CronTask, error)
	// UpdateCronTask crontask generates tasks periodically, and this update
	// needs to be in a transaction. Update cron task and insert a new task.
	// This update must be transactional and needs to be done conditionally
	// using CronTask.TriggerTimes and the task.Metadata.ID field.
	UpdateCronTask(context.Context, task.CronTask, task.AsyncTask) (int, error)

	// AddDaemonTask adds daemon tasks and returns number of successful added.
	AddDaemonTask(ctx context.Context, tasks ...task.DaemonTask) (int, error)
	// UpdateDaemonTask updates daemon tasks and returns number of successful updated.
	UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error)
	// DeleteDaemonTask deletes daemon tasks and returns number of successful deleted.
	DeleteDaemonTask(ctx context.Context, condition ...Condition) (int, error)
	// QueryDaemonTask queries daemon tasks by conditions.
	QueryDaemonTask(ctx context.Context, condition ...Condition) ([]task.DaemonTask, error)
	// HeartbeatDaemonTask update the last heartbeat field of the task.
	HeartbeatDaemonTask(ctx context.Context, task []task.DaemonTask) (int, error)
}

// TaskServiceHolder create and hold the task service in the cn, tn and log node. Create
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

type Getter func() (TaskService, bool)
