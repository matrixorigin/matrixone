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
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var sqlTaskAllocationMissing = logutil.Event{
	Name: "task.sql.allocate.notfound", Message: "task not found during allocation, may have been completed or deleted",
}

var sqlTaskReceiptRetry = logutil.Event{
	Name: "task.sql.receipt.retry", Message: "SQL task metadata recovery will retry",
}

var sqlTaskRetryIncomplete = logutil.Event{
	Name: "task.sql.retry.incomplete", Message: "SQL task business retry did not execute",
}

var sqlTaskReceiptAbandoned = logutil.Event{
	Name: "task.sql.receipt.abandoned", Message: "SQL task metadata receipts abandoned during shutdown",
}

var sqlTaskUnclaimedRetry = logutil.Event{
	Name: "task.sql.unclaimed.retry", Message: "unclaimed SQL task completion deferred",
}

type taskService struct {
	store      TaskStorage
	cronParser cron.Parser
	rt         runtime.Runtime

	crons       crons
	sqlCrons    sqlTaskCrons
	sqlRecovery *sqlTaskRecovery
}

func (s *taskService) SQLTaskService() TaskService { return s }

// Interfaces can hold a nil pointer (or another nil-capable concrete value).
func isNilTaskValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// SQL task recovery and SQL admission must address the same storage. A
// decorator can forward the owner by embedding TaskService, but a decorator
// that substitutes a different store must construct its own owning service.
func sqlTaskServiceOwner(service TaskService) *taskService {
	if service == nil {
		return nil
	}
	if owner, ok := service.(*taskService); ok {
		if owner == nil || owner.store == nil {
			return nil
		}
		return owner
	}
	if isNilTaskValue(service) {
		return nil
	}
	owner, ok := service.SQLTaskService().(*taskService)
	if !ok || owner == nil {
		return nil
	}
	store := service.GetStorage()
	if store == nil || owner.store == nil || !reflect.ValueOf(store).Comparable() || !reflect.ValueOf(owner.store).Comparable() || store != owner.store {
		return nil
	}
	return owner
}

// NewTaskService create a task service based on a task storage.
func NewTaskService(
	rt runtime.Runtime,
	store TaskStorage) TaskService {
	if isNilTaskValue(store) {
		store = nil
	}
	s := &taskService{
		rt:    rt,
		store: store,
		cronParser: cron.NewParser(
			cron.Second |
				cron.Minute |
				cron.Hour |
				cron.Dom |
				cron.Month |
				cron.Dow |
				cron.Descriptor),
	}
	s.sqlRecovery = newSQLTaskRecovery(sqlTaskManualCapacity, sqlTaskScheduledCapacity(1), s.recoverSQLTaskReceipt)
	return s
}

func (s *taskService) recoverSQLTaskReceipt(ctx context.Context, receipt *sqlTaskReceipt) bool {
	resolved, err := s.store.PublishSQLTaskReceipt(ctx, receipt.SQLTaskReceiptRequest)
	if err != nil {
		v2.SQLTaskReceiptRetryCounter.Inc()
		s.rt.Logger().WarnEventLazy(sqlTaskReceiptRetry, func() []zap.Field {
			return []zap.Field{zap.Uint64("run-id", receipt.Run.RunID), zap.Uint64("async-id", receipt.Claim.ID),
				zap.String("error", util.Abbreviate(err.Error(), 997))}
		})
	}
	return err == nil && resolved
}

func (s *taskService) reportAbandonedSQLTaskReceipts(count int, sample []uint64) {
	if count == 0 {
		return
	}
	v2.SQLTaskReceiptAbandonedCounter.Add(float64(count))
	s.rt.Logger().WarnEvent(sqlTaskReceiptAbandoned, zap.Int("count", count), zap.Uint64s("run-ids", sample))
}

func (s *taskService) abandonSQLTaskReceipt(receipt *sqlTaskReceipt) {
	if s.sqlRecovery.release(receipt.credit) {
		s.reportAbandonedSQLTaskReceipts(1, []uint64{receipt.Run.RunID})
	}
}

func (s *taskService) finishUnclaimedSQLTask(caller context.Context, claim SQLTaskClaim, cause error, cleanupTaskID uint64) bool {
	// Only the runner participates in the CAS. Parent/metadata can be invalid
	// multibyte strings and are not retained by this path.
	if claim.ID == 0 || len(claim.Runner) > 512 {
		return false
	}
	guard := s.sqlRecovery.tryGuard(sqlTaskGuardKey{kind: 3, value: "unclaimed"}, 50)
	if guard == nil {
		return false
	}
	defer s.sqlRecovery.releaseGuard(guard)
	ctx, cancel := context.WithTimeout(s.sqlRecovery.ctx, 5*time.Second)
	defer cancel()
	result := task.ExecuteResult{Code: task.ResultCode_Success}
	if cause != nil {
		result.Code, result.Error = task.ResultCode_Failed, util.Abbreviate(cause.Error(), 997)
	}
	logFailure := func(err error) bool {
		if err != errSQLTaskStoreBusy {
			deferFailedSQLTask(caller)
		}
		v2.SQLTaskUnclaimedFailureCounter.Inc()
		s.rt.Logger().WarnEventLazy(sqlTaskUnclaimedRetry, func() []zap.Field {
			return []zap.Field{zap.Uint64("async-id", claim.ID), zap.String("error", util.Abbreviate(err.Error(), 997))}
		})
		return false
	}
	if err := s.store.FinishSQLTaskUnclaimed(ctx, claim, result, time.Now().UnixMilli()); err != nil {
		return logFailure(err)
	}
	if cleanupTaskID != 0 {
		for {
			deleted, err := deleteStaleSQLTaskBatch(ctx, s.store, cleanupTaskID)
			if err != nil {
				return logFailure(err)
			}
			if deleted < sqlTaskStaleBatchSize {
				break
			}
		}
	}
	return true
}

func (s *taskService) CreateAsyncTask(ctx context.Context, value task.TaskMetadata) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), ErrNotReady)
		default:
			if _, err := s.store.AddAsyncTask(ctx, newTaskFromMetadata(value)); err != nil {
				if errors.Is(err, ErrNotReady) {
					time.Sleep(300 * time.Millisecond)
					continue
				}
				return err
			}
			return nil
		}
	}
}

func (s *taskService) CreateBatch(ctx context.Context, tasks []task.TaskMetadata) error {
	values := make([]task.AsyncTask, 0, len(tasks))
	for _, v := range tasks {
		values = append(values, newTaskFromMetadata(v))
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), ErrNotReady)
		default:
			if _, err := s.store.AddAsyncTask(ctx, values...); err != nil {
				if errors.Is(err, ErrNotReady) {
					time.Sleep(300 * time.Millisecond)
					continue
				}
				return err
			}
			return nil
		}
	}
}

func (s *taskService) CreateCronTask(ctx context.Context, value task.TaskMetadata, cronExpr string) error {
	sche, err := s.cronParser.Parse(cronExpr)
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()
	next := sche.Next(time.UnixMilli(now))

	_, err = s.store.AddCronTask(ctx, task.CronTask{
		Metadata:     value,
		CronExpr:     cronExpr,
		NextTime:     next.UnixMilli(),
		TriggerTimes: 0,
		CreateAt:     now,
		UpdateAt:     now,
	})
	return err
}

func (s *taskService) CreateDaemonTask(ctx context.Context, metadata task.TaskMetadata, details *task.Details) error {
	taskType, err := daemonTaskType(ctx, details)
	if err != nil {
		return err
	}
	now := time.Now()

	dt := task.DaemonTask{
		Metadata:   metadata,
		TaskType:   taskType,
		TaskStatus: task.TaskStatus_Created,
		Details:    details,
		CreateAt:   now,
		UpdateAt:   now,
	}
	_, err = s.store.AddDaemonTask(ctx, dt)
	return err
}

func (s *taskService) Allocate(ctx context.Context, value task.AsyncTask, taskRunner string) error {
	exists, err := s.store.QueryAsyncTask(ctx, WithTaskIDCond(EQ, value.ID))
	if err != nil {
		return err
	}
	if len(exists) == 0 {
		// Task not found, it may have been completed and deleted by another scheduler
		// or CN node. This is a normal race condition in distributed systems.
		fields := []zap.Field{zap.Uint64("task-id", value.ID), zap.String("task-metadata-id", value.Metadata.ID), zap.String("task-runner", taskRunner)}
		if value.Metadata.Executor == task.TaskCode_SQLTask {
			s.rt.Logger().WarnEvent(sqlTaskAllocationMissing, fields...)
		} else {
			s.rt.Logger().Warn(sqlTaskAllocationMissing.Message, fields...)
		}
		return moerr.NewInvalidTask(ctx, taskRunner, value.ID)
	}
	if len(exists) > 1 {
		// Multiple records for the same primary key indicates a data consistency issue
		s.rt.Logger().Fatal(fmt.Sprintf("query task by primary key, return %d records", len(exists)))
	}

	old := exists[0]
	previousStatus := old.Status
	switch old.Status {
	case task.TaskStatus_Running:
		old.Epoch++
		old.TaskRunner = taskRunner
		old.LastHeartbeat = time.Now().UnixMilli()
	case task.TaskStatus_Created:
		old.Status = task.TaskStatus_Running
		old.Epoch = 1
		old.TaskRunner = taskRunner
		old.LastHeartbeat = time.Now().UnixMilli()
	default:
		return moerr.NewInvalidTask(ctx, taskRunner, value.ID)
	}

	n, err := s.store.UpdateAsyncTask(ctx,
		[]task.AsyncTask{old},
		WithTaskIDCond(EQ, old.ID),
		WithTaskStatusCond(previousStatus),
		WithTaskEpochCond(EQ, old.Epoch-1))
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, taskRunner, value.ID)
	}
	return nil
}

func (s *taskService) Complete(
	ctx context.Context,
	taskRunner string,
	value task.AsyncTask,
	result task.ExecuteResult) error {
	value.CompletedAt = time.Now().UnixMilli()
	value.Status = task.TaskStatus_Completed
	value.ExecuteResult = &result
	n, err := s.store.UpdateAsyncTask(ctx, []task.AsyncTask{value},
		WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskRunnerCond(EQ, taskRunner),
		WithTaskEpochCond(EQ, value.Epoch))
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, value.TaskRunner, value.ID)
	}
	return nil
}

func (s *taskService) Heartbeat(ctx context.Context, value task.AsyncTask) error {
	value.LastHeartbeat = time.Now().UnixMilli()
	n, err := s.store.UpdateAsyncTask(ctx, []task.AsyncTask{value},
		WithTaskIDCond(EQ, value.ID),
		WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskEpochCond(LE, value.Epoch),
		WithTaskRunnerCond(EQ, value.TaskRunner))
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, value.TaskRunner, value.ID)
	}
	return nil
}

func (s *taskService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	return s.store.QueryAsyncTask(ctx, conds...)
}

func (s *taskService) QueryCronTask(ctx context.Context, c ...Condition) ([]task.CronTask, error) {
	return s.store.QueryCronTask(ctx, c...)
}

func (s *taskService) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error) {
	return s.store.UpdateDaemonTask(ctx, tasks, conds...)
}

func (s *taskService) UpdateDaemonTaskError(ctx context.Context, claim task.DaemonTask, release bool) (int, error) {
	return s.store.UpdateDaemonTaskError(ctx, claim, release)
}

func (s *taskService) RequestDaemonTaskStatus(ctx context.Context, observed task.DaemonTask, status task.TaskStatus) (int, error) {
	return s.store.RequestDaemonTaskStatus(ctx, observed, status)
}

func (s *taskService) UpdateDaemonTaskStatus(
	ctx context.Context,
	taskID uint64,
	status task.TaskStatus,
	updateAt time.Time,
	endAt time.Time,
	conds ...Condition,
) (int, error) {
	return s.store.UpdateDaemonTaskStatus(ctx, taskID, status, updateAt, endAt, conds...)
}

func (s *taskService) QueryDaemonTask(ctx context.Context, conds ...Condition) ([]task.DaemonTask, error) {
	return s.store.QueryDaemonTask(ctx, conds...)
}

func (s *taskService) AddCDCTask(ctx context.Context, metadata task.TaskMetadata, details *task.Details, callback func(context.Context, SqlExecutor) (int, error)) (int, error) {
	taskType, err := daemonTaskType(ctx, details)
	if err != nil {
		return 0, err
	}
	if taskType != task.TaskType_CreateCdc {
		return 0, moerr.NewInvalidInput(ctx, "CDC task details must be CreateCdc")
	}
	now := time.Now()

	dt := task.DaemonTask{
		Metadata:   metadata,
		TaskType:   taskType,
		TaskStatus: task.TaskStatus_Created,
		Details:    details,
		CreateAt:   now,
		UpdateAt:   now,
	}

	return s.store.AddCDCTask(ctx, dt, callback)
}

func daemonTaskType(ctx context.Context, details *task.Details) (task.TaskType, error) {
	taskType := details.Type()
	if taskType == task.TaskType_TypeUnknown {
		return taskType, moerr.NewInvalidInput(ctx, "daemon task details must have a known type")
	}
	return taskType, nil
}

func (s *taskService) UpdateCDCTask(
	ctx context.Context,
	targetStatus task.TaskStatus,
	taskCollector func(
		context.Context,
		task.TaskStatus,
		map[CDCTaskKey]struct{},
		SqlExecutor,
	) (int, error),
	conds ...Condition,
) (int, error) {
	return s.store.UpdateCDCTask(ctx, targetStatus, taskCollector, conds...)
}

func (s *taskService) Close() error {
	s.StopScheduleCronTask()
	s.StopScheduleSQLTask()
	count, sample := s.sqlRecovery.close()
	s.reportAbandonedSQLTaskReceipts(count, sample)
	if s.store == nil {
		return nil
	}
	return s.store.Close()
}

func (s *taskService) GetStorage() TaskStorage {
	return s.store
}

func (s *taskService) HeartbeatDaemonTask(ctx context.Context, t task.DaemonTask) error {
	t.LastHeartbeat = time.Now().UTC()
	n, err := s.store.HeartbeatDaemonTask(ctx, []task.DaemonTask{t})
	if err != nil {
		return err
	}
	if n == 0 {
		return moerr.NewInvalidTask(ctx, t.TaskRunner, t.ID)
	}
	return nil
}

func (s *taskService) ValidateDaemonTask(ctx context.Context, t task.DaemonTask) error {
	valid, err := s.store.ValidateDaemonTask(ctx, t)
	if err != nil {
		return err
	}
	if !valid {
		return moerr.NewInvalidTask(ctx, t.TaskRunner, t.ID)
	}
	return nil
}

// daemonTaskStatusAuthorizesEffect ties status authority to the immutable claim
// snapshot captured by one executor generation. A normal Running generation
// loses authority as soon as a control request is durable. Resume/Restart first
// publish a new last_run while deliberately retaining their request status until
// startup succeeds, so only that new request-generation may use the transitional
// status and remain valid after its status is promoted to Running.
func daemonTaskStatusAuthorizesEffect(claimStatus, currentStatus task.TaskStatus) bool {
	switch claimStatus {
	case task.TaskStatus_Running:
		return currentStatus == task.TaskStatus_Running
	case task.TaskStatus_ResumeRequested, task.TaskStatus_RestartRequested:
		return currentStatus == claimStatus || currentStatus == task.TaskStatus_Running
	default:
		return false
	}
}

func (s *taskService) TruncateCompletedTasks(ctx context.Context) error {
	_, err := s.store.DeleteAsyncTask(ctx, WithTaskStatusCond(task.TaskStatus_Completed))
	return err
}
