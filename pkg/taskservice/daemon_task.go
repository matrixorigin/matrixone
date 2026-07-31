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

package taskservice

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

type TaskHandler interface {
	Handle(ctx context.Context) error
}

var (
	eventCDCRestartEnqueued              = logutil.Event{Name: "taskservice.cdc.restart.enqueued", Message: "CDC restart task was enqueued"}
	eventCDCRestartNoCandidate           = logutil.Event{Name: "taskservice.cdc.restart.no-candidate", Message: "CDC restart scan found no candidate"}
	eventCDCRestartQueryFailed           = logutil.Event{Name: "taskservice.cdc.restart.query-failed", Message: "CDC restart task query failed"}
	eventCDCRestartQueryWrongCount       = logutil.Event{Name: "taskservice.cdc.restart.query-wrong-count", Message: "CDC restart task query returned an unexpected count"}
	eventCDCRestartStarted               = logutil.Event{Name: "taskservice.cdc.restart.started", Message: "CDC restart execution started"}
	eventCDCRestartSkippedAlreadyRunning = logutil.Event{Name: "taskservice.cdc.restart.skipped-already-running", Message: "CDC restart was skipped because task is already running"}
	eventCDCRestartSkippedInvalidStatus  = logutil.Event{Name: "taskservice.cdc.restart.skipped-invalid-status", Message: "CDC restart was skipped because task status is invalid"}
	eventCDCRestartWrongRunner           = logutil.Event{Name: "taskservice.cdc.restart.wrong-runner", Message: "CDC restart task belongs to another runner"}
	eventCDCRestartRunnerAssigned        = logutil.Event{Name: "taskservice.cdc.restart.runner-assigned", Message: "CDC restart task was assigned to the local runner"}
	eventCDCRestartStatusUpdateFailed    = logutil.Event{Name: "taskservice.cdc.restart.status-update-failed", Message: "CDC restart status update failed"}
	eventCDCRestartStatusUpdated         = logutil.Event{Name: "taskservice.cdc.restart.status-updated", Message: "CDC restart status was updated"}
	eventCDCRestartActiveRoutineMissing  = logutil.Event{Name: "taskservice.cdc.restart.active-routine-missing", Message: "CDC restart has no active routine"}
	eventCDCRestartActiveRoutineCalling  = logutil.Event{Name: "taskservice.cdc.restart.active-routine-calling", Message: "CDC restart is calling the active routine"}
	eventCDCRestartFailed                = logutil.Event{Name: "taskservice.cdc.restart.failed", Message: "CDC active routine restart failed"}
	eventCDCRestartCompleted             = logutil.Event{Name: "taskservice.cdc.restart.completed", Message: "CDC active routine restart completed"}
	eventCDCRestartRequestStateUpdated   = logutil.Event{Name: "taskservice.cdc.restart.request-state-updated", Message: "CDC restart request state was updated"}
)

func cdcRestartEventFields(t task.DaemonTask, fields ...zap.Field) []zap.Field {
	out := logutil.StringFingerprintFields("task-name", taskNameFromDetails(t))
	out = append(out, logutil.StringFingerprintFields("task-id", strconv.FormatUint(t.ID, 10))...)
	out = append(out, logutil.StringFingerprintFields("account-id", strconv.FormatUint(uint64(t.AccountID), 10))...)
	return append(out, fields...)
}

func cdcRestartHandleError(message string) error {
	return moerr.NewInternalErrorNoCtx(message)
}

type startTask struct {
	runner       *taskRunner
	task         *daemonTask
	restartClaim bool
}

type restartAdmissionContextKey struct{}

type taskExecutorTaskSchedulerContextKey struct{}

// TaskExecutorTaskScheduler admits a child task into the same task-runner
// stopper that owns its TaskExecutor. The stopper supplies cancellation and,
// importantly, joins the child before TaskRunner.Stop returns.
type TaskExecutorTaskScheduler func(string, func(context.Context)) error

func withTaskExecutorTaskScheduler(
	ctx context.Context,
	scheduler TaskExecutorTaskScheduler,
) context.Context {
	return context.WithValue(ctx, taskExecutorTaskSchedulerContextKey{}, scheduler)
}

// TaskExecutorTaskSchedulerFromContext returns the task-runner owner captured
// by a TaskExecutor invocation. Executors that create replacement generations
// after their initial invocation returns can retain this function without
// retaining attempt-specific context values.
func TaskExecutorTaskSchedulerFromContext(ctx context.Context) TaskExecutorTaskScheduler {
	scheduler, _ := ctx.Value(taskExecutorTaskSchedulerContextKey{}).(TaskExecutorTaskScheduler)
	return scheduler
}

// WithRestartAdmission marks a fresh executor invocation that atomically
// claimed a RestartRequested daemon task. It carries the claim across the
// taskservice/frontend boundary without changing persisted task details.
func WithRestartAdmission(ctx context.Context) context.Context {
	return context.WithValue(ctx, restartAdmissionContextKey{}, true)
}

// IsRestartAdmission reports whether this executor invocation owns a fresh
// restart admission claim.
func IsRestartAdmission(ctx context.Context) bool {
	admitted, _ := ctx.Value(restartAdmissionContextKey{}).(bool)
	return admitted
}

func newStartTask(r *taskRunner, t *daemonTask) *startTask {
	return &startTask{
		runner: r,
		task:   t,
	}
}

func newRestartStartTask(r *taskRunner, t *daemonTask) *startTask {
	return &startTask{
		runner:       r,
		task:         t,
		restartClaim: true,
	}
}

func (t *startTask) Handle(_ context.Context) error {
	if err := t.runner.stopper.RunTask(func(ctx context.Context) {
		var err error
		var ok bool
		defer func() {
			// if cdc task quit without error
			if t.task.task.TaskType == task.TaskType_CreateCdc && err == nil {
				return
			}
			// Only remove the daemon task if this goroutine successfully
			// started it. Otherwise we may remove a task that was started
			// by a different goroutine for the same task ID.
			if ok {
				t.runner.removeDaemonTask(t.task.task.ID)
			}
		}()

		ok, err = t.runner.startDaemonTask(ctx, t.task, t.restartClaim)
		if err != nil {
			if t.restartClaim {
				// A failed restart claim does not own the persisted task. Do
				// not write the observed RestartRequested snapshot back: a
				// newer PAUSE/CANCEL request may already have superseded it.
				eventCDCRestartStatusUpdateFailed.ErrorLazy(func() []zap.Field {
					return cdcRestartEventFields(t.task.task, append([]zap.Field{
						zap.String("reason", "restart-claim-failed"),
					}, logutil.ErrorFingerprintFields("error", err)...)...)
				})
			} else {
				// The admission update did not establish ownership. Do not write
				// the pre-claim snapshot back as an executor error: the update may
				// have failed after another runner or control request won.
				t.runner.logger.Error("failed to claim daemon task",
					zap.Uint64("task ID", t.task.task.ID),
					zap.Error(err))
			}
			return
		}

		// ok value is false, means that the task cannot be started by
		// this runner, maybe it has been started by another runner.
		if !ok {
			return
		}

		// Start the go-routine to execute the task. It hangs here until
		// the task encounters some error or be canceled.
		executorCtx := ctx
		if t.restartClaim {
			executorCtx = WithRestartAdmission(ctx)
		}
		if t.task.task.Metadata.Executor == task.TaskCode_InitCdc {
			executorCtx = withTaskExecutorTaskScheduler(
				executorCtx,
				t.runner.stopper.RunNamedTask,
			)
		}
		if err = t.task.executor(executorCtx, &t.task.task); err != nil {
			if t.restartClaim {
				t.runner.releaseRestartClaim(t.task, err)
			} else {
				// set the record of this task error message.
				t.runner.setDaemonTaskError(ctx, t.task, err)
			}
		}
	}); err != nil {
		return err
	}
	return nil
}

func taskNameFromDetails(t task.DaemonTask) string {
	if t.Details == nil || t.Details.Details == nil {
		return ""
	}
	if d, ok := t.Details.Details.(*task.Details_CreateCdc); ok && d != nil && d.CreateCdc != nil {
		return d.CreateCdc.TaskName
	}
	return ""
}

type resumeTask struct {
	runner *taskRunner
	task   *daemonTask
}

func newResumeTask(r *taskRunner, t *daemonTask) *resumeTask {
	return &resumeTask{
		runner: r,
		task:   t,
	}
}

func (t *resumeTask) Handle(ctx context.Context) error {
	handleCtx, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CauseResumeTaskHandle)
	defer cancel()
	tasks, err := t.runner.service.QueryDaemonTask(handleCtx, WithTaskIDCond(EQ, t.task.task.ID))
	if err != nil {
		return moerr.AttachCause(handleCtx, err)
	}
	if len(tasks) != 1 {
		return moerr.NewInternalErrorf(handleCtx, "count of tasks is wrong %d", len(tasks))
	}

	tk := tasks[0]
	t.runner.clearPauseTaskCompleted(tk.ID)
	start := time.Now()
	t.runner.logger.Info("cdc.task.resume.start",
		zap.Uint64("task-id", tk.ID),
		zap.String("task-name", taskNameFromDetails(tk)),
		zap.String("task-runner", tk.TaskRunner),
		zap.String("target-runner", t.runner.runnerID),
		zap.String("current-status", tk.TaskStatus.String()),
	)
	// We cannot resume a task which is not on local runner.
	if !strings.EqualFold(tk.TaskRunner, t.runner.runnerID) {
		return moerr.NewInternalErrorf(handleCtx, "the task is not on local runner, prev runner %s, "+
			"local runner %s", tk.TaskRunner, t.runner.runnerID)
	}
	// Skip duplicate or stale control requests to keep resume idempotent.
	if tk.TaskStatus != task.TaskStatus_ResumeRequested {
		if tk.TaskStatus == task.TaskStatus_Running {
			t.runner.logger.Debug("cdc.task.resume.skip.already-running",
				zap.Uint64("task-id", tk.ID),
				zap.String("task-name", taskNameFromDetails(tk)))
			return nil
		}
		t.runner.logger.Warn("cdc.task.resume.skip.invalid-status",
			zap.Uint64("task-id", tk.ID),
			zap.String("task-name", taskNameFromDetails(tk)),
			zap.String("current-status", tk.TaskStatus.String()))
		return nil
	}

	tk.TaskStatus = task.TaskStatus_Running
	nowTime := time.Now()
	tk.LastRun = nowTime
	tk.LastHeartbeat = nowTime
	_, err = t.runner.service.UpdateDaemonTask(handleCtx, []task.DaemonTask{tk})
	if err != nil {
		return moerr.AttachCause(handleCtx, err)
	}
	t.runner.logger.Info("cdc.task.resume.finish",
		zap.Uint64("task-id", tk.ID),
		zap.String("task-name", taskNameFromDetails(tk)),
		zap.String("new-status", tk.TaskStatus.String()),
		zap.Time("last-run", tk.LastRun),
		zap.Duration("elapsed", time.Since(start)),
	)

	ar := t.task.activeRoutine.Load()
	if ar == nil || *ar == nil {
		return moerr.NewInternalErrorf(handleCtx, "cannot handle resume operation, "+
			"active routine not set for task %d", t.task.task.ID)
	}
	return (*ar).Resume()
}

type restartTask struct {
	runner *taskRunner
	task   *daemonTask
}

func newRestartTask(r *taskRunner, t *daemonTask) *restartTask {
	return &restartTask{
		runner: r,
		task:   t,
	}
}

func (t *restartTask) Handle(ctx context.Context) error {
	handleCtx, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CauseRestartTaskHandle)
	defer cancel()
	tasks, err := t.runner.service.QueryDaemonTask(handleCtx, WithTaskIDCond(EQ, t.task.task.ID))
	if err != nil {
		eventCDCRestartQueryFailed.ErrorLazy(func() []zap.Field {
			return cdcRestartEventFields(t.task.task, logutil.ErrorFingerprintFields("error", err)...)
		})
		return cdcRestartHandleError("CDC restart task query failed")
	}
	if len(tasks) != 1 {
		eventCDCRestartQueryWrongCount.ErrorLazy(func() []zap.Field {
			return cdcRestartEventFields(t.task.task, zap.Int("task-count", len(tasks)))
		})
		return cdcRestartHandleError("CDC restart task query returned an unexpected count")
	}

	tk := tasks[0]
	requestRunner := tk.TaskRunner
	t.runner.clearPauseTaskCompleted(tk.ID)
	start := time.Now()
	// Restart should only be executed from RestartRequested.
	// Any duplicated request is treated as a no-op.
	if tk.TaskStatus != task.TaskStatus_RestartRequested {
		if tk.TaskStatus == task.TaskStatus_Running {
			eventCDCRestartSkippedAlreadyRunning.DebugLazy(func() []zap.Field {
				return cdcRestartEventFields(tk, zap.String("current-status", tk.TaskStatus.String()))
			})
			return nil
		}
		eventCDCRestartSkippedInvalidStatus.WarnLazy(func() []zap.Field {
			return cdcRestartEventFields(tk, zap.String("current-status", tk.TaskStatus.String()))
		})
		return nil
	}

	// We cannot restart a task which is not on local runner.
	// However, if TaskRunner is empty, we allow the local runner to take over.
	if tk.TaskRunner != "" && !strings.EqualFold(tk.TaskRunner, t.runner.runnerID) {
		eventCDCRestartWrongRunner.WarnLazy(func() []zap.Field {
			return cdcRestartEventFields(tk, append(logutil.StringFingerprintFields("task-runner", tk.TaskRunner), logutil.StringFingerprintFields("local-runner", t.runner.runnerID)...)...)
		})
		return cdcRestartHandleError("CDC restart task belongs to another runner")
	}

	// If TaskRunner is empty, assign it to the current runner
	if tk.TaskRunner == "" {
		tk.TaskRunner = t.runner.runnerID
		eventCDCRestartRunnerAssigned.InfoLazy(func() []zap.Field {
			return cdcRestartEventFields(tk, logutil.StringFingerprintFields("assigned-runner", tk.TaskRunner)...)
		})
	}

	eventCDCRestartStarted.InfoLazy(func() []zap.Field {
		return cdcRestartEventFields(tk, append([]zap.Field{
			zap.String("current-status", tk.TaskStatus.String()),
		}, append(logutil.StringFingerprintFields("task-runner", tk.TaskRunner), logutil.StringFingerprintFields("target-runner", t.runner.runnerID)...)...)...)
	})

	ar := t.task.activeRoutine.Load()
	if ar == nil || *ar == nil {
		eventCDCRestartActiveRoutineMissing.ErrorLazy(func() []zap.Field {
			return cdcRestartEventFields(tk)
		})
		return cdcRestartHandleError("CDC restart active routine is unavailable")
	}

	eventCDCRestartActiveRoutineCalling.DebugLazy(func() []zap.Field {
		return cdcRestartEventFields(tk)
	})

	err = (*ar).Restart()
	if err != nil {
		eventCDCRestartFailed.ErrorLazy(func() []zap.Field {
			return cdcRestartEventFields(tk, append([]zap.Field{
				zap.Duration("elapsed", time.Since(start)),
			}, logutil.ErrorFingerprintFields("error", err)...)...)
		})
		return cdcRestartHandleError("CDC restart failed")
	}

	// A successful Restart means the replacement generation is locally Running
	// and has committed its restarting -> running catalog transition. Give the
	// final daemon-task CAS an independent bounded context: the query context
	// may legitimately expire while Restart drains the old generation and
	// starts its replacement.
	tk.TaskStatus = task.TaskStatus_Running
	nowTime := time.Now()
	tk.LastRun = nowTime
	tk.LastHeartbeat = nowTime
	updateCtx, updateCancel := context.WithTimeoutCause(
		context.Background(),
		time.Second*5,
		moerr.CauseRestartTaskHandle,
	)
	defer updateCancel()
	updated, err := t.runner.service.UpdateDaemonTask(
		updateCtx,
		[]task.DaemonTask{tk},
		WithTaskStatusCond(task.TaskStatus_RestartRequested),
		WithTaskRunnerCond(EQ, requestRunner),
	)
	if err != nil {
		eventCDCRestartStatusUpdateFailed.ErrorLazy(func() []zap.Field {
			return cdcRestartEventFields(tk, logutil.ErrorFingerprintFields("error", err)...)
		})
		return cdcRestartHandleError("CDC restart status update failed")
	}
	if updated != 1 {
		eventCDCRestartSkippedInvalidStatus.InfoLazy(func() []zap.Field {
			return cdcRestartEventFields(tk, zap.String("reason", "restart-request-superseded"))
		})
		return nil
	}

	eventCDCRestartStatusUpdated.InfoLazy(func() []zap.Field {
		return cdcRestartEventFields(tk,
			zap.String("new-status", tk.TaskStatus.String()),
			zap.Time("last-run", tk.LastRun),
		)
	})

	eventCDCRestartCompleted.InfoLazy(func() []zap.Field {
		return cdcRestartEventFields(tk,
			zap.String("new-status", tk.TaskStatus.String()),
			zap.Duration("elapsed", time.Since(start)),
		)
	})

	return nil
}

type pauseTask struct {
	runner *taskRunner
	task   *daemonTask
}

func newPauseTask(r *taskRunner, t *daemonTask) *pauseTask {
	return &pauseTask{
		runner: r,
		task:   t,
	}
}

func (t *pauseTask) Handle(ctx context.Context) error {
	handleCtx, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CausePauseTaskHandle)
	defer cancel()
	start := time.Now()
	tasks, err := t.runner.service.QueryDaemonTask(handleCtx, WithTaskIDCond(EQ, t.task.task.ID))
	if err != nil {
		return moerr.AttachCause(handleCtx, err)
	}
	if len(tasks) != 1 {
		return moerr.NewInternalErrorf(handleCtx, "count of tasks is wrong %d", len(tasks))
	}

	tk := tasks[0]
	t.runner.logger.Info("cdc.task.pause.start",
		zap.Uint64("task-id", tk.ID),
		zap.String("task-name", taskNameFromDetails(tk)),
		zap.String("task-runner", tk.TaskRunner),
		zap.String("target-runner", t.runner.runnerID),
		zap.String("current-status", tk.TaskStatus.String()),
	)
	// Pause must be idempotent; repeated pause requests should not call activeRoutine.Pause again.
	if tk.TaskStatus != task.TaskStatus_PauseRequested {
		if tk.TaskStatus == task.TaskStatus_Paused {
			if t.runner.isPauseTaskCompleted(tk.ID) {
				t.runner.logger.Debug("cdc.task.pause.skip.completed",
					zap.Uint64("task-id", tk.ID),
					zap.String("task-name", taskNameFromDetails(tk)))
				return nil
			}
			t.runner.logger.Debug("cdc.task.pause.skip.already-paused",
				zap.Uint64("task-id", tk.ID),
				zap.String("task-name", taskNameFromDetails(tk)))
			return t.runner.pauseTaskCompleted(ctx, tk)
		}
		t.runner.logger.Warn("cdc.task.pause.skip.invalid-status",
			zap.Uint64("task-id", tk.ID),
			zap.String("task-name", taskNameFromDetails(tk)),
			zap.String("current-status", tk.TaskStatus.String()))
		return nil
	}
	t.runner.clearPauseTaskCompleted(tk.ID)
	if t.runner.exists(tk.ID) {
		ar := t.task.activeRoutine.Load()
		if ar == nil || *ar == nil {
			return moerr.NewInternalErrorf(handleCtx, "cannot handle pause operation, "+
				"active routine not set for task %d", t.task.task.ID)
		}
		if err := (*ar).Pause(); err != nil {
			return err
		}
	}

	tk.TaskStatus = task.TaskStatus_Paused
	updateCtx, updateCancel := context.WithTimeoutCause(context.Background(), time.Second*5, moerr.CausePauseTaskHandle)
	defer updateCancel()
	_, err = t.runner.service.UpdateDaemonTask(updateCtx, []task.DaemonTask{tk})
	if err != nil {
		return moerr.AttachCause(updateCtx, err)
	}

	if err := t.runner.pauseTaskCompleted(ctx, tk); err != nil {
		return err
	}
	t.runner.logger.Info("cdc.task.pause.finish",
		zap.Uint64("task-id", tk.ID),
		zap.String("task-name", taskNameFromDetails(tk)),
		zap.String("new-status", tk.TaskStatus.String()),
		zap.Duration("elapsed", time.Since(start)),
	)
	return nil
}

func (r *taskRunner) pauseTaskCompleted(ctx context.Context, tk task.DaemonTask) error {
	if r.options.pauseTaskCompleted == nil {
		return nil
	}
	if err := r.options.pauseTaskCompleted(ctx, tk); err != nil {
		r.logger.Error("cdc.task.pause.complete-hook.failed",
			zap.Uint64("task-id", tk.ID),
			zap.String("task-name", taskNameFromDetails(tk)),
			zap.Error(err))
		return err
	}
	r.markPauseTaskCompleted(tk.ID)
	return nil
}

func (r *taskRunner) markPauseTaskCompleted(id uint64) {
	r.pauseCompletedTasks.Lock()
	defer r.pauseCompletedTasks.Unlock()
	r.pauseCompletedTasks.m[id] = struct{}{}
}

func (r *taskRunner) clearPauseTaskCompleted(id uint64) {
	r.pauseCompletedTasks.Lock()
	defer r.pauseCompletedTasks.Unlock()
	delete(r.pauseCompletedTasks.m, id)
}

func (r *taskRunner) isPauseTaskCompleted(id uint64) bool {
	r.pauseCompletedTasks.Lock()
	defer r.pauseCompletedTasks.Unlock()
	_, ok := r.pauseCompletedTasks.m[id]
	return ok
}

func (r *taskRunner) filterUncompletedPauseTasks(tasks []task.DaemonTask) []task.DaemonTask {
	if len(tasks) == 0 {
		return tasks
	}
	r.pauseCompletedTasks.Lock()
	defer r.pauseCompletedTasks.Unlock()

	n := 0
	for _, tk := range tasks {
		if _, ok := r.pauseCompletedTasks.m[tk.ID]; ok {
			continue
		}
		tasks[n] = tk
		n++
	}
	return tasks[:n]
}

type cancelTask struct {
	runner *taskRunner
	taskID uint64
}

func newCancelTask(r *taskRunner, taskID uint64) *cancelTask {
	return &cancelTask{
		runner: r,
		taskID: taskID,
	}
}

func (t *cancelTask) Handle(ctx context.Context) error {
	handleCtx, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CauseCancelTaskHandle)
	defer cancel()
	tasks, err := t.runner.service.QueryDaemonTask(handleCtx, WithTaskIDCond(EQ, t.taskID))
	if err != nil {
		return moerr.AttachCause(handleCtx, err)
	}
	if len(tasks) != 1 {
		return moerr.NewInternalErrorf(handleCtx, "count of tasks is wrong %d", len(tasks))
	}

	tk := tasks[0]
	t.runner.clearPauseTaskCompleted(tk.ID)
	// Cancel should only be executed from CancelRequested.
	if tk.TaskStatus != task.TaskStatus_CancelRequested {
		if tk.TaskStatus == task.TaskStatus_Canceled {
			t.runner.logger.Debug("cdc.task.cancel.skip.already-canceled",
				zap.Uint64("task-id", tk.ID),
				zap.String("task-name", taskNameFromDetails(tk)))
			return nil
		}
		t.runner.logger.Warn("cdc.task.cancel.skip.invalid-status",
			zap.Uint64("task-id", tk.ID),
			zap.String("task-name", taskNameFromDetails(tk)),
			zap.String("current-status", tk.TaskStatus.String()))
		return nil
	}

	// Revalidate dispatch eligibility against the current storage row. A queued
	// handler must not retire a task that has since acquired a fresh foreign
	// owner; that owner's runner is responsible for stopping its local routine.
	now := time.Now()
	localOwner := strings.EqualFold(tk.TaskRunner, t.runner.runnerID)
	heartbeatExpired := tk.LastHeartbeat.IsZero() ||
		!tk.LastHeartbeat.After(now.Add(-t.runner.options.heartbeatTimeout))
	if tk.TaskRunner != "" && !localOwner && !heartbeatExpired {
		return nil
	}

	// Resolve the local generation at handling time, not at dispatch time. The
	// task map is published before an executor attaches its ActiveRoutine, so a
	// missing routine is an in-progress admission and must remain
	// CancelRequested for the next poll. Likewise, a freshly claimed local task
	// can be between the storage CAS and map publication. A removed executor can
	// never complete such an admission and is safe to retire immediately.
	localTask, hasLocalTask := t.runner.getDaemonTask(tk.ID)
	var activeRoutine ActiveRoutine
	if hasLocalTask {
		ar := localTask.activeRoutine.Load()
		if ar == nil || *ar == nil {
			return nil
		}
		activeRoutine = *ar
	} else if localOwner && !heartbeatExpired &&
		t.runner.GetExecutor(tk.Metadata.Executor) != nil {
		return nil
	}

	tk.TaskStatus = task.TaskStatus_Canceled
	tk.UpdateAt = now
	tk.EndAt = now
	updated, err := t.runner.service.UpdateDaemonTask(
		handleCtx,
		[]task.DaemonTask{tk},
		WithTaskStatusCond(task.TaskStatus_CancelRequested),
		WithTaskRunnerCond(EQ, tk.TaskRunner),
	)
	if err != nil {
		return moerr.AttachCause(handleCtx, err)
	}
	if updated != 1 {
		return nil
	}
	if activeRoutine != nil {
		return activeRoutine.Cancel()
	}
	return nil
}

// ActiveRoutine is an interface that the go routine of the daemon task
// should implement.
type ActiveRoutine interface {
	// Resume resumes the go routine of the daemon task.
	Resume() error
	// Pause pauses the go routine of the daemon task.
	Pause() error
	// Cancel cancels the go routine of the daemon task.
	Cancel() error
	// Restart restart the go routine of the daemon task.
	Restart() error
}

type daemonTask struct {
	task     task.DaemonTask
	executor TaskExecutor
	// activeRoutine is the go-routine runs in background to execute
	// the daemon task.
	activeRoutine atomic.Pointer[ActiveRoutine]
}

func (r *taskRunner) newDaemonTask(t task.DaemonTask) (*daemonTask, error) {
	executor, err := r.getExecutor(t.Metadata.Executor)
	if err != nil {
		return nil, err
	}
	dt := &daemonTask{
		task:     t,
		executor: executor,
	}
	return dt, nil
}

func (r *taskRunner) startDaemonTaskWorker() error {
	if err := r.stopper.RunNamedTask("poll-daemon-tasks", r.poll); err != nil {
		return err
	}
	if err := r.stopper.RunNamedTask("handle-daemon-tasks", r.handleTask); err != nil {
		return err
	}
	if err := r.stopper.RunNamedTask("daemon-tasks-heartbeat", r.sendHeartbeat); err != nil {
		return err
	}
	return nil
}

func (r *taskRunner) poll(ctx context.Context) {
	timer := time.NewTimer(r.options.fetchInterval)
	defer timer.Stop()
	r.pollWithTimer(ctx, timer.C, func() {
		timer.Reset(r.options.fetchInterval)
	})
}

func (r *taskRunner) pollWithTimer(
	ctx context.Context,
	timerC <-chan time.Time,
	resetTimer func(),
) {
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("daemon task poll worker stopped")
			return

		case <-timerC:
			if !taskFrameworkDisabled() {
				r.dispatchTaskHandle(ctx)
			}
			resetTimer()
		}
	}
}

func (r *taskRunner) enqueue(handler TaskHandler) {
	r.pendingTaskHandle <- handler
}

func (r *taskRunner) newStartTask(t task.DaemonTask) {
	dt, err := r.newDaemonTask(t)
	if err != nil {
		r.logger.Error("failed to dispatch daemon task",
			zap.Uint64("task ID", t.ID), zap.Error(err))
		return
	}
	r.enqueue(newStartTask(r, dt))
}

func (r *taskRunner) dispatchTaskHandle(ctx context.Context) {
	// Build handlers first, then enqueue outside daemonTasks lock usage
	// to avoid lock + channel send blocking cycles.
	handlers := make([]TaskHandler, 0, 16)
	for _, t := range r.startTasks(ctx) {
		dt, err := r.newDaemonTask(t)
		if err != nil {
			r.logger.Error("failed to dispatch daemon task",
				zap.Uint64("task ID", t.ID), zap.Error(err))
			continue
		}
		handlers = append(handlers, newStartTask(r, dt))
	}
	for _, t := range r.resumeTasks(ctx) {
		dt, ok := r.getDaemonTask(t.ID)
		if ok {
			handlers = append(handlers, newResumeTask(r, dt))
		} else {
			dt, err := r.newDaemonTask(t)
			if err != nil {
				r.logger.Error("failed to dispatch daemon task",
					zap.Uint64("task ID", t.ID), zap.Error(err))
				continue
			}
			handlers = append(handlers, newStartTask(r, dt))
		}
	}
	for _, t := range r.restartTasks(ctx) {
		// A restart of an executor already owned by this runner is an
		// in-process lifecycle transition. An unassigned or stale foreign task
		// has no local active routine to restart, so claim it through the normal
		// start path, which fences ownership with the heartbeat condition.
		if !strings.EqualFold(t.TaskRunner, r.runnerID) {
			dt, err := r.newDaemonTask(t)
			if err != nil {
				r.logger.Error("failed to dispatch daemon task",
					zap.Uint64("task ID", t.ID), zap.Error(err))
				continue
			}
			handlers = append(handlers, newRestartStartTask(r, dt))
			continue
		}
		dt, ok := r.getDaemonTask(t.ID)
		if ok {
			handlers = append(handlers, newRestartTask(r, dt))
		} else {
			dt, err := r.newDaemonTask(t)
			if err != nil {
				r.logger.Error("failed to dispatch daemon task",
					zap.Uint64("task ID", t.ID), zap.Error(err))
				continue
			}
			handlers = append(handlers, newRestartStartTask(r, dt))
		}
	}
	for _, t := range r.pauseTasks(ctx) {
		dt, ok := r.getDaemonTask(t.ID)
		if ok {
			handlers = append(handlers, newPauseTask(r, dt))
		} else {
			dt, err := r.newDaemonTask(t)
			if err != nil {
				r.logger.Error("failed to dispatch daemon task",
					zap.Uint64("task ID", t.ID), zap.Error(err))
				continue
			}
			handlers = append(handlers, newPauseTask(r, dt))
		}
	}
	for _, t := range r.cancelTasks(ctx) {
		// Cancellation resolves the current local generation in Handle. Carrying
		// a daemonTask snapshot across the dispatch queue would make an old
		// generation look authoritative.
		handlers = append(handlers, newCancelTask(r, t.ID))
	}
	for _, h := range handlers {
		r.enqueue(h)
	}
}

func (r *taskRunner) queryDaemonTasks(ctx context.Context, c ...Condition) []task.DaemonTask {
	queryCtx, cancel := context.WithTimeoutCause(ctx, r.options.fetchTimeout, moerr.CauseQueryDaemonTasks)
	defer cancel()
	t, err := r.service.QueryDaemonTask(queryCtx, c...)
	if err != nil {
		err = moerr.AttachCause(queryCtx, err)
		r.logger.Error("failed to get tasks", zap.Error(err))
		return nil
	}
	return t
}

// mergeTasks merges all the tasks in all the slices. It not only remove the duplicated tasks,
// but also filter out the tasks if the runner cannot run.
func (r *taskRunner) mergeTasks(tasksSlice ...[]task.DaemonTask) []task.DaemonTask {
	taskIDs := make(map[uint64]struct{})
	var res []task.DaemonTask
	for _, tasks := range tasksSlice {
		for _, t := range tasks {
			if _, ok := taskIDs[t.ID]; ok {
				continue
			}
			if !r.canClaimDaemonTask(t.Account) {
				continue
			}
			taskIDs[t.ID] = struct{}{}
			res = append(res, t)
		}
	}
	return res
}

// resumeTasks gets the tasks that need to start.
// - status: task.TaskStatus_Created
// - status: task.TaskStatus_Running AND last-heartbeat: timeout
func (r *taskRunner) startTasks(ctx context.Context) []task.DaemonTask {
	r.hakeeper.RLock()
	getClient := r.hakeeper.getClient
	cnUUID := r.hakeeper.cnUUID
	r.hakeeper.RUnlock()

	labels := NewCnLabels(cnUUID)
	if getClient != nil {
		hakeeperClient := getClient()
		// account -> cn map. in all c
		if hakeeperClient != nil {
			ctx2, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CauseStartTasks)
			defer cancel()
			state, err := hakeeperClient.GetClusterState(ctx2)
			if err != nil {
				err = moerr.AttachCause(ctx2, err)
				r.logger.Error("failed to get cluster state", zap.Error(err))
			} else {
				var ok bool
				var labelList metadata.LabelList
				//cn -> cnStoreInfo
				for cn, cnInfo := range state.CNState.Stores {
					//account -> account list
					if labelList, ok = cnInfo.Labels["account"]; ok {
						//account list
						labels.Add(cn, labelList.GetLabels())
					}
				}
			}
		}
	}

	return r.mergeTasks(
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_Created),
			WithLabels(IN, labels),
		),
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_Running),
			WithLastHeartbeat(LE, time.Now().UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
		),
	)
}

// resumeTasks gets the tasks that need to resume.
// - status equals to task.TaskStatus_ResumeRequested and runner equals to local
func (r *taskRunner) resumeTasks(ctx context.Context) []task.DaemonTask {
	// We only resume the tasks that already running on this runner. For the tasks that
	// run on other runners and heartbeat timeout, startTasks() will handle them.
	tasks := r.mergeTasks(
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_ResumeRequested),
			WithTaskRunnerCond(EQ, r.runnerID),
		),
	)
	if len(tasks) > 0 {
		for _, t := range tasks {
			r.logger.Info("cdc.task.resume.enqueue",
				zap.Uint64("task-id", t.ID),
				zap.String("task-name", taskNameFromDetails(t)),
				zap.String("current-status", t.TaskStatus.String()),
				zap.String("task-runner", t.TaskRunner),
			)
		}
	}
	return tasks
}

// restartTasks returns local restart requests plus restart requests that can
// be atomically claimed through startDaemonTask after their owner heartbeat is
// stale. The dispatcher chooses Restart only for local ownership; stale and
// unassigned tasks take the fenced start path instead.
func (r *taskRunner) restartTasks(ctx context.Context) []task.DaemonTask {
	localRestart := r.queryDaemonTasks(ctx,
		WithTaskStatusCond(task.TaskStatus_RestartRequested),
		WithTaskRunnerCond(EQ, r.runnerID),
	)
	unassignedRestart := r.queryDaemonTasks(ctx,
		WithTaskStatusCond(task.TaskStatus_RestartRequested),
		WithTaskRunnerCond(EQ, ""),
	)
	laggedRestart := r.queryDaemonTasks(ctx,
		WithTaskStatusCond(task.TaskStatus_RestartRequested),
		WithLastHeartbeat(LE, time.Now().UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
	)
	tasks := r.mergeTasks(localRestart, unassignedRestart, laggedRestart)
	if len(tasks) > 0 {
		for _, t := range tasks {
			eventCDCRestartEnqueued.InfoLazy(func() []zap.Field {
				return cdcRestartEventFields(t, append([]zap.Field{
					zap.String("current-status", t.TaskStatus.String()),
				}, append(logutil.StringFingerprintFields("task-runner", t.TaskRunner), logutil.StringFingerprintFields("local-runner", r.runnerID)...)...)...)
			})
		}
	} else {
		eventCDCRestartNoCandidate.DebugLazy(func() []zap.Field {
			return []zap.Field{
				zap.Int("local-candidates", len(localRestart)),
				zap.Int("unassigned-candidates", len(unassignedRestart)),
				zap.Int("lagged-candidates", len(laggedRestart)),
			}
		})
	}
	return tasks
}

// pauseTasks gets the tasks that need to pause.
// - status equals to task.TaskStatus_PauseRequested and runner equals to local
func (r *taskRunner) pauseTasks(ctx context.Context) []task.DaemonTask {
	// Handle the tasks which is in PauseRequested status:
	//   1. the task is on current runner
	//   2. the task is on other runners, but heartbeat timeout or null. In the handler,
	//      do NOT pause the active routine in this case.
	localPause := r.queryDaemonTasks(ctx,
		WithTaskStatusCond(task.TaskStatus_PauseRequested),
		WithTaskRunnerCond(EQ, r.runnerID),
	)
	laggedPause := r.queryDaemonTasks(ctx,
		WithTaskStatusCond(task.TaskStatus_PauseRequested),
		WithLastHeartbeat(LE, time.Now().UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
	)
	// A CDC pause completion hook may fail after the daemon task has already been
	// persisted as Paused. Keep polling Paused CDC tasks so the hook has a retry path.
	var localPausedFinalize, laggedPausedFinalize []task.DaemonTask
	if r.options.pauseTaskCompleted != nil {
		localPausedFinalize = r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_Paused),
			WithTaskRunnerCond(EQ, r.runnerID),
			WithTaskExecutorCond(EQ, task.TaskCode_InitCdc),
		)
		localPausedFinalize = r.filterUncompletedPauseTasks(localPausedFinalize)
		laggedPausedFinalize = r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_Paused),
			WithLastHeartbeat(LE, time.Now().UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
			WithTaskExecutorCond(EQ, task.TaskCode_InitCdc),
		)
		laggedPausedFinalize = r.filterUncompletedPauseTasks(laggedPausedFinalize)
	}
	tasks := r.mergeTasks(localPause, laggedPause, localPausedFinalize, laggedPausedFinalize)
	if len(tasks) > 0 {
		for _, t := range tasks {
			r.logger.Info("cdc.task.pause.enqueue",
				zap.Uint64("task-id", t.ID),
				zap.String("task-name", taskNameFromDetails(t)),
				zap.String("current-status", t.TaskStatus.String()),
				zap.String("task-runner", t.TaskRunner),
			)
		}
	} else {
		r.logger.Debug("cdc.task.pause.enqueue.none",
			zap.Int("local-candidates", len(localPause)),
			zap.Int("lagged-candidates", len(laggedPause)),
			zap.Int("local-paused-finalize-candidates", len(localPausedFinalize)),
			zap.Int("lagged-paused-finalize-candidates", len(laggedPausedFinalize)),
		)
	}
	return tasks
}

// cancelTasks gets the tasks that need to cancel.
func (r *taskRunner) cancelTasks(ctx context.Context) []task.DaemonTask {
	// Handle the tasks which is in CancelRequested status:
	//   1. the task is on current runner
	//   2. the task is on other runners, but heartbeat timeout or null. In the handler,
	//      do NOT cancel the active routine in this case.
	return r.mergeTasks(
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_CancelRequested),
			WithTaskRunnerCond(EQ, r.runnerID),
		),
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_CancelRequested),
			WithLastHeartbeat(LE, time.Now().UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
		),
	)
}

func (r *taskRunner) handleTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case h := <-r.pendingTaskHandle:
			if err := h.Handle(ctx); err != nil {
				r.logger.Error("failed to handle task", zap.Error(err))
			}
		}
	}
}

func (r *taskRunner) sendHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(r.options.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("heartbeat task stopped")
			return
		case <-ticker.C:
			if taskFrameworkDisabled() {
				continue
			}
			r.doSendHeartbeat(ctx)
		}
	}
}

func (r *taskRunner) doSendHeartbeat(ctx context.Context) {
	r.daemonTasks.Lock()
	tasks := make([]*daemonTask, 0, len(r.daemonTasks.m))
	for _, dt := range r.daemonTasks.m {
		tasks = append(tasks, dt)
	}
	r.daemonTasks.Unlock()

	for _, dt := range tasks {
		if err := r.service.HeartbeatDaemonTask(ctx, dt.task); err != nil {
			r.logger.Error("task heartbeat failed",
				zap.Uint64("task ID", dt.task.ID),
				zap.Error(err))
		}
	}
}

func (r *taskRunner) startDaemonTask(ctx context.Context, dt *daemonTask, restartClaim bool) (bool, error) {
	t := dt.task
	expectedStatus := t.TaskStatus
	if restartClaim {
		expectedStatus = task.TaskStatus_RestartRequested
	}
	t.TaskRunner = r.runnerID
	t.TaskStatus = task.TaskStatus_Running
	nowTime := time.Now()
	t.UpdateAt = nowTime
	t.LastRun = nowTime

	// Update the last heartbeat if the daemon task is started successfully.
	// The new value is used to prevent other runners to start this task at
	// the same time.
	t.LastHeartbeat = nowTime

	// Clear the error message of the task when start it. And if it fails to
	// start, new error message will be set again.
	t.Details = cloneDaemonTaskDetails(t.Details)
	t.Details.Error = ""

	// Claim only the state that was observed by the dispatcher, with a stale or
	// null heartbeat. The status fence prevents an old start snapshot from
	// overwriting a concurrent pause, cancel, or restart request.
	conditions := []Condition{
		WithTaskStatusCond(expectedStatus),
		WithLastHeartbeat(LE, nowTime.UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
	}
	c, err := r.service.UpdateDaemonTask(ctx, []task.DaemonTask{t}, conditions...)
	if err != nil {
		return false, err
	}

	// The daemon task may be updated by other runners, so do not start the task on this runner.
	if c != 1 {
		return false, nil
	}

	// Publish the claimed snapshot. All later heartbeat, error, and lifecycle
	// writes must carry the generation's actual Running status and owner rather
	// than the stale pre-claim dispatcher snapshot.
	dt.task = t
	r.addDaemonTask(dt)
	return true, nil
}

func (r *taskRunner) setDaemonTaskError(ctx context.Context, dt *daemonTask, errMsg error) {
	r.logger.Info("daemon task stopped with error", zap.Uint64("task ID", dt.task.ID),
		zap.Error(errMsg))
	t := dt.task
	nowTime := time.Now()
	t.UpdateAt = nowTime
	t.Details = cloneDaemonTaskDetails(t.Details)
	t.Details.Error = errMsg.Error()
	// TODO(volgariver6): if it is a retryable error, do not update the status,
	// otherwise, set the status to Error.
	updated, err := r.service.UpdateDaemonTask(
		ctx,
		[]task.DaemonTask{t},
		WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskRunnerCond(EQ, r.runnerID),
	)
	if err != nil {
		r.logger.Error("failed to set error message to task",
			zap.Uint64("task ID", t.ID),
			zap.String("error message", errMsg.Error()),
			zap.Error(err))
		return
	}
	if updated == 0 {
		r.logger.Debug("skip stale daemon task error update",
			zap.Uint64("task ID", t.ID),
			zap.String("error message", errMsg.Error()))
	}
}

// releaseRestartClaim makes a failed fresh takeover immediately retryable.
// The status/runner CAS preserves a newer PAUSE/CANCEL and prevents an older
// failed executor from releasing a later claim.
func (r *taskRunner) releaseRestartClaim(dt *daemonTask, startErr error) {
	retry := dt.task
	retry.TaskStatus = task.TaskStatus_RestartRequested
	retry.TaskRunner = ""
	retry.LastHeartbeat = time.Time{}
	retry.UpdateAt = time.Now()
	retry.Details = cloneDaemonTaskDetails(retry.Details)
	retry.Details.Error = "CDC restart startup failed"

	updateCtx, cancel := context.WithTimeoutCause(
		context.Background(),
		time.Second*5,
		moerr.CauseRestartTaskHandle,
	)
	defer cancel()
	updated, err := r.service.UpdateDaemonTask(
		updateCtx,
		[]task.DaemonTask{retry},
		WithTaskStatusCond(task.TaskStatus_Running),
		WithTaskRunnerCond(EQ, r.runnerID),
	)
	if err != nil {
		eventCDCRestartStatusUpdateFailed.ErrorLazy(func() []zap.Field {
			return cdcRestartEventFields(dt.task, append([]zap.Field{
				zap.String("reason", "release-failed-restart-claim"),
			}, logutil.ErrorFingerprintFields("error", err)...)...)
		})
		return
	}
	if updated != 1 {
		eventCDCRestartSkippedInvalidStatus.InfoLazy(func() []zap.Field {
			return cdcRestartEventFields(
				dt.task,
				zap.String("reason", "failed-restart-claim-superseded"),
			)
		})
		return
	}
	eventCDCRestartFailed.ErrorLazy(func() []zap.Field {
		return cdcRestartEventFields(dt.task, append([]zap.Field{
			zap.String("reason", "fresh-startup-failed"),
		}, logutil.ErrorFingerprintFields("error", startErr)...)...)
	})
}

func cloneDaemonTaskDetails(d *task.Details) *task.Details {
	if d == nil {
		return &task.Details{}
	}
	// Shallow copy is enough for current use because callers only mutate the
	// top-level Error field. If nested mutable fields are updated in future,
	// this should be changed to a deep copy.
	clone := *d
	return &clone
}

func (r *taskRunner) addDaemonTask(dt *daemonTask) {
	r.daemonTasks.Lock()
	defer r.daemonTasks.Unlock()
	if _, ok := r.daemonTasks.m[dt.task.ID]; ok {
		return
	}
	r.daemonTasks.m[dt.task.ID] = dt
}

func (r *taskRunner) getDaemonTask(id uint64) (*daemonTask, bool) {
	// Keep map access serialized and return the pointer snapshot.
	r.daemonTasks.Lock()
	defer r.daemonTasks.Unlock()
	dt, ok := r.daemonTasks.m[id]
	return dt, ok
}

func (r *taskRunner) removeDaemonTask(id uint64) {
	r.clearPauseTaskCompleted(id)
	r.daemonTasks.Lock()
	defer r.daemonTasks.Unlock()
	delete(r.daemonTasks.m, id)
}

func (r *taskRunner) exists(id uint64) bool {
	r.daemonTasks.Lock()
	defer r.daemonTasks.Unlock()
	if _, ok := r.daemonTasks.m[id]; ok {
		return true
	}
	return false
}
