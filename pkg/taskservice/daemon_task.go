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
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/zap"
)

type TaskHandler interface {
	Handle(ctx context.Context) error
}

type startTask struct {
	runner *taskRunner
	task   *daemonTask
}

func newStartTask(r *taskRunner, t *daemonTask) *startTask {
	return &startTask{
		runner: r,
		task:   t,
	}
}

func (t *startTask) Handle(_ context.Context) error {
	if err := t.runner.stopper.RunTask(func(ctx context.Context) {
		defer t.runner.removeDaemonTask(t.task.task.ID)

		ok, err := t.runner.startDaemonTask(ctx, t.task)
		if err != nil {
			t.runner.setDaemonTaskError(ctx, t.task, err)
			return
		}

		// ok value is false, means that the task cannot be started by
		// this runner, maybe it has been started by another runner.
		if !ok {
			return
		}

		// Start the go-routine to execute the task. It hangs here until
		// the task encounters some error or be canceled.
		if err := t.task.executor(ctx, &t.task.task); err != nil {
			// set the record of this task error message.
			t.runner.setDaemonTaskError(ctx, t.task, err)
		}
	}); err != nil {
		return err
	}
	return nil
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
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	tasks, err := t.runner.service.QueryDaemonTask(ctx, WithTaskIDCond(EQ, t.task.task.ID))
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewInternalErrorf(ctx, "count of tasks is wrong %d", len(tasks))
	}

	tk := tasks[0]
	// We cannot resume a task which is not on local runner.
	if !strings.EqualFold(tk.TaskRunner, t.runner.runnerID) {
		return moerr.NewInternalErrorf(ctx, "the task is not on local runner, prev runner %s, "+
			"local runner %s", tk.TaskRunner, t.runner.runnerID)
	}

	tk.TaskStatus = task.TaskStatus_Running
	nowTime := time.Now()
	tk.LastRun = nowTime
	tk.LastHeartbeat = nowTime
	_, err = t.runner.service.UpdateDaemonTask(ctx, []task.DaemonTask{tk})
	if err != nil {
		return err
	}

	ar := t.task.activeRoutine.Load()
	if ar == nil || *ar == nil {
		return moerr.NewInternalErrorf(ctx, "cannot handle resume operation, "+
			"active routine not set for task %d", t.task.task.ID)
	}
	return (*ar).Resume()
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
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	tasks, err := t.runner.service.QueryDaemonTask(ctx, WithTaskIDCond(EQ, t.task.task.ID))
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewInternalErrorf(ctx, "count of tasks is wrong %d", len(tasks))
	}

	tk := tasks[0]
	tk.TaskStatus = task.TaskStatus_Paused
	_, err = t.runner.service.UpdateDaemonTask(ctx, []task.DaemonTask{tk})
	if err != nil {
		return err
	}

	if t.runner.exists(tk.ID) {
		ar := t.task.activeRoutine.Load()
		if ar == nil || *ar == nil {
			return moerr.NewInternalErrorf(ctx, "cannot handle pause operation, "+
				"active routine not set for task %d", t.task.task.ID)
		}
		if err := (*ar).Pause(); err != nil {
			return err
		}
	}
	return nil
}

type cancelTask struct {
	runner *taskRunner
	task   *daemonTask
}

func newCancelTask(r *taskRunner, t *daemonTask) *cancelTask {
	return &cancelTask{
		runner: r,
		task:   t,
	}
}

func (t *cancelTask) Handle(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	tasks, err := t.runner.service.QueryDaemonTask(ctx, WithTaskIDCond(EQ, t.task.task.ID))
	if err != nil {
		return err
	}
	if len(tasks) != 1 {
		return moerr.NewInternalErrorf(ctx, "count of tasks is wrong %d", len(tasks))
	}

	tk := tasks[0]
	tk.TaskStatus = task.TaskStatus_Canceled
	tk.EndAt = time.Now()
	_, err = t.runner.service.UpdateDaemonTask(ctx, []task.DaemonTask{tk})
	if err != nil {
		return err
	}
	if t.runner.exists(tk.ID) {
		ar := t.task.activeRoutine.Load()
		if ar == nil || *ar == nil {
			return moerr.NewInternalErrorf(ctx, "cannot handle cancel operation, "+
				"active routine not set for task %d", t.task.task.ID)
		}
		return (*ar).Cancel()
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
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("daemon task poll worker stopped")
			return

		case <-timer.C:
			if taskFrameworkDisabled() {
				continue
			}
			r.dispatchTaskHandle(ctx)
			timer.Reset(r.options.fetchInterval)
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
	r.daemonTasks.Lock()
	defer r.daemonTasks.Unlock()
	for _, t := range r.startTasks(ctx) {
		r.newStartTask(t)
	}
	for _, t := range r.resumeTasks(ctx) {
		dt, ok := r.daemonTasks.m[t.ID]
		if ok {
			r.enqueue(newResumeTask(r, dt))
		} else {
			r.newStartTask(t)
		}
	}
	for _, t := range r.pauseTasks(ctx) {
		dt, ok := r.daemonTasks.m[t.ID]
		if ok {
			r.enqueue(newPauseTask(r, dt))
		} else {
			dt, err := r.newDaemonTask(t)
			if err != nil {
				r.logger.Error("failed to dispatch daemon task",
					zap.Uint64("task ID", t.ID), zap.Error(err))
				return
			}
			r.enqueue(newPauseTask(r, dt))
		}
	}
	for _, t := range r.cancelTasks(ctx) {
		dt, ok := r.daemonTasks.m[t.ID]
		if ok {
			r.enqueue(newCancelTask(r, dt))
		} else {
			dt, err := r.newDaemonTask(t)
			if err != nil {
				r.logger.Error("failed to dispatch daemon task",
					zap.Uint64("task ID", t.ID), zap.Error(err))
				return
			}
			r.enqueue(newCancelTask(r, dt))
		}
	}
}

func (r *taskRunner) queryDaemonTasks(ctx context.Context, c ...Condition) []task.DaemonTask {
	ctx, cancel := context.WithTimeout(ctx, r.options.fetchTimeout)
	defer cancel()
	t, err := r.service.QueryDaemonTask(ctx, c...)
	if err != nil {
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
	return r.mergeTasks(
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_Created),
		),
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_Running, task.TaskStatus_ResumeRequested),
			WithLastHeartbeat(LE, time.Now().UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
		),
	)
}

// resumeTasks gets the tasks that need to resume.
// - status equals to task.TaskStatus_ResumeRequested and runner equals to local
func (r *taskRunner) resumeTasks(ctx context.Context) []task.DaemonTask {
	// We only resume the tasks that already running on this runner. For the tasks that
	// run on other runners and heartbeat timeout, startTasks() will handle them.
	return r.mergeTasks(
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_ResumeRequested),
			WithTaskRunnerCond(EQ, r.runnerID),
		),
	)
}

// pauseTasks gets the tasks that need to pause.
// - status equals to task.TaskStatus_PauseRequested and runner equals to local
func (r *taskRunner) pauseTasks(ctx context.Context) []task.DaemonTask {
	// Handle the tasks which is in PauseRequested status:
	//   1. the task is on current runner
	//   2. the task is on other runners, but heartbeat timeout or null. In the handler,
	//      do NOT pause the active routine in this case.
	return r.mergeTasks(
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_PauseRequested),
			WithTaskRunnerCond(EQ, r.runnerID),
		),
		r.queryDaemonTasks(ctx,
			WithTaskStatusCond(task.TaskStatus_PauseRequested),
			WithLastHeartbeat(LE, time.Now().UnixNano()-r.options.heartbeatTimeout.Nanoseconds()),
		),
	)
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

func (r *taskRunner) startDaemonTask(ctx context.Context, dt *daemonTask) (bool, error) {
	t := dt.task
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
	t.Details.Error = ""

	// When update the daemon task, add the condition that last heartbeat of
	// the task must be timeout or be null, which means that other runners does
	// NOT try to start this task.
	c, err := r.service.UpdateDaemonTask(ctx, []task.DaemonTask{t},
		WithLastHeartbeat(LE, nowTime.UnixNano()-r.options.heartbeatTimeout.Nanoseconds()))
	if err != nil {
		return false, err
	}

	// The daemon task may be updated by other runners, so do not start the task on this runner.
	if c != 1 {
		return false, nil
	}

	r.addDaemonTask(dt)
	return true, nil
}

func (r *taskRunner) setDaemonTaskError(ctx context.Context, dt *daemonTask, errMsg error) {
	r.logger.Info("daemon task stopped with error", zap.Uint64("task ID", dt.task.ID),
		zap.Error(errMsg))
	t := dt.task
	nowTime := time.Now()
	t.UpdateAt = nowTime
	t.Details.Error = errMsg.Error()
	// TODO(volgariver6): if it is a retryable error, do not update the status,
	// otherwise, set the status to Error.
	_, err := r.service.UpdateDaemonTask(ctx, []task.DaemonTask{t})
	if err != nil {
		r.logger.Error("failed to set error message to task",
			zap.Uint64("task ID", t.ID),
			zap.String("error message", errMsg.Error()),
			zap.Error(err))
	}
}

func (r *taskRunner) addDaemonTask(dt *daemonTask) {
	r.daemonTasks.Lock()
	defer r.daemonTasks.Unlock()
	if _, ok := r.daemonTasks.m[dt.task.ID]; ok {
		return
	}
	r.daemonTasks.m[dt.task.ID] = dt
}

func (r *taskRunner) removeDaemonTask(id uint64) {
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
