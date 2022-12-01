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
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/zap"
)

// RunnerOption option for create task runner
type RunnerOption func(*taskRunner)

// WithRunnerLogger set logger
func WithRunnerLogger(logger *zap.Logger) RunnerOption {
	return func(r *taskRunner) {
		r.logger = logger
	}
}

// WithRunnerFetchLimit set fetch tasks limit
func WithRunnerFetchLimit(limit int) RunnerOption {
	return func(r *taskRunner) {
		r.options.queryLimit = limit
	}
}

// WithRunnerParallelism set the parallelism for execute tasks.
func WithRunnerParallelism(parallelism int) RunnerOption {
	return func(r *taskRunner) {
		r.options.parallelism = parallelism
	}
}

// WithRunnerMaxWaitTasks set the maximum number of tasks waiting to be executed, more than that
// will block fetching tasks.
func WithRunnerMaxWaitTasks(maxWaitTasks int) RunnerOption {
	return func(r *taskRunner) {
		r.options.maxWaitTasks = maxWaitTasks
	}
}

// WithRunnerFetchInterval set fetch tasks interval duration
func WithRunnerFetchInterval(interval time.Duration) RunnerOption {
	return func(r *taskRunner) {
		r.options.fetchInterval = interval
	}
}

// WithRunnerFetchTimeout set fetch timeout
func WithRunnerFetchTimeout(timeout time.Duration) RunnerOption {
	return func(r *taskRunner) {
		r.options.fetchTimeout = timeout
	}
}

// WithRunnerHeartbeatInterval set heartbeat duration
func WithRunnerHeartbeatInterval(interval time.Duration) RunnerOption {
	return func(r *taskRunner) {
		r.options.heartbeatInterval = interval
	}
}

// WithOptions set all options needed by taskRunner
func WithOptions(
	queryLimit int,
	parallelism int,
	maxWaitTasks int,
	fetchInterval time.Duration,
	fetchTimeout time.Duration,
	retryInterval time.Duration,
	heartbeatInterval time.Duration,
) RunnerOption {
	return func(r *taskRunner) {
		r.options.queryLimit = queryLimit
		r.options.parallelism = parallelism
		r.options.maxWaitTasks = maxWaitTasks
		r.options.fetchInterval = fetchInterval
		r.options.fetchTimeout = fetchTimeout
		r.options.retryInterval = retryInterval
		r.options.heartbeatInterval = heartbeatInterval
	}
}

// WithRunnerRetryInterval set retry interval duration for operation
func WithRunnerRetryInterval(interval time.Duration) RunnerOption {
	return func(r *taskRunner) {
		r.options.retryInterval = interval
	}
}

type taskRunner struct {
	logger       *zap.Logger
	runnerID     string
	service      TaskService
	stopper      stopper.Stopper
	lastTaskID   uint64
	waitTasksC   chan task.Task
	parallelismC chan struct{}
	doneC        chan runningTask

	mu struct {
		sync.RWMutex
		started      bool
		executors    map[uint32]TaskExecutor
		runningTasks map[uint64]runningTask
		retryTasks   []runningTask
	}

	options struct {
		queryLimit        int
		parallelism       int
		maxWaitTasks      int
		fetchInterval     time.Duration
		fetchTimeout      time.Duration
		retryInterval     time.Duration
		heartbeatInterval time.Duration
	}
}

// NewTaskRunner new task runner. The TaskRunner can be created by CN nodes and pull tasks from TaskService to
// execute periodically.
func NewTaskRunner(runnerID string, service TaskService, opts ...RunnerOption) TaskRunner {
	r := &taskRunner{
		runnerID: runnerID,
		service:  service,
	}
	r.mu.executors = make(map[uint32]TaskExecutor)
	for _, opt := range opts {
		opt(r)
	}
	r.adjust()

	r.logger = logutil.Adjust(r.logger).Named("task-runner").With(zap.String("runner-id", r.runnerID))
	r.stopper = *stopper.NewStopper("task-runner", stopper.WithLogger(r.logger))
	r.parallelismC = make(chan struct{}, r.options.parallelism)
	r.waitTasksC = make(chan task.Task, r.options.maxWaitTasks)
	r.doneC = make(chan runningTask, r.options.maxWaitTasks)
	r.mu.runningTasks = make(map[uint64]runningTask)
	return r
}

func (r *taskRunner) adjust() {
	if r.options.parallelism == 0 {
		r.options.parallelism = runtime.NumCPU() / 16
		if r.options.parallelism == 0 {
			r.options.parallelism = 1
		}
	}
	if r.options.fetchInterval == 0 {
		r.options.fetchInterval = time.Second * 10
	}
	if r.options.fetchTimeout == 0 {
		r.options.fetchTimeout = time.Second * 5
	}
	if r.options.heartbeatInterval == 0 {
		r.options.heartbeatInterval = time.Second * 5
	}
	if r.options.maxWaitTasks == 0 {
		r.options.maxWaitTasks = 256
	}
	if r.options.queryLimit == 0 {
		r.options.queryLimit = r.options.parallelism
	}
	if r.options.retryInterval == 0 {
		r.options.retryInterval = time.Second
	}
}

func (r *taskRunner) ID() string {
	return r.runnerID
}

func (r *taskRunner) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.started {
		return nil
	}

	r.mu.started = true

	if err := r.stopper.RunNamedTask("fetch-task", r.fetch); err != nil {
		return err
	}
	if err := r.stopper.RunNamedTask("dispatch-task", r.dispatch); err != nil {
		return err
	}
	if err := r.stopper.RunNamedTask("done-task", r.done); err != nil {
		return err
	}
	if err := r.stopper.RunNamedTask("heartbeat-task", r.heartbeat); err != nil {
		return err
	}
	if err := r.stopper.RunNamedTask("retry-task", r.retry); err != nil {
		return err
	}
	return nil
}

func (r *taskRunner) Stop() error {
	r.mu.Lock()
	if !r.mu.started {
		r.mu.Unlock()
		return nil
	}
	r.mu.started = false
	r.mu.Unlock()

	r.stopper.Stop()
	close(r.waitTasksC)
	close(r.parallelismC)
	close(r.doneC)
	return nil
}

func (r *taskRunner) Parallelism() int {
	return r.options.parallelism
}

func (r *taskRunner) RegisterExecutor(code uint32, executor TaskExecutor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.mu.executors[code]; !ok {
		r.logger.Debug("executor registered", zap.Uint32("code", code))
		r.mu.executors[code] = executor
	}
}

func (r *taskRunner) fetch(ctx context.Context) {
	r.logger.Info("fetch task started")
	timer := time.NewTimer(r.options.fetchInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("fetch task stopped")
			return
		case <-timer.C:
			if !taskFrameworkDisabled() {
				tasks, err := r.doFetch()
				if err != nil {
					break
				}
				r.addTasks(ctx, tasks)
			}
		}
		timer.Reset(r.options.fetchInterval)
	}
}

func (r *taskRunner) doFetch() ([]task.Task, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.options.fetchTimeout)
	tasks, err := r.service.QueryTask(ctx,
		WithTaskIDCond(GT, r.lastTaskID),
		WithLimitCond(r.options.queryLimit),
		WithTaskRunnerCond(EQ, r.runnerID))
	cancel()
	if err != nil {
		r.logger.Error("fetch task failed", zap.Error(err))
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}

	r.lastTaskID = tasks[len(tasks)-1].ID
	r.logger.Debug("new task fetched",
		zap.Int("count", len(tasks)),
		zap.Uint64("last-task-id", r.lastTaskID))
	return tasks, nil
}

func (r *taskRunner) addTasks(ctx context.Context, tasks []task.Task) {
	for _, task := range tasks {
		r.addToWait(ctx, task)
	}
}

func (r *taskRunner) addToWait(ctx context.Context, task task.Task) bool {
	select {
	case <-ctx.Done():
		return false
	case r.waitTasksC <- task:
		r.logger.Debug("task added", zap.String("task", task.DebugString()))
		return true
	}
}

func (r *taskRunner) dispatch(ctx context.Context) {
	r.logger.Info("dispatch task started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("dispatch task stopped")
			return
		case task := <-r.waitTasksC:
			if !taskFrameworkDisabled() {
				r.runTask(ctx, task)
			}
		}
	}
}

func (r *taskRunner) retry(ctx context.Context) {
	r.logger.Info("retry task started")
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	var needRetryTasks []runningTask
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("retry task stopped")
			return
		case <-timer.C:
			if !taskFrameworkDisabled() {
				now := time.Now()
				needRetryTasks = needRetryTasks[:0]
				r.mu.Lock()
				for idx, rt := range r.mu.retryTasks {
					if rt.retryAt.After(now) {
						r.mu.retryTasks = r.mu.retryTasks[:copy(r.mu.retryTasks, r.mu.retryTasks[idx:])]
						break
					}
					needRetryTasks = append(needRetryTasks, rt)
				}
				r.mu.Unlock()
				if len(needRetryTasks) > 0 {
					for _, rt := range needRetryTasks {
						r.runTask(ctx, rt)
					}
				}
			}
		}
		timer.Reset(time.Millisecond * 100)
	}
}

func (r *taskRunner) runTask(ctx context.Context, value any) bool {
	select {
	case <-ctx.Done():
		return false
	case r.parallelismC <- struct{}{}:
		var rt runningTask
		switch value := value.(type) {
		case task.Task:
			rt = runningTask{task: value}
			rt.ctx, rt.cancel = context.WithCancel(ctx)
			r.mu.Lock()
			r.mu.runningTasks[rt.task.ID] = rt
			r.mu.Unlock()
		case runningTask:
			rt = value
		}

		r.run(rt)
		return true
	}
}

func (r *taskRunner) run(rt runningTask) {
	err := r.stopper.RunTask(func(ctx context.Context) {
		start := time.Now()
		r.logger.Debug("task start execute",
			zap.String("task", rt.task.DebugString()))
		defer r.logger.Debug("task execute completed",
			zap.String("task", rt.task.DebugString()),
			zap.Duration("cost", time.Since(start)))

		executor, err := r.getExecutor(rt.task.Metadata.Executor)
		result := &task.ExecuteResult{Code: task.ResultCode_Success}
		if err == nil {
			if err = executor(rt.ctx, rt.task); err == nil {
				goto taskDone
			}
		}

		// task failed
		r.logger.Error("run task failed",
			zap.String("task", rt.task.DebugString()),
			zap.Error(err))
		if rt.canRetry() {
			rt.retryTimes++
			rt.retryAt = time.Now().Add(time.Duration(rt.task.Metadata.Options.RetryInterval))
			if !r.addRetryTask(rt) {
				// retry queue is full, let scheduler re-allocate.
				r.removeRunningTask(rt.task.ID)
				r.releaseParallel()
			}
			return
		}
		result.Code = task.ResultCode_Failed
		result.Error = err.Error()
	taskDone:
		rt.task.ExecuteResult = result
		r.addDoneTask(rt)
	})
	if err != nil {
		r.logger.Error("run task failed", zap.Error(err))
	}
}

func (r *taskRunner) addDoneTask(rt runningTask) {
	r.releaseParallel()
	r.doneC <- rt
}

func (r *taskRunner) addRetryTask(task runningTask) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.mu.retryTasks) >= r.options.maxWaitTasks {
		return false
	}

	r.mu.retryTasks = append(r.mu.retryTasks, task)
	sort.Slice(r.mu.retryTasks, func(i, j int) bool {
		return r.mu.retryTasks[i].retryAt.Before(r.mu.retryTasks[j].retryAt)
	})
	return true
}

func (r *taskRunner) releaseParallel() {
	// other task can execute
	select {
	case <-r.parallelismC:
	default:
		panic("BUG")
	}
}

func (r *taskRunner) done(ctx context.Context) {
	r.logger.Info("done task started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("done task stopped")
			return
		case task := <-r.doneC:
			if !taskFrameworkDisabled() {
				r.doTaskDone(ctx, task)
			}
		}
	}
}

func (r *taskRunner) doTaskDone(ctx context.Context, rt runningTask) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		default:
			err := r.service.Complete(rt.ctx, r.runnerID, rt.task, *rt.task.ExecuteResult)
			if err == nil || moerr.IsMoErrCode(err, moerr.ErrInvalidTask) {
				r.removeRunningTask(rt.task.ID)
				return true
			}

			r.logger.Error("task done failed, retry later",
				zap.String("task", rt.task.DebugString()),
				zap.Error(err))
			time.Sleep(r.options.retryInterval)
		}
	}
}

func (r *taskRunner) heartbeat(ctx context.Context) {
	r.logger.Info("heartbeat task started")
	timer := time.NewTimer(r.options.heartbeatInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("heartbeat task stopped")
			return
		case <-timer.C:
			if !taskFrameworkDisabled() {
				r.doHeartbeat(ctx)
			}
		}
		timer.Reset(r.options.heartbeatInterval)
	}
}

func (r *taskRunner) doHeartbeat(ctx context.Context) {
	r.mu.RLock()
	tasks := make([]runningTask, 0, len(r.mu.runningTasks))
	for _, rt := range r.mu.runningTasks {
		tasks = append(tasks, rt)
	}
	r.mu.RUnlock()

	for _, rt := range tasks {
		if err := r.service.Heartbeat(ctx, rt.task); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidTask) {
				r.removeRunningTask(rt.task.ID)
				rt.cancel()
			}
			r.logger.Error("task heartbeat failed", zap.Error(err))
		}
	}
}

func (r *taskRunner) removeRunningTask(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.mu.runningTasks, id)
}

func (r *taskRunner) getExecutor(code uint32) (TaskExecutor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if executor, ok := r.mu.executors[code]; ok {
		return executor, nil
	}
	return nil, moerr.NewInternalErrorNoCtx("executor with code %d not exists", code)
}

type runningTask struct {
	task       task.Task
	ctx        context.Context
	cancel     context.CancelFunc
	retryTimes uint32
	retryAt    time.Time
}

func (rt runningTask) canRetry() bool {
	return rt.retryTimes < rt.task.Metadata.Options.MaxRetryTimes
}
