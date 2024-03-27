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
	"sync/atomic"
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

// WithRunnerHeartbeatTimeout set heartbeat timeout.
func WithRunnerHeartbeatTimeout(timeout time.Duration) RunnerOption {
	return func(r *taskRunner) {
		r.options.heartbeatTimeout = timeout
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
	heartbeatTimeout time.Duration,
) RunnerOption {
	return func(r *taskRunner) {
		r.options.queryLimit = queryLimit
		r.options.parallelism = parallelism
		r.options.maxWaitTasks = maxWaitTasks
		r.options.fetchInterval = fetchInterval
		r.options.fetchTimeout = fetchTimeout
		r.options.retryInterval = retryInterval
		r.options.heartbeatInterval = heartbeatInterval
		r.options.heartbeatTimeout = heartbeatTimeout
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
	waitTasksC   chan runningTask
	parallelismC chan struct{}
	doneC        chan runningTask

	started atomic.Bool

	executors struct {
		sync.RWMutex
		m map[task.TaskCode]TaskExecutor
	}

	runningTasks struct {
		sync.RWMutex
		m map[uint64]runningTask

		completedTasks map[uint64]struct{}
	}

	retryTasks struct {
		sync.Mutex
		s []runningTask
	}

	// accountID indicates the runner belongs to the account.
	canClaimDaemonTask func(string) bool

	pendingTaskHandle chan TaskHandler
	// daemonTasks contains all daemon tasks that run on this node.
	daemonTasks struct {
		sync.Mutex
		m map[uint64]*daemonTask
	}

	options struct {
		queryLimit        int
		parallelism       int
		maxWaitTasks      int
		fetchInterval     time.Duration
		fetchTimeout      time.Duration
		retryInterval     time.Duration
		heartbeatInterval time.Duration
		heartbeatTimeout  time.Duration
	}
}

// NewTaskRunner new task runner. The TaskRunner can be created by CN nodes and pull tasks from TaskService to
// execute periodically.
func NewTaskRunner(runnerID string, service TaskService, claimFn func(string) bool, opts ...RunnerOption) TaskRunner {
	r := &taskRunner{
		runnerID: runnerID,
		service:  service,
		// set the claim checker function for daemon task.
		canClaimDaemonTask: claimFn,
	}
	r.executors.m = make(map[task.TaskCode]TaskExecutor)
	for _, opt := range opts {
		opt(r)
	}
	r.adjust()

	r.logger = logutil.Adjust(r.logger).Named("task-runner").With(zap.String("runner-id", r.runnerID))
	r.stopper = *stopper.NewStopper("task-runner", stopper.WithLogger(r.logger))
	r.parallelismC = make(chan struct{}, r.options.parallelism)
	r.waitTasksC = make(chan runningTask, r.options.maxWaitTasks)
	r.doneC = make(chan runningTask, r.options.maxWaitTasks)
	r.runningTasks.m = make(map[uint64]runningTask)
	r.runningTasks.completedTasks = make(map[uint64]struct{})
	r.pendingTaskHandle = make(chan TaskHandler, 20)
	r.daemonTasks.m = make(map[uint64]*daemonTask)
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
		r.options.fetchTimeout = time.Second * 10
	}
	if r.options.heartbeatInterval == 0 {
		r.options.heartbeatInterval = time.Second * 5
	}
	if r.options.heartbeatTimeout == 0 {
		r.options.heartbeatTimeout = time.Second * 30
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
	if !r.started.CompareAndSwap(false, true) {
		return nil
	}
	if err := r.startAsyncTaskWorker(); err != nil {
		return err
	}
	if err := r.startDaemonTaskWorker(); err != nil {
		return err
	}
	return nil
}

func (r *taskRunner) startAsyncTaskWorker() error {
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
	if !r.started.CompareAndSwap(true, false) {
		return nil
	}

	r.stopper.Stop()
	close(r.waitTasksC)
	close(r.parallelismC)
	close(r.doneC)
	return nil
}

func (r *taskRunner) Parallelism() int {
	return r.options.parallelism
}

func (r *taskRunner) RegisterExecutor(code task.TaskCode, executor TaskExecutor) {
	r.executors.Lock()
	defer r.executors.Unlock()

	if _, ok := r.executors.m[code]; !ok {
		r.logger.Debug("executor registered", zap.Any("code", code))
		r.executors.m[code] = executor
	}
}

func (r *taskRunner) GetExecutor(code task.TaskCode) TaskExecutor {
	r.executors.RLock()
	defer r.executors.RUnlock()

	if executor, ok := r.executors.m[code]; ok {
		return executor
	}

	return nil
}

func (r *taskRunner) Attach(ctx context.Context, taskID uint64, routine ActiveRoutine) error {
	r.daemonTasks.Lock()
	defer r.daemonTasks.Unlock()
	t, ok := r.daemonTasks.m[taskID]
	if !ok {
		return moerr.NewErrTaskNotFound(ctx, taskID)
	}
	t.activeRoutine.Store(&routine)
	return nil
}

func (r *taskRunner) fetch(ctx context.Context) {
	r.logger.Debug("fetch task started")
	ticker := time.NewTicker(r.options.fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("fetch task stopped")
			return
		case <-ticker.C:
			if taskFrameworkDisabled() {
				continue
			}
			tasks, err := r.doFetch()
			if err != nil {
				r.logger.Error("fetch task failed", zap.Error(err))
				break
			}
			for _, t := range tasks {
				r.addToWait(ctx, t)
			}
		}
	}
}

func (r *taskRunner) doFetch() ([]task.AsyncTask, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.options.fetchTimeout)
	tasks, err := r.service.QueryAsyncTask(ctx,
		WithTaskStatusCond(task.TaskStatus_Running),
		WithLimitCond(r.options.queryLimit),
		WithTaskRunnerCond(EQ, r.runnerID))
	cancel()
	if err != nil {
		return nil, err
	}
	newTasks := tasks[:0]
	r.runningTasks.Lock()
	for _, t := range tasks {
		if _, ok := r.runningTasks.m[t.ID]; !ok {
			if _, ok := r.runningTasks.completedTasks[t.ID]; !ok {
				r.logger.Info("new task fetched",
					zap.String("task", t.DebugString()))
				newTasks = append(newTasks, t)
			}
		}
	}
	for k := range r.runningTasks.completedTasks {
		delete(r.runningTasks.completedTasks, k)
	}
	r.runningTasks.Unlock()

	if len(newTasks) == 0 {
		return nil, nil
	}

	return newTasks, nil
}

func (r *taskRunner) addToWait(ctx context.Context, task task.AsyncTask) bool {
	ctx2, cancel := context.WithCancel(ctx)
	rt := runningTask{
		task:   task,
		ctx:    ctx2,
		cancel: cancel,
	}

	select {
	case <-ctx.Done():
		return false
	case r.waitTasksC <- rt:
		r.runningTasks.Lock()
		r.runningTasks.m[task.ID] = rt
		r.runningTasks.Unlock()
		r.logger.Info("task added to wait queue",
			zap.String("task", task.DebugString()))
		return true
	}
}

func (r *taskRunner) dispatch(ctx context.Context) {
	r.logger.Debug("dispatch task started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("dispatch task stopped")
			return
		case rt := <-r.waitTasksC:
			if taskFrameworkDisabled() {
				continue
			}
			r.runTask(ctx, rt)
		}
	}
}

func (r *taskRunner) retry(ctx context.Context) {
	r.logger.Debug("retry task started")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var needRetryTasks []runningTask
	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("retry task stopped")
			return
		case <-ticker.C:
			if taskFrameworkDisabled() {
				continue
			}
			needRetryTasks = needRetryTasks[:0]
			r.retryTasks.Lock()
			for i, rt := range r.retryTasks.s {
				if rt.retryAt.After(time.Now()) {
					r.retryTasks.s = r.retryTasks.s[:copy(r.retryTasks.s, r.retryTasks.s[i:])]
					break
				}
				needRetryTasks = append(needRetryTasks, rt)
			}
			r.retryTasks.Unlock()
			for _, rt := range needRetryTasks {
				r.runTask(ctx, rt)
			}
		}
	}
}

func (r *taskRunner) runTask(ctx context.Context, rt runningTask) {
	select {
	case <-ctx.Done():
	case r.parallelismC <- struct{}{}:
		r.run(rt)
	}
}

func (r *taskRunner) run(rt runningTask) {
	err := r.stopper.RunTask(func(ctx context.Context) {
		start := time.Now()
		r.logger.Debug("task start execute",
			zap.String("task", rt.task.DebugString()))
		defer func() {
			r.logger.Debug("task execute completed",
				zap.String("task", rt.task.DebugString()),
				zap.Duration("cost", time.Since(start)))
		}()

		if executor, err := r.getExecutor(rt.task.Metadata.Executor); err != nil {
			r.taskExecResult(rt, err, false)
		} else if err := executor(rt.ctx, &rt.task); err != nil {
			r.taskExecResult(rt, err, true)
		} else {
			r.taskExecResult(rt, nil, false)
		}
	})
	if err != nil {
		r.logger.Error("run task failed", zap.Error(err))
	}
}

func (r *taskRunner) taskExecResult(rt runningTask, err error, mayRetry bool) {
	if err == nil {
		rt.task.ExecuteResult = &task.ExecuteResult{
			Code: task.ResultCode_Success,
		}
	} else {
		r.logger.Error("run task failed",
			zap.String("task", rt.task.DebugString()),
			zap.Error(err))
		rt.task.ExecuteResult = &task.ExecuteResult{
			Code:  task.ResultCode_Failed,
			Error: err.Error(),
		}
	}

	if mayRetry && rt.canRetry() {
		rt.retryTimes++
		rt.retryAt = time.Now().Add(time.Duration(rt.task.Metadata.Options.RetryInterval))
		if !r.addRetryTask(rt) {
			// retry queue is full, let scheduler re-allocate.
			r.removeRunningTask(rt.task.ID)
			r.releaseParallel()
		}
		return
	}
	r.addDoneTask(rt)
}

func (r *taskRunner) addDoneTask(rt runningTask) {
	r.releaseParallel()
	r.doneC <- rt
}

func (r *taskRunner) addRetryTask(task runningTask) bool {
	r.retryTasks.Lock()
	defer r.retryTasks.Unlock()
	if len(r.retryTasks.s) >= r.options.maxWaitTasks {
		return false
	}

	r.retryTasks.s = append(r.retryTasks.s, task)
	sort.Slice(r.retryTasks.s, func(i, j int) bool {
		return r.retryTasks.s[i].retryAt.Before(r.retryTasks.s[j].retryAt)
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
	r.logger.Debug("done task started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("done task stopped")
			return
		case rt := <-r.doneC:
			if taskFrameworkDisabled() {
				continue
			}
			r.doTaskDone(ctx, rt)
		}
	}
}

func (r *taskRunner) doTaskDone(ctx context.Context, rt runningTask) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-rt.ctx.Done():
			return false
		default:
			err := r.service.Complete(rt.ctx, r.runnerID, rt.task, *rt.task.ExecuteResult)
			if err == nil || moerr.IsMoErrCode(err, moerr.ErrInvalidTask) {
				r.removeRunningTask(rt.task.ID)
				r.logger.Info("task completed",
					zap.String("task", rt.task.DebugString()),
					zap.Error(err))
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
	r.logger.Debug("heartbeat task started")
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
			r.doHeartbeat(ctx)
		}
	}
}

func (r *taskRunner) doHeartbeat(ctx context.Context) {
	r.runningTasks.RLock()
	tasks := make([]runningTask, 0, len(r.runningTasks.m))
	for _, rt := range r.runningTasks.m {
		tasks = append(tasks, rt)
	}
	r.runningTasks.RUnlock()

	for _, rt := range tasks {
		if err := r.service.Heartbeat(ctx, rt.task); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidTask) {
				r.removeRunningTask(rt.task.ID)
				rt.cancel()
			}
			r.logger.Error("task heartbeat failed",
				zap.String("task", rt.task.DebugString()),
				zap.Error(err))
		}
	}
}

func (r *taskRunner) removeRunningTask(id uint64) {
	r.runningTasks.Lock()
	defer r.runningTasks.Unlock()
	delete(r.runningTasks.m, id)
	r.runningTasks.completedTasks[id] = struct{}{}
	r.logger.Info("task removed", zap.Uint64("task-id", id))
}

func (r *taskRunner) getExecutor(code task.TaskCode) (TaskExecutor, error) {
	r.executors.RLock()
	defer r.executors.RUnlock()

	if executor, ok := r.executors.m[code]; ok {
		return executor, nil
	}
	return nil, moerr.NewInternalErrorNoCtx("executor with code %d not exists", code)
}

type runningTask struct {
	task       task.AsyncTask
	ctx        context.Context
	cancel     context.CancelFunc
	retryTimes uint32
	retryAt    time.Time
}

func (rt runningTask) canRetry() bool {
	return rt.retryTimes < rt.task.Metadata.Options.MaxRetryTimes
}
