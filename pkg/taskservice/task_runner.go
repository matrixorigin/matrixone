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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util"
)

var (
	sqlTaskFetchFailed     = logutil.Event{Name: "task.sql.fetch.failed", Message: "SQL task recovery fetch failed"}
	sqlTaskExecutorMissing = logutil.Event{Name: "task.sql.executor.missing", Message: "SQL task executor not registered; task deferred"}
	sqlTaskExecutorEarly   = logutil.Event{Name: "task.sql.executor.early", Message: "SQL task executor returned before ownership"}
	sqlTaskHeartbeatFailed = logutil.Event{Name: "task.sql.heartbeat.failed", Message: "SQL task heartbeat failed"}
)

// RunnerOption option for create task runner
type RunnerOption func(*taskRunner)

// PauseTaskCompletedHook is called after a daemon task pause has been completed.
type PauseTaskCompletedHook func(context.Context, task.DaemonTask) error

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

func WithRunnerPauseTaskCompleted(hook PauseTaskCompletedHook) RunnerOption {
	return func(r *taskRunner) {
		r.options.pauseTaskCompleted = hook
	}
}

func WithHaKeeperClient(getClient func() util.HAKeeperClient) RunnerOption {
	return func(r *taskRunner) {
		r.hakeeper.getClient = getClient
	}
}

func WithCnUUID(uuid string) RunnerOption {
	return func(r *taskRunner) {
		r.hakeeper.cnUUID = uuid
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
	sqlFetchWake chan struct{}

	started     atomic.Bool
	sqlDeferred atomic.Uint64
	sqlBackoff  atomic.Pointer[sqlTaskDeferredParent]
	sqlFetch    sqlTaskFetchState // owned by the fetch goroutine

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
		m          map[uint64]*daemonTask
		admissions map[uint64]*daemonTask
	}

	pauseCompletedTasks struct {
		sync.Mutex
		m map[uint64]struct{}
	}

	options struct {
		queryLimit         int
		parallelism        int
		maxWaitTasks       int
		fetchInterval      time.Duration
		fetchTimeout       time.Duration
		retryInterval      time.Duration
		heartbeatInterval  time.Duration
		heartbeatTimeout   time.Duration
		pauseTaskCompleted PauseTaskCompletedHook
	}

	hakeeper struct {
		sync.RWMutex
		getClient func() util.HAKeeperClient
		cnUUID    string
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
	if service := sqlTaskServiceOwner(service); service != nil {
		service.sqlRecovery.setScheduledCapacity(r.options.parallelism)
	}

	r.logger = logutil.Adjust(r.logger).Named("task-runner").With(zap.String("runner-id", r.runnerID))
	r.stopper = *stopper.NewStopper("task-runner", stopper.WithLogger(r.logger))
	r.parallelismC = make(chan struct{}, r.options.parallelism)
	r.waitTasksC = make(chan runningTask, r.options.maxWaitTasks)
	r.doneC = make(chan runningTask, r.options.maxWaitTasks)
	r.sqlFetchWake = make(chan struct{}, 1)
	r.runningTasks.m = make(map[uint64]runningTask)
	r.runningTasks.completedTasks = make(map[uint64]struct{})
	r.pendingTaskHandle = make(chan TaskHandler, 20)
	r.daemonTasks.m = make(map[uint64]*daemonTask)
	r.daemonTasks.admissions = make(map[uint64]*daemonTask)
	r.pauseCompletedTasks.m = make(map[uint64]struct{})
	return r
}

func (r *taskRunner) adjust() {
	if r.options.parallelism == 0 {
		r.options.parallelism = runtime.NumCPU() / 4
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
	if origin, bound := ctx.Value(daemonAttachmentContextKey{}).(daemonAttachment); bound {
		if t != origin.owner || t.claimLost.Load() || !sameDaemonClaim(t.taskSnapshot(), origin.claim) {
			return moerr.NewInvalidTask(ctx, origin.claim.TaskRunner, taskID)
		}
	}
	t.activeRoutine.Store(&routine)
	return nil
}

func (r *taskRunner) fetch(ctx context.Context) {
	r.logger.Debug("fetch task started")
	ticker := time.NewTicker(r.options.fetchInterval)
	defer ticker.Stop()

	extra := 32
	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("fetch task stopped")
			return
		case <-ticker.C:
			extra = 32
			r.fetchTasks(ctx)
		case <-r.sqlFetchWake:
			for i := 0; i < 4 && extra > 0; i++ {
				// A ready periodic poll takes precedence over another hint.
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					extra = 32
					r.fetchTasks(ctx)
					i = 4
					continue
				default:
				}
				extra--
				if !r.fetchTasks(ctx) {
					break
				}
			}
		}
	}
}

// fetchTasks returns false when a wake burst should stop; the periodic timer
// will retry after capacity, cancellation, or a storage failure.
func (r *taskRunner) fetchTasks(ctx context.Context) bool {
	if taskFrameworkDisabled() || ctx.Err() != nil || len(r.waitTasksC) == cap(r.waitTasksC) {
		return false
	}
	fetchCtx, cancel := context.WithTimeoutCause(ctx, r.options.fetchTimeout, moerr.CauseDoFetch)
	defer cancel()
	tasks, err := r.doFetch(fetchCtx, ctx)
	if err != nil {
		r.reportFetchError(ctx, err)
		return false
	}
	var firstID uint64
	if len(tasks) != 0 {
		firstID = tasks[0].ID
	}
	tasks, held, err := groupSQLTaskBatch(fetchCtx, tasks)
	if err != nil {
		r.revisitUnqueued(ctx, firstID)
		r.reportFetchError(ctx, moerr.AttachCause(fetchCtx, err))
		return false
	}
	if len(held) != 0 {
		if r.sqlFetch.active {
			r.sqlFetch.pendingWork = true
		} else if r.sqlDeferred.Load() == r.sqlFetch.observed {
			// Normal fetching may already have armed the same revisit.
			r.sqlDeferred.Add(1)
		}
	}
	continueBurst := true
	for _, t := range tasks {
		if err := fetchCtx.Err(); err != nil {
			r.revisitUnqueued(ctx, t.ID)
			r.reportFetchError(ctx, moerr.AttachCause(fetchCtx, err))
			return false
		}
		if !r.addToWait(ctx, t, held[t.ID]) {
			r.revisitUnqueued(ctx, t.ID)
			return false
		}
		if t.Metadata.Executor == task.TaskCode_SQLTask {
			// Let this batch reach admission before another hint fetch can
			// select a sibling from a different page of the same parent.
			continueBurst = false
		}
	}
	return continueBurst
}

func (r *taskRunner) reportFetchError(runnerCtx context.Context, err error) {
	if runnerCtx.Err() != nil {
		return
	}
	if r.sqlFetch.active {
		sqlTaskFetchFailed.Warn(zap.Error(err))
	} else {
		r.logger.Error("fetch task failed", zap.Error(err))
	}
}

// A fair cursor can rewind directly. A normal fetch has no cursor, so keep
// recovery armed if an unqueued candidate was interrupted while the runner
// remains live. An already-signaled generation also covers this revisit.
func (r *taskRunner) revisitUnqueued(ctx context.Context, id uint64) {
	if id == 0 {
		return
	}
	if r.sqlFetch.last != nil {
		r.sqlFetch.rewind(id)
		return
	}
	if ctx.Err() == nil && !r.sqlFetch.active && r.sqlDeferred.Load() == r.sqlFetch.observed &&
		sqlTaskServiceOwner(r.service) != nil {
		r.sqlDeferred.Add(1)
	}
}

// Leave same-parent firings in storage until one selected firing has had an
// opportunity to acquire the parent guard. Other parents remain eligible.
func groupSQLTaskBatch(ctx context.Context, tasks []task.AsyncTask) ([]task.AsyncTask, map[uint64]bool, error) {
	if len(tasks) < 2 {
		return tasks, nil, ctx.Err()
	}
	type group struct {
		id      uint64
		count   int
		emitted bool
	}
	var groups map[string]group
	for _, value := range tasks {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if value.Metadata.Executor != task.TaskCode_SQLTask {
			continue
		}
		if _, ok := sqlTaskParentID(value.ParentTaskID); !ok {
			continue
		}
		if groups == nil {
			groups = make(map[string]group)
		}
		g := groups[value.ParentTaskID]
		if g.count == 0 || value.ID < g.id {
			g.id = value.ID
		}
		g.count++
		groups[value.ParentTaskID] = g
	}
	var held map[uint64]bool
	selected := tasks[:0]
	for _, value := range tasks {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if g, ok := groups[value.ParentTaskID]; ok && value.Metadata.Executor == task.TaskCode_SQLTask {
			if value.ID != g.id || g.emitted {
				continue
			}
			g.emitted = true
			groups[value.ParentTaskID] = g
			if g.count > 1 {
				if held == nil {
					held = make(map[uint64]bool)
				}
				held[value.ID] = true
			}
		}
		selected = append(selected, value)
	}
	return selected, held, nil
}

func (r *taskRunner) doFetch(parent, runnerCtx context.Context) ([]task.AsyncTask, error) {
	if err := parent.Err(); err != nil {
		if runnerCtx.Err() == nil && sqlTaskServiceOwner(r.service) != nil && !r.sqlFetch.active {
			r.sqlDeferred.Add(1)
		}
		return nil, moerr.AttachCause(parent, err)
	}
	ctx, cancel := context.WithTimeoutCause(parent, r.options.fetchTimeout, moerr.CauseDoFetch)
	defer cancel()
	var tasks []task.AsyncTask
	var err error
	var trustedSource bool
	service := sqlTaskServiceOwner(r.service)
	fair := service != nil && r.useSQLTaskFairFetch(service)
	if fair {
		tasks, err = r.doSQLTaskFairFetch(ctx, service)
	} else {
		conditions := []Condition{
			WithTaskStatusCond(task.TaskStatus_Running),
			WithLimitCond(r.options.queryLimit),
			WithTaskRunnerCond(EQ, r.runnerID),
			withSQLTaskUnclaimed(),
		}
		if service == nil {
			conditions = append(conditions, withSQLTaskLane(false))
		}
		if service == nil {
			tasks, err = r.service.QueryAsyncTask(ctx, conditions...)
		} else {
			tasks, err, trustedSource = r.queryNormalTaskPage(ctx, service, conditions...)
		}
	}
	if err != nil {
		if runnerCtx.Err() == nil && service != nil && !r.sqlFetch.active {
			r.sqlDeferred.Add(1)
		}
		return nil, moerr.AttachCause(ctx, err)
	}
	armed := service != nil && !fair && !trustedSource && runnerCtx.Err() == nil
	if armed {
		r.sqlDeferred.Add(1)
	}
	if !fair && !trustedSource {
		limit := max(r.options.queryLimit, 1)
		if service != nil {
			limit = min(limit, 32)
		}
		if len(tasks) > limit {
			tasks = tasks[:limit]
		}
	}
	var firstID uint64
	if len(tasks) != 0 {
		firstID = tasks[0].ID
	}
	if err := ctx.Err(); err != nil {
		r.sqlFetch.rewind(firstID)
		if service != nil && !fair && !armed && runnerCtx.Err() == nil {
			r.sqlDeferred.Add(1)
		}
		return nil, moerr.AttachCause(ctx, err)
	}
	available := cap(r.waitTasksC) - len(r.waitTasksC)
	newTasks := tasks[:0]
	truncated := false
	r.runningTasks.Lock()
	for _, t := range tasks {
		if err := ctx.Err(); err != nil {
			r.runningTasks.Unlock()
			r.sqlFetch.rewind(firstID)
			if service != nil && !fair && !armed && runnerCtx.Err() == nil {
				r.sqlDeferred.Add(1)
			}
			return nil, moerr.AttachCause(ctx, err)
		}
		if t.ID == 0 || t.Status != task.TaskStatus_Running || t.TaskRunner != r.runnerID ||
			service == nil && t.Metadata.Executor == task.TaskCode_SQLTask {
			continue
		}
		if _, ok := r.runningTasks.m[t.ID]; !ok {
			if _, ok := r.runningTasks.completedTasks[t.ID]; !ok {
				if len(newTasks) >= available {
					truncated = true
					continue
				}
				if t.Metadata.Executor == task.TaskCode_SQLTask {
					r.logger.Debug("new SQL task fetched", zap.Uint64("task-id", t.ID))
				} else {
					r.logger.Info("new task fetched", zap.String("task", t.DebugString()))
				}
				newTasks = append(newTasks, t)
			}
		}
	}
	for k := range r.runningTasks.completedTasks {
		delete(r.runningTasks.completedTasks, k)
	}
	r.runningTasks.Unlock()
	if service != nil && !fair && !armed && runnerCtx.Err() == nil && (truncated ||
		len(tasks) >= r.options.queryLimit && len(newTasks) < len(tasks) && available > 0) {
		r.sqlDeferred.Add(1)
	}

	if len(newTasks) == 0 {
		return nil, nil
	}

	return newTasks, nil
}

func (r *taskRunner) addToWait(ctx context.Context, value task.AsyncTask, hasWithheldSiblings bool) bool {
	ctx2, cancel := context.WithCancelCause(ctx)
	enqueued := false
	defer func() {
		if !enqueued {
			cancel(nil)
		}
	}()
	rt := runningTask{
		task:                value,
		ctx:                 ctx2,
		cancel:              cancel,
		hasWithheldSiblings: hasWithheldSiblings,
	}

	select {
	case <-ctx.Done():
		return false
	default:
	}

	r.runningTasks.Lock()
	defer r.runningTasks.Unlock()
	if _, ok := r.runningTasks.m[value.ID]; ok {
		return true
	}
	if _, ok := r.runningTasks.completedTasks[value.ID]; ok {
		return true
	}
	select {
	case r.waitTasksC <- rt:
		r.runningTasks.m[value.ID] = rt
		enqueued = true
		if value.Metadata.Executor == task.TaskCode_SQLTask {
			r.logger.Debug("SQL task added to wait queue", zap.Uint64("task-id", value.ID))
		} else {
			r.logger.Info("task added to wait queue", zap.String("task", value.DebugString()))
		}
		return true
	default:
	}
	return false
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
			needRetryTasks = r.retryDueTasks(time.Now(), needRetryTasks)
		}
	}
}

// retryDueTasks removes all retry entries whose deadline has passed and
// dispatches them. Keeping the queue transition separate from the ticker
// makes the retry contract deterministic for tests and keeps the scheduler
// responsible only for choosing when to check.
func (r *taskRunner) retryDueTasks(now time.Time, needRetryTasks []runningTask) []runningTask {
	needRetryTasks = needRetryTasks[:0]
	r.retryTasks.Lock()
	next := 0
	for ; next < len(r.retryTasks.s); next++ {
		rt := r.retryTasks.s[next]
		if rt.retryAt.After(now) {
			break
		}
		needRetryTasks = append(needRetryTasks, rt)
	}
	remaining := copy(r.retryTasks.s, r.retryTasks.s[next:])
	clear(r.retryTasks.s[remaining:])
	r.retryTasks.s = r.retryTasks.s[:remaining]
	r.retryTasks.Unlock()

	for _, rt := range needRetryTasks {
		r.run(rt)
	}
	return needRetryTasks
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
		if rt.task.Metadata.Executor == task.TaskCode_SQLTask {
			if parent := r.deferredSQLTaskParent(); parent != nil && parent.parent == rt.task.ParentTaskID {
				r.removeRunningTask(rt, nil)
				r.releaseParallel()
				return
			}
		}

		if executor, err := r.getExecutor(rt.task.Metadata.Executor); err != nil {
			if rt.task.Metadata.Executor == task.TaskCode_SQLTask {
				r.sqlDeferred.Add(1)
				sqlTaskExecutorMissing.Warn(zap.Uint64("task-id", rt.task.ID), zap.Error(err))
				r.removeRunningTask(rt, nil)
				r.releaseParallel()
				return
			}
			r.taskExecResult(rt, err, false)
		} else {
			var disposition sqlTaskRunnerDisposition
			execCtx := rt.ctx
			if rt.task.Metadata.Executor == task.TaskCode_SQLTask {
				execCtx = context.WithValue(execCtx, sqlTaskDispositionContextKey{}, &disposition)
			}
			err := executor(execCtx, &rt.task)
			if rt.task.Metadata.Executor == task.TaskCode_SQLTask && !disposition.handled {
				// A wrapper may reject a firing before calling the SQL executor.
				// Never use generic completion: another invocation may have claimed it.
				if sqlTaskRunnerStopOnly(err, execCtx.Err()) {
					disposition.deferred = true
				} else {
					sqlTaskExecutorEarly.Warn(zap.Uint64("task-id", rt.task.ID), zap.Error(err))
					owner := sqlTaskServiceOwner(r.service)
					claim := SQLTaskClaim{ID: rt.task.ID, Runner: rt.task.TaskRunner, Epoch: rt.task.Epoch}
					if owner == nil || !owner.finishUnclaimedSQLTask(execCtx, claim, err, 0) {
						disposition.deferred = true
					}
				}
				disposition.handled = true
			}
			if disposition.handled {
				if disposition.deferred {
					if disposition.failed {
						r.recordDeferredSQLTaskParent(rt.task.ParentTaskID)
					} else {
						r.sqlDeferred.Add(1)
					}
				}
				r.removeRunningTask(rt, nil)
				r.releaseParallel()
				if rt.hasWithheldSiblings && !disposition.deferred && ctx.Err() == nil && r.started.Load() {
					select {
					case r.sqlFetchWake <- struct{}{}:
					default:
					}
				}
				return
			}
			r.taskExecResult(rt, err, err != nil)
		}
	})
	if err != nil {
		r.logger.Error("run task failed", zap.Error(err))
	}
}

// sqlTaskRunnerStopOnly recognizes only the runner's stop, not a precheck
// failure joined with it. Unknown identity hooks and error trees that cannot
// be fully inspected within the fixed budget remain selected results.
func sqlTaskRunnerStopOnly(err, stop error) bool {
	if err == nil || stop == nil {
		return false
	}
	const limit = 64
	var pending [limit]error
	pending[0] = err
	size := 1
	for visited := 0; size > 0; visited++ {
		if visited == limit {
			return false
		}
		size--
		current := pending[size]
		pending[size] = nil
		if current == nil {
			return false
		}
		if converted, ok := current.(*moerr.Error); ok {
			if converted == nil || !converted.Is(stop) {
				return false
			}
			continue
		}
		if _, ok := current.(interface{ Is(error) bool }); ok {
			return false
		}
		if _, ok := current.(interface{ As(any) bool }); ok {
			return false
		}
		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			if len(children) == 0 || len(children) > limit-size {
				return false
			}
			for _, child := range children {
				pending[size] = child
				size++
			}
		case interface{ Unwrap() error }:
			pending[size] = wrapped.Unwrap()
			size++
		default:
			if current != stop { //nolint:errorlint // only the actual stop sentinel proves cancellation
				return false
			}
		}
	}
	return true
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
			r.removeRunningTask(rt, nil)
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
	slices.SortFunc(r.retryTasks.s, func(a, b runningTask) int {
		return a.retryAt.Compare(b.retryAt)
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
				r.removeRunningTask(rt, nil)
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
				r.removeRunningTask(rt, err)
			}
			if rt.task.Metadata.Executor == task.TaskCode_SQLTask {
				sqlTaskHeartbeatFailed.Warn(zap.Uint64("task-id", rt.task.ID), zap.Error(err))
			} else {
				r.logger.Error("task heartbeat failed", zap.String("task", rt.task.DebugString()), zap.Error(err))
			}
		}
	}
}

// A late completion or heartbeat belongs to its original invocation, not a
// newer local execution of the same async task ID.
func (r *taskRunner) removeRunningTask(rt runningTask, cause error) {
	r.runningTasks.Lock()
	current, exists := r.runningTasks.m[rt.task.ID]
	if exists && current.ctx == rt.ctx {
		delete(r.runningTasks.m, rt.task.ID)
		r.runningTasks.completedTasks[rt.task.ID] = struct{}{}
	} else {
		exists = false
	}
	r.runningTasks.Unlock()
	if rt.cancel != nil {
		rt.cancel(cause)
	}
	if !exists {
		return
	}
	if rt.task.Metadata.Executor == task.TaskCode_SQLTask {
		r.logger.Debug("SQL task removed", zap.Uint64("task-id", rt.task.ID))
	} else {
		r.logger.Info("task removed", zap.Uint64("task-id", rt.task.ID))
	}
}

func (r *taskRunner) getExecutor(code task.TaskCode) (TaskExecutor, error) {
	r.executors.RLock()
	defer r.executors.RUnlock()

	if executor, ok := r.executors.m[code]; ok {
		return executor, nil
	}
	return nil, moerr.NewInternalErrorNoCtxf("executor with code %d not exists", code)
}

type runningTask struct {
	task                task.AsyncTask
	ctx                 context.Context
	cancel              context.CancelCauseFunc
	retryTimes          uint32
	retryAt             time.Time
	hasWithheldSiblings bool
}

func (rt runningTask) canRetry() bool {
	return rt.retryTimes < rt.task.Metadata.Options.MaxRetryTimes
}
