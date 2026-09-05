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

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var CDCExectorError_QueryDaemonTaskTimeout = moerr.NewInternalErrorNoCtx("query daemon task timeout")

// All CDC tasks in one CN share the same admission controller because they
// allocate from the same cgroup/host memory budget.
var cnInitialSnapshotLimiter = cdc.NewInitialSnapshotLimiter()

var CDCExeutorAllocator *mpool.MPool

var (
	eventCDCExecutorRestartStarting = logutil.Event{Name: "frontend.cdc.executor.restart.starting", Message: "CDC executor restart is starting a replacement generation"}
	eventCDCExecutorRestartReady    = logutil.Event{Name: "frontend.cdc.executor.restart.ready", Message: "CDC executor replacement generation is running"}
	eventCDCExecutorRestartFailed   = logutil.Event{Name: "frontend.cdc.executor.restart.start-failed", Message: "CDC executor replacement generation failed to start"}
	eventCDCExecutorRestartClearErr = logutil.Event{Name: "frontend.cdc.executor.restart.clear-errors-failed", Message: "CDC executor could not clear table errors before restart"}
)

func init() {
	var err error
	mpool.DeleteMPool(CDCExeutorAllocator)
	if CDCExeutorAllocator, err = mpool.NewMPool("cdc_executor", 0, mpool.NoFixed); err != nil {
		panic(err)
	}
}

func CDCTaskExecutorFactory(
	logger *zap.Logger,
	sqlExecutorFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cnUUID string,
	ts taskservice.TaskService,
	fs fileservice.FileService,
	txnClient client.TxnClient,
	txnEngine engine.Engine,
) taskservice.TaskExecutor {
	return func(ctx context.Context, spec task.Task) error {
		ctx1, cancel := context.WithTimeoutCause(
			ctx, time.Second*5, CDCExectorError_QueryDaemonTaskTimeout,
		)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(
			ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, spec.GetID()),
		)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return moerr.NewInternalErrorf(ctx, "invalid tasks count %d", len(tasks))
		}
		claim, ok := spec.(*task.DaemonTask)
		if !ok || claim.TaskRunner != cnUUID || tasks[0].TaskRunner != claim.TaskRunner ||
			!tasks[0].LastRun.Equal(claim.LastRun) {
			return moerr.NewInvalidTask(ctx, cnUUID, spec.GetID())
		}
		details, ok := tasks[0].Details.Details.(*task.Details_CreateCdc)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}

		exec := NewCDCTaskExecutor(
			logger,
			sqlExecutorFactory(),
			details.CreateCdc,
			cnUUID,
			fs,
			txnClient,
			txnEngine,
			CDCExeutorAllocator,
		)
		exec.taskService = ts
		exec.UpdateDaemonTaskClaim(*claim)
		// Restart timeout persistence is a control-plane path. It must use a
		// fresh executor so it cannot queue behind the serialized executor held
		// by the Start attempt that just timed out.
		exec.restartCatalogExecutorFactory = sqlExecutorFactory
		exec.setActiveRoutine(cdc.NewCdcActiveRoutine())
		// Bind replacement generations to the task-runner lifecycle before
		// publishing the ActiveRoutine. Resume/Restart can then never detach a
		// replacement Start from runner/CN shutdown.
		exec.bindLifecycleContext(ctx)
		// Attach publishes the executor to taskservice cancellation. Enter a
		// cancelable state first so Cancel can always fence a Start that has not
		// entered the factory call below yet.
		if err = exec.stateMachine.Transition(TransitionStart); err != nil {
			exec.cancelLifecycleContext()
			return err
		}
		if err = attachToTask(ctx, spec.GetID(), exec); err != nil {
			exec.cancelLifecycleContext()
			return err
		}
		if err = exec.Start(ctx); err != nil {
			// Attach transferred lifetime ownership to taskservice. Start's done
			// notification may already have admitted a same-object replacement;
			// only generation-fenced runner completion may cancel that lifetime.
			return err
		}
		return nil
	}
}

type CDCTaskExecutor struct {
	sync.Mutex

	logger  *zap.Logger
	claimMu sync.RWMutex
	ie      ie.InternalExecutor

	cnUUID      string
	claimTask   *task.DaemonTask
	claimFence  *cdc.OwnerFence
	taskService taskservice.TaskService
	cnTxnClient client.TxnClient
	cnEngine    engine.Engine
	fileService fileservice.FileService

	spec *task.CreateCdcDetails

	mp         *mpool.MPool
	packerPool *fileservice.Pool[*types.Packer]

	sinkUri               cdc.UriInfo
	tables                cdc.PatternTuples
	exclude               *regexp.Regexp
	startTs, endTs        types.TS
	stableInitialSnapshot bool
	noFull                bool
	additionalConfig      map[string]interface{}
	// initialSnapshotLimiter bounds retained initial-snapshot batches across all
	// CDC tasks in this CN while allowing tables to make progress independently.
	initialSnapshotLimiter *cdc.InitialSnapshotLimiter

	activeRoutineMu sync.RWMutex
	activeRoutine   *cdc.ActiveRoutine
	// watermarkUpdater update the watermark of the items that has been sunk to downstream
	watermarkUpdater *cdc.CDCWatermarkUpdater
	// runningReaders store the running execute pipelines, map key pattern: db.table
	runningReaders *sync.Map
	// removedReaderShutdowns stores in-progress shutdowns for readers that disappeared from scan results.
	removedReaderShutdowns sync.Map

	// stateMachine manages executor state transitions
	stateMachine *ExecutorStateMachine
	holdCh       chan int

	callbackMu                    sync.RWMutex
	callbackCount                 int
	callbackDone                  chan struct{}
	callbackCtx                   context.Context
	callbackCancel                context.CancelFunc
	callbackGeneration            atomic.Uint64
	readerStopMu                  sync.Mutex
	readerShutdownMu              sync.Mutex
	readerShutdownDone            <-chan struct{}
	restartWaitMu                 sync.Mutex
	restartWaiters                map[uint64]chan error
	restartCatalogState           map[uint64]string
	restartMu                     sync.Mutex
	restartCatalogMu              sync.Mutex
	restartCatalogPersistence     *cdcRestartCatalogPersistence
	restartCatalogExecutorFactory func() ie.InternalExecutor
	startAttemptMu                sync.Mutex
	activeStartAttempt            *cdcStartAttempt
	lifecycleMu                   sync.RWMutex
	lifecycleCtx                  context.Context
	lifecycleCancel               context.CancelFunc
	lifecycleRootStop             func() bool
	lifecycleTaskScheduler        taskservice.TaskExecutorTaskScheduler
	// restartStartupTimeout is test-only when non-zero. Production keeps the
	// historical four-second admission bound.
	restartStartupTimeout time.Duration
	// restartStartupTimeoutSignal lets tests deterministically choose when the
	// replacement-startup wait times out. Production leaves it nil and uses
	// restartStartupTimeout through a real timer.
	restartStartupTimeoutSignal <-chan time.Time

	// start wrapper, for ut
	startFunc func(ctx context.Context) error
}

// cdcStartAttempt owns one invocation of Start. A replacement never begins
// until the prior attempt has exited, so the executor's legacy shared
// lifecycle fields (activeRoutine, runningReaders, and detector registration)
// cannot be cleaned up by an older generation after being reused.
type cdcStartAttempt struct {
	generation   uint64
	cancel       context.CancelFunc
	done         chan struct{}
	doneOnce     sync.Once
	timeoutFence atomic.Uint64
	restartOwner atomic.Uint32
}

const (
	cdcRestartOwnerPending uint32 = iota
	cdcRestartOwnerCompleted
	cdcRestartOwnerTimedOut
)

func (attempt *cdcStartAttempt) completeRestart() bool {
	return attempt.restartOwner.CompareAndSwap(
		cdcRestartOwnerPending,
		cdcRestartOwnerCompleted,
	)
}

func (attempt *cdcStartAttempt) timeoutRestart() bool {
	return attempt.restartOwner.CompareAndSwap(
		cdcRestartOwnerPending,
		cdcRestartOwnerTimedOut,
	)
}

// cdcRestartCatalogPersistence owns the bounded best-effort catalog write for
// one timed-out restart generation. Restart never waits for this write on its
// timeout return path. A later retry does wait for completion before admitting
// a new generation, preventing the old restarting -> failed CAS from racing the
// new generation's restarting -> running publication.
type cdcRestartCatalogPersistence struct {
	done chan struct{}
	err  error
}

type cdcStartAttemptContextKey struct{}

func newCDCStartAttempt(ctx context.Context, generation uint64) (context.Context, *cdcStartAttempt) {
	ctx, cancel := context.WithCancel(ctx)
	attempt := &cdcStartAttempt{
		generation: generation,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	return context.WithValue(ctx, cdcStartAttemptContextKey{}, attempt), attempt
}

// bindLifecycleContext captures only the cancellation lifetime of the
// task-runner context. Replacement attempts must inherit runner/CN shutdown,
// but must not inherit attempt-specific values such as a fresh-takeover
// restart admission marker.
func (exec *CDCTaskExecutor) bindLifecycleContext(rootCtx context.Context) {
	exec.lifecycleMu.Lock()
	defer exec.lifecycleMu.Unlock()
	if exec.lifecycleCtx != nil {
		return
	}
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	exec.lifecycleCtx, exec.lifecycleCancel = context.WithCancel(context.Background())
	exec.lifecycleRootStop = context.AfterFunc(rootCtx, exec.lifecycleCancel)
	exec.lifecycleTaskScheduler = taskservice.TaskExecutorTaskSchedulerFromContext(rootCtx)
}

func (exec *CDCTaskExecutor) replacementStartContext() context.Context {
	exec.lifecycleMu.RLock()
	defer exec.lifecycleMu.RUnlock()
	if exec.lifecycleCtx == nil {
		// Direct unit construction predates lifecycle binding. Production binds
		// before ActiveRoutine publication, and Start binds as a safety net.
		return context.Background()
	}
	return exec.lifecycleCtx
}

func (exec *CDCTaskExecutor) cancelLifecycleContext() {
	exec.lifecycleMu.Lock()
	cancel := exec.lifecycleCancel
	stop := exec.lifecycleRootStop
	exec.lifecycleRootStop = nil
	exec.lifecycleMu.Unlock()
	if stop != nil {
		stop()
	}
	if cancel != nil {
		cancel()
	}
}

// callbackContextLocked returns the context for the currently admitted table
// detector generation. The runner lifecycle remains live across Restart and
// only this generation context is canceled when replacing callbacks.
func (exec *CDCTaskExecutor) callbackContextLocked() context.Context {
	if exec.callbackCtx == nil {
		exec.callbackCtx, exec.callbackCancel = context.WithCancel(exec.replacementStartContext())
	}
	return exec.callbackCtx
}

func (exec *CDCTaskExecutor) rotateCallbackContextLocked() {
	if exec.callbackCancel != nil {
		exec.callbackCancel()
	}
	exec.callbackCtx = nil
	exec.callbackCancel = nil
	if exec.stateMachine.State() != StateCancelling && exec.stateMachine.State() != StateCancelled {
		exec.callbackContextLocked()
	}
}

func (exec *CDCTaskExecutor) cancelCallbackContextLocked() {
	if exec.callbackCancel != nil {
		exec.callbackCancel()
	}
	exec.callbackCtx = nil
	exec.callbackCancel = nil
}

func (exec *CDCTaskExecutor) runLifecycleTask(
	name string,
	task func(),
) error {
	return exec.runLifecycleContextTask(name, func(context.Context) {
		task()
	})
}

func (exec *CDCTaskExecutor) runLifecycleContextTask(
	name string,
	task func(context.Context),
) error {
	exec.lifecycleMu.RLock()
	scheduler := exec.lifecycleTaskScheduler
	exec.lifecycleMu.RUnlock()
	if scheduler == nil {
		// Direct construction is retained for unit tests and legacy embedding.
		// Production TaskExecutors always capture the task-runner scheduler in
		// bindLifecycleContext.
		go task(exec.replacementStartContext())
		return nil
	}
	return scheduler(name, task)
}

func cdcStartAttemptFromContext(ctx context.Context) *cdcStartAttempt {
	attempt, _ := ctx.Value(cdcStartAttemptContextKey{}).(*cdcStartAttempt)
	return attempt
}

func (exec *CDCTaskExecutor) installStartAttempt(attempt *cdcStartAttempt) bool {
	exec.startAttemptMu.Lock()
	defer exec.startAttemptMu.Unlock()
	if exec.activeStartAttempt != nil {
		return false
	}
	exec.activeStartAttempt = attempt
	return true
}

func (exec *CDCTaskExecutor) beginImplicitStartAttempt(ctx context.Context) (context.Context, *cdcStartAttempt, error) {
	ctx, attempt := newCDCStartAttempt(ctx, exec.callbackGeneration.Load())
	if !exec.installStartAttempt(attempt) {
		attempt.cancel()
		return nil, nil, moerr.NewInternalErrorNoCtx("CDC start already has an active generation")
	}
	return ctx, attempt, nil
}

func (exec *CDCTaskExecutor) activeStart() *cdcStartAttempt {
	exec.startAttemptMu.Lock()
	defer exec.startAttemptMu.Unlock()
	return exec.activeStartAttempt
}

func (exec *CDCTaskExecutor) isActiveStartAttempt(attempt *cdcStartAttempt) bool {
	if attempt == nil {
		return false
	}
	exec.startAttemptMu.Lock()
	defer exec.startAttemptMu.Unlock()
	return exec.activeStartAttempt == attempt
}

func (exec *CDCTaskExecutor) finishStartAttempt(attempt *cdcStartAttempt) {
	if attempt == nil {
		return
	}
	exec.startAttemptMu.Lock()
	if exec.activeStartAttempt == attempt {
		exec.activeStartAttempt = nil
	}
	exec.startAttemptMu.Unlock()
	// Clear the active owner before waking a replacement waiter. Otherwise a
	// waiter can observe done, race installStartAttempt, and falsely conclude
	// that the completed attempt is still active.
	// Cancel also detaches this child from the long-lived lifecycle context.
	// Without it, every completed Resume/Restart generation remains retained
	// by that parent until the whole executor shuts down.
	attempt.cancel()
	attempt.doneOnce.Do(func() { close(attempt.done) })
}

func (exec *CDCTaskExecutor) isCurrentStartAttempt(attempt *cdcStartAttempt) bool {
	if attempt == nil || !exec.isCurrentCallbackGeneration(attempt.generation) {
		return false
	}
	exec.startAttemptMu.Lock()
	defer exec.startAttemptMu.Unlock()
	return exec.activeStartAttempt == attempt
}

func (exec *CDCTaskExecutor) restartTimeout() time.Duration {
	if exec.restartStartupTimeout > 0 {
		return exec.restartStartupTimeout
	}
	return 4 * time.Second
}

func (exec *CDCTaskExecutor) waitForRestartStartup(
	completion <-chan error,
	timeout time.Duration,
) (error, bool) {
	if exec.restartStartupTimeoutSignal != nil {
		return selectCDCCompletion(completion, exec.restartStartupTimeoutSignal)
	}
	return waitForCDCCompletion(completion, timeout)
}

func waitForCDCCompletion[T any](
	completion <-chan T,
	timeout time.Duration,
) (T, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	return selectCDCCompletion(completion, timer.C)
}

func selectCDCCompletion[T any](
	completion <-chan T,
	timeout <-chan time.Time,
) (T, bool) {
	select {
	case result := <-completion:
		return result, false
	case <-timeout:
		// A completion that became ready with the timer is authoritative. A
		// plain select can choose the timeout arm pseudo-randomly when both are
		// ready and turn an on-time restart into a false failure.
		select {
		case result := <-completion:
			return result, false
		default:
			var zero T
			return zero, true
		}
	}
}

func (exec *CDCTaskExecutor) setActiveRoutine(routine *cdc.ActiveRoutine) {
	exec.activeRoutineMu.Lock()
	exec.activeRoutine = routine
	exec.activeRoutineMu.Unlock()
}

func (exec *CDCTaskExecutor) currentActiveRoutine() *cdc.ActiveRoutine {
	exec.activeRoutineMu.RLock()
	defer exec.activeRoutineMu.RUnlock()
	return exec.activeRoutine
}

func (exec *CDCTaskExecutor) closeActiveRoutinePause() {
	if routine := exec.currentActiveRoutine(); routine != nil {
		routine.ClosePause()
	}
}

func (exec *CDCTaskExecutor) closeActiveRoutineCancel() {
	if routine := exec.currentActiveRoutine(); routine != nil {
		routine.CloseCancel()
	}
}

func (exec *CDCTaskExecutor) beginRestartWaiter(generation uint64, catalogState string) chan error {
	exec.restartWaitMu.Lock()
	defer exec.restartWaitMu.Unlock()
	if exec.restartWaiters == nil {
		exec.restartWaiters = make(map[uint64]chan error)
	}
	if exec.restartCatalogState == nil {
		exec.restartCatalogState = make(map[uint64]string)
	}
	ready := make(chan error, 1)
	exec.restartWaiters[generation] = ready
	exec.restartCatalogState[generation] = catalogState
	return ready
}

func (exec *CDCTaskExecutor) finishRestartWaiter(generation uint64, err error) bool {
	exec.restartWaitMu.Lock()
	defer exec.restartWaitMu.Unlock()
	ready := exec.restartWaiters[generation]
	if ready == nil {
		return false
	}
	select {
	case ready <- err:
	default:
	}
	return true
}

func (exec *CDCTaskExecutor) removeRestartWaiter(generation uint64) {
	exec.restartWaitMu.Lock()
	defer exec.restartWaitMu.Unlock()
	delete(exec.restartWaiters, generation)
	delete(exec.restartCatalogState, generation)
}

func (exec *CDCTaskExecutor) restartCatalogStateForGeneration(generation uint64) (string, bool) {
	exec.restartWaitMu.Lock()
	defer exec.restartWaitMu.Unlock()
	if state := exec.restartCatalogState[generation]; state != "" {
		return state, true
	}
	return "", false
}

// publishStartupCatalogTransition moves the durable catalog admission to
// running. The caller must still claim the attempt's in-memory completion
// token before publishing readiness: timeout and completion race on that token,
// rather than on channel scheduling.
func (exec *CDCTaskExecutor) publishStartupCatalogTransition(
	ctx context.Context,
	generation uint64,
	restartAdmission bool,
) (required bool, updateErr error) {
	catalogState, hasRestartCatalogState := exec.restartCatalogStateForGeneration(generation)
	required = hasRestartCatalogState || restartAdmission
	updateErr = exec.updateErrMsgForStartup(
		ctx,
		"",
		catalogState,
		hasRestartCatalogState,
		restartAdmission,
	)
	return required, updateErr
}

func (exec *CDCTaskExecutor) restartFields(fields ...zap.Field) []zap.Field {
	out := logutil.StringFingerprintFields("task-id", exec.spec.TaskId)
	out = append(out, logutil.StringFingerprintFields("task-name", exec.spec.TaskName)...)
	return append(out, fields...)
}

func NewCDCTaskExecutor(
	logger *zap.Logger,
	ie ie.InternalExecutor,
	spec *task.CreateCdcDetails,
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	cdcMp *mpool.MPool,
) *CDCTaskExecutor {
	task := &CDCTaskExecutor{
		logger:      logger,
		ie:          ie,
		spec:        spec,
		cnUUID:      cnUUID,
		fileService: fileService,
		cnTxnClient: cnTxnClient,
		cnEngine:    cnEngine,
		mp:          cdcMp,
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
		stateMachine:           NewExecutorStateMachine(), // Initialize state machine
		holdCh:                 make(chan int, 1),         // Initialize holdCh to prevent race condition
		initialSnapshotLimiter: cnInitialSnapshotLimiter,
	}
	task.startFunc = task.Start
	return task
}

// currentDaemonClaimFence returns the immutable claim generation installed by
// taskservice. Existing table streams retain the old object when Resume or
// Restart publishes a replacement claim, so stale work cannot borrow the new
// generation's identity.
func (exec *CDCTaskExecutor) currentDaemonClaimFence() *cdc.OwnerFence {
	exec.claimMu.RLock()
	defer exec.claimMu.RUnlock()
	return exec.claimFence
}

func classifyStableSnapshotRestart(
	watermark types.TS,
	watermarkGeneration uint64,
	sourceTableID uint64,
	state cdc.InitialSnapshotEpochState,
) (incomplete, resetTarget, metadataMissing, generationAhead bool) {
	hasProgress := !watermark.IsEmpty()
	generationAhead = watermarkGeneration > sourceTableID || state.HasNewerGeneration
	sameGeneration := watermarkGeneration == sourceTableID
	// A same-generation watermark cannot exist before its immutable epoch. A
	// non-empty generation-zero watermark with no retired epoch is likewise not
	// attributable to this stable protocol and must fail closed.
	metadataMissing = state.Created && hasProgress &&
		(sameGeneration || (watermarkGeneration == 0 && !state.HasOtherGeneration))
	incomplete = !sameGeneration || watermark.LT(&state.Epoch)
	resetTarget = incomplete && (state.HasOtherGeneration ||
		(hasProgress && watermarkGeneration > 0 && watermarkGeneration < sourceTableID))
	return
}

func shouldCompactStableSnapshotEpochs(
	targetWillReset bool,
	incomplete bool,
	hasOtherGeneration bool,
) bool {
	return targetWillReset || (!incomplete && hasOtherGeneration)
}

func capInitialSnapshotEpoch(candidate, end types.TS) types.TS {
	if !end.IsEmpty() && candidate.GT(&end) {
		return end
	}
	return candidate
}

// UpdateDaemonTaskClaim advances the exact token used by target and watermark
// fences after taskservice has durably installed a Resume/Restart generation.
func (exec *CDCTaskExecutor) UpdateDaemonTaskClaim(claim task.DaemonTask) {
	exec.claimMu.Lock()
	if exec.claimTask != nil &&
		exec.claimTask.ID == claim.ID &&
		exec.claimTask.TaskRunner == claim.TaskRunner &&
		exec.claimTask.LastRun.Equal(claim.LastRun) {
		// Status is an authority phase within one immutable generation. Keep the
		// same fence pointer (existing pipelines own it), but advance the snapshot
		// used by future durable checks, notably RestartRequested -> Running.
		claimCopy := claim
		exec.claimTask = &claimCopy
		exec.claimMu.Unlock()
		return
	}
	claimCopy := claim
	service := exec.taskService
	var fence *cdc.OwnerFence
	if service != nil {
		fence = cdc.NewOwnerFenceForGeneration(claimCopy.LastRun, func(ctx context.Context) error {
			// Resume/Restart on this CN publishes a new immutable fence before
			// starting replacement work. Reject a delayed old pipeline locally.
			// taskservice canonicalizes and monotonically advances the persisted
			// microsecond last_run token, so status-only publication is the only
			// path that intentionally retains this fence pointer.
			exec.claimMu.RLock()
			currentFence := exec.claimFence
			currentClaim := exec.claimTask
			var validationClaim task.DaemonTask
			if currentClaim != nil {
				validationClaim = *currentClaim
			}
			exec.claimMu.RUnlock()
			if currentFence != fence || currentClaim == nil {
				return moerr.NewInvalidTask(ctx, claimCopy.TaskRunner, claimCopy.ID)
			}
			fenceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return service.ValidateDaemonTask(fenceCtx, validationClaim)
		})
	}
	exec.claimTask = &claimCopy
	exec.claimFence = fence
	exec.claimMu.Unlock()
}

func (exec *CDCTaskExecutor) Start(rootCtx context.Context) (err error) {
	// Factory binding closes the attach-before-start publication window.
	// Retain this fallback for direct callers and tests.
	exec.bindLifecycleContext(rootCtx)
	attempt := cdcStartAttemptFromContext(rootCtx)
	if attempt == nil {
		rootCtx, attempt, err = exec.beginImplicitStartAttempt(rootCtx)
		if err != nil {
			return err
		}
	} else if !exec.isActiveStartAttempt(attempt) {
		if !exec.installStartAttempt(attempt) {
			attempt.cancel()
			return moerr.NewInternalErrorNoCtx("CDC start already has an active generation")
		}
	}
	// Keep the attempt installed until all cleanup below has completed. A
	// replacement is admitted only after done is closed, which makes the
	// resource cleanup below generation-owned rather than best-effort.
	defer exec.finishStartAttempt(attempt)
	if !exec.isCurrentStartAttempt(attempt) {
		return moerr.NewInternalErrorNoCtx("CDC start was superseded by a newer lifecycle generation")
	}
	state := exec.stateMachine.State()
	if state == StateCancelling || state == StateCancelled {
		return moerr.NewInternalErrorNoCtx("CDC start was canceled before execution")
	}

	taskId := exec.spec.TaskId
	taskName := exec.spec.TaskName
	cnUUID := exec.cnUUID
	accountId := uint32(exec.spec.Accounts[0].GetId())
	restartGeneration := attempt.generation
	restartAdmission := taskservice.IsRestartAdmission(rootCtx)
	detector := cdc.GetTableDetector(cnUUID)
	var (
		registered      bool
		enteredStarting = exec.stateMachine.State() == StateStarting
	)

	// A fresh takeover retry may follow a prior startup failure that durably
	// moved the catalog admission to failed before taskservice released its
	// daemon claim. Reopen that exact failure state; an already-restarting first
	// attempt is accepted idempotently, while PAUSE/DROP remains conflicting.
	if restartAdmission {
		if err = exec.admitRestartCatalogState(rootCtx); err != nil {
			return err
		}
	}

	// Check if this task is already registered in TableDetector
	// This prevents duplicate task execution when taskservice schedules the same task twice
	if detector.IsTaskRegistered(taskId) {
		logutil.Warn(
			"cdc.frontend.task.already_registered",
			zap.String("task-id", taskId),
			zap.String("task-name", taskName),
			zap.String("cn-uuid", cnUUID),
			zap.Uint32("account-id", accountId),
			zap.String("reason", "task is already registered in TableDetector, skipping duplicate start"),
		)
		return moerr.NewInternalErrorf(rootCtx, "task %s is already running", taskId)
	}

	defer func() {
		if err != nil {
			if registered {
				detector.UnRegister(taskId)
			}

			// A timed-out generation may finish later. It still owns the
			// detector/routine until this defer returns, but it no longer owns
			// lifecycle state, metrics, catalog state, or a restart waiter.
			// Never let that late completion overwrite its replacement.
			ownsLifecycle := enteredStarting && exec.isCurrentStartAttempt(attempt)
			if ownsLifecycle {
				if setFailErr := exec.stateMachine.SetFailed(err.Error()); setFailErr != nil {
					logutil.Warn(
						"cdc.frontend.task.set_state_failed",
						zap.String("target-state", StateFailed.String()),
						zap.Error(setFailErr),
					)
				}

				// Metrics: task failed
				v2.CdcTaskTotalGauge.WithLabelValues("failed").Inc()
				v2.CdcTaskErrorCounter.WithLabelValues("start_failed", "false").Inc()
			}

			// Start retains exclusive ownership of these resources until its
			// attempt finishes. Close them even when the generation has been
			// fenced so a timed-out startup cannot leak workers.
			exec.closeActiveRoutinePause()
			exec.closeActiveRoutineCancel()

			if ownsLifecycle {
				catalogState, hasRestartCatalogState := exec.restartCatalogStateForGeneration(restartGeneration)
				restartFailed := false
				if attempt.completeRestart() {
					restartFailed = exec.finishRestartWaiter(restartGeneration, err)
				}
				updateErrMsgErr := exec.updateErrMsgForStartup(rootCtx, err.Error(), catalogState, hasRestartCatalogState, restartAdmission)
				if restartFailed || restartAdmission {
					eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
						fields := append([]zap.Field{
							zap.String("state", exec.stateMachine.State().String()),
						}, logutil.ErrorFingerprintFields("error", err)...)
						if updateErrMsgErr != nil {
							fields = append(
								fields,
								logutil.ErrorFingerprintFields(
									"catalog-update-error",
									updateErrMsgErr,
								)...,
							)
						}
						return exec.restartFields(fields...)
					})
				} else {
					logutil.Error(
						"cdc.frontend.task.start_failed",
						zap.String("task-id", taskId),
						zap.String("task-name", taskName),
						zap.String("state", exec.stateMachine.State().String()),
						zap.Error(err),
						zap.NamedError("update-err-msg-err", updateErrMsgErr),
					)
				}
			}
		}
	}()

	ctx := defines.AttachAccountId(rootCtx, accountId)

	// get cdc task definition
	if err = exec.retrieveCdcTask(ctx); err != nil {
		return err
	}

	dbs := make([]string, 0, len(exec.tables.Pts))
	tables := make([]string, 0, len(exec.tables.Pts))
	for _, pt := range exec.tables.Pts {
		dbs = append(dbs, pt.Source.Database)
		tables = append(tables, pt.Source.Table)
	}

	// Clean up old readers instead of replacing the map
	// This ensures old readers are properly stopped and prevents goroutine leaks
	if exec.runningReaders != nil {
		exec.runningReaders.Range(func(key, value interface{}) bool {
			reader := value.(cdc.ChangeReader)
			reader.Close()
			return true
		})

		exec.runningReaders.Range(func(key, value interface{}) bool {
			reader := value.(cdc.ChangeReader)
			reader.Wait()
			return true
		})

		exec.runningReaders.Range(func(key, value interface{}) bool {
			exec.runningReaders.Delete(key)
			return true
		})
	} else {
		exec.runningReaders = &sync.Map{}
	}

	// start watermarkUpdater
	exec.watermarkUpdater = cdc.GetCDCWatermarkUpdater(exec.cnUUID, exec.ie)

	// register to table scanner
	callbackGeneration := restartGeneration
	if !detector.RegisterIfAbsent(taskId, accountId, dbs, tables, func(tbls map[uint32]cdc.TblMap) error {
		return exec.handleNewTablesForGeneration(callbackGeneration, tbls)
	}) {
		logutil.Warn(
			"cdc.frontend.task.duplicate_registration_detected",
			zap.String("task-id", taskId),
			zap.String("task-name", taskName),
			zap.String("cn-uuid", cnUUID),
			zap.Uint32("account-id", accountId),
			zap.String("reason", "RegisterIfAbsent rejected duplicate task"),
		)
		return moerr.NewInternalErrorf(ctx, "task %s is already running", taskId)
	}
	registered = true

	// Transition to Starting state (skip if already Starting, e.g., from Resume)
	if exec.stateMachine.State() != StateStarting {
		if err = exec.stateMachine.Transition(TransitionStart); err != nil {
			detector.UnRegister(taskId)
			registered = false
			return moerr.NewInternalErrorf(ctx, "cannot start: %v", err)
		}
	}
	enteredStarting = true

	logutil.Info(
		"cdc.frontend.task.start",
		zap.String("task-id", taskId),
		zap.String("task-name", taskName),
		zap.String("cn-uuid", cnUUID),
		zap.Uint32("account-id", accountId),
		zap.String("state", exec.stateMachine.State().String()),
	)

	// A restart waiter may have timed out while this Start was still doing its
	// admission work. Its generation is then invalidated, so it must not publish
	// a late Running state into a newer restart attempt.
	if !exec.isCurrentStartAttempt(attempt) {
		return moerr.NewInternalErrorNoCtx("CDC start was superseded by a newer lifecycle generation")
	}

	// Transition to Running state
	if err = exec.stateMachine.Transition(TransitionStartSuccess); err != nil {
		return moerr.NewInternalErrorf(ctx, "cannot transition to running: %v", err)
	}

	requiredRestartTransition, clearErrMsgErr :=
		exec.publishStartupCatalogTransition(
			ctx,
			restartGeneration,
			restartAdmission,
		)
	if requiredRestartTransition && clearErrMsgErr != nil {
		return moerr.NewInternalErrorf(
			ctx,
			"cannot publish CDC restart catalog state: %v",
			clearErrMsgErr,
		)
	}

	// A timeout and catalog publication can cross while ExecWithStatus is in
	// flight. The attempt token is the in-memory linearization point: only one
	// side may claim the result. If timeout won, repair a catalog write that
	// committed late and enter normal error cleanup without publishing metrics
	// or leaving the detector registered.
	if !exec.isCurrentStartAttempt(attempt) ||
		(requiredRestartTransition && !attempt.completeRestart()) {
		if requiredRestartTransition && clearErrMsgErr == nil {
			exec.reconcileTimedOutStartupPublication(attempt)
		}
		return moerr.NewInternalErrorNoCtx("CDC start was superseded by a newer lifecycle generation")
	}

	restartReady := false
	if requiredRestartTransition {
		restartReady = exec.finishRestartWaiter(restartGeneration, nil)
	}

	// Metrics and readiness are published only after the required restart
	// catalog transition succeeds. Ordinary CREATE startup keeps its historical
	// best-effort error-message cleanup semantics.
	v2.CdcTaskTotalGauge.WithLabelValues("running").Inc()
	v2.CdcTaskStateChangeCounter.WithLabelValues("starting", "running").Inc()

	if restartReady {
		eventCDCExecutorRestartReady.InfoLazy(func() []zap.Field {
			return exec.restartFields(zap.String("state", exec.stateMachine.State().String()))
		})
	} else {
		logutil.Info(
			"cdc.frontend.task.start_success",
			zap.String("task-id", taskId),
			zap.String("task-name", taskName),
			zap.String("state", exec.stateMachine.State().String()),
			zap.NamedError("clear-err-msg-err", clearErrMsgErr),
		)
	}

	// hold - wait for Pause/Cancel/Restart signal
	select {
	case <-ctx.Done():
		break
	case <-exec.holdCh:
		break
	}
	return
}

// Resume cdc task from last recorded watermark
func (exec *CDCTaskExecutor) Resume() error {
	exec.callbackMu.Lock()
	callbackLocked := true
	defer func() {
		if callbackLocked {
			exec.callbackMu.Unlock()
		}
	}()

	// If the table detector has not completed permanent-error cleanup yet, the
	// executor is still Running and RESUME only needs to clear the persisted
	// table errors. If cleanup won the race, the executor is Failed and follows
	// the ordinary resume replacement below, which rebuilds from the recorded
	// watermarks without applying restart/reset-watermark semantics.
	if exec.stateMachine.State() == StateRunning {
		ctx := defines.AttachAccountId(context.Background(), uint32(exec.spec.Accounts[0].GetId()))
		if err := exec.clearAllTableErrors(ctx); err != nil {
			return moerr.NewInternalErrorf(context.Background(), "cannot clear CDC table errors: %v", err)
		}
		logutil.Info(
			"cdc.frontend.task.resume_running_recovery",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.String("state", exec.stateMachine.State().String()),
		)
		return nil
	}

	stateBeforeResume := exec.stateMachine.State()
	if !exec.previousReaderGenerationStoppedLocked(stateBeforeResume) {
		return moerr.NewInternalErrorNoCtx("cannot resume: previous CDC reader generation is still stopping")
	}
	// Paused and table-error Failed executions both resume from their recorded
	// watermarks. Other failure recovery remains the explicit RESTART command.
	if err := exec.stateMachine.Transition(TransitionResume); err != nil {
		return moerr.NewInternalErrorf(context.Background(), "cannot resume: %v", err)
	}
	exec.recordLeavingFailedMetrics(stateBeforeResume, StateStarting)
	generation := exec.callbackGeneration.Add(1)
	exec.rotateCallbackContextLocked()
	failedRecovery := stateBeforeResume == StateFailed
	var (
		recoveryReady   chan error
		recoveryAttempt atomic.Pointer[cdcStartAttempt]
	)
	if failedRecovery {
		// The durable CDC catalog row remains Failed until Start has rebuilt the
		// execution from its existing watermarks. Reuse the bounded startup
		// publication waiter so taskservice cannot publish daemon Running merely
		// because the replacement goroutine was scheduled.
		recoveryReady = exec.beginRestartWaiter(generation, cdc.CDCState_Failed)
		defer exec.removeRestartWaiter(generation)
	}

	// Log watermark states before resume
	exec.logCurrentWatermarks("before_resume")

	logutil.Info(
		"cdc.frontend.task.resume_start",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
		zap.String("state", exec.stateMachine.State().String()),
	)
	resumeScheduled := false
	defer func() {
		if !resumeScheduled {
			return
		}
		if stateBeforeResume == StatePaused {
			// Failed recovery was accounted when it left Failed above. Only a
			// normal paused resume owns the paused -> starting metrics.
			v2.CdcTaskTotalGauge.WithLabelValues("paused").Dec()
			v2.CdcTaskStateChangeCounter.WithLabelValues("paused", "starting").Inc()
		}

		logutil.Info(
			"cdc.frontend.task.resume_success",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.String("state", exec.stateMachine.State().String()),
		)
	}()

	// Clear all table errors before resuming
	// This allows tables with non-retryable errors to be retried after user fixes the issues
	ctx := defines.AttachAccountId(context.Background(), uint32(exec.spec.Accounts[0].GetId()))
	if err := exec.clearAllTableErrors(ctx); err != nil {
		if failedRecovery {
			if failErr := exec.stateMachine.SetFailed(err.Error()); failErr == nil {
				v2.CdcTaskTotalGauge.WithLabelValues("failed").Inc()
				v2.CdcTaskStateChangeCounter.WithLabelValues("starting", "failed").Inc()
			}
			return moerr.NewInternalErrorf(context.Background(), "cannot clear CDC table errors: %v", err)
		}
		logutil.Warn(
			"cdc.frontend.task.resume_clear_errors_failed",
			zap.String("task-id", exec.spec.TaskId),
			zap.Error(err),
		)
		// Don't fail Resume if clearing errors fails - continue anyway
	}

	// FIX: Unmark task as paused to allow watermark updates
	if exec.watermarkUpdater != nil {
		exec.watermarkUpdater.UnmarkTaskPaused(exec.spec.TaskId)
	}

	if err := exec.runLifecycleTask("cdc-resume-replacement", func() {
		// Pause releases Start through holdCh, but its goroutine can still be
		// unwinding while Resume returns. Preserve the same resource-ownership
		// rule as Restart: do not replace activeRoutine until that Start exits.
		lifecycleCtx := exec.replacementStartContext()
		if previous := exec.activeStart(); previous != nil {
			select {
			case <-previous.done:
			case <-lifecycleCtx.Done():
				return
			}
		}
		if !exec.isCurrentCallbackGeneration(generation) {
			return
		}

		startCtx, attempt := newCDCStartAttempt(lifecycleCtx, generation)
		if !exec.installStartAttempt(attempt) {
			// A concurrent lifecycle operation owns the newer generation. Its
			// own result is authoritative; this stale resume must stay silent.
			attempt.cancel()
			return
		}
		defer exec.finishStartAttempt(attempt)
		if failedRecovery {
			recoveryAttempt.Store(attempt)
		}

		// closed in Pause, need renew
		if !exec.isCurrentCallbackGeneration(generation) {
			return
		}
		exec.setActiveRoutine(cdc.NewCdcActiveRoutine())
		if !exec.isCurrentCallbackGeneration(generation) {
			exec.closeActiveRoutineCancel()
			return
		}
		if err := exec.startFunc(startCtx); err != nil {
			if failedRecovery && attempt.completeRestart() {
				exec.finishRestartWaiter(generation, err)
			}
			logutil.Error(
				"cdc.frontend.task.resume_start_failed",
				zap.String("task-id", exec.spec.TaskId),
				zap.String("task-name", exec.spec.TaskName),
				zap.String("state", exec.stateMachine.State().String()),
				zap.Error(err),
			)
		} else {
			if failedRecovery && attempt.completeRestart() {
				exec.finishRestartWaiter(generation, nil)
			}
			// Log watermark states after resume completed
			exec.logCurrentWatermarks("after_resume")
		}
	}); err != nil {
		if stateBeforeResume == StateFailed {
			if failErr := exec.stateMachine.SetFailed(err.Error()); failErr == nil {
				v2.CdcTaskTotalGauge.WithLabelValues("failed").Inc()
				v2.CdcTaskStateChangeCounter.WithLabelValues("starting", "failed").Inc()
			}
		}
		return moerr.NewInternalErrorf(context.Background(), "cannot schedule CDC resume replacement: %v", err)
	}
	resumeScheduled = !failedRecovery
	if !failedRecovery {
		return nil
	}

	// Start and its table-detector callback must not wait behind the control
	// mutex while taskservice waits for durable recovery readiness.
	exec.callbackMu.Unlock()
	callbackLocked = false
	if err, timedOut := exec.waitForRestartStartup(recoveryReady, exec.restartTimeout()); !timedOut {
		resumeScheduled = err == nil
		return err
	}

	timeoutErr := moerr.NewInternalErrorNoCtx("CDC resume recovery startup timed out")
	attempt := recoveryAttempt.Load()
	if attempt != nil && !attempt.timeoutRestart() {
		// Completion won the generation token even if the timeout channel became
		// ready at the same instant. Its buffered result is authoritative.
		err := <-recoveryReady
		resumeScheduled = err == nil
		return err
	}
	if attempt != nil {
		attempt.cancel()
		attempt.timeoutFence.Store(generation + 1)
	}
	exec.closeActiveRoutineCancel()
	select {
	case exec.holdCh <- 1:
	default:
	}
	if !exec.callbackGeneration.CompareAndSwap(generation, generation+1) && attempt != nil {
		attempt.timeoutFence.Store(0)
	}
	stateBeforeTimeout := exec.stateMachine.State()
	if err := exec.stateMachine.SetFailed(timeoutErr.Error()); err == nil {
		v2.CdcTaskTotalGauge.WithLabelValues("failed").Inc()
		v2.CdcTaskStateChangeCounter.WithLabelValues(
			cdcTaskMetricStateLabel(stateBeforeTimeout),
			"failed",
		).Inc()
	}
	return timeoutErr
}

// Restart cdc task from init watermark
func (exec *CDCTaskExecutor) Restart() error {
	// A restart generation spans the in-memory transition, Start ownership,
	// and timeout catalog publication. Serializing the full operation closes
	// the entry-time TOCTOU window where a concurrent retry could pass the
	// persistence check before the older generation installs its pending
	// restarting -> failed write. Fail fast instead of waiting on the mutex:
	// every Restart caller must retain a bounded response time.
	if !exec.restartMu.TryLock() {
		return moerr.NewInternalErrorNoCtx("CDC restart is already in progress")
	}
	defer exec.restartMu.Unlock()

	timeout := exec.restartTimeout()
	if _, timedOut := exec.waitForRestartCatalogPersistence(timeout); timedOut {
		return moerr.NewInternalErrorNoCtx("CDC restart timed out waiting for the previous timeout record")
	}

	exec.callbackMu.Lock()

	stateBeforeRestart := exec.stateMachine.State()
	if !exec.previousReaderGenerationStoppedLocked(stateBeforeRestart) {
		exec.callbackMu.Unlock()
		return moerr.NewInternalErrorNoCtx("cannot restart: previous CDC reader generation is still stopping")
	}
	shouldStopOldExecution := stateBeforeRestart == StateRunning || stateBeforeRestart == StateStarting
	shouldClearTableErrors := stateBeforeRestart == StateFailed || stateBeforeRestart == StatePaused

	// Transition to Restarting state
	if err := exec.stateMachine.Transition(TransitionRestart); err != nil {
		exec.callbackMu.Unlock()
		return moerr.NewInternalErrorf(context.Background(), "cannot restart: %v", err)
	}
	exec.recordLeavingFailedMetrics(stateBeforeRestart, StateRestarting)
	generation := exec.callbackGeneration.Add(1)
	exec.rotateCallbackContextLocked()
	// Complete the lifecycle/generation critical section before performing
	// potentially slow cleanup or waiting for the replacement. Existing table
	// detector callbacks captured the previous generation and will reject
	// themselves after this fence.
	if err := exec.stateMachine.Transition(TransitionRestartBegin); err != nil {
		exec.callbackMu.Unlock()
		return moerr.NewInternalErrorf(context.Background(), "cannot begin restart: %v", err)
	}
	// The catalog restart request has already installed this durable admission
	// marker. Requiring it at ready/failure publication prevents a concurrent
	// PAUSE that reaches paused from being changed back to running.
	ready := exec.beginRestartWaiter(generation, cdc.CDCState_Restarting)
	callbackDone := exec.callbackDone
	if callbackDone == nil {
		callbackDone = closedChan()
	}
	exec.callbackMu.Unlock()
	defer exec.removeRestartWaiter(generation)
	if _, timedOut := waitForCDCCompletion(callbackDone, timeout); timedOut {
		drainTimeoutErr := moerr.NewInternalErrorNoCtx("CDC restart timed out waiting for table detector callbacks")
		// TransitionRestartBegin has already published local Starting. Fence the
		// timed-out generation and restore a retryable Failed state; otherwise the
		// durable RestartRequested owner retries into an in-memory state from
		// which TransitionRestart is impossible.
		exec.callbackGeneration.Add(1)
		cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
		exec.closeActiveRoutineCancel()
		// Register completion ownership before returning. Do not add the normal
		// ten-second synchronous reader wait to an already expired four-second
		// restart control path; the next durable retry is gated on this channel.
		exec.initiateReaderShutdown()
		select {
		case exec.holdCh <- 1:
		default:
		}
		_ = exec.stateMachine.SetFailed(drainTimeoutErr.Error())
		exec.recordRestartTimeoutAsync(nil, drainTimeoutErr)
		return drainTimeoutErr
	}
	// A Start owns mutable executor resources until it exits. Do not publish a
	// replacement while a previous Start can still run its deferred cleanup.
	// This is intentionally stronger than a generation check: generation fences
	// publication, while this drain fence protects ownership of the legacy
	// shared fields themselves.
	if oldAttempt := exec.activeStart(); oldAttempt != nil {
		oldAttempt.cancel()
		cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
		exec.closeActiveRoutineCancel()
		select {
		case <-exec.holdCh:
		default:
		}
		select {
		case exec.holdCh <- 1:
		default:
		}

		if _, timedOut := waitForCDCCompletion(oldAttempt.done, timeout); timedOut {
			drainTimeoutErr := moerr.NewInternalErrorNoCtx("CDC restart startup timed out while waiting for previous start to exit")
			// The replacement has not been launched, so the stale attempt is
			// the only owner of the shared resources. Fence its later failure
			// publication and persist this terminal admission result.
			exec.callbackGeneration.Add(1)
			_ = exec.stateMachine.SetFailed(drainTimeoutErr.Error())
			exec.recordRestartTimeoutAsync(nil, drainTimeoutErr)
			return drainTimeoutErr
		}
	} else if shouldStopOldExecution {
		// Some callers created the old run before start-attempt ownership was
		// introduced. Preserve its cleanup contract while the rollout has both
		// forms in flight.
		cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
		exec.closeActiveRoutineCancel()
		select {
		case <-exec.holdCh:
		default:
		}
		select {
		case exec.holdCh <- 1:
		default:
		}
	}

	// The first restart request normally changed the catalog to restarting
	// before reaching this executor. A retry after a timeout starts from the
	// failed state we recorded above, so reopen that exact state explicitly.
	if stateBeforeRestart == StateFailed {
		if err := exec.admitRestartCatalogState(context.Background()); err != nil {
			_ = exec.stateMachine.SetFailed(err.Error())
			return err
		}
	}

	// FIX: Unmark task as paused to allow watermark updates after restart
	// Without this, if task was paused before restart, it would remain in pausedTasks
	// and all watermark updates would be blocked, causing CDC to stop working
	if exec.watermarkUpdater != nil {
		exec.watermarkUpdater.UnmarkTaskPaused(exec.spec.TaskId)
	}

	if shouldClearTableErrors {
		ctx := defines.AttachAccountId(context.Background(), uint32(exec.spec.Accounts[0].GetId()))
		if err := exec.clearAllTableErrors(ctx); err != nil {
			eventCDCExecutorRestartClearErr.WarnLazy(func() []zap.Field {
				return exec.restartFields(logutil.ErrorFingerprintFields("error", err)...)
			})
			// Don't fail Restart if clearing errors fails - continue anyway
		}
	}

	eventCDCExecutorRestartStarting.InfoLazy(func() []zap.Field {
		return exec.restartFields(zap.String("state", exec.stateMachine.State().String()))
	})

	startCtx, attempt := newCDCStartAttempt(exec.replacementStartContext(), generation)
	if !exec.installStartAttempt(attempt) {
		attempt.cancel()
		return moerr.NewInternalErrorNoCtx("CDC restart found an active startup after drain")
	}
	if !exec.isCurrentCallbackGeneration(generation) {
		exec.finishStartAttempt(attempt)
		return moerr.NewInternalErrorNoCtx("CDC restart was superseded by a newer lifecycle generation")
	}

	exec.setActiveRoutine(cdc.NewCdcActiveRoutine())
	if err := exec.runLifecycleTask("cdc-restart-replacement", func() {
		defer exec.finishStartAttempt(attempt)
		if err := exec.startFunc(startCtx); err != nil {
			exec.refineRestartTimeoutCause(attempt, err)
			if attempt.completeRestart() && exec.finishRestartWaiter(generation, err) {
				eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
					return exec.restartFields(append([]zap.Field{
						zap.String("state", exec.stateMachine.State().String()),
					}, logutil.ErrorFingerprintFields("error", err)...)...)
				})
			}
			return
		}
		if attempt.completeRestart() {
			exec.finishRestartWaiter(generation, nil)
		}
	}); err != nil {
		exec.finishStartAttempt(attempt)
		return moerr.NewInternalErrorf(context.Background(), "cannot schedule CDC restart replacement: %v", err)
	}

	if err, timedOut := exec.waitForRestartStartup(ready, timeout); !timedOut {
		return err
	}
	// Completion and timeout race on the attempt token, not on which select arm
	// happened to run first. If startup already claimed completion, its buffered
	// result is authoritative even when the timer became ready concurrently.
	if !attempt.timeoutRestart() {
		return <-ready
	}
	attempt.cancel()
	exec.closeActiveRoutineCancel()
	select {
	case exec.holdCh <- 1:
	default:
	}
	// Fence the late Start before it can publish Running. The active attempt
	// remains installed until its goroutine exits, so the next restart will
	// drain it instead of reusing its resources.
	// Publish the expected timeout fence before incrementing, so a start
	// goroutine that races this path can emit late-error evidence only after
	// the fence is visible and still current.
	attempt.timeoutFence.Store(generation + 1)
	if exec.callbackGeneration.Add(1) != generation+1 {
		attempt.timeoutFence.Store(0)
	}
	// Constructing a moerr reports it. Only emit timeout evidence after the
	// timeout has actually won, never during a successful restart.
	startupTimeoutErr := moerr.NewInternalErrorNoCtx("CDC restart startup timed out")
	_ = exec.stateMachine.SetFailed(startupTimeoutErr.Error())
	exec.recordRestartTimeoutAsync(attempt, startupTimeoutErr)
	return startupTimeoutErr
}

// Pause cdc task
func (exec *CDCTaskExecutor) Pause() error {
	exec.callbackMu.Lock()
	state := exec.stateMachine.State()
	if state == StatePaused {
		exec.callbackMu.Unlock()
		logutil.Info(
			"cdc.frontend.task.pause_skip_already_paused",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
		)
		return nil
	}

	// Check if running before state transition
	wasRunning := state == StateRunning || state == StateStarting
	// Failed startup/table callbacks can still own readers while unwinding,
	// and a retry already in Pausing must finish the same drain.
	needsProducerDrain := wasRunning || state == StatePausing || state == StateFailed

	// Transition to Pausing state
	if state != StatePausing {
		if err := exec.stateMachine.Transition(TransitionPause); err != nil {
			exec.callbackMu.Unlock()
			return moerr.NewInternalErrorf(context.Background(), "cannot pause: %v", err)
		}
		// A Resume goroutine may still be waiting for the previous Start to
		// unwind. Fence it before pause completion so it cannot revive the task
		// after this pause wins the lifecycle transition.
		exec.callbackGeneration.Add(1)
		exec.recordLeavingFailedMetrics(state, StatePausing)
	}
	exec.cancelCallbackContextLocked()
	callbackDone := exec.callbackDone
	if callbackDone == nil {
		callbackDone = closedChan()
	}
	exec.callbackMu.Unlock()

	// FIX: Mark task as paused ASAP to maximize blocking window
	// This prevents watermark updates from commits that start after pause signal
	// Trade-off: May block legitimate commits during stopAllReaders (causing data duplication)
	// but prevents data loss which is more severe
	// CDC design: duplication is acceptable (handled by downstream), loss is not
	if exec.watermarkUpdater != nil {
		exec.watermarkUpdater.MarkTaskPaused(exec.spec.TaskId)
	}

	// Log watermark states for all running tables before pause
	exec.logCurrentWatermarks("before_pause")

	pauseStartTime := time.Now()
	logutil.Info(
		"cdc.frontend.task.pause_start",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
		zap.String("state", exec.stateMachine.State().String()),
		zap.Bool("was-running", wasRunning),
	)

	if needsProducerDrain {
		cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
		exec.closeActiveRoutinePause()
		if _, timedOut := waitForCDCCompletion(callbackDone, 30*time.Second); timedOut {
			return moerr.NewInternalErrorNoCtx("CDC pause timed out waiting for table detector callbacks")
		}

		// Synchronously wait for all readers to stop before proceeding
		// This ensures no goroutine leaks and clean pause state
		exec.stopAllReaders()

		// Note: task was marked as paused earlier (before ClosePause) to maximize blocking window
		// This may cause some watermark updates during stopAllReaders to be blocked,
		// leading to minor data duplication on resume, but prevents data loss
	}

	// FIX: Force flush watermarks with timeout
	// This ensures all legitimate watermarks (from commits completed before pause)
	// are persisted to database before marking pause as complete
	// Without this, watermarks in cacheUncommitted would be lost, causing data duplication on resume
	if exec.watermarkUpdater != nil {
		flushCtx, cancel := context.WithTimeout(
			defines.AttachAccountId(context.Background(), uint32(exec.spec.Accounts[0].GetId())),
			30*time.Second, // 30s timeout to prevent hanging
		)
		defer cancel()

		if err := exec.watermarkUpdater.ForceFlush(flushCtx); err != nil {
			logutil.Error(
				"cdc.frontend.task.pause_force_flush_failed",
				zap.String("task-id", exec.spec.TaskId),
				zap.Error(err),
			)
			// Return error to ensure data consistency
			// Pause failure is acceptable, data inconsistency is not
			return moerr.NewInternalErrorf(context.Background(),
				"pause failed: unable to flush watermarks: %v", err)
		}

		logutil.Info(
			"cdc.frontend.task.pause_watermark_flushed",
			zap.String("task-id", exec.spec.TaskId),
		)
	}

	// Log watermark states after all readers stopped and watermarks flushed
	exec.logCurrentWatermarks("after_pause")
	// let Start() go after the critical pause work has completed successfully
	select {
	case exec.holdCh <- 1:
		// Signal sent successfully
	default:
		// Channel full or Start() already exited, ignore
	}
	if err := exec.stateMachine.Transition(TransitionPauseComplete); err != nil {
		return moerr.NewInternalErrorf(context.Background(), "cannot complete pause: %v", err)
	}

	if wasRunning {
		v2.CdcTaskTotalGauge.WithLabelValues("running").Dec()
		v2.CdcTaskStateChangeCounter.WithLabelValues("running", "paused").Inc()
	}
	v2.CdcTaskTotalGauge.WithLabelValues("paused").Inc()

	logutil.Info(
		"cdc.frontend.task.pause_success",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
		zap.String("state", exec.stateMachine.State().String()),
		zap.Duration("pause-duration", time.Since(pauseStartTime)),
	)
	return nil
}

// Cancel cdc task
func (exec *CDCTaskExecutor) Cancel() (err error) {
	exec.callbackMu.Lock()
	// Check if running before state transition
	stateBeforeCancel := exec.stateMachine.State()
	wasRunning := stateBeforeCancel == StateRunning

	// Transition to Cancelling state
	if stateBeforeCancel != StateCancelling {
		if err := exec.stateMachine.Transition(TransitionCancel); err != nil {
			exec.callbackMu.Unlock()
			return moerr.NewInternalErrorf(context.Background(), "cannot cancel: %v", err)
		}
	}
	// A Resume goroutine may be waiting for a paused Start to unwind. Fence it
	// before cancellation completes so it cannot install a new routine after we
	// have reached Cancelled.
	exec.callbackGeneration.Add(1)
	exec.cancelCallbackContextLocked()
	callbackDone := exec.callbackDone
	if callbackDone == nil {
		callbackDone = closedChan()
	}
	exec.callbackMu.Unlock()
	// A table-detector callback that passed its generation check can still be
	// initializing a watermark or publishing a reader. Drain that old callback
	// generation before taking the reader snapshot and performing the terminal
	// watermark delete. Callbacks queued behind this fence observe the increment
	// above and return without publishing work.
	exec.cancelLifecycleContext()
	if exec.watermarkUpdater != nil && exec.spec != nil {
		// The tombstone is installed before waiting for any control mutex or
		// reader shutdown so late callbacks remain fenced on every timeout path.
		exec.watermarkUpdater.MarkTaskDeleted(exec.spec.TaskId)
	}
	callbackDrainCtx, callbackDrainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	callbacksDrained := false
	select {
	case <-callbackDone:
		callbacksDrained = true
	case <-callbackDrainCtx.Done():
	}
	callbackDrainCancel()
	exec.recordLeavingFailedMetrics(stateBeforeCancel, StateCancelling)

	logutil.Info(
		"cdc.frontend.task.cancel_start",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
		zap.String("state", exec.stateMachine.State().String()),
		zap.Bool("was-running", wasRunning),
	)
	cancelSucceeded := false
	defer func() {
		if !cancelSucceeded {
			logutil.Warn(
				"cdc.frontend.task.cancel_incomplete",
				zap.String("task-id", exec.spec.TaskId),
				zap.Error(err),
			)
			return
		}
		// Transition to Cancelled state
		if err := exec.stateMachine.Transition(TransitionCancelComplete); err != nil {
			logutil.Warn(
				"cdc.frontend.task.transition_cancelled_failed",
				zap.Error(err),
			)
		}

		// Metrics: task cancelled
		if wasRunning {
			v2.CdcTaskTotalGauge.WithLabelValues("running").Dec()
			v2.CdcTaskStateChangeCounter.WithLabelValues("running", "cancelled").Inc()
		}

		logutil.Info(
			"cdc.frontend.task.cancel_success",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.String("state", exec.stateMachine.State().String()),
		)
	}()

	if attempt := exec.activeStart(); attempt != nil {
		attempt.cancel()
	}
	// Terminal cancellation owns every local producer regardless of the state
	// from which it was entered. Pausing, Restarting, and Failed can all retain
	// an old reader or startup attempt, so state is not evidence of quiescence.
	cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
	exec.closeActiveRoutineCancel()
	readersStopped, readersDone := exec.stopAllReaders()
	// let Start() go, including the no-reader path where there is no
	// completion channel to wait on.
	select {
	case exec.holdCh <- 1:
		// Signal sent successfully
	default:
		// Channel full or Start() already exited, ignore
	}

	// DROP CDC removes metadata before taskservice asynchronously reaches this
	// routine. Drain all earlier updater work after readers have stopped, remove
	// the task from the shared updater caches, then perform the terminal delete.
	// This also covers paused tasks, whose readers were stopped by Pause.
	if exec.watermarkUpdater != nil && exec.spec != nil && len(exec.spec.Accounts) > 0 {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := exec.watermarkUpdater.DeleteTaskWatermarks(
			cleanupCtx,
			uint64(exec.spec.Accounts[0].GetId()),
			exec.spec.TaskId,
		); err != nil {
			logutil.Error(
				"cdc.frontend.task.cancel_watermark_cleanup_failed",
				zap.String("task-id", exec.spec.TaskId),
				zap.String("task-name", exec.spec.TaskName),
				zap.Error(err),
			)
			return err
		}
		if callbacksDrained && readersStopped {
			exec.watermarkUpdater.ForgetTaskDeleted(exec.spec.TaskId)
		} else {
			// The timeout above only bounds cancellation; it must not release
			// the tombstone while an old callback or reader can still publish a
			// watermark. Keep one completion owner until both producer classes
			// have actually exited, then reclaim the CN-local tombstone.
			exec.reclaimDeletedWatermark(exec.spec.TaskId, callbackDone, readersDone)
		}
	}
	cancelSucceeded = true
	return nil
}

func (exec *CDCTaskExecutor) recordLeavingFailedMetrics(fromState ExecutorState, toState ExecutorState) {
	if fromState != StateFailed {
		return
	}
	v2.CdcTaskTotalGauge.WithLabelValues("failed").Dec()
	v2.CdcTaskStateChangeCounter.WithLabelValues("failed", cdcTaskMetricStateLabel(toState)).Inc()
}

func cdcTaskMetricStateLabel(state ExecutorState) string {
	switch state {
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StatePausing:
		return "pausing"
	case StatePaused:
		return "paused"
	case StateRestarting:
		return "restarting"
	case StateCancelling:
		return "cancelling"
	case StateCancelled:
		return "cancelled"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// logCurrentWatermarks logs current watermarks for all tables in this task
func (exec *CDCTaskExecutor) logCurrentWatermarks(phase string) {
	if exec.watermarkUpdater == nil {
		return
	}

	accountId := uint64(exec.spec.Accounts[0].GetId())
	taskId := exec.spec.TaskId
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)

	// Query current watermarks from database
	sql := cdc.CDCSQLBuilder.GetTaskWatermarksSQL(accountId, taskId)
	res := exec.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		logutil.Warn(
			"cdc.frontend.task.log_watermarks_failed",
			zap.String("task-id", taskId),
			zap.String("phase", phase),
			zap.Error(res.Error()),
		)
		return
	}

	// Log each table's watermark
	for i := uint64(0); i < res.RowCount(); i++ {
		dbName, _ := res.GetString(ctx, i, 0)
		tableName, _ := res.GetString(ctx, i, 1)
		watermarkStr, _ := res.GetString(ctx, i, 2)

		logutil.Info(
			"cdc.frontend.task.watermark_snapshot",
			zap.String("task-id", taskId),
			zap.String("phase", phase),
			zap.String("db", dbName),
			zap.String("table", tableName),
			zap.String("watermark", watermarkStr),
		)
	}
}

// initiateReaderShutdown signals every currently visible reader and registers
// an aggregate completion owner without waiting for it.
func (exec *CDCTaskExecutor) initiateReaderShutdown() (<-chan struct{}, int) {
	exec.readerStopMu.Lock()
	defer exec.readerStopMu.Unlock()
	readersDone := make(chan struct{})
	if exec.runningReaders == nil {
		close(readersDone)
		return exec.setReaderShutdownCompletion(readersDone), 0
	}

	logutil.Info("cdc.frontend.task.stop_all_readers_start", zap.String("task-id", exec.spec.TaskId))
	type shutdownEntry struct {
		key    string
		reader cdc.ChangeReader
	}
	readers := make([]shutdownEntry, 0)
	exec.runningReaders.Range(func(key, value interface{}) bool {
		readers = append(readers, shutdownEntry{key: key.(string), reader: value.(cdc.ChangeReader)})
		return true
	})
	// Atomically transfer only the captured instances from map ownership to the
	// completion channel below. A later publication at the same key survives the
	// compare, while a repeated cleanup cannot launch another Close/Wait pair for
	// an already-owned reader.
	for _, entry := range readers {
		exec.runningReaders.CompareAndDelete(entry.key, entry.reader)
	}
	if len(readers) == 0 {
		close(readersDone)
		return exec.setReaderShutdownCompletion(readersDone), 0
	}

	var readerWG sync.WaitGroup
	readerWG.Add(len(readers))
	for _, entry := range readers {
		go func(entry shutdownEntry) {
			defer readerWG.Done()
			closeStart := time.Now()
			logutil.Debug("cdc.frontend.task.stop_reader_close_start", zap.String("task-id", exec.spec.TaskId), zap.String("table", entry.key))
			entry.reader.Close()
			logutil.Debug("cdc.frontend.task.stop_reader_close_done", zap.String("task-id", exec.spec.TaskId), zap.String("table", entry.key), zap.Duration("cost", time.Since(closeStart)))
			entry.reader.Wait()
		}(entry)
	}
	go func() {
		readerWG.Wait()
		close(readersDone)
	}()

	return exec.setReaderShutdownCompletion(readersDone), len(readers)
}

// stopAllReaders stops all running readers and waits for them to exit up to the
// synchronous lifecycle bound. The returned completion remains authoritative
// after a timeout.
func (exec *CDCTaskExecutor) stopAllReaders() (bool, <-chan struct{}) {
	allReadersDone, readerCount := exec.initiateReaderShutdown()
	_, timedOut := waitForCDCCompletion(allReadersDone, 10*time.Second)
	if timedOut {
		logutil.Warn("cdc.frontend.task.stop_reader_wait_timeout", zap.String("task-id", exec.spec.TaskId), zap.Duration("waited", 10*time.Second))
	}
	allStopped := !timedOut && completionReady(allReadersDone)
	logutil.Debug("cdc.frontend.task.stop_all_readers_complete", zap.String("task-id", exec.spec.TaskId), zap.Int("reader-count", readerCount))
	return allStopped, allReadersDone
}

func completionReady(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// previousReaderGenerationStoppedLocked prevents Pause/Failed recovery from
// reopening watermark admission while a callback or reader from that terminal
// generation still owns work. Callers hold callbackMu, which seals callback
// admission while this readiness snapshot is evaluated.
func (exec *CDCTaskExecutor) previousReaderGenerationStoppedLocked(state ExecutorState) bool {
	if state != StatePaused && state != StateFailed {
		return true
	}
	if exec.callbackDone != nil && !completionReady(exec.callbackDone) {
		return false
	}
	readersDone := exec.readerShutdownCompletion()
	return readersDone == nil || completionReady(readersDone)
}

func (exec *CDCTaskExecutor) readerShutdownCompletion() <-chan struct{} {
	exec.readerShutdownMu.Lock()
	defer exec.readerShutdownMu.Unlock()
	return exec.readerShutdownDone
}

func (exec *CDCTaskExecutor) setReaderShutdownCompletion(done <-chan struct{}) <-chan struct{} {
	if done == nil {
		return exec.readerShutdownCompletion()
	}
	exec.readerShutdownMu.Lock()
	previous := exec.readerShutdownDone
	if previous == nil || previous == done {
		exec.readerShutdownDone = done
		exec.readerShutdownMu.Unlock()
		return done
	}
	// Do not grow one waiter goroutine per repeated cleanup retry when either
	// side of the aggregate is already complete. This is especially important
	// after a timed-out reader has been removed from runningReaders and a later
	// retry takes an empty snapshot.
	if completionReady(previous) {
		exec.readerShutdownDone = done
		exec.readerShutdownMu.Unlock()
		return done
	}
	if completionReady(done) {
		exec.readerShutdownMu.Unlock()
		return previous
	}
	combined := make(chan struct{})
	go func() {
		<-previous
		<-done
		close(combined)
	}()
	exec.readerShutdownDone = combined
	exec.readerShutdownMu.Unlock()
	return combined
}

func (exec *CDCTaskExecutor) reclaimDeletedWatermark(
	taskID string,
	callbacksDone <-chan struct{},
	readersDone <-chan struct{},
) {
	if callbacksDone == nil {
		callbacksDone = closedChan()
	}
	if readersDone == nil {
		readersDone = closedChan()
	}
	go func() {
		// If the bounded Cancel wait expired, a callback admitted before the
		// generation fence may publish a reader after Cancel's first snapshot.
		// RegistrationDone guarantees that once callbacksDone closes, every such
		// reader is visible. Take one final snapshot before releasing the local
		// terminal tombstone.
		<-callbacksDone
		_, lateReadersDone := exec.stopAllReaders()
		<-readersDone
		<-lateReadersDone
		if exec.watermarkUpdater != nil {
			exec.watermarkUpdater.ForgetTaskDeleted(taskID)
		}
	}()
}

type removedReaderShutdown struct {
	reader cdc.ChangeReader
	done   chan struct{}
}

func (exec *CDCTaskExecutor) stopReadersMissingFromScan(accountTbls cdc.TblMap) {
	if exec.runningReaders == nil {
		return
	}

	exec.runningReaders.Range(func(key, value interface{}) bool {
		tableKey, ok := key.(string)
		if !ok {
			return true
		}
		if _, ok = accountTbls[tableKey]; ok {
			return true
		}

		reader, ok := value.(cdc.ChangeReader)
		if !ok {
			exec.runningReaders.Delete(key)
			return true
		}

		if !exec.matchesAnySourcePattern(tableKey) {
			return true
		}

		exec.stopRemovedReader(tableKey, key, reader)
		return true
	})
}

func (exec *CDCTaskExecutor) stopRemovedReader(tableKey string, mapKey interface{}, reader cdc.ChangeReader) {
	shutdown := &removedReaderShutdown{
		reader: reader,
		done:   make(chan struct{}),
	}

	actual, loaded := exec.removedReaderShutdowns.LoadOrStore(tableKey, shutdown)
	if loaded {
		existing, ok := actual.(*removedReaderShutdown)
		if ok && existing.reader == reader {
			select {
			case <-existing.done:
				exec.removedReaderShutdowns.CompareAndDelete(tableKey, existing)
			default:
				return
			}
		} else {
			exec.removedReaderShutdowns.CompareAndDelete(tableKey, actual)
		}
		_, loaded = exec.removedReaderShutdowns.LoadOrStore(tableKey, shutdown)
		if loaded {
			return
		}
	}

	logutil.Info(
		"cdc.frontend.task.stop_reader_removed_from_scan",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
		zap.String("table", tableKey),
	)

	go func() {
		reader.Close()
		reader.Wait()
		exec.runningReaders.CompareAndDelete(mapKey, reader)
		close(shutdown.done)
		exec.removedReaderShutdowns.CompareAndDelete(tableKey, shutdown)
	}()
}

func (exec *CDCTaskExecutor) removedReaderShutdownInProgress(tableKey string, reader cdc.ChangeReader) bool {
	actual, ok := exec.removedReaderShutdowns.Load(tableKey)
	if !ok {
		return false
	}
	shutdown, ok := actual.(*removedReaderShutdown)
	if !ok || shutdown.reader != reader {
		return false
	}
	select {
	case <-shutdown.done:
		return false
	default:
		return true
	}
}

func (exec *CDCTaskExecutor) initAesKeyByInternalExecutor(ctx context.Context, accountId uint32) (err error) {
	if len(cdc.AesKey) > 0 {
		return nil
	}

	querySql := cdc.CDCSQLBuilder.GetDataKeySQL(uint64(accountId), cdc.InitKeyId)
	res := exec.ie.Query(ctx, querySql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	} else if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "no data key record for account %d", accountId)
	}

	encryptedKey, err := res.GetString(ctx, 0, 0)
	if err != nil {
		return err
	}

	cdc.AesKey, err = cdc.AesCFBDecodeWithKey(
		ctx, encryptedKey,
		[]byte(getGlobalPuWrapper(exec.cnUUID).SV.KeyEncryptionKey),
	)
	return
}

func (exec *CDCTaskExecutor) updateErrMsg(ctx context.Context, errMsg string) (err error) {
	return exec.updateErrMsgWithCurrentState(ctx, errMsg, cdc.CDCState_Running)
}

func (exec *CDCTaskExecutor) updateErrMsgWithCurrentState(
	ctx context.Context,
	errMsg string,
	currentState string,
) (err error) {
	state := cdc.CDCState_Running
	if errMsg != "" {
		state = cdc.CDCState_Failed
	}
	return exec.updateCatalogStateAndErrMsg(ctx, state, errMsg, currentState)
}

func (exec *CDCTaskExecutor) updateCatalogStateAndErrMsg(
	ctx context.Context,
	state string,
	errMsg string,
	currentState string,
) (err error) {
	return exec.updateCatalogStateAndErrMsgWithExecutor(ctx, exec.ie, state, errMsg, currentState)
}

func (exec *CDCTaskExecutor) updateCatalogStateAndErrMsgWithExecutor(
	ctx context.Context,
	sqlExecutor ie.InternalExecutor,
	state string,
	errMsg string,
	currentState string,
) (err error) {
	if sqlExecutor == nil || exec.spec == nil || len(exec.spec.Accounts) == 0 {
		return nil
	}
	accId := exec.spec.Accounts[0].GetId()
	if len(errMsg) > cdc.CDCWatermarkErrMsgMaxLen {
		errMsg = errMsg[:cdc.CDCWatermarkErrMsgMaxLen]
	}

	sql := cdc.CDCSQLBuilder.UpdateTaskStateAndErrMsgByStateSQL(
		uint64(accId),
		exec.spec.TaskId,
		state,
		errMsg,
		currentState,
	)
	expectedErrMsg := errMsg
	return execCDCSQLWithAffectedRows(
		ctx,
		sqlExecutor,
		sql,
		uint64(accId),
		exec.spec.TaskId,
		state,
		currentState,
		&expectedErrMsg,
	)
}

// admitRestartCatalogState reopens only the failure persisted by a prior local
// or fresh restart attempt. If the request path already put the row in
// restarting, exact-state validation makes the transition idempotent.
func (exec *CDCTaskExecutor) admitRestartCatalogState(ctx context.Context) error {
	if exec.spec == nil || len(exec.spec.Accounts) == 0 {
		return nil
	}
	ctx = defines.AttachAccountId(ctx, uint32(exec.spec.Accounts[0].GetId()))
	return exec.updateCatalogStateAndErrMsg(ctx, cdc.CDCState_Restarting, "", cdc.CDCState_Failed)
}

func (exec *CDCTaskExecutor) recordRestartTimeoutAsync(
	attempt *cdcStartAttempt,
	timeoutErr error,
) {
	if exec.spec == nil || len(exec.spec.Accounts) == 0 {
		return
	}
	persistence := &cdcRestartCatalogPersistence{done: make(chan struct{})}
	exec.restartCatalogMu.Lock()
	if previous := exec.restartCatalogPersistence; previous != nil {
		select {
		case <-previous.done:
		default:
			exec.restartCatalogMu.Unlock()
			eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
				return exec.restartFields(append([]zap.Field{
					zap.String("reason", "startup-timeout-record-already-pending"),
				}, logutil.ErrorFingerprintFields("error", timeoutErr)...)...)
			})
			return
		}
	}
	exec.restartCatalogPersistence = persistence
	exec.restartCatalogMu.Unlock()

	runPersistence := func(lifecycleCtx context.Context) {
		defer close(persistence.done)
		factory := exec.restartCatalogExecutorFactory
		if factory == nil {
			persistence.err = moerr.NewInternalErrorNoCtx("CDC restart timeout catalog executor is unavailable")
		} else {
			// This work is independent of the request that timed out, but it is
			// still owned by task-runner/CN shutdown. The scheduler context
			// preserves both properties: Restart returns immediately, while
			// Stop cancels and joins the catalog repair before dependencies
			// are torn down.
			ctx, cancel := context.WithTimeout(lifecycleCtx, exec.restartTimeout())
			defer cancel()
			ctx = defines.AttachAccountId(ctx, uint32(exec.spec.Accounts[0].GetId()))
			sqlExecutor := factory()
			if sqlExecutor == nil {
				persistence.err = moerr.NewInternalErrorNoCtx("CDC restart timeout catalog executor factory returned nil")
			} else {
				persistence.err = exec.updateCatalogStateAndErrMsgWithExecutor(
					ctx,
					sqlExecutor,
					cdc.CDCState_Failed,
					timeoutErr.Error(),
					cdc.CDCState_Restarting,
				)
				// The startup publication may have held the catalog row lock
				// across the timeout. If it committed running first, repair
				// that exact late state while this persistence fence still
				// prevents a newer restart generation from being admitted.
				if persistence.err != nil &&
					attempt != nil &&
					attempt.restartOwner.Load() == cdcRestartOwnerTimedOut {
					persistence.err = exec.updateCatalogStateAndErrMsgWithExecutor(
						ctx,
						sqlExecutor,
						cdc.CDCState_Failed,
						timeoutErr.Error(),
						cdc.CDCState_Running,
					)
				}
			}
		}
		if persistence.err != nil {
			eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
				return exec.restartFields(append([]zap.Field{
					zap.String("reason", "startup-timeout-catalog-update"),
				}, logutil.ErrorFingerprintFields("error", persistence.err)...)...)
			})
		}
	}
	if err := exec.runLifecycleContextTask(
		"cdc-restart-timeout-persistence",
		runPersistence,
	); err != nil {
		// RunNamedTask either admits exactly one task or returns an error. Close
		// the generation fence on rejection so a later control request cannot
		// wait forever for work that was never started.
		persistence.err = moerr.NewInternalErrorf(
			context.Background(),
			"cannot schedule CDC restart timeout persistence: %v",
			err,
		)
		close(persistence.done)
		eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
			return exec.restartFields(append([]zap.Field{
				zap.String("reason", "startup-timeout-persistence-schedule"),
			}, logutil.ErrorFingerprintFields("error", persistence.err)...)...)
		})
	}
}

func (exec *CDCTaskExecutor) waitForRestartCatalogPersistence(timeout time.Duration) (error, bool) {
	exec.restartCatalogMu.Lock()
	persistence := exec.restartCatalogPersistence
	exec.restartCatalogMu.Unlock()
	if persistence == nil {
		return nil, false
	}

	_, timedOut := waitForCDCCompletion(persistence.done, timeout)
	if timedOut {
		return nil, true
	}
	exec.restartCatalogMu.Lock()
	if exec.restartCatalogPersistence == persistence {
		exec.restartCatalogPersistence = nil
	}
	exec.restartCatalogMu.Unlock()
	return persistence.err, false
}

// refineRestartTimeoutCause preserves late failure evidence without mutating
// catalog state. Once Restart has returned a timeout, a later generation may
// already be retrying; a delayed failed -> failed update has no durable
// generation token and could overwrite that newer generation's error.
func (exec *CDCTaskExecutor) refineRestartTimeoutCause(attempt *cdcStartAttempt, startErr error) {
	if attempt == nil || errors.Is(startErr, context.Canceled) || errors.Is(startErr, context.DeadlineExceeded) {
		return
	}
	fence := attempt.timeoutFence.Load()
	if fence == 0 || exec.callbackGeneration.Load() != fence {
		return
	}
	eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
		return exec.restartFields(append([]zap.Field{
			zap.String("reason", "late-startup-error-after-timeout"),
		}, logutil.ErrorFingerprintFields("error", startErr)...)...)
	})
}

// reconcileTimedOutStartupPublication repairs the only ambiguous catalog
// outcome: restarting -> running committed after the timeout side had already
// claimed the attempt. A newer restart cannot be admitted while this attempt
// remains installed, and the exact running -> failed CAS preserves a
// concurrent PAUSE/CANCEL/DROP that has already moved the row elsewhere.
func (exec *CDCTaskExecutor) reconcileTimedOutStartupPublication(
	attempt *cdcStartAttempt,
) {
	if attempt == nil ||
		attempt.restartOwner.Load() != cdcRestartOwnerTimedOut ||
		!exec.isActiveStartAttempt(attempt) ||
		exec.spec == nil ||
		len(exec.spec.Accounts) == 0 {
		return
	}

	sqlExecutor := exec.ie
	if factory := exec.restartCatalogExecutorFactory; factory != nil {
		sqlExecutor = factory()
	}
	if sqlExecutor == nil {
		eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
			return exec.restartFields(zap.String("reason", "late-running-catalog-reconcile-executor-unavailable"))
		})
		return
	}

	timeoutErr := moerr.NewInternalErrorNoCtx("CDC restart startup timed out")
	ctx, cancel := context.WithTimeout(context.Background(), exec.restartTimeout())
	defer cancel()
	ctx = defines.AttachAccountId(ctx, uint32(exec.spec.Accounts[0].GetId()))
	if err := exec.updateCatalogStateAndErrMsgWithExecutor(
		ctx,
		sqlExecutor,
		cdc.CDCState_Failed,
		timeoutErr.Error(),
		cdc.CDCState_Running,
	); err != nil {
		eventCDCExecutorRestartFailed.ErrorLazy(func() []zap.Field {
			return exec.restartFields(append([]zap.Field{
				zap.String("reason", "late-running-catalog-reconcile"),
			}, logutil.ErrorFingerprintFields("error", err)...)...)
		})
	}
}

func (exec *CDCTaskExecutor) updateErrMsgForStartup(
	ctx context.Context,
	errMsg string,
	catalogState string,
	hasRestartCatalogState bool,
	restartAdmission bool,
) error {
	if hasRestartCatalogState {
		return exec.updateErrMsgWithCurrentState(ctx, errMsg, catalogState)
	}
	if restartAdmission {
		return exec.updateErrMsgWithCurrentState(ctx, errMsg, cdc.CDCState_Restarting)
	}
	return exec.updateErrMsgWithCurrentState(ctx, errMsg, cdc.CDCState_Running)
}

func execCDCSQLWithAffectedRows(
	ctx context.Context,
	sqlExecutor ie.InternalExecutor,
	sql string,
	accountID uint64,
	taskID string,
	targetState string,
	currentState string,
	targetErrMsg *string,
) error {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	fault.TriggerFault(cdcStateTransitionFaultPoint(currentState, targetState))
	if sqlExecutorWithStatus, ok := sqlExecutor.(ie.InternalExecutorWithStatus); ok {
		status, err := sqlExecutorWithStatus.ExecWithStatus(ctx, sql, ie.SessionOverrideOptions{})
		if err != nil {
			return err
		}
		switch status.AffectedRows {
		case 1:
			return nil
		case 0:
			return validateCDCStateTransitionResult(
				ctx,
				sqlExecutor,
				accountID,
				taskID,
				currentState,
				targetState,
				targetErrMsg,
			)
		default:
			return moerr.NewInternalErrorf(
				ctx,
				"cdc task state transition affected %d rows, task_id=%s, current_state=%s, target_state=%s",
				status.AffectedRows,
				taskID,
				currentState,
				targetState,
			)
		}
	}
	return sqlExecutor.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func validateCDCStateTransitionResult(
	ctx context.Context,
	sqlExecutor ie.InternalExecutor,
	accountID uint64,
	taskID string,
	currentState string,
	targetState string,
	targetErrMsg *string,
) error {
	querySQL := cdc.CDCSQLBuilder.GetTaskStateSQL(accountID, taskID)
	result := sqlExecutor.Query(ctx, querySQL, ie.SessionOverrideOptions{})
	if result == nil {
		return moerr.NewInternalErrorf(
			ctx,
			"cdc task state transition query returned no result, task_id=%s, current_state=%s, target_state=%s",
			taskID,
			currentState,
			targetState,
		)
	}
	if err := result.Error(); err != nil {
		return err
	}
	if result.RowCount() == 0 {
		return moerr.NewInternalErrorf(
			ctx,
			"cdc task state transition found no catalog row, task_id=%s, current_state=%s, target_state=%s",
			taskID,
			currentState,
			targetState,
		)
	}
	state, err := result.GetString(ctx, 0, 0)
	if err != nil {
		return err
	}
	if state == targetState {
		if targetErrMsg == nil {
			return nil
		}
		errMsg, err := result.GetString(ctx, 0, 1)
		if err != nil {
			return err
		}
		if errMsg == *targetErrMsg {
			return nil
		}
		return moerr.NewInternalErrorf(
			ctx,
			"cdc task state transition found conflicting catalog err_msg, task_id=%s, current_state=%s, target_state=%s",
			taskID,
			currentState,
			targetState,
		)
	}
	return moerr.NewInternalErrorf(
		ctx,
		"cdc task state transition found conflicting catalog state %s, task_id=%s, current_state=%s, target_state=%s",
		state,
		taskID,
		currentState,
		targetState,
	)
}

func cdcStateTransitionFaultPoint(currentState string, targetState string) string {
	return "cdc/state_transition/" + currentState + "_to_" + targetState + "/before_exec"
}

func CDCPauseTaskCompleteHook(sqlExecutorFactory func() ie.InternalExecutor) taskservice.PauseTaskCompletedHook {
	return func(_ context.Context, daemonTask task.DaemonTask) error {
		if daemonTask.Details == nil {
			return nil
		}
		details, ok := daemonTask.Details.Details.(*task.Details_CreateCdc)
		if !ok || details.CreateCdc == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		return updateCDCTaskState(
			ctx,
			sqlExecutorFactory,
			details.CreateCdc,
			cdc.CDCState_Paused,
		)
	}
}

func updateCDCTaskState(
	ctx context.Context,
	sqlExecutorFactory func() ie.InternalExecutor,
	spec *task.CreateCdcDetails,
	state string,
) error {
	if sqlExecutorFactory == nil || spec == nil || len(spec.Accounts) == 0 {
		logutil.Warn(
			"cdc.frontend.task.update_state.skipped",
			zap.String("state", state),
		)
		return nil
	}
	sqlExecutor := sqlExecutorFactory()
	if sqlExecutor == nil {
		logutil.Warn(
			"cdc.frontend.task.update_state.skipped",
			zap.String("state", state),
		)
		return nil
	}
	accountID := uint64(spec.Accounts[0].GetId())
	sql := cdc.CDCSQLBuilder.UpdateTaskStateByTaskIdAndStateSQL(
		accountID,
		spec.TaskId,
		state,
		cdc.CDCState_Pausing,
	)
	if err := execCDCSQLWithAffectedRows(ctx, sqlExecutor, sql, accountID, spec.TaskId, state, cdc.CDCState_Pausing, nil); err != nil {
		logutil.Error(
			"cdc.frontend.task.update_state.failed",
			zap.String("task-id", spec.TaskId),
			zap.String("task-name", spec.TaskName),
			zap.Uint64("account-id", accountID),
			zap.String("state", state),
			zap.Error(err),
		)
		return err
	}
	return nil
}

// clearAllTableErrors clears error messages for all tables in this task
// This is called during Resume/Restart to allow retrying tables that had non-retryable errors
// after user has fixed the underlying issues
func (exec *CDCTaskExecutor) clearAllTableErrors(ctx context.Context) error {
	if exec.ie == nil {
		return moerr.NewInternalErrorNoCtx("cannot clear CDC table errors: internal executor is not initialized")
	}

	accountId := uint64(exec.spec.Accounts[0].GetId())
	taskId := exec.spec.TaskId

	// Use SQL builder to construct safe SQL
	sql := cdc.CDCSQLBuilder.ClearTaskTableErrorsSQL(accountId, taskId)

	logutil.Info(
		"cdc.frontend.task.clear_table_errors",
		zap.String("task-id", taskId),
		zap.Uint64("account-id", accountId),
	)

	return exec.ie.Exec(
		defines.AttachAccountId(ctx, catalog.System_Account),
		sql,
		ie.SessionOverrideOptions{},
	)
}

func (exec *CDCTaskExecutor) handleNewTables(allAccountTbls map[uint32]cdc.TblMap) error {
	return exec.handleNewTablesForGeneration(exec.callbackGeneration.Load(), allAccountTbls)
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (exec *CDCTaskExecutor) handleNewTablesForGeneration(
	callbackGeneration uint64,
	allAccountTbls map[uint32]cdc.TblMap,
) error {
	exec.callbackMu.Lock()
	if !exec.isCurrentCallbackGeneration(callbackGeneration) ||
		!exec.tableCallbackStateAllowsAdmission() {
		exec.callbackMu.Unlock()
		return nil
	}
	if exec.callbackCount == 0 {
		exec.callbackDone = make(chan struct{})
	}
	exec.callbackCount++
	callbackDone := exec.callbackDone
	callbackCtx := exec.callbackContextLocked()
	exec.callbackMu.Unlock()
	defer func() {
		exec.callbackMu.Lock()
		exec.callbackCount--
		if exec.callbackCount == 0 && exec.callbackDone == callbackDone {
			close(exec.callbackDone)
		}
		exec.callbackMu.Unlock()
	}()
	accountId := uint32(exec.spec.Accounts[0].GetId())
	ctx := defines.AttachAccountId(callbackCtx, accountId)

	// lock to avoid create pipelines for the same table
	// 2025.7, this lock might be needless now
	exec.Lock()
	defer exec.Unlock()

	if !exec.isCurrentCallbackGeneration(callbackGeneration) {
		return nil
	}

	// if injected, we expect nothing
	if sleepSeconds, injected := objectio.CDCHandleSlowInjected(); injected {
		timer := time.NewTimer(time.Duration(sleepSeconds) * time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	txnOp, err := cdc.GetTxnOp(ctx, exec.cnEngine, exec.cnTxnClient, "cdc-handleNewTables")
	if err != nil {
		logutil.Error(
			"cdc.frontend.task.handle_new_tables_get_txnop_failed",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.Error(err),
		)
		return err
	}
	defer func() {
		cdc.FinishTxnOp(ctx, err, txnOp, exec.cnEngine)
	}()
	err = exec.cnEngine.New(ctx, txnOp)

	// if injected, we expect the handleNewTables to keep retrying
	if objectio.CDCHandleErrInjected() {
		err = moerr.NewInternalError(context.Background(), "CDC_HANDLENEWTABLES_ERR")
	}

	if err != nil {
		logutil.Error(
			"cdc.frontend.task.handle_new_tables_new_engine_failed",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.Error(err),
		)
		return err
	}

	// Track failed tables for better error reporting
	failedTables := make(map[string]error)
	successCount := 0
	accountTbls := allAccountTbls[accountId]
	exec.stopReadersMissingFromScan(accountTbls)

	for key, info := range accountTbls {
		// already running
		if val, ok := exec.runningReaders.Load(key); ok {
			if reader, ok := val.(cdc.ChangeReader); ok {
				readerInfo := reader.GetTableInfo()
				// wait the old reader to stop
				if info.OnlyDiffinTblId(readerInfo) {
					if exec.removedReaderShutdownInProgress(key, reader) {
						logutil.Info(
							"cdc.frontend.task.skip_wait_removed_reader_shutdown",
							zap.String("table", key),
							zap.Uint64("old-table-id", readerInfo.SourceTblId),
							zap.Uint64("new-table-id", info.SourceTblId),
						)
						continue
					}
					logutil.Info(
						"cdc.frontend.task.wait_old_reader",
						zap.String("table", key),
						zap.Uint64("old-table-id", readerInfo.SourceTblId),
						zap.Uint64("new-table-id", info.SourceTblId),
					)
					waitChan := make(chan struct{})
					go func() {
						defer close(waitChan)
						reader.Wait()
					}()
					select {
					case <-waitChan:
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					continue
				}
			}
		}

		if exec.exclude != nil && exec.exclude.MatchString(key) {
			continue
		}

		newTableInfo := info.Clone()
		if !exec.matchAnyPattern(key, newTableInfo) {
			continue
		}
		hasError, err := GetTableErrMsg(ctx, accountId, exec.ie, exec.spec.TaskId, newTableInfo)
		if err != nil {
			logutil.Error(
				"cdc.frontend.task.get_table_errmsg_failed",
				zap.String("task-name", exec.spec.TaskName),
				zap.String("table", key),
				zap.Error(err),
			)
			// Don't return immediately - try other tables
			failedTables[key] = err
			continue
		}
		if hasError {
			if !exec.isCurrentCallbackGeneration(callbackGeneration) {
				return nil
			}
			err = exec.failTaskForPermanentTableError(ctx, newTableInfo)
			return err
		}

		logutil.Info(
			"cdc.frontend.task.new_table_detected",
			zap.String("task-name", exec.spec.TaskName),
			zap.String("table", key),
			zap.String("source-db", newTableInfo.SourceDbName),
			zap.String("source-table", newTableInfo.SourceTblName),
		)
		var pipelineOwnerFence *cdc.OwnerFence
		if exec.stableInitialSnapshot {
			// Capture one immutable identity for both pipeline effects and any
			// diagnostic produced while constructing that pipeline. Re-reading the
			// current fence after a failure could lend a replacement generation's
			// identity to obsolete work.
			pipelineOwnerFence = exec.currentDaemonClaimFence()
		}
		if err = exec.addExecPipelineForTable(
			ctx, newTableInfo, txnOp, pipelineOwnerFence); err != nil {
			logutil.Error(
				"cdc.frontend.task.add_exec_pipeline_failed",
				zap.String("task-name", exec.spec.TaskName),
				zap.String("table", key),
				zap.Error(err),
			)
			// Ownership loss is a control-plane result for this obsolete executor.
			// Do not poison shared table metadata that belongs to the new owner.
			if cdc.IsOwnerFenceLostError(err) {
				return err
			}
			// Pause, cancel, restart, or CN shutdown can cancel this callback
			// while target initialization is in flight. The obsolete callback is
			// lifecycle cleanup, not a table-data failure; never persist it into
			// shared err_msg state owned by a later generation.
			if ctx.Err() != nil || !exec.isCurrentCallbackGeneration(callbackGeneration) {
				return err
			}
			// Persist data/setup errors, and retain transient fence/epoch backend
			// failures as retryable rather than permanently failing the table.
			// A stable diagnostic without the exact pipeline fence would silently
			// fall back to the legacy upsert and escape generation ownership. If the
			// fence itself is missing, leave reporting to the task-level startup error.
			if exec.watermarkUpdater != nil &&
				(!exec.stableInitialSnapshot || pipelineOwnerFence != nil) {
				watermarkKey := cdc.WatermarkKey{
					AccountId: uint64(exec.spec.Accounts[0].GetId()),
					TaskId:    exec.spec.TaskId,
					DBName:    newTableInfo.SourceDbName,
					TableName: newTableInfo.SourceTblName,
				}
				errorCtx := &cdc.ErrorContext{
					IsRetryable: cdc.IsRetryableSnapshotEpochError(err) ||
						cdc.IsRetryableOwnerFenceError(err) ||
						cdc.IsRetryableTargetLockError(err) ||
						cdc.IsRetryableConnectionError(err),
				}
				errorUpdateCtx := cdc.WithWatermarkOwnerFence(
					ctx, pipelineOwnerFence, newTableInfo.SourceTblId)
				if updateErr := exec.watermarkUpdater.UpdateWatermarkErrMsg(
					errorUpdateCtx, &watermarkKey, err.Error(), errorCtx); updateErr != nil {
					logutil.Warn(
						"cdc.frontend.task.persist_table_error_failed",
						zap.String("table", key),
						zap.Error(updateErr),
					)
				}
			}
			// Don't return immediately - try other tables
			failedTables[key] = err
			continue
		}

		info.IdChanged = newTableInfo.IdChanged
		successCount++
		logutil.Info(
			"cdc.frontend.task.add_exec_pipeline_success",
			zap.String("task-name", exec.spec.TaskName),
			zap.String("table", key),
		)
	}

	// Log summary
	if len(failedTables) > 0 {
		failedKeys := make([]string, 0, len(failedTables))
		for k := range failedTables {
			failedKeys = append(failedKeys, k)
		}
		logutil.Warn(
			"cdc.frontend.task.add_exec_pipeline_summary",
			zap.String("task-name", exec.spec.TaskName),
			zap.Int("success-count", successCount),
			zap.Int("failed-count", len(failedTables)),
			zap.Strings("failed-tables", failedKeys),
		)
		// Return error to trigger retry by TableDetector
		return moerr.NewInternalErrorf(ctx, "failed to add pipeline for %d tables", len(failedTables))
	}

	return nil
}

func (exec *CDCTaskExecutor) tableCallbackStateAllowsAdmission() bool {
	// Production callbacks are registered while Starting and normally execute
	// while Running. Permit Idle for directly constructed legacy/unit callers,
	// but make every state that has fenced or stopped producers reject new work.
	if exec.stateMachine == nil {
		return true
	}
	switch exec.stateMachine.State() {
	case StatePausing, StatePaused, StateRestarting, StateCancelling, StateCancelled, StateFailed:
		return false
	default:
		return true
	}
}

func (exec *CDCTaskExecutor) isCurrentCallbackGeneration(callbackGeneration uint64) bool {
	return exec.callbackGeneration.Load() == callbackGeneration
}

func (exec *CDCTaskExecutor) failTaskForPermanentTableError(ctx context.Context, tbl *cdc.DbTableInfo) error {
	taskErr := moerr.NewInternalErrorf(
		ctx,
		"CDC task %s has permanent table error on %s.%s; check mo_catalog.mo_cdc_watermark.err_msg for details",
		exec.spec.TaskName,
		tbl.SourceDbName,
		tbl.SourceTblName,
	)

	stateBeforeFail := StateIdle
	if exec.stateMachine != nil {
		stateBeforeFail = exec.stateMachine.State()
		if err := exec.stateMachine.SetFailed(taskErr.Error()); err != nil {
			logutil.Warn(
				"cdc.frontend.task.set_state_failed",
				zap.String("task-id", exec.spec.TaskId),
				zap.String("task-name", exec.spec.TaskName),
				zap.String("db", tbl.SourceDbName),
				zap.String("table", tbl.SourceTblName),
				zap.Error(err),
			)
			return err
		}
	}

	wasRunning := stateBeforeFail == StateRunning || stateBeforeFail == StateStarting

	if err := exec.updateErrMsg(ctx, taskErr.Error()); err != nil {
		logutil.Warn(
			"cdc.frontend.task.update_task_error_failed",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.String("db", tbl.SourceDbName),
			zap.String("table", tbl.SourceTblName),
			zap.Error(err),
		)
	}

	cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
	exec.closeActiveRoutineCancel()
	// Keep the completion owner even when a permanent table error leaves a
	// reader unwinding past the bounded wait.  A later DROP may observe
	// StateFailed and otherwise assume that no reader survived this path.
	exec.stopAllReaders()
	if exec.holdCh != nil {
		select {
		case exec.holdCh <- 1:
		default:
		}
	}

	if wasRunning {
		v2.CdcTaskTotalGauge.WithLabelValues("running").Dec()
		v2.CdcTaskStateChangeCounter.WithLabelValues("running", "failed").Inc()
	}
	v2.CdcTaskTotalGauge.WithLabelValues("failed").Inc()
	v2.CdcTaskErrorCounter.WithLabelValues("permanent_table_error", "false").Inc()

	logutil.Error(
		"cdc.frontend.task.failed_by_permanent_table_error",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
		zap.String("db", tbl.SourceDbName),
		zap.String("table", tbl.SourceTblName),
		zap.Error(taskErr),
	)

	return taskErr
}

var GetTableErrMsg = func(
	ctx context.Context,
	accountId uint32,
	ieExecutor ie.InternalExecutor,
	taskId string,
	tbl *cdc.DbTableInfo) (
	hasError bool, err error,
) {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := cdc.CDCSQLBuilder.GetTableErrMsgSQL(uint64(accountId), taskId, tbl.SourceDbName, tbl.SourceTblName)
	res := ieExecutor.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return false, res.Error()
	} else if res.RowCount() < 1 {
		return false, nil
	}

	errMsg, err := res.GetString(ctx, 0, 0)
	if err != nil {
		return false, err
	}
	if errMsg == "" {
		return false, nil
	}

	// Parse error metadata using unified parser
	metadata := cdc.ParseErrorMetadata(errMsg)
	if metadata == nil {
		return false, nil
	}

	// Use unified retry logic
	if cdc.ShouldRetry(metadata) {
		// Log detailed retry information
		if metadata.IsRetryable {
			logutil.Info(
				"cdc.frontend.task.retryable_table_error",
				zap.String("db", tbl.SourceDbName),
				zap.String("table", tbl.SourceTblName),
				zap.Int("retry-count", metadata.RetryCount),
				zap.Int("max-retry", cdc.MaxRetryCount),
				zap.String("message", metadata.Message),
			)
		} else {
			// Expired non-retryable error
			age := time.Since(metadata.FirstSeen)
			logutil.Info(
				"cdc.frontend.task.expired_non_retryable_error",
				zap.String("db", tbl.SourceDbName),
				zap.String("table", tbl.SourceTblName),
				zap.Duration("age", age),
				zap.String("message", metadata.Message),
			)
		}
		return false, nil
	}

	// Cannot retry
	if metadata.IsRetryable {
		// Exceeded max retry count
		logutil.Warn(
			"cdc.frontend.task.max_retry_exceeded",
			zap.String("db", tbl.SourceDbName),
			zap.String("table", tbl.SourceTblName),
			zap.Int("retry-count", metadata.RetryCount),
			zap.String("message", metadata.Message),
		)
	} else {
		// Fresh non-retryable error
		age := time.Since(metadata.FirstSeen)
		logutil.Info(
			"cdc.frontend.task.permanent_table_error",
			zap.String("db", tbl.SourceDbName),
			zap.String("table", tbl.SourceTblName),
			zap.Duration("age", age),
			zap.String("message", metadata.Message),
		)
	}

	hasError = true
	return
}

func (exec *CDCTaskExecutor) matchAnyPattern(key string, info *cdc.DbTableInfo) bool {
	match := func(s, p string) bool {
		if p == cdc.CDCPitrGranularity_All {
			return true
		}
		return s == p
	}

	db, table := cdc.SplitDbTblKey(key)
	for _, pt := range exec.tables.Pts {
		if match(db, pt.Source.Database) && match(table, pt.Source.Table) {
			// complete sink info
			info.SinkDbName = pt.Sink.Database
			if info.SinkDbName == cdc.CDCPitrGranularity_All {
				info.SinkDbName = db
			}
			info.SinkTblName = pt.Sink.Table
			if info.SinkTblName == cdc.CDCPitrGranularity_All {
				info.SinkTblName = table
			}
			return true
		}
	}
	return false
}

func (exec *CDCTaskExecutor) matchesAnySourcePattern(key string) bool {
	match := func(s, p string) bool {
		if p == cdc.CDCPitrGranularity_All {
			return true
		}
		return s == p
	}

	db, table := cdc.SplitDbTblKey(key)
	for _, pt := range exec.tables.Pts {
		if match(db, pt.Source.Database) && match(table, pt.Source.Table) {
			return true
		}
	}
	return false
}

// reader ----> sinker ----> remote db
func (exec *CDCTaskExecutor) addExecPipelineForTable(
	ctx context.Context,
	info *cdc.DbTableInfo,
	txnOp client.TxnOperator,
	ownerFence *cdc.OwnerFence,
) (err error) {
	// for ut
	if objectio.CDCAddExecConsumeTruncateInjected() {
		info.IdChanged = false
		return nil
	}

	if objectio.CDCAddExecErrInjected() {
		return moerr.NewInternalErrorNoCtx("CDC_AddExecPipelineForTable_ERR")
	}

	// step 1. init watermarkUpdater
	// get watermark from db
	watermark := exec.startTs
	if exec.noFull {
		watermark = types.TimestampToTS(txnOp.SnapshotTS())
	}
	watermarkKey := cdc.WatermarkKey{
		AccountId: uint64(exec.spec.Accounts[0].GetId()),
		TaskId:    exec.spec.TaskId,
		DBName:    info.SourceDbName,
		TableName: info.SourceTblName,
	}
	var initialSnapshotEpoch types.TS
	var initialSnapshotPending bool
	var compactSnapshotEpochs bool
	if exec.stableInitialSnapshot {
		if ownerFence == nil {
			return moerr.NewInternalErrorNoCtx("stable CDC executor has no daemon claim fence")
		}
	}
	if watermark, err = exec.watermarkUpdater.GetOrAddCommitted(
		ctx,
		&watermarkKey,
		&watermark,
	); err != nil {
		return err
	}
	initialSnapshotPending = !exec.noFull && exec.startTs.IsEmpty() && watermark.IsEmpty()
	if exec.stableInitialSnapshot {
		if err = ownerFence.Check(ctx); err != nil {
			return err
		}
		var watermarkGeneration uint64
		watermark, watermarkGeneration, err = exec.watermarkUpdater.GetWatermarkProgress(ctx, &watermarkKey)
		if err != nil {
			return err
		}
		var epochState cdc.InitialSnapshotEpochState
		initialSnapshot := !exec.noFull && exec.startTs.IsEmpty()
		if initialSnapshot {
			candidate := types.TimestampToTS(txnOp.SnapshotTS())
			// Persist the actual bounded snapshot endpoint. Persisting a later
			// transaction timestamp and only capping it inside the reader would make
			// a completed EndTs task look permanently pre-epoch after restart.
			candidate = capInitialSnapshotEpoch(candidate, exec.endTs)
			epochState, err = exec.watermarkUpdater.GetOrCreateInitialSnapshotEpochStateForProgress(
				ctx,
				&watermarkKey,
				info.SourceTblId,
				candidate,
				watermark,
				watermarkGeneration,
			)
			if err != nil {
				return err
			}
			initialSnapshotEpoch = epochState.Epoch
		}
		// Publish the execution generation before using progress for admission.
		// This claim and guarded checkpoints serialize on the same watermark row:
		// the reread sees an old checkpoint that won first, or fences one that lost.
		// Explicit StartTs and no-full tasks need the same protection even though
		// they intentionally have no initial-snapshot epoch row.
		watermark, watermarkGeneration, err = exec.watermarkUpdater.ClaimWatermarkOwner(
			ctx, &watermarkKey, ownerFence)
		if err != nil {
			return err
		}
		if initialSnapshot {
			// A stable task can only have a non-empty watermark after its epoch
			// metadata was durable. Missing metadata without an older generation is
			// corruption/manual deletion; choosing a fresh epoch would strand target
			// rows from an unknown source image.
			incomplete, resetTarget, metadataMissing, generationAhead := classifyStableSnapshotRestart(
				watermark, watermarkGeneration, info.SourceTblId, epochState)
			if generationAhead {
				return cdc.NewRetryableSnapshotEpochError(moerr.NewInternalErrorf(
					ctx,
					"CDC source table generation %d is older than durable CDC metadata for %s (watermark generation %d, newer snapshot generation present: %t)",
					info.SourceTblId, watermarkKey.String(), watermarkGeneration,
					epochState.HasNewerGeneration,
				))
			}
			if metadataMissing {
				return moerr.NewInternalErrorf(
					ctx,
					"CDC stable snapshot metadata is missing for %s generation %d with watermark %s",
					watermarkKey.String(), info.SourceTblId, watermark.ToString(),
				)
			}

			// Empty or pre-epoch progress means the initial snapshot is incomplete.
			// If another table ID exists, reset the target under the ownership lock;
			// otherwise retain partial same-epoch target groups for idempotent replay.
			initialSnapshotPending = incomplete
			if resetTarget {
				info.IdChanged = true
			}
			// NewSinker clears IdChanged after a successful target reset, so capture
			// cleanup intent before handing it the mutable table descriptor. A
			// completed current generation may also compact a retired row left by a
			// crash after target commit but before metadata cleanup.
			compactSnapshotEpochs = shouldCompactStableSnapshotEpochs(
				info.IdChanged, incomplete, epochState.HasOtherGeneration)
		}
	}

	// Note: Do NOT clear err_msg here
	// Error should only be cleared when reader successfully syncs data (lazy, eventual consistency)
	// This allows retry count to accumulate properly (1→2→3→4)
	// If cleared here, retry count would reset on every rebuild, making max retry limit ineffective

	tableDef, err := cdc.GetTableDef(ctx, txnOp, exec.cnEngine, info.SourceTblId)
	if err != nil {
		return
	}

	// The attempt owns this routine until it exits. Take one snapshot so a
	// lifecycle transition cannot make the sinker and reader observe different
	// routine pointers.
	routine := exec.currentActiveRoutine()
	if routine == nil {
		return moerr.NewInternalErrorNoCtx("CDC active routine is not initialized")
	}
	info.SetOwnerFence(ownerFence)

	// step 2. new sinker
	sinker, err := cdc.NewSinker(
		ctx,
		exec.sinkUri,
		uint64(exec.spec.Accounts[0].GetId()),
		exec.spec.TaskId,
		info,
		exec.watermarkUpdater,
		tableDef,
		cdc.CDCDefaultRetryTimes,
		cdc.CDCDefaultRetryDuration,
		routine,
		uint64(exec.additionalConfig[cdc.CDCTaskExtraOptions_MaxSqlLength].(float64)),
		exec.additionalConfig[cdc.CDCTaskExtraOptions_SendSqlTimeout].(string),
	)
	info.SetOwnerFence(nil)
	if err != nil {
		return err
	}
	// Sink initialization owns target DDL. A lifecycle transition can race the
	// final successful statement, so reject publication after initialization as
	// well as relying on the statement context itself.
	if err = ctx.Err(); err != nil {
		sinker.Close()
		return err
	}
	if exec.stableInitialSnapshot && compactSnapshotEpochs {
		if err = ownerFence.Check(ctx); err != nil {
			sinker.Close()
			return err
		}
		if err = exec.watermarkUpdater.DeleteInitialSnapshotGenerationsBefore(
			ctx, &watermarkKey, info.SourceTblId); err != nil {
			sinker.Close()
			return err
		}
		if err = ownerFence.Check(ctx); err != nil {
			sinker.Close()
			return err
		}
	}

	// step 3. new reader (using V2 tableChangeStream)
	frequencyStr := exec.additionalConfig[cdc.CDCTaskExtraOptions_Frequency].(string)
	frequency := cdc.ParseFrequencyToDuration(frequencyStr)
	initSnapshotSplitTxn := exec.additionalConfig[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn].(bool)
	if exec.stableInitialSnapshot {
		// Stable-epoch tasks persist the legacy boolean as false so an older CN
		// safely falls back to an atomic transaction during rolling upgrades.
		initSnapshotSplitTxn = true
	}

	reader := cdc.NewTableChangeStream(
		exec.cnTxnClient,
		exec.cnEngine,
		exec.mp,
		exec.packerPool,
		uint64(exec.spec.Accounts[0].GetId()),
		exec.spec.TaskId,
		info,
		sinker,
		exec.watermarkUpdater,
		tableDef,
		initSnapshotSplitTxn,
		exec.runningReaders,
		exec.startTs,
		exec.endTs,
		exec.noFull,
		frequency,
		cdc.WithInitialSnapshotLimiter(exec.initialSnapshotLimiter),
		cdc.WithInitialSnapshotEpoch(initialSnapshotEpoch),
		cdc.WithInitialSnapshotPending(initialSnapshotPending),
		cdc.WithOwnerFence(ownerFence),
	)

	// step 4. start goroutines (sinker first, then reader)
	// Note: Reader will register itself in runningReaders during Run()
	// to prevent duplicate readers (see TableChangeStream.Run line 287)
	go sinker.Run(ctx, routine)
	go reader.Run(ctx, routine)
	// Reader publication happens inside Run. Do not let this callback return
	// before publication, otherwise Cancel can observe both callbackDone and an
	// empty reader map while the newly launched reader is still starting.
	<-reader.RegistrationDone()

	return
}

func (exec *CDCTaskExecutor) retrieveCdcTask(ctx context.Context) error {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	accId := exec.spec.Accounts[0].GetId()
	sql := cdc.CDCSQLBuilder.GetTaskSQL(accId, exec.spec.TaskId)
	res := exec.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "none cdc task for %d %s", accId, exec.spec.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalErrorf(ctx, "duplicate cdc task for %d %s", accId, exec.spec.TaskId)
	}

	//sink_type
	sinkTyp, err := res.GetString(ctx, 0, 1)
	if err != nil {
		return err
	}

	if sinkTyp != cdc.CDCSinkType_Console {
		//sink uri
		jsonSinkUri, err := res.GetString(ctx, 0, 0)
		if err != nil {
			return err
		}

		if err = cdc.JsonDecode(jsonSinkUri, &exec.sinkUri); err != nil {
			return err
		}

		//sink_password
		sinkPwd, err := res.GetString(ctx, 0, 2)
		if err != nil {
			return err
		}

		// TODO replace with creatorAccountId
		if err = exec.initAesKeyByInternalExecutor(ctx, catalog.System_Account); err != nil {
			return err
		}

		if exec.sinkUri.Password, err = cdc.AesCFBDecode(ctx, sinkPwd); err != nil {
			return err
		}
	}

	//update sink type after deserialize
	exec.sinkUri.SinkTyp = sinkTyp

	// tables
	jsonTables, err := res.GetString(ctx, 0, 3)
	if err != nil {
		return err
	}

	if err = cdc.JsonDecode(jsonTables, &exec.tables); err != nil {
		return err
	}

	// exclude
	exclude, err := res.GetString(ctx, 0, 4)
	if err != nil {
		return err
	}
	if exclude != "" {
		if exec.exclude, err = regexp.Compile(exclude); err != nil {
			return err
		}
	}

	// startTs
	startTs, err := res.GetString(ctx, 0, 5)
	if err != nil {
		return err
	}
	if exec.startTs, err = CDCStrToTS(startTs); err != nil {
		return err
	}
	// endTs
	endTs, err := res.GetString(ctx, 0, 6)
	if err != nil {
		return err
	}
	if exec.endTs, err = CDCStrToTS(endTs); err != nil {
		return err
	}

	// noFull
	noFull, err := res.GetString(ctx, 0, 7)
	if err != nil {
		return err
	}
	exec.noFull, _ = strconv.ParseBool(noFull)

	// additionalConfig
	additionalConfigStr, err := res.GetString(ctx, 0, 8)
	if err != nil {
		return err
	}
	if err = json.Unmarshal([]byte(additionalConfigStr), &exec.additionalConfig); err != nil {
		return err
	}

	protocol, _ := exec.additionalConfig[cdc.CDCTaskExtraOptions_InitialSnapshotProtocol].(string)
	exec.stableInitialSnapshot = protocol == cdc.CDCInitialSnapshotProtocolStableEpoch
	return nil
}
