// Copyright 2021 Matrix Origin
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

package dispatch

import (
	"bytes"
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/pSpool"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Dispatch)

const (
	maxMessageSizeToMoRpc = 64 * mpool.MB

	SendToAllLocalFunc = iota
	SendToAllFunc
	SendToAnyLocalFunc
	SendToAnyFunc
	ShuffleToAllFunc
)

type container struct {
	sp *pSpool.PipelineSpool

	server *colexec.Server

	// the clientsession info for the channel you want to dispatch
	remoteReceivers []*process.WrapCs
	remoteInfo      process.RemotePipelineInformationChannel
	remoteProc      *process.Process
	remoteTerminal  *colexec.RemoteReceiverTerminal

	// sendFunc is the rule you want to send batch
	sendFunc func(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error)

	// isRemote specify it is a remote receiver or not
	isRemote bool
	// prepared specify waiting remote receiver ready or not
	prepared bool
	hasData  bool

	// for send-to-any function decide send to which reg
	sendCnt       int
	aliveRegCnt   int
	localRegsCnt  int
	remoteRegsCnt int

	remoteToIdx map[uuid.UUID]int

	batchCnt []int
	rowCnt   []int

	marshalBuf bytes.Buffer

	// pendingBatch is retained across VM quanta while a local spool slot or
	// receiver edge is full. The batch is copied exactly once; signal progress
	// is tracked per receiver so retry never duplicates data.
	pendingBatch     *batch.Batch
	pendingStop      bool
	pendingSpoolSent bool
	pendingSignals   []bool
	retiredRegs      []bool

	// pendingRemoteBatch is retained while an external remote send is running.
	// remoteTask publishes exactly one completion event; the VM continuation
	// then resumes with the original batch without re-reading its child.
	pendingRemoteBatch *batch.Batch
	remoteTask         *remoteDispatchTask
}

type remoteDispatchTask struct {
	mu        sync.Mutex
	done      bool
	end       bool
	err       error
	callbacks []func()
}

func (t *remoteDispatchTask) complete(end bool, err error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return
	}
	t.done = true
	t.end = end
	t.err = err
	callbacks := append([]func(){}, t.callbacks...)
	t.callbacks = nil
	t.mu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

func (t *remoteDispatchTask) result() (done, end bool, err error) {
	if t == nil {
		return true, true, moerr.NewInternalErrorNoCtx("nil remote dispatch task")
	}
	t.mu.Lock()
	done, end, err = t.done, t.end, t.err
	t.mu.Unlock()
	return
}

func (t *remoteDispatchTask) RegisterReady(callback func()) error {
	if callback == nil {
		return moerr.NewInvalidInputNoCtx("nil remote dispatch readiness callback")
	}
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		callback()
		return nil
	}
	t.callbacks = append(t.callbacks, callback)
	t.mu.Unlock()
	return nil
}

type Dispatch struct {
	ctr               *container
	cleanupSpool      *pSpool.PipelineSpool
	allocationAccount *mpool.AllocationAccount

	// MaterializedSource is used by a multi-reference CTE whose consumers can
	// have execution dependencies on one another. It is local-only and bypasses
	// the lock-step pipeline spool fan-out.
	MaterializedSource *materialized.Source

	// IsSink means this is a Sink Node
	IsSink bool
	// RecSink means this is the dispatch operator for `mergeRecursive` pipeline.
	RecSink bool
	// RecCTE means this is the dispatch operator for `mergeCTE` pipeline.
	RecCTE bool

	ShuffleType int32
	// FuncId means the sendFunc you want to call
	FuncId int
	// LocalRegs means the local register you need to send to.
	LocalRegs []*process.WaitRegister
	// RemoteRegs specific the remote reg you need to send to.
	RemoteRegs []colexec.ReceiveInfo
	// for shuffle dispatch
	ShuffleRegIdxLocal  []int
	ShuffleRegIdxRemote []int

	vm.OperatorBase
}

func (dispatch *Dispatch) GetOperatorBase() *vm.OperatorBase {
	return &dispatch.OperatorBase
}

func (dispatch *Dispatch) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if dispatch.allocationAccount != nil && dispatch.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	dispatch.allocationAccount = account
	return nil
}

// ActivatesAllocationAccountLifecycle reports that Dispatch only participates
// in an account already required by an allocation-producing operator.
func (dispatch *Dispatch) ActivatesAllocationAccountLifecycle() bool {
	return false
}

func (dispatch *Dispatch) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if dispatch.allocationAccount == nil {
		return nil
	}
	if dispatch.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if dispatch.ctr != nil && dispatch.ctr.sp != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if dispatch.cleanupSpool != nil {
		dispatch.cleanupSpool.FinalizeAfterConsumersQuiesced()
		dispatch.cleanupSpool = nil
	}
	dispatch.allocationAccount = nil
	return nil
}

func init() {
	reuse.CreatePool[Dispatch](
		func() *Dispatch {
			return &Dispatch{}
		},
		func(a *Dispatch) {
			*a = Dispatch{}
		},
		reuse.DefaultOptions[Dispatch]().
			WithEnableChecker(),
	)
}

func (dispatch Dispatch) TypeName() string {
	return opName
}

func (dispatch *Dispatch) OpType() vm.OpType {
	return vm.Dispatch
}

func NewArgument() *Dispatch {
	return reuse.Alloc[Dispatch](nil)
}

func (dispatch *Dispatch) Release() {
	if dispatch != nil {
		reuse.Free[Dispatch](dispatch, nil)
	}
}

func (dispatch *Dispatch) AdoptCleanupState(from *Dispatch) {
	if dispatch == nil || from == nil {
		return
	}
	dispatch.ctr = from.ctr
	from.ctr = nil
}

func (dispatch *Dispatch) retireLocalReceiver(idx int) {
	if dispatch == nil || dispatch.ctr == nil || idx < 0 || idx >= len(dispatch.LocalRegs) {
		return
	}
	if len(dispatch.ctr.retiredRegs) != len(dispatch.LocalRegs) {
		dispatch.ctr.retiredRegs = make([]bool, len(dispatch.LocalRegs))
	}
	if dispatch.ctr.retiredRegs[idx] {
		return
	}
	dispatch.ctr.retiredRegs[idx] = true
	if dispatch.ctr.sp != nil {
		dispatch.ctr.sp.RetireReceiver(idx)
	}
	if idx < len(dispatch.ctr.pendingSignals) {
		dispatch.ctr.pendingSignals[idx] = true
	}
}

func (dispatch *Dispatch) retireFinishedLocalReceivers() {
	if dispatch == nil || dispatch.ctr == nil {
		return
	}
	for i, reg := range dispatch.LocalRegs {
		if localReceiverTerminal(reg) {
			dispatch.retireLocalReceiver(i)
		}
	}
}

func (dispatch *Dispatch) hasActiveLocalReceiver() bool {
	if dispatch == nil || dispatch.ctr == nil {
		return false
	}
	for i := range dispatch.LocalRegs {
		if i >= len(dispatch.ctr.retiredRegs) || !dispatch.ctr.retiredRegs[i] {
			return true
		}
	}
	return false
}

// sendTerminalSignalsToLocalRegs sends terminalSignal to each local receiver.
// It first tries non-blocking sends via TrySendPipelineSignal, then retries
// any pending receivers with the caller-provided cleanup context.
// Timeout failures are logged via WarnPipelineCleanupf.
func sendTerminalSignalsToLocalRegs(ctx context.Context, proc *process.Process, localRegs []*process.WaitRegister, signal process.PipelineSignal, pipelineFailed bool, err error) []bool {
	if ctx == nil {
		ctx = context.TODO()
	}
	delivered := make([]bool, len(localRegs))
	pendingLocalRegs := make([]int, 0, len(localRegs))
	for i, reg := range localRegs {
		if reg == nil {
			pendingLocalRegs = append(pendingLocalRegs, i)
			continue
		}
		select {
		case <-reg.Done():
			// The receiver already published its terminal state.  Treat the
			// signal as delivered; waiting for a channel that no longer has a
			// consumer would turn normal fanout cleanup into a timeout.
			delivered[i] = true
			continue
		default:
		}
		if process.TrySendPipelineSignal(reg, signal) {
			delivered[i] = true
			continue
		}
		pendingLocalRegs = append(pendingLocalRegs, i)
	}
	if len(pendingLocalRegs) == 0 {
		return delivered
	}
	for _, i := range pendingLocalRegs {
		if process.SendPipelineSignalWithContext(ctx, localRegs[i], signal) {
			delivered[i] = true
			continue
		}
		chLen, chCap := process.WaitRegisterChannelState(localRegs[i])
		process.WarnPipelineCleanupf(
			proc,
			"dispatch_cleanup_send_terminal_signal",
			"dispatch cleanup timed out sending terminal %s signal: timeout=%s local_reg_idx=%d channel_len=%d channel_cap=%d pipeline_failed=%t err=%v",
			signal.EventType.String(),
			process.PipelineSignalSendTimeout,
			i,
			chLen,
			chCap,
			pipelineFailed,
			err)
	}
	return delivered
}

func allTerminalSignalsDelivered(delivered []bool) bool {
	for _, ok := range delivered {
		if !ok {
			return false
		}
	}
	return true
}

func sendAbortSignalsToFailedLocalRegs(ctx context.Context, proc *process.Process, localRegs []*process.WaitRegister, delivered []bool, err error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	fallbackSignal := process.NewAbortSignal(err)
	for i, ok := range delivered {
		if ok {
			continue
		}
		if process.SendPipelineSignalWithContext(ctx, localRegs[i], fallbackSignal) {
			continue
		}
		chLen, chCap := process.WaitRegisterChannelState(localRegs[i])
		process.WarnPipelineCleanupf(
			proc,
			"dispatch_cleanup_send_fallback_abort_signal",
			"dispatch cleanup timed out sending fallback abort signal after end delivery failure: timeout=%s local_reg_idx=%d channel_len=%d channel_cap=%d err=%v",
			process.PipelineSignalSendTimeout,
			i,
			chLen,
			chCap,
			err)
	}
}

func (dispatch *Dispatch) Reset(proc *process.Process, pipelineFailed bool, err error) {
	terminalSignal := process.BuildCleanupSignal(pipelineFailed, err)
	terminalErr := terminalSignal.TerminalErr()
	if dispatch.MaterializedSource != nil {
		dispatch.MaterializedSource.Finish(terminalErr)
		dispatch.ctr = nil
		return
	}
	if dispatch.ctr != nil {
		if dispatch.ctr.isRemote {
			if dispatch.ctr.remoteTerminal != nil {
				dispatch.ctr.remoteTerminal.Finish(terminalErr)
			}
			for _, r := range dispatch.ctr.remoteReceivers {
				if r != nil && r.TerminalBacked {
					// The generation terminal above is the only terminal owner.
					// A legacy Err write here would race the immutable result and
					// can fill the compatibility channel during cleanup.
					continue
				}
				if r == nil || r.Err == nil {
					process.WarnPipelineCleanupf(
						proc,
						"dispatch_cleanup_remote_receiver_nil",
						"dispatch cleanup skipped remote receiver error notification because receiver is nil: pipeline_failed=%t err=%v",
						pipelineFailed,
						terminalErr)
					continue
				}
				select {
				case r.Err <- terminalErr:
				default:
					process.WarnPipelineCleanupf(
						proc,
						"dispatch_cleanup_remote_err_channel_full",
						"dispatch cleanup skipped remote receiver error notification because channel is full: receiver_uuid=%s msg_id=%d pipeline_failed=%t err=%v",
						r.Uid.String(),
						r.MsgId,
						pipelineFailed,
						terminalErr)
				}
			}

			uuids := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
			for i := range dispatch.RemoteRegs {
				uuids = append(uuids, dispatch.RemoteRegs[i].Uuid)
			}
			if dispatch.ctr.server != nil {
				dispatch.ctr.server.CloseRemoteReceivers(uuids, dispatch.ctr.remoteInfo)
			}
		}
	}

	signalCtx, signalCancel := context.WithTimeout(context.TODO(), process.PipelineSignalSendTimeout)
	defer signalCancel()

	if dispatch.ctr != nil && dispatch.ctr.sp != nil {
		sp := dispatch.ctr.sp
		dispatch.retireFinishedLocalReceivers()

		// Send typed terminal signals to all local receivers.
		terminalDelivered := sendTerminalSignalsToLocalRegs(signalCtx, proc, dispatch.LocalRegs, terminalSignal, pipelineFailed, terminalErr)

		if terminalSignal.EventType == process.EventEnd && allTerminalSignalsDelivered(terminalDelivered) {
			dispatch.cleanupSpool = sp
		} else {
			abortErr := terminalErr
			if terminalSignal.EventType == process.EventEnd {
				fallbackErr := process.ResolvePipelineSpoolAbortError(dispatch.LocalRegs...)
				sendAbortSignalsToFailedLocalRegs(signalCtx, proc, dispatch.LocalRegs, terminalDelivered, fallbackErr)
				abortErr = fallbackErr
			}
			sp.Abort(abortErr)
			if dispatch.allocationAccount != nil {
				dispatch.cleanupSpool = sp
			} else {
				dispatch.cleanupSpool = nil
			}
		}
		dispatch.ctr.sp = nil
	} else {
		// No spool: send typed terminal signals directly.
		terminalDelivered := sendTerminalSignalsToLocalRegs(signalCtx, proc, dispatch.LocalRegs, terminalSignal, pipelineFailed, terminalErr)
		if terminalSignal.EventType == process.EventEnd && !allTerminalSignalsDelivered(terminalDelivered) {
			fallbackErr := process.ErrPipelineEndSignalDeliveryFailed
			sendAbortSignalsToFailedLocalRegs(signalCtx, proc, dispatch.LocalRegs, terminalDelivered, fallbackErr)
		}
	}
	dispatch.ctr = nil
}

// CleanupDeferredSpool reclaims spool cache memory after the paired Merge
// cleanup has returned on a normal End path. The normal path drains queued
// GetFromSpool signals; a cleanup-time timeout releases the current reference
// and leaves no receiver goroutine that can read pending signals later.
func (dispatch *Dispatch) CleanupDeferredSpool() {
	if dispatch.cleanupSpool == nil {
		return
	}
	if dispatch.allocationAccount != nil {
		dispatch.cleanupSpool.ReleaseReusableCacheAfterProducerQuiesced()
		return
	}
	dispatch.cleanupSpool.ForceCleanupAfterTerminalSignal()
	dispatch.cleanupSpool = nil
}

func (dispatch *Dispatch) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (dispatch *Dispatch) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
