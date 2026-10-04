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

	"github.com/matrixorigin/matrixone/pkg/container/pSpool"

	"github.com/google/uuid"
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

// Publish once per sender/receiver. Queue delivery is only an optimization;
// the edge owns durable completion and the first fatal cause.
func publishTerminalSignalsToLocalRegs(proc *process.Process, localRegs []*process.WaitRegister, signal process.PipelineSignal) bool {
	allEnded := true
	for i, reg := range localRegs {
		effective, ok := reg.PublishTerminal(signal)
		if ok {
			if effective.EventType != process.EventEnd {
				allEnded = false
			}
			continue
		}
		allEnded = false
		chLen, chCap := process.WaitRegisterChannelState(reg)
		process.WarnPipelineCleanupf(
			proc,
			"dispatch_cleanup_send_terminal_signal",
			"dispatch cleanup could not publish terminal %s signal: local_reg_idx=%d channel_len=%d channel_cap=%d err=%v",
			signal.EventType.String(),
			i,
			chLen,
			chCap,
			signal.TerminalErr())
	}
	return allEnded
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

	allEnded := publishTerminalSignalsToLocalRegs(proc, dispatch.LocalRegs, terminalSignal)
	if dispatch.ctr != nil && dispatch.ctr.sp != nil {
		sp := dispatch.ctr.sp

		if terminalSignal.EventType == process.EventEnd && allEnded {
			dispatch.cleanupSpool = sp
		} else {
			abortErr := terminalErr
			if !allEnded {
				// The common resolver prefers a substantive recorded cause over
				// synthetic delivery fallout on an earlier receiver.
				effectiveErr := process.ResolvePipelineSpoolAbortError(dispatch.LocalRegs...)
				if abortErr == nil || effectiveErr != process.ErrPipelineEndSignalDeliveryFailed {
					abortErr = effectiveErr
				}
			}
			sp.Abort(abortErr)
			if dispatch.allocationAccount != nil {
				dispatch.cleanupSpool = sp
			} else {
				dispatch.cleanupSpool = nil
			}
		}
		dispatch.ctr.sp = nil
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
