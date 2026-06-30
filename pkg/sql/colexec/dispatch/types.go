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

	"github.com/matrixorigin/matrixone/pkg/container/pSpool"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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

	// the clientsession info for the channel you want to dispatch
	remoteReceivers []*process.WrapCs
	remoteInfo      process.RemotePipelineInformationChannel

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
	ctr          *container
	cleanupSpool *pSpool.PipelineSpool

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

// sendTerminalSignalsToLocalRegs sends terminalSignal to each local receiver.
// It first tries non-blocking sends via TrySendPipelineSignal, then retries
// any pending receivers with a timeout via SendPipelineSignalWithContext.
// Timeout failures are logged via WarnPipelineCleanupf.
func sendTerminalSignalsToLocalRegs(proc *process.Process, localRegs []*process.WaitRegister, signal process.PipelineSignal, pipelineFailed bool, err error) {
	pendingLocalRegs := make([]int, 0, len(localRegs))
	for i, reg := range localRegs {
		if process.TrySendPipelineSignal(reg, signal) {
			continue
		}
		pendingLocalRegs = append(pendingLocalRegs, i)
	}
	if len(pendingLocalRegs) == 0 {
		return
	}
	signalCtx, signalCancel := context.WithTimeout(context.TODO(), process.PipelineSignalSendTimeout)
	defer signalCancel()
	for _, i := range pendingLocalRegs {
		if process.SendPipelineSignalWithContext(signalCtx, localRegs[i], signal) {
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
}

func (dispatch *Dispatch) Reset(proc *process.Process, pipelineFailed bool, err error) {
	terminalSignal := process.BuildCleanupSignal(pipelineFailed, err)

	if dispatch.ctr != nil {
		if dispatch.ctr.isRemote {
			for _, r := range dispatch.ctr.remoteReceivers {
				if r == nil || r.Err == nil {
					process.WarnPipelineCleanupf(
						proc,
						"dispatch_cleanup_remote_receiver_nil",
						"dispatch cleanup skipped remote receiver error notification because receiver is nil: pipeline_failed=%t err=%v",
						pipelineFailed,
						err)
					continue
				}
				select {
				case r.Err <- err:
				default:
					process.WarnPipelineCleanupf(
						proc,
						"dispatch_cleanup_remote_err_channel_full",
						"dispatch cleanup skipped remote receiver error notification because channel is full: receiver_uuid=%s msg_id=%d pipeline_failed=%t err=%v",
						r.Uid.String(),
						r.MsgId,
						pipelineFailed,
						err)
				}
			}

			uuids := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
			for i := range dispatch.RemoteRegs {
				uuids = append(uuids, dispatch.RemoteRegs[i].Uuid)
			}
			colexec.Get().DeleteUuids(uuids)
		}
	}

	if dispatch.ctr != nil && dispatch.ctr.sp != nil {
		sp := dispatch.ctr.sp

		// Send typed terminal signals to all local receivers.
		sendTerminalSignalsToLocalRegs(proc, dispatch.LocalRegs, terminalSignal, pipelineFailed, err)

		// Since we send typed terminal signals directly (not via spool),
		// the receiver exits without draining the spool. Use Abort()
		// for immediate resource release instead of CloseWithTimeout.
		sp.Abort()
		dispatch.ctr.sp = nil
	} else {
		// No spool: send typed terminal signals directly.
		sendTerminalSignalsToLocalRegs(proc, dispatch.LocalRegs, terminalSignal, pipelineFailed, err)
	}
	dispatch.ctr = nil
}

// cleanupSpool is deprecated. With typed terminal signals + sp.Abort() in Reset(),
// deferred cleanup is no longer needed. The field and method exist only to satisfy
// the vm.Operator interface (called by pkg/vm/pipeline/types.go cleanup walks).
// Kept for backward compatibility — will be removed when the interface is updated.
func (dispatch *Dispatch) CleanupDeferredSpool() {
	if dispatch.cleanupSpool == nil {
		return
	}
	dispatch.cleanupSpool.ForceCleanup()
	dispatch.cleanupSpool = nil
}

func (dispatch *Dispatch) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (dispatch *Dispatch) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
