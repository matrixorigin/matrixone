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
	ctr *container

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

func (dispatch *Dispatch) Reset(proc *process.Process, pipelineFailed bool, err error) {
	newDirectSignal := func() process.PipelineSignal {
		if proc == nil {
			return process.NewPipelineSignalToDirectly(nil, err, nil)
		}
		return process.NewPipelineSignalToDirectly(nil, err, proc.Mp())
	}

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

	// told the local receiver to stop if it is still running.
	if dispatch.ctr != nil && dispatch.ctr.sp != nil {
		sendCtx, cancel := context.WithTimeout(context.TODO(), process.PipelineSignalSendTimeout)
		queryDone, sendErr := dispatch.ctr.sp.SendBatch(sendCtx, pSpool.SendToAllLocal, nil, err)
		cancel()

		terminalSignalName := "direct"
		if sendErr == nil && !queryDone {
			terminalSignalName = "spool"
		} else {
			process.WarnPipelineCleanupf(
				proc,
				"dispatch_cleanup_enqueue_terminal_spool",
				"dispatch cleanup timed out enqueueing terminal spool message: timeout=%s query_done=%t err=%v",
				process.PipelineSignalSendTimeout,
				queryDone,
				sendErr)
		}

		allTerminalSignalsSent := true
		pendingLocalRegs := make([]int, 0, len(dispatch.LocalRegs))
		for i, reg := range dispatch.LocalRegs {
			terminalSignal := newDirectSignal()
			if terminalSignalName == "spool" {
				terminalSignal = process.NewPipelineSignalToGetFromSpool(dispatch.ctr.sp, i)
			}
			if process.TrySendPipelineSignal(reg, terminalSignal) {
				continue
			}
			pendingLocalRegs = append(pendingLocalRegs, i)
		}

		signalCtx, signalCancel := context.WithTimeout(context.TODO(), process.PipelineSignalSendTimeout)
		for _, i := range pendingLocalRegs {
			terminalSignal := newDirectSignal()
			if terminalSignalName == "spool" {
				terminalSignal = process.NewPipelineSignalToGetFromSpool(dispatch.ctr.sp, i)
			}
			sentTerminalSignal := process.SendPipelineSignalWithContext(signalCtx, dispatch.LocalRegs[i], terminalSignal)
			if !sentTerminalSignal {
				allTerminalSignalsSent = false
			}
			if !sentTerminalSignal {
				chLen, chCap := process.WaitRegisterChannelState(dispatch.LocalRegs[i])
				process.WarnPipelineCleanupf(
					proc,
					"dispatch_cleanup_send_terminal_signal",
					"dispatch cleanup timed out sending terminal %s signal: timeout=%s local_reg_idx=%d channel_len=%d channel_cap=%d pipeline_failed=%t err=%v",
					terminalSignalName,
					process.PipelineSignalSendTimeout,
					i,
					chLen,
					chCap,
					pipelineFailed,
					err)
			}
		}
		signalCancel()

		if terminalSignalName == "spool" && allTerminalSignalsSent {
			if !dispatch.ctr.sp.CloseWithTimeout(process.PipelineSignalSendTimeout) {
				process.WarnPipelineCleanupf(
					proc,
					"dispatch_cleanup_close_spool",
					"dispatch cleanup timed out closing pipeline spool: timeout=%s pipeline_failed=%t err=%v",
					process.PipelineSignalSendTimeout,
					pipelineFailed,
					err)
			}
		} else {
			process.WarnPipelineCleanupf(
				proc,
				"dispatch_cleanup_skip_spool_close",
				"dispatch cleanup skipped waiting for pipeline spool close after terminal %s signal delivery failed or fell back: delivered_all=%t pipeline_failed=%t err=%v",
				terminalSignalName,
				allTerminalSignalsSent,
				pipelineFailed,
				err)
		}
		dispatch.ctr.sp = nil
	} else {
		pendingLocalRegs := make([]int, 0, len(dispatch.LocalRegs))
		for i, reg := range dispatch.LocalRegs {
			if process.TrySendPipelineSignal(reg, newDirectSignal()) {
				continue
			}
			pendingLocalRegs = append(pendingLocalRegs, i)
		}

		signalCtx, signalCancel := context.WithTimeout(context.TODO(), process.PipelineSignalSendTimeout)
		for _, i := range pendingLocalRegs {
			if !process.SendPipelineSignalWithContext(
				signalCtx,
				dispatch.LocalRegs[i],
				newDirectSignal()) {
				chLen, chCap := process.WaitRegisterChannelState(dispatch.LocalRegs[i])
				process.WarnPipelineCleanupf(
					proc,
					"dispatch_cleanup_send_terminal_signal",
					"dispatch cleanup timed out sending terminal direct signal: timeout=%s local_reg_idx=%d channel_len=%d channel_cap=%d pipeline_failed=%t err=%v",
					process.PipelineSignalSendTimeout,
					i,
					chLen,
					chCap,
					pipelineFailed,
					err)
			}
		}
		signalCancel()
	}
	dispatch.ctr = nil
}

func (dispatch *Dispatch) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (dispatch *Dispatch) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
