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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "dispatch"

func (dispatch *Dispatch) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": dispatch")
}

func (dispatch *Dispatch) Prepare(proc *process.Process) error {
	if dispatch.OpAnalyzer == nil {
		dispatch.OpAnalyzer = process.NewAnalyzer(dispatch.GetIdx(), dispatch.IsFirst, dispatch.IsLast, "dispatch")
	} else {
		dispatch.OpAnalyzer.Reset()
	}

	ctr := dispatch.ctr
	if ctr == nil {
		ctr = new(container)
	}
	dispatch.ctr = ctr
	ctr.localRegsCnt = len(dispatch.LocalRegs)
	ctr.remoteRegsCnt = len(dispatch.RemoteRegs)
	ctr.aliveRegCnt = ctr.localRegsCnt + ctr.remoteRegsCnt
	ctr.pendingBatch = nil
	ctr.pendingSpoolSent = false
	ctr.pendingRemoteBatch = nil
	ctr.remoteTask = nil
	ctr.pendingSignals = make([]bool, ctr.localRegsCnt)
	if dispatch.MaterializedSource != nil {
		if dispatch.FuncId != SendToAllLocalFunc || ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "materialized dispatch must be local send-to-all")
		}
		return nil
	}
	ctr.sp = pSpool.InitMyPipelineSpool(proc.Mp(), uint32(len(dispatch.LocalRegs)))

	switch dispatch.FuncId {
	case SendToAllFunc:
		if ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAllFunc should include RemoteRegs")
		}
		if len(dispatch.LocalRegs) == 0 {
			ctr.sendFunc = sendToAllRemoteFunc
		} else {
			ctr.sendFunc = sendToAllFunc
		}
		return dispatch.prepareRemote(proc)

	case ShuffleToAllFunc:
		dispatch.ctr.sendFunc = shuffleToAllFunc
		if dispatch.ctr.remoteRegsCnt > 0 {
			if err := dispatch.prepareRemote(proc); err != nil {
				return err
			}
		} else {
			dispatch.prepareLocal()
		}
		dispatch.ctr.batchCnt = make([]int, ctr.aliveRegCnt)
		dispatch.ctr.rowCnt = make([]int, ctr.aliveRegCnt)

	case SendToAnyFunc:
		if ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAnyFunc should include RemoteRegs")
		}
		if len(dispatch.LocalRegs) == 0 {
			ctr.sendFunc = sendToAnyRemoteFunc
		} else {
			ctr.sendFunc = sendToAnyFunc
		}
		return dispatch.prepareRemote(proc)

	case SendToAllLocalFunc:
		if ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAllLocalFunc should not send to remote")
		}
		ctr.sendFunc = sendToAllLocalFunc
		dispatch.prepareLocal()

	case SendToAnyLocalFunc:
		if ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAnyLocalFunc should not send to remote")
		}
		dispatch.ctr.sendFunc = sendToAnyLocalFunc
		dispatch.prepareLocal()

	default:
		return moerr.NewInternalError(proc.Ctx, "wrong sendFunc id for dispatch")
	}

	return nil
}

func printShuffleResult(dispatch *Dispatch) {
	if dispatch.ctr.batchCnt != nil && dispatch.ctr.rowCnt != nil {
		maxNum := 0
		minNum := 100000000
		for i := range dispatch.ctr.batchCnt {
			if dispatch.ctr.batchCnt[i] > maxNum {
				maxNum = dispatch.ctr.batchCnt[i]
			}
			if dispatch.ctr.batchCnt[i] < minNum {
				minNum = dispatch.ctr.batchCnt[i]
			}
		}
		if maxNum > minNum*10 {
			logutil.Warnf("shuffle imbalance!  type %v,  dispatch result: batchcnt %v, rowcnt %v", dispatch.ShuffleType, dispatch.ctr.batchCnt, dispatch.ctr.rowCnt)
		}
	}
}

func (dispatch *Dispatch) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := dispatch.OpAnalyzer

	result := vm.NewCallResult()
	var err error
	if dispatch.ctr.remoteTask != nil {
		return dispatch.completeRemoteTask(result)
	}

	if dispatch.ctr.pendingBatch != nil {
		result.Batch = dispatch.ctr.pendingBatch
	} else {
		result, err = vm.ChildrenCall(dispatch.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}
	}

	whichToSend := result.Batch
	if result.Batch == nil {
		// A remote dispatch must attach every receiver even when its child
		// produces no batches. Otherwise an early NotRegistered response can
		// outlive this pipeline: cleanup removes the registration before the
		// client's next retry, which then waits until query cancellation.
		if dispatch.ctr.isRemote && !dispatch.ctr.prepared {
			if err = dispatch.startRemoteTask(proc, nil); err != nil {
				return result, err
			}
			if !proc.HasEventSubmitter() {
				return dispatch.completeRemoteTask(result)
			}
			result.Status = vm.ExecWaiting
			result.OnReady = dispatch.ctr.remoteTask.RegisterReady
			return result, nil
		}
		result.Status = vm.ExecStop
		printShuffleResult(dispatch)
		return result, nil
	}

	if whichToSend.Recursive == 1 {
		if !dispatch.ctr.hasData {
			result.Status = vm.ExecStop
			whichToSend.SetEnd()
		} else {
			dispatch.ctr.hasData = false
		}
	} else if whichToSend.IsEmpty() {
		return result, nil
	} else {
		dispatch.ctr.hasData = true
	}

	if dispatch.MaterializedSource != nil {
		// Last/End batches are pipeline control messages, not rows. Ordinary
		// SINK_SCAN consumers discard them in merge.Call; a materialized source
		// must do the same before persisting fanout data.
		if whichToSend.Last() {
			return result, nil
		}
		stats, err := dispatch.MaterializedSource.AppendWithStats(whichToSend)
		analyzer.SetMemUsed(stats.RetainedBytes)
		if stats.SpilledBytes > 0 {
			analyzer.Spill(stats.SpilledBytes)
			analyzer.SpillRows(stats.SpilledRows)
		}
		return result, err
	}

	if dispatch.ctr.pendingBatch == nil &&
		(dispatch.FuncId == SendToAllLocalFunc || dispatch.FuncId == SendToAnyLocalFunc) {
		dispatch.ctr.pendingBatch = whichToSend
		dispatch.ctr.pendingSpoolSent = false
		for i := range dispatch.ctr.pendingSignals {
			dispatch.ctr.pendingSignals[i] = false
		}
	}

	if dispatch.FuncId == SendToAllLocalFunc || dispatch.FuncId == SendToAnyLocalFunc {
		done, onReady, sendErr := dispatch.sendLocalPending(proc)
		if sendErr != nil {
			return result, sendErr
		}
		if onReady != nil {
			result.Status = vm.ExecWaiting
			result.OnReady = onReady
			return result, nil
		}
		if done {
			result.Status = vm.ExecStop
		}
		return result, nil
	}

	// sending.
	if dispatch.ctr.isRemote || dispatch.FuncId == ShuffleToAllFunc {
		dispatch.ctr.pendingRemoteBatch = whichToSend
		if err = dispatch.startRemoteTask(proc, whichToSend); err != nil {
			dispatch.ctr.pendingRemoteBatch = nil
			return result, err
		}
		// Direct operator callers (unit tests and small embedded execution
		// helpers) do not install a query scheduler.  startRemoteTask executes
		// synchronously at that boundary, so consume the completed task now;
		// production continuations always take the readiness path below.
		if !proc.HasEventSubmitter() {
			return dispatch.completeRemoteTask(result)
		}
		result.Status = vm.ExecWaiting
		result.OnReady = dispatch.ctr.remoteTask.RegisterReady
		return result, nil
	}
	ok, err := dispatch.ctr.sendFunc(whichToSend, dispatch, proc)
	if ok {
		result.Status = vm.ExecStop
	}
	return result, err
}

func (dispatch *Dispatch) completeRemoteTask(result vm.CallResult) (vm.CallResult, error) {
	done, end, taskErr := dispatch.ctr.remoteTask.result()
	if !done {
		result.Status = vm.ExecWaiting
		result.OnReady = dispatch.ctr.remoteTask.RegisterReady
		return result, nil
	}
	dispatch.ctr.remoteTask = nil
	if taskErr != nil {
		dispatch.ctr.pendingRemoteBatch = nil
		return result, taskErr
	}
	if dispatch.ctr.pendingRemoteBatch == nil {
		// The task only attached remote registrations for an empty child.
		// The child has already reached its terminal batch, so do not call it
		// a second time merely to observe the registration result.
		result.Status = vm.ExecStop
		return result, nil
	}
	result.Batch = dispatch.ctr.pendingRemoteBatch
	dispatch.ctr.pendingRemoteBatch = nil
	if end {
		result.Status = vm.ExecStop
	}
	return result, nil
}

func (dispatch *Dispatch) startRemoteTask(proc *process.Process, bat *batch.Batch) error {
	if dispatch == nil || dispatch.ctr == nil || dispatch.ctr.sendFunc == nil {
		return moerr.NewInternalErrorNoCtx("dispatch task requested before send function setup")
	}
	if dispatch.ctr.remoteTask != nil {
		return moerr.NewInternalErrorNoCtx("remote dispatch task already pending")
	}
	task := &remoteDispatchTask{}
	dispatch.ctr.remoteTask = task
	taskFn := func() {
		var end bool
		var err error
		if bat == nil {
			if !dispatch.ctr.isRemote {
				task.complete(true, moerr.NewInternalErrorNoCtx("local dispatch cannot wait for remote registration"))
				return
			}
			_, err = dispatch.waitRemoteRegsReady(proc)
		} else {
			end, err = dispatch.ctr.sendFunc(bat, dispatch, proc)
		}
		task.complete(end, err)
	}
	if !proc.HasEventSubmitter() {
		taskFn()
		return nil
	}
	if err := proc.SubmitEvent("dispatch-remote-send", taskFn); err != nil {
		dispatch.ctr.remoteTask = nil
		return err
	}
	return nil
}

func (dispatch *Dispatch) waitRemoteRegsReady(proc *process.Process) (bool, error) {
	cnt := len(dispatch.RemoteRegs)

	for cnt > 0 {
		select {
		case <-proc.Ctx.Done():
			dispatch.ctr.prepared = true
			return false, remoteRegistrationCancelCause(proc.Ctx)

		case csinfo, ok := <-dispatch.ctr.remoteInfo:
			if !ok || csinfo == nil {
				return false, moerr.NewInternalError(
					proc.Ctx,
					"remote receiver registration channel closed before all receivers attached",
				)
			}
			dispatch.ctr.remoteReceivers = append(dispatch.ctr.remoteReceivers, csinfo)
			cnt--
		}
	}
	dispatch.ctr.prepared = true
	return false, nil
}

// sendLocalPending is the resumable local dispatch state machine. It copies a
// batch into the spool once, then publishes each receiver signal as capacity
// becomes available. No polling or channel send can park a VM worker.
func (dispatch *Dispatch) sendLocalPending(proc *process.Process) (bool, func(func()) error, error) {
	if dispatch.ctr.pendingBatch == nil {
		return false, nil, nil
	}
	receiverID := pSpool.SendToAllLocal
	if dispatch.FuncId == SendToAnyLocalFunc {
		if dispatch.ctr.localRegsCnt == 0 {
			return true, nil, nil
		}
		receiverID = dispatch.ctr.sendCnt % dispatch.ctr.localRegsCnt
		if localReceiverTerminal(dispatch.LocalRegs[receiverID]) {
			dispatch.ctr.pendingBatch = nil
			return true, nil, nil
		}
	} else {
		// A terminal receiver is a completed local dispatch, not a full
		// downstream edge.  Check before copying the batch into the spool so
		// an aborted remote receiver cannot leave an un-signalled spool slot
		// that repeatedly re-arms the continuation.
		for _, reg := range dispatch.LocalRegs {
			if localReceiverTerminal(reg) {
				dispatch.ctr.pendingBatch = nil
				return true, nil, nil
			}
		}
	}
	if !dispatch.ctr.pendingSpoolSent {
		queryDone, sent, err := dispatch.ctr.sp.TrySendBatch(
			receiverID, dispatch.ctr.pendingBatch, nil)
		if err != nil {
			return false, nil, err
		}
		if queryDone {
			dispatch.ctr.pendingBatch = nil
			return true, nil, nil
		}
		if !sent {
			return false, dispatch.ctr.sp.RegisterSendReady, nil
		}
		dispatch.ctr.pendingSpoolSent = true
	}

	if dispatch.FuncId == SendToAnyLocalFunc {
		if !dispatch.ctr.pendingSignals[receiverID] {
			reg := dispatch.LocalRegs[receiverID]
			if !reg.TrySendData(dispatch.ctr.sp, receiverID) {
				return false, reg.RegisterCapacityReady, nil
			}
			dispatch.ctr.pendingSignals[receiverID] = true
		}
		dispatch.ctr.sendCnt++
	} else {
		for i, reg := range dispatch.LocalRegs {
			if dispatch.ctr.pendingSignals[i] {
				continue
			}
			if !reg.TrySendData(dispatch.ctr.sp, i) {
				return false, reg.RegisterCapacityReady, nil
			}
			dispatch.ctr.pendingSignals[i] = true
		}
	}
	dispatch.ctr.pendingBatch = nil
	dispatch.ctr.pendingSpoolSent = false
	for i := range dispatch.ctr.pendingSignals {
		dispatch.ctr.pendingSignals[i] = false
	}
	return false, nil, nil
}

func localReceiverTerminal(reg *process.WaitRegister) bool {
	if reg == nil {
		return true
	}
	select {
	case <-reg.Done():
		return true
	default:
		return false
	}
}

func remoteRegistrationCancelCause(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return context.Canceled
}

// RemoteReceiverRegistration owns one early-published set of receiver UUIDs.
type RemoteReceiverRegistration struct {
	dispatch *Dispatch
	ctr      *container
	proc     *process.Process
	ch       process.RemotePipelineInformationChannel
	uuids    []uuid.UUID
	server   *colexec.Server
	terminal *colexec.RemoteReceiverTerminal
	ctx      context.Context
}

// Cancel stops the process that owns this registration.
func (r *RemoteReceiverRegistration) Cancel(err error) {
	if r != nil {
		r.terminal.Cancel(err)
	}
}

// Cleanup removes only the exact registration represented by this handle.
// It is safe after Dispatch.Reset because the handle retains the owner tuple.
func (r *RemoteReceiverRegistration) Cleanup() {
	if r == nil {
		return
	}
	// A registration rolled back before execution never reaches Reset. Fail
	// closed, preserving its cancellation cause, rather than stranding an
	// already-attached notify on an unpublished terminal result.
	select {
	case <-r.terminal.Done():
	default:
		var err error
		if r.ctx != nil {
			err = context.Cause(r.ctx)
		}
		if err == nil {
			err = moerr.NewInternalErrorNoCtx("remote dispatch registration released before termination")
		}
		r.terminal.Finish(err)
	}
	r.server.CloseRemoteReceivers(r.uuids, r.ch)
	r.server.RemoveUuidsOwned(r.uuids, r.ch)
	if r.dispatch.ctr == r.ctr && r.ctr.remoteProc == r.proc && r.ctr.remoteInfo == r.ch {
		r.ctr.remoteInfo = nil
		r.ctr.remoteProc = nil
		r.ctr.remoteReceivers = nil
		r.ctr.prepared = false
	}
}

// RegisterRemoteReceivers publishes remote receiver UUIDs before the dispatch
// operator reaches Prepare, so remote notify streams can attach early.
func (dispatch *Dispatch) RegisterRemoteReceivers(proc *process.Process) error {
	_, err := dispatch.RegisterRemoteReceiversWithHandle(proc)
	return err
}

// RegisterRemoteReceiversWithHandle publishes the receivers and returns their
// cleanup handle. A nil handle means an earlier traversal already registered
// this dispatch.
func (dispatch *Dispatch) RegisterRemoteReceiversWithHandle(proc *process.Process) (*RemoteReceiverRegistration, error) {
	if len(dispatch.RemoteRegs) == 0 {
		return nil, nil
	}
	if dispatch.ctr == nil {
		dispatch.ctr = new(container)
	}
	alreadyRegistered := dispatch.ctr.remoteInfo != nil
	if err := dispatch.prepareRemote(proc); err != nil {
		return nil, err
	}
	if alreadyRegistered {
		return nil, nil
	}
	uuids := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
	for i := range dispatch.RemoteRegs {
		uuids = append(uuids, dispatch.RemoteRegs[i].Uuid)
	}
	return &RemoteReceiverRegistration{
		dispatch: dispatch,
		ctr:      dispatch.ctr,
		proc:     proc,
		ch:       dispatch.ctr.remoteInfo,
		uuids:    uuids,
		server:   dispatch.ctr.server,
		terminal: dispatch.ctr.remoteTerminal,
		ctx:      proc.Ctx,
	}, nil
}

func (dispatch *Dispatch) prepareRemote(proc *process.Process) error {
	server := colexec.GetServer(proc.GetService())
	if server == nil {
		return moerr.NewInternalErrorf(proc.Ctx, "colexec server is not initialized for CN %s", proc.GetService())
	}
	dispatch.ctr.server = server
	if dispatch.ctr.remoteInfo != nil && dispatch.ctr.remoteProc != nil && dispatch.ctr.remoteProc != proc {
		return moerr.NewInternalErrorNoCtx("remote receiver registered with a different process")
	}
	dispatch.ctr.prepared = false
	dispatch.ctr.isRemote = true
	dispatch.ctr.remoteRegsCnt = len(dispatch.RemoteRegs)
	dispatch.ctr.remoteReceivers = make([]*process.WrapCs, 0, dispatch.ctr.remoteRegsCnt)
	dispatch.ctr.remoteToIdx = make(map[uuid.UUID]int)
	needRegister := dispatch.ctr.remoteInfo == nil
	if needRegister {
		dispatch.ctr.remoteInfo = make(chan *process.WrapCs)
		dispatch.ctr.remoteProc = proc
		dispatch.ctr.remoteTerminal = colexec.NewRemoteReceiverTerminal(proc.Cancel)
	}
	registered := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
	for i, rr := range dispatch.RemoteRegs {
		if dispatch.FuncId == ShuffleToAllFunc {
			dispatch.ctr.remoteToIdx[rr.Uuid] = dispatch.ShuffleRegIdxRemote[i]
		}
		if needRegister {
			if err := server.PutProcIntoUuidMapWithTerminal(rr.Uuid, proc, dispatch.ctr.remoteInfo, dispatch.ctr.remoteTerminal); err != nil {
				dispatch.ctr.remoteTerminal.Finish(err)
				if proc != nil && proc.Cancel != nil {
					proc.Cancel(err)
				}
				rollbackRemoteReceiverRegistrations(server, registered, dispatch.ctr.remoteInfo)
				dispatch.ctr.remoteInfo = nil
				dispatch.ctr.remoteProc = nil
				return err
			}
			registered = append(registered, rr.Uuid)
		}
	}
	return nil
}

func rollbackRemoteReceiverRegistrations(
	server *colexec.Server,
	registered []uuid.UUID,
	ch process.RemotePipelineInformationChannel,
) {
	server.CloseRemoteReceivers(registered, ch)
	server.RemoveUuidsOwned(registered, ch)
}

func (dispatch *Dispatch) prepareLocal() {
	dispatch.ctr.prepared = true
	dispatch.ctr.isRemote = false
	dispatch.ctr.remoteReceivers = nil
}
