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

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	ctr := new(container)
	dispatch.ctr = ctr
	ctr.localRegsCnt = len(dispatch.LocalRegs)
	ctr.remoteRegsCnt = len(dispatch.RemoteRegs)
	ctr.aliveRegCnt = ctr.localRegsCnt + ctr.remoteRegsCnt
	ctr.sp = pSpool.InitMyPipelineSpool(proc.Mp(), ctr.localRegsCnt)

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
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	result, err := dispatch.Children[0].Call(proc)
	if err != nil {
		return result, err
	}

	whichToSend := result.Batch
	if result.Batch == nil {
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

	// sending.
	ok, err := dispatch.ctr.sendFunc(whichToSend, dispatch, proc)
	if ok {
		result.Status = vm.ExecStop
	}
	return result, err
}

func (dispatch *Dispatch) waitRemoteRegsReady(proc *process.Process) (bool, error) {
	cnt := len(dispatch.RemoteRegs)
	for cnt > 0 {
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), waitNotifyTimeout)
		select {
		case <-timeoutCtx.Done():
			timeoutCancel()
			return false, moerr.NewInternalErrorNoCtx("wait notify message timeout")

		case <-proc.Ctx.Done():
			timeoutCancel()
			dispatch.ctr.prepared = true
			return true, nil

		case csinfo := <-proc.DispatchNotifyCh:
			timeoutCancel()
			dispatch.ctr.remoteReceivers = append(dispatch.ctr.remoteReceivers, csinfo)
			cnt--
		}
	}
	dispatch.ctr.prepared = true
	return false, nil
}

func (dispatch *Dispatch) prepareRemote(proc *process.Process) error {
	dispatch.ctr.prepared = false
	dispatch.ctr.isRemote = true
	dispatch.ctr.remoteReceivers = make([]*process.WrapCs, 0, dispatch.ctr.remoteRegsCnt)
	dispatch.ctr.remoteToIdx = make(map[uuid.UUID]int)
	for i, rr := range dispatch.RemoteRegs {
		if dispatch.FuncId == ShuffleToAllFunc {
			dispatch.ctr.remoteToIdx[rr.Uuid] = dispatch.ShuffleRegIdxRemote[i]
		}
		if err := colexec.Get().PutProcIntoUuidMap(rr.Uuid, proc); err != nil {
			return err
		}
	}
	return nil
}

func (dispatch *Dispatch) prepareLocal() {
	dispatch.ctr.prepared = true
	dispatch.ctr.isRemote = false
	dispatch.ctr.remoteReceivers = nil
}
