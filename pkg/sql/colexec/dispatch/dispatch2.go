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
	"context"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = new(container)
	arg.ctr.startSending()
	arg.ctr.localRegsCnt = len(arg.LocalRegs)
	arg.ctr.remoteRegsCnt = len(arg.RemoteRegs)
	arg.ctr.aliveRegCnt = arg.ctr.localRegsCnt + arg.ctr.remoteRegsCnt

	switch arg.FuncId {
	case SendToAllFunc:
		if arg.ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "invalid dispatch argument: SendToAllFunc requires RemoteRegs")
		}
		if err = arg.waitRemoteReceiversReady(proc); err != nil {
			return err
		}
		if arg.ctr.localRegsCnt == 0 {
			arg.ctr.sendFunc2 = arg.sendToAllRemoteReceivers
		} else {
			arg.ctr.sendFunc2 = arg.sendToAllReceivers
		}

	case ShuffleToAllFunc:
		if arg.ctr.remoteRegsCnt > 0 {
			if err = arg.waitRemoteReceiversReady(proc); err != nil {
				return err
			}
		}
		arg.ctr.batchCnt = make([]int, arg.ctr.aliveRegCnt)
		arg.ctr.rowCnt = make([]int, arg.ctr.aliveRegCnt)

	case SendToAnyFunc:
		if arg.ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "invalid dispatch argument: SendToAnyFunc requires RemoteRegs")
		}
		if err = arg.waitRemoteReceiversReady(proc); err != nil {
			return err
		}
		if arg.ctr.localRegsCnt == 0 {
			arg.ctr.sendFunc2 = arg.sendToAnyRemoteReceiver
		} else {
			arg.ctr.sendFunc2 = arg.sendToAnyReceiver
		}

	case SendToAllLocalFunc:
		if arg.ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "invalid dispatch argument: SendToAllLocalFunc requires no RemoteRegs")
		}
		arg.ctr.sendFunc2 = arg.sendToAllLocalReceivers

	case SendToAnyLocalFunc:
		if arg.ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "invalid dispatch argument: SendToAnyLocalFunc requires no RemoteRegs")
		}
		arg.ctr.sendFunc2 = arg.sendToAnyLocalReceiver

	default:
		return moerr.NewInternalError(proc.Ctx, "unknown send function id for dispatch")
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}
	result, err := arg.Children[0].Call(proc)
	if err != nil {
		return result, err
	}
	analyze := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	analyze.Start()
	defer analyze.Stop()

	sendBatch := result.Batch
	if sendBatch == nil {
		if arg.RecSink {
			sendBatch, err = makeEndBatch(proc)
			if err != nil {
				return result, err
			}
			defer func() {
				if sendBatch != nil {
					proc.PutBatch(sendBatch)
				}
			}()

		} else {
			result.Status = vm.ExecStop
			return result, nil
		}
	}

	// I just know that the hasData field is used for the CTE.
	// and I kept it.
	if sendBatch.Last() {
		if !arg.ctr.hasData {
			sendBatch.SetEnd()
		} else {
			arg.ctr.hasData = false
		}
	} else if sendBatch.IsEmpty() {
		result.Batch = batch.EmptyBatch
		return result, nil
	} else {
		arg.ctr.hasData = true
	}

	if err = arg.ctr.sendFunc2(proc, sendBatch); err != nil {
		return result, err
	}
	if arg.ctr.isStopSending() {
		result.Status = vm.ExecStop
	}
	return result, nil
}

// waitRemoteReceiversReady do prepare work for remote receivers. and wait for all remote receivers ready.
func (arg *Argument) waitRemoteReceiversReady(proc *process.Process) (err error) {
	// prepare work for remote receivers.
	arg.ctr.remoteReceivers = make([]process.WrapCs, 0, arg.ctr.remoteRegsCnt)
	if arg.FuncId == ShuffleToAllFunc {
		arg.ctr.remoteToIdx = make(map[uuid.UUID]int, arg.ctr.remoteRegsCnt)
		for i, rr := range arg.RemoteRegs {
			arg.ctr.remoteToIdx[rr.Uuid] = arg.ShuffleRegIdxRemote[i]
			if err = colexec.Srv.PutProcIntoUuidMap(rr.Uuid, proc); err != nil {
				return err
			}
		}
	} else {
		for _, rr := range arg.RemoteRegs {
			if err = colexec.Srv.PutProcIntoUuidMap(rr.Uuid, proc); err != nil {
				return err
			}
		}
	}

	// wait for all remote receivers ready.
	cnt := arg.ctr.remoteRegsCnt
	for cnt > 0 {
		ctx, cancel := context.WithTimeout(context.TODO(), waitNotifyTimeout)
		select {
		case <-ctx.Done():
			cancel()
			return moerr.NewInternalErrorNoCtx("wait notify message timeout")

		case <-proc.Ctx.Done():
			cancel()
			return moerr.NewInternalErrorNoCtx("process has done")

		case receiver := <-proc.DispatchNotifyCh:
			cancel()
			arg.ctr.remoteReceivers = append(arg.ctr.remoteReceivers, receiver)
			cnt--
		}
	}

	return err
}
