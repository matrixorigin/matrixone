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

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "dispatch"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": dispatch")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ctr := new(container)
	arg.ctr = ctr
	ctr.localRegsCnt = len(arg.LocalRegs)
	ctr.remoteRegsCnt = len(arg.RemoteRegs)
	ctr.aliveRegCnt = ctr.localRegsCnt + ctr.remoteRegsCnt

	switch arg.FuncId {
	case SendToAllFunc:
		if ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAllFunc should include RemoteRegs")
		}
		if len(arg.LocalRegs) == 0 {
			ctr.sendFunc = sendToAllRemoteFunc
		} else {
			ctr.sendFunc = sendToAllFunc
		}
		return arg.prepareRemote(proc)

	case ShuffleToAllFunc:
		arg.ctr.sendFunc = shuffleToAllFunc
		if arg.ctr.remoteRegsCnt > 0 {
			if err := arg.prepareRemote(proc); err != nil {
				return err
			}
		} else {
			arg.prepareLocal()
		}
		arg.ctr.batchCnt = make([]int, ctr.aliveRegCnt)
		arg.ctr.rowCnt = make([]int, ctr.aliveRegCnt)

	case SendToAnyFunc:
		if ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAnyFunc should include RemoteRegs")
		}
		if len(arg.LocalRegs) == 0 {
			ctr.sendFunc = sendToAnyRemoteFunc
		} else {
			ctr.sendFunc = sendToAnyFunc
		}
		return arg.prepareRemote(proc)

	case SendToAllLocalFunc:
		if ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAllLocalFunc should not send to remote")
		}
		ctr.sendFunc = sendToAllLocalFunc
		arg.prepareLocal()

	case SendToAnyLocalFunc:
		if ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAnyLocalFunc should not send to remote")
		}
		arg.ctr.sendFunc = sendToAnyLocalFunc
		arg.prepareLocal()

	default:
		return moerr.NewInternalError(proc.Ctx, "wrong sendFunc id for dispatch")
	}

	return nil
}

func printShuffleResult(arg *Argument) {
	if arg.ctr.batchCnt != nil && arg.ctr.rowCnt != nil {
		logutil.Debugf("shuffle type %v,  dispatch result: batchcnt %v, rowcnt %v", arg.ShuffleType, arg.ctr.batchCnt, arg.ctr.rowCnt)
	}
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ap := arg

	result, err := arg.Children[0].Call(proc)
	if err != nil {
		return result, err
	}

	bat := result.Batch

	if result.Batch == nil {
		if ap.RecSink {
			bat, err = makeEndBatch(proc)
			if err != nil {
				return result, err
			}
			defer func() {
				if bat != nil {
					proc.PutBatch(bat)
				}
			}()
		} else {
			printShuffleResult(ap)
			result.Status = vm.ExecStop
			return result, nil
		}
	}

	if bat.Last() {
		if !ap.ctr.hasData {
			bat.SetEnd()
		} else {
			ap.ctr.hasData = false
		}
	} else if bat.IsEmpty() {
		result.Batch = batch.EmptyBatch
		return result, nil
	} else {
		ap.ctr.hasData = true
	}
	bat.AddCnt(1)
	ok, err := ap.ctr.sendFunc(bat, ap, proc)
	if ok {
		result.Status = vm.ExecStop
		return result, err
	} else {
		// result.Batch = nil
		return result, err
	}
}

func makeEndBatch(proc *process.Process) (*batch.Batch, error) {
	b := batch.NewWithSize(1)
	b.Attrs = []string{
		"recursive_col",
	}
	b.SetVector(0, proc.GetVector(types.T_varchar.ToType()))
	err := vector.AppendBytes(b.GetVector(0), []byte("check recursive status"), false, proc.GetMPool())
	if err == nil {
		batch.SetLength(b, 1)
		b.SetEnd()
	}
	return b, err
}

func (arg *Argument) waitRemoteRegsReady(proc *process.Process) (bool, error) {
	cnt := len(arg.RemoteRegs)
	for cnt > 0 {
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), waitNotifyTimeout)
		select {
		case <-timeoutCtx.Done():
			timeoutCancel()
			return false, moerr.NewInternalErrorNoCtx("wait notify message timeout")

		case <-proc.Ctx.Done():
			timeoutCancel()
			arg.ctr.prepared = true
			return true, nil

		case csinfo := <-proc.DispatchNotifyCh:
			timeoutCancel()
			arg.ctr.remoteReceivers = append(arg.ctr.remoteReceivers, csinfo)
			cnt--
		}
	}
	arg.ctr.prepared = true
	return false, nil
}

func (arg *Argument) prepareRemote(proc *process.Process) error {
	arg.ctr.prepared = false
	arg.ctr.isRemote = true
	arg.ctr.remoteReceivers = make([]process.WrapCs, 0, arg.ctr.remoteRegsCnt)
	arg.ctr.remoteToIdx = make(map[uuid.UUID]int)
	for i, rr := range arg.RemoteRegs {
		if arg.FuncId == ShuffleToAllFunc {
			arg.ctr.remoteToIdx[rr.Uuid] = arg.ShuffleRegIdxRemote[i]
		}
		if err := colexec.Get().PutProcIntoUuidMap(rr.Uuid, proc); err != nil {
			return err
		}
	}
	return nil
}

func (arg *Argument) prepareLocal() {
	arg.ctr.prepared = true
	arg.ctr.isRemote = false
	arg.ctr.remoteReceivers = nil
}
