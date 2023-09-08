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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("dispatch")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ctr := new(container)
	ap.ctr = ctr
	ctr.localRegsCnt = len(ap.LocalRegs)
	ctr.remoteRegsCnt = len(ap.RemoteRegs)
	ctr.aliveRegCnt = ctr.localRegsCnt + ctr.remoteRegsCnt

	switch ap.FuncId {
	case SendToAllFunc:
		if ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAllFunc should include RemoteRegs")
		}
		if len(ap.LocalRegs) == 0 {
			ctr.sendFunc = sendToAllRemoteFunc
		} else {
			ctr.sendFunc = sendToAllFunc
		}
		ap.prepareRemote(proc)

	case ShuffleToAllFunc:
		ap.ctr.sendFunc = shuffleToAllFunc
		if ap.ctr.remoteRegsCnt > 0 {
			ap.prepareRemote(proc)
		} else {
			ap.prepareLocal()
		}

	case SendToAnyFunc:
		if ctr.remoteRegsCnt == 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAnyFunc should include RemoteRegs")
		}
		if len(ap.LocalRegs) == 0 {
			ctr.sendFunc = sendToAnyRemoteFunc
		} else {
			ctr.sendFunc = sendToAnyFunc
		}
		ap.prepareRemote(proc)

	case SendToAllLocalFunc:
		if ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAllLocalFunc should not send to remote")
		}
		ctr.sendFunc = sendToAllLocalFunc
		ap.prepareLocal()

	case SendToAnyLocalFunc:
		if ctr.remoteRegsCnt != 0 {
			return moerr.NewInternalError(proc.Ctx, "SendToAnyLocalFunc should not send to remote")
		}
		ap.ctr.sendFunc = sendToAnyLocalFunc
		ap.prepareLocal()

	default:
		return moerr.NewInternalError(proc.Ctx, "wrong sendFunc id for dispatch")
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	ap := arg.(*Argument)
	bat := proc.InputBatch()
	if bat == nil && ap.RecSink {
		bat = makeEndBatch(proc)
	} else if bat == nil {
		return process.ExecStop, nil
	}
	if bat.Last() {
		if !ap.ctr.hasData {
			bat.SetEnd()
		} else {
			ap.ctr.hasData = false
		}
	} else if bat.IsEmpty() {
		proc.PutBatch(bat)
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	} else {
		ap.ctr.hasData = true
	}
	ok, err := ap.ctr.sendFunc(bat, ap, proc)
	if ok {
		return process.ExecStop, err
	} else {
		return process.ExecNext, err
	}
}

func makeEndBatch(proc *process.Process) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Attrs = []string{
		"recursive_col",
	}
	b.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	vector.AppendBytes(b.GetVector(0), []byte("check recursive status"), false, proc.GetMPool())
	batch.SetLength(b, 1)
	b.SetEnd()
	return b
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
			arg.ctr.remoteReceivers = append(arg.ctr.remoteReceivers, &WrapperClientSession{
				msgId:  csinfo.MsgId,
				cs:     csinfo.Cs,
				uuid:   csinfo.Uid,
				doneCh: csinfo.DoneCh,
			})
			cnt--
		}
	}
	arg.ctr.prepared = true
	return false, nil
}

func (arg *Argument) prepareRemote(proc *process.Process) {
	arg.ctr.prepared = false
	arg.ctr.isRemote = true
	arg.ctr.remoteReceivers = make([]*WrapperClientSession, 0, arg.ctr.remoteRegsCnt)
	arg.ctr.remoteToIdx = make(map[uuid.UUID]int)
	for i, rr := range arg.RemoteRegs {
		if arg.FuncId == ShuffleToAllFunc {
			arg.ctr.remoteToIdx[rr.Uuid] = arg.ShuffleRegIdxRemote[i]
		}
		colexec.Srv.PutProcIntoUuidMap(rr.Uuid, proc)
	}
}

func (arg *Argument) prepareLocal() {
	arg.ctr.prepared = true
	arg.ctr.isRemote = false
	arg.ctr.remoteReceivers = nil
}
