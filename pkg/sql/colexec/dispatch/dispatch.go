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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)

	if ap.FuncId == ShuffleToAllFunc {
		bats := proc.InputBatches()
		if len(bats) == 0 {
			return true, nil
		}
		for _, bat := range bats {
			end, err := shuffleToAllFunc(bat, ap, proc)
			if err != nil {
				return false, err
			}
			if end {
				return true, nil
			}
		}
		return false, nil
	}

	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		return false, nil
	}
	return ap.ctr.sendFunc(bat, ap, proc)
}

func (arg *Argument) waitRemoteRegsReady(proc *process.Process) (bool, error) {
	cnt := len(arg.RemoteRegs)
	for cnt > 0 {
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), waitNotifyTimeout)
		defer timeoutCancel()
		select {
		case <-timeoutCtx.Done():
			logutil.Errorf("waiting notify msg timeout")
			return false, moerr.NewInternalErrorNoCtx("wait notify message timeout")
		case <-proc.Ctx.Done():
			arg.ctr.prepared = true
			logutil.Warn("conctx done during dispatch")
			return true, nil
		case csinfo := <-proc.DispatchNotifyCh:
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
		colexec.Srv.PutNotifyChIntoUuidMap(rr.Uuid, proc)
	}
}

func (arg *Argument) prepareLocal() {
	arg.ctr.prepared = true
	arg.ctr.isRemote = false
	arg.ctr.remoteReceivers = nil
}
