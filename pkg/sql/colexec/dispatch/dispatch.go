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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("dispatch")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.prepared = false
	if len(ap.RemoteRegs) == 0 {
		ap.ctr.prepared = true
		ap.ctr.remoteReceivers = nil
	} else {
		ap.ctr.remoteReceivers = make([]*WrapperClientSession, 0, len(ap.RemoteRegs))
	}

	switch ap.FuncId {
	case SendToAllFunc:
		ap.ctr.sendFunc = sendToAllFunc
	case SendToAnyFunc:
		ap.ctr.sendFunc = sendToAnyFunc
	default:
		return moerr.NewInternalError(proc.Ctx, "wrong sendFunc id for dispatch")
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)

	// waiting all remote receive prepared
	// put it in Call() for better parallel
	if !ap.ctr.prepared {
		cnt := len(ap.RemoteRegs)
		for cnt > 0 {
			csinfo := <-proc.DispatchNotifyCh
			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
			_ = cancel

			ap.ctr.remoteReceivers = append(ap.ctr.remoteReceivers, &WrapperClientSession{
				msgId: csinfo.MsgId,
				ctx:   timeoutCtx,
				cs:    csinfo.Cs,
				uuid:  csinfo.Uid,
			})
			// TODO: add check the receive info's correctness
			cnt--
		}
		ap.ctr.prepared = true
	}

	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}

	if err := ap.ctr.sendFunc(bat, ap.ctr.bid, ap.LocalRegs, ap.ctr.remoteReceivers, proc); err != nil {
		return false, err
	}
	proc.SetInputBatch(nil)
	ap.ctr.bid++
	return false, nil
}
