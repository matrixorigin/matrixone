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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("dispatch")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.prepared = false
	if len(ap.RemoteRegs) == 0 {
		ap.prepared = true
		ap.ctr.remoteReceivers = nil
	} else {
		ap.ctr.remoteReceivers = make([]*WrapperClientSession, 0, len(ap.RemoteRegs))
	}

	switch ap.FuncId {
	case SendToAllFunc:
		ap.ctr.sendFunc = sendToAllFunc
	case SendToAnyLocalFunc:
		if !ap.prepared {
			return moerr.NewInternalError(proc.Ctx, "SendToAnyLocalFunc should not send to remote")
		}
		ap.ctr.sendFunc = sendToAnyLocalFunc
	default:
		return moerr.NewInternalError(proc.Ctx, "wrong sendFunc id for dispatch")
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)

	// waiting all remote receive prepared
	// put it in Call() for better parallel

	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}

	if err := ap.ctr.sendFunc(bat, ap, proc); err != nil {
		return false, err
	}
	if len(ap.LocalRegs) == 0 {
		return true, nil
	}
	proc.SetInputBatch(nil)
	return false, nil
}
