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

package merge

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" union all ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, true)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()
	var end bool
	result := vm.NewCallResult()
	if arg.buf != nil {
		proc.PutBatch(arg.buf)
		arg.buf = nil
	}

	for {
		arg.buf, end, _ = arg.ctr.ReceiveFromAllRegs(anal)
		if end {
			result.Status = vm.ExecStop
			return result, nil
		}

		if arg.buf.Last() && arg.SinkScan {
			proc.PutBatch(arg.buf)
			continue
		}
		break
	}

	anal.Input(arg.buf, arg.info.IsFirst)
	anal.Output(arg.buf, arg.info.IsLast)
	result.Batch = arg.buf
	return result, nil
}
