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

package mergerecursive

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" merge recursive ")
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

	result := vm.NewCallResult()
	for !arg.ctr.last {
		bat, _, err := arg.ctr.ReceiveFromSingleReg(0, anal)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		if bat == nil || bat.End() {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		if bat.Last() {
			arg.ctr.last = true
		}
		arg.ctr.bats = append(arg.ctr.bats, bat)
	}
	arg.buf = arg.ctr.bats[0]
	arg.ctr.bats = arg.ctr.bats[1:]

	if arg.buf.Last() {
		arg.ctr.last = false
	}

	if arg.buf.End() {
		arg.buf.Clean(proc.Mp())
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	anal.Input(arg.buf, arg.info.IsFirst)
	anal.Output(arg.buf, arg.info.IsLast)
	result.Batch = arg.buf
	return result, nil
}
