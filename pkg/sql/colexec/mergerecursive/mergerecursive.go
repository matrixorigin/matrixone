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

const argName = "merge_recursive"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": merge recursive ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, true)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	for !arg.ctr.last {
		msg := arg.ctr.ReceiveFromSingleReg(0, anal)
		if msg.Err != nil {
			result.Status = vm.ExecStop
			return result, msg.Err
		}
		bat := msg.Batch
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
	arg.ctr.buf = arg.ctr.bats[0]
	arg.ctr.bats = arg.ctr.bats[1:]

	if arg.ctr.buf.Last() {
		arg.ctr.last = false
	}

	if arg.ctr.buf.End() {
		arg.ctr.buf.Clean(proc.Mp())
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	anal.Input(arg.ctr.buf, arg.GetIsFirst())
	anal.Output(arg.ctr.buf, arg.GetIsLast())
	result.Batch = arg.ctr.buf
	return result, nil
}
