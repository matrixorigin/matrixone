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

const opName = "merge_recursive"

func (mergeRecursive *MergeRecursive) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": merge recursive ")
}

func (mergeRecursive *MergeRecursive) OpType() vm.OpType {
	return vm.MergeRecursive
}

func (mergeRecursive *MergeRecursive) Prepare(proc *process.Process) error {
	mergeRecursive.ctr = new(container)
	mergeRecursive.ctr.InitReceiver(proc, true)
	return nil
}

func (mergeRecursive *MergeRecursive) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(mergeRecursive.GetIdx(), mergeRecursive.GetParallelIdx(), mergeRecursive.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	for !mergeRecursive.ctr.last {
		msg := mergeRecursive.ctr.ReceiveFromSingleReg(0, anal)
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
			mergeRecursive.ctr.last = true
		}
		mergeRecursive.ctr.bats = append(mergeRecursive.ctr.bats, bat)
	}
	mergeRecursive.ctr.buf = mergeRecursive.ctr.bats[0]
	mergeRecursive.ctr.bats = mergeRecursive.ctr.bats[1:]

	if mergeRecursive.ctr.buf.Last() {
		mergeRecursive.ctr.last = false
	}

	if mergeRecursive.ctr.buf.End() {
		mergeRecursive.ctr.buf.Clean(proc.Mp())
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	anal.Input(mergeRecursive.ctr.buf, mergeRecursive.GetIsFirst())
	anal.Output(mergeRecursive.ctr.buf, mergeRecursive.GetIsLast())
	result.Batch = mergeRecursive.ctr.buf
	return result, nil
}
