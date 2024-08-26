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
	mergeRecursive.OpAnalyzer = process.NewAnalyzer(mergeRecursive.GetIdx(), mergeRecursive.IsFirst, mergeRecursive.IsLast, "merge recursive")
	return nil
}

func (mergeRecursive *MergeRecursive) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(mergeRecursive.GetIdx(), mergeRecursive.GetParallelIdx(), mergeRecursive.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := mergeRecursive.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &mergeRecursive.ctr

	result := vm.NewCallResult()
	var err error
	for !ctr.last {
		//result, err = mergeRecursive.GetChildren(0).Call(proc)
		result, err = vm.ChildrenCallV1(mergeRecursive.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}
		bat := result.Batch
		if bat == nil || bat.End() {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		if bat.Last() {
			ctr.last = true
		}

		if len(ctr.freeBats) > ctr.i {
			if ctr.freeBats[ctr.i] != nil {
				ctr.freeBats[ctr.i].CleanOnlyData()
			}
			ctr.freeBats[ctr.i], err = ctr.freeBats[ctr.i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
			if err != nil {
				return result, err
			}
		} else {
			appBat, err := result.Batch.Dup(proc.Mp())
			if err != nil {
				return result, err
			}
			analyzer.Alloc(int64(appBat.Size()))
			ctr.freeBats = append(ctr.freeBats, appBat)
		}
		mergeRecursive.ctr.bats = append(mergeRecursive.ctr.bats, ctr.freeBats[ctr.i])
		ctr.i++
	}
	mergeRecursive.ctr.buf = mergeRecursive.ctr.bats[0]
	mergeRecursive.ctr.bats = mergeRecursive.ctr.bats[1:]

	if mergeRecursive.ctr.buf.Last() {
		mergeRecursive.ctr.last = false
	}

	if mergeRecursive.ctr.buf.End() {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	//anal.Input(mergeRecursive.ctr.buf, mergeRecursive.GetIsFirst())
	//anal.Output(mergeRecursive.ctr.buf, mergeRecursive.GetIsLast())
	result.Batch = mergeRecursive.ctr.buf
	result.Status = vm.ExecHasMore
	analyzer.Output(result.Batch)
	return result, nil
}
