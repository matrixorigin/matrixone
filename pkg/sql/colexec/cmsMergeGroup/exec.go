// Copyright 2024 Matrix Origin
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

package cmsMergeGroup

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (mergeGroup *MergeGroup) Prepare(proc *process.Process) error {
	mergeGroup.ctr.state = vm.Build
	mergeGroup.prepareAnalyzer()
	return mergeGroup.prepareProjection(proc)
}

func (mergeGroup *MergeGroup) prepareAnalyzer() {
	if mergeGroup.OpAnalyzer != nil {
		mergeGroup.OpAnalyzer.Reset()
		return
	}
	mergeGroup.OpAnalyzer = process.NewAnalyzer(mergeGroup.GetIdx(), mergeGroup.IsFirst, mergeGroup.IsLast, "merge_group")
}

func (mergeGroup *MergeGroup) prepareProjection(proc *process.Process) error {
	if mergeGroup.ProjectList != nil {
		err := mergeGroup.PrepareProjection(proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mergeGroup *MergeGroup) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}
	mergeGroup.OpAnalyzer.Start()
	defer mergeGroup.OpAnalyzer.Stop()

	for {
		switch mergeGroup.ctr.state {
		case vm.Build:
			// receive data and merge.
			for {
				b, err := mergeGroup.getInputBatch(proc)
				if err != nil {
					return vm.CancelResult, err
				}
				if b == nil {
					break
				}
				if b.IsEmpty() {
					continue
				}

				if err = mergeGroup.consumeBatch(proc, b); err != nil {
					return vm.CancelResult, err
				}
			}
			if err := mergeGroup.ctr.result.DealPartialResult(mergeGroup.PartialResults); err != nil {
				return vm.CancelResult, err
			}
			mergeGroup.ctr.state = vm.Eval

		case vm.Eval:
			// output result.
			if mergeGroup.ctr.result.IsEmpty() {
				mergeGroup.ctr.state = vm.End
				continue
			}

			b, err := mergeGroup.ctr.result.PopResult(proc.Mp())
			if err != nil {
				return vm.CancelResult, err
			}
			result := vm.NewCallResult()
			result.Batch = b
			mergeGroup.OpAnalyzer.Output(b)
			return result, nil

		default:
			// END status.
			result := vm.NewCallResult()
			result.Batch, result.Status = nil, vm.ExecStop
			return result, nil
		}
	}
}

func (mergeGroup *MergeGroup) getInputBatch(proc *process.Process) (*batch.Batch, error) {
	r, err := vm.ChildrenCall(mergeGroup.GetChildren(0), proc, mergeGroup.OpAnalyzer)
	return r.Batch, err
}

func (mergeGroup *MergeGroup) consumeBatch(proc *process.Process, b *batch.Batch) error {

	return nil
}
