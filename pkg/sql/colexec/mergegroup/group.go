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

package mergegroup

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_group"

func (mergeGroup *MergeGroup) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
}

func (mergeGroup *MergeGroup) OpType() vm.OpType {
	return vm.MergeGroup
}

func (mergeGroup *MergeGroup) Prepare(proc *process.Process) error {
	if mergeGroup.OpAnalyzer == nil {
		mergeGroup.OpAnalyzer = process.NewAnalyzer(mergeGroup.GetIdx(), mergeGroup.IsFirst, mergeGroup.IsLast, "merge_group")
	} else {
		mergeGroup.OpAnalyzer.Reset()
	}

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

	analyzer := mergeGroup.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &mergeGroup.ctr
	for {
		switch ctr.state {
		case Build:
			for {
				b, err := mergeGroup.getInputBatch(proc)
				if err != nil {
					return vm.CancelResult, err
				}
				if b == nil {
					break
				}

				if err = ctr.process(b, proc); err != nil {
					return vm.CancelResult, err
				}
			}
			if err := ctr.res.DealPartialResult(mergeGroup.PartialResults); err != nil {
				return vm.CancelResult, err
			}
			ctr.state = Eval

		case Eval:
			if ctr.res.IsEmpty() {
				ctr.state = End
				continue
			}

			result := vm.NewCallResult()
			b, err := ctr.res.BlockingGroupRelated.PopResult(proc.Mp())
			if err != nil {
				return result, err
			}
			result.Batch = b
			analyzer.Output(result.Batch)
			return result, nil

		case End:
			result := vm.NewCallResult()
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) process(bat *batch.Batch, proc *process.Process) error {
	var err error

	// calculate hash key width and nullability
	if ctr.hashKeyWidth == NeedCalculationForKeyWidth {
		ctr.hashKeyWidth = 0
		ctr.keyNullability = false
		ctr.groupByCol = len(bat.Vecs)

		for _, vec := range bat.Vecs {
			ctr.keyNullability = ctr.keyNullability || (!vec.GetType().GetNotNull())
		}

		for _, vec := range bat.Vecs {
			width := vec.GetType().TypeSize()
			if vec.GetType().IsVarlen() {
				if vec.GetType().Width == 0 {
					switch vec.GetType().Oid {
					case types.T_array_float32:
						width = 128 * 4
					case types.T_array_float64:
						width = 128 * 8
					default:
						width = 128
					}
				} else {
					switch vec.GetType().Oid {
					case types.T_array_float32:
						width = int(vec.GetType().Width) * 4
					case types.T_array_float64:
						width = int(vec.GetType().Width) * 8
					default:
						width = int(vec.GetType().Width)
					}
				}
			}
			ctr.hashKeyWidth += width
			if ctr.keyNullability {
				ctr.hashKeyWidth += 1
			}
		}

		switch {
		case ctr.hashKeyWidth == 0:
			// no group by.
			ctr.typ = H0

		case ctr.hashKeyWidth <= 8:
			ctr.typ = H8

		default:
			ctr.typ = HStr
		}
	}

	if ctr.typ == H0 {
		return ctr.res.consumeInputBatchOnlyAgg(bat)
	}

	if err = ctr.res.BuildHashTable(false, ctr.typ == HStr, ctr.keyNullability, 0); err != nil {
		return err
	}
	return ctr.res.consumeInputBatch(proc, bat)
}
