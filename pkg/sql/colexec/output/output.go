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

package output

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "output"

func (output *Output) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": sql output")
}

func (output *Output) OpType() vm.OpType {
	return vm.Output
}

func (output *Output) Prepare(_ *process.Process) error {
	if output.OpAnalyzer == nil {
		output.OpAnalyzer = process.NewAnalyzer(output.GetIdx(), output.IsFirst, output.IsLast, "output")
	} else {
		output.OpAnalyzer.Reset()
	}

	if output.ctr.block {
		output.ctr.blockStep = stepCollect
		output.ctr.cachedBatches = make([]*batch.Batch, 0)
	}

	return nil
}

func (output *Output) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := output.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	if !output.ctr.block {
		result, err := vm.ChildrenCall(output.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}

		if result.Batch == nil {
			result.Status = vm.ExecStop
			return result, nil
		}

		if result.Batch.IsEmpty() {
			return result, nil
		}
		bat := result.Batch

		if err = output.Func(bat); err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		// TODO: analyzer.Output(result.Batch)
		return result, nil
	} else {
		if output.ctr.blockStep == stepCollect {
			for {
				result, err := vm.ChildrenCall(output.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				bat := result.Batch
				if bat == nil {
					output.ctr.blockStep = stepSend
					if len(output.ctr.cachedBatches) == 0 {
						output.ctr.blockStep = stepEnd
					}
					break
				}

				if bat.IsEmpty() {
					continue
				}
				appendBat, err := bat.Dup(proc.GetMPool())
				if err != nil {
					return result, err
				}
				output.ctr.cachedBatches = append(output.ctr.cachedBatches, appendBat)
			}
		}

		result := vm.NewCallResult()

		if output.ctr.blockStep == stepSend {
			if output.ctr.currentIdx == len(output.ctr.cachedBatches) {
				output.ctr.blockStep = stepEnd
				return result, nil
			} else {
				bat := output.ctr.cachedBatches[output.ctr.currentIdx]
				output.ctr.currentIdx = output.ctr.currentIdx + 1
				if err := output.Func(bat); err != nil {
					result.Status = vm.ExecStop
					return result, err
				}
				result.Batch = bat
				// same as nonBlock
				// analyzer.Output(result.Batch)
				return result, nil
			}
		}

		if output.ctr.blockStep == stepEnd {
			result.Status = vm.ExecStop
			return result, nil
		}

		panic("BUG")
	}
}
