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

package apply

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "apply"

func (apply *Apply) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	switch apply.ApplyType {
	case CROSS:
		buf.WriteString(": cross apply ")
	case OUTER:
		buf.WriteString(": outer apply ")
	}
}

func (apply *Apply) OpType() vm.OpType {
	return vm.Apply
}

func (apply *Apply) Prepare(proc *process.Process) (err error) {
	if apply.OpAnalyzer == nil {
		apply.OpAnalyzer = process.NewAnalyzer(apply.GetIdx(), apply.IsFirst, apply.IsLast, "apply")
	} else {
		apply.OpAnalyzer.Reset()
	}

	if apply.ctr.sels == nil {
		apply.ctr.sels = make([]int32, colexec.DefaultBatchSize)
		for i := range apply.ctr.sels {
			apply.ctr.sels[i] = int32(i)
		}
	}

	err = apply.TableFunction.ApplyPrepare(proc)
	if err != nil {
		return
	}
	err = apply.PrepareProjection(proc)
	return
}

func (apply *Apply) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := apply.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	ctr := &apply.ctr
	for {
		if ctr.inbat == nil {
			input, err = vm.ChildrenCall(apply.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			ctr.inbat = input.Batch
			if ctr.inbat == nil {
				result.Batch = nil
				result.Status = vm.ExecStop
				return result, nil
			}
			if ctr.inbat.Last() {
				result.Batch = ctr.inbat
				analyzer.Output(result.Batch)
				return result, nil
			}
			if ctr.inbat.IsEmpty() {
				continue
			}
			ctr.batIdx = 0
			ctr.tfFinish = true
			apply.TableFunction.ApplyArgsEval(ctr.inbat, proc)
		}
		if ctr.rbat == nil {
			ctr.rbat = batch.NewWithSize(len(apply.Result))
			for i, rp := range apply.Result {
				if rp.Rel == 0 {
					ctr.rbat.Vecs[i] = vector.NewVec(*ctr.inbat.Vecs[rp.Pos].GetType())
				} else {
					ctr.rbat.Vecs[i] = vector.NewVec(apply.Typs[rp.Pos])
				}
			}
		} else {
			ctr.rbat.CleanOnlyData()
		}

		err = ctr.probe(apply, proc, &probeResult)
		if err != nil {
			return result, err
		}

		result.Batch, err = apply.EvalProjection(probeResult.Batch, proc)
		if err != nil {
			return result, err
		}

		analyzer.Output(result.Batch)
		return result, nil

	}
}

func (ctr *container) probe(ap *Apply, proc *process.Process, result *vm.CallResult) error {
	inbat := ctr.inbat
	count := inbat.RowCount()
	tfResult := vm.NewCallResult()
	var err error
	for i := ctr.batIdx; i < count; i++ {
		if ctr.tfFinish {
			err = ap.TableFunction.ApplyStart(i, proc)
			if err != nil {
				return err
			}
			ctr.tfFinish = false
		}
		for {
			tfResult, err = ap.TableFunction.ApplyCall(proc)
			if err != nil {
				return err
			}
			if tfResult.Batch.IsDone() {
				ctr.tfFinish = true
				break
			}
			rowCountIncrease := tfResult.Batch.RowCount()
			for j, rp := range ap.Result {
				if rp.Rel == 0 {
					err = ctr.rbat.Vecs[j].UnionMulti(ctr.inbat.Vecs[rp.Pos], int64(i), rowCountIncrease, proc.Mp())
				} else {
					err = ctr.rbat.Vecs[j].UnionInt32(tfResult.Batch.Vecs[rp.Pos], ctr.sels[:rowCountIncrease], proc.Mp())
				}
				if err != nil {
					return err
				}
			}
			ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
			if ctr.rbat.RowCount() >= colexec.DefaultBatchSize {
				ctr.batIdx = i
				break
			}
		}
		if !ctr.tfFinish {
			break
		}
	}
	if ctr.tfFinish {
		ctr.inbat = nil
	}
	result.Batch = ctr.rbat
	return nil
}
