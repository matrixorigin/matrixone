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

package mergeorder

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "merge_order"

func (ctr *container) mergeAndEvaluateOrderColumn(proc *process.Process, bat *batch.Batch) error {
	ctr.batchList = append(ctr.batchList, bat)
	ctr.orderCols = append(ctr.orderCols, nil)
	// if only one batch, no need to evaluate the order column.
	if len(ctr.batchList) == 1 {
		return nil
	}

	index := len(ctr.orderCols) - 1
	return ctr.evaluateOrderColumn(proc, index)
}

func (ctr *container) evaluateOrderColumn(proc *process.Process, index int) error {
	inputs := []*batch.Batch{ctr.batchList[index]}

	ctr.orderCols[index] = make([]*vector.Vector, len(ctr.executors))
	for i := 0; i < len(ctr.executors); i++ {
		vec, err := ctr.executors[i].EvalWithoutResultReusing(proc, inputs)
		if err != nil {
			return err
		}
		ctr.orderCols[index][i] = vec
	}
	return nil
}

func (ctr *container) generateCompares(fs []*plan.OrderBySpec) {
	var desc, nullsLast bool

	ctr.compares = make([]compare.Compare, len(fs))
	for i := range ctr.compares {
		desc = fs[i].Flag&plan2.OrderBySpec_DESC != 0
		if fs[i].Flag&plan2.OrderBySpec_NULLS_FIRST != 0 {
			nullsLast = false
		} else if fs[i].Flag&plan2.OrderBySpec_NULLS_LAST != 0 {
			nullsLast = true
		} else {
			nullsLast = desc
		}

		exprTyp := fs[i].Expr.Typ
		typ := types.New(types.T(exprTyp.Id), exprTyp.Width, exprTyp.Scale)
		ctr.compares[i] = compare.New(typ, desc, nullsLast)
	}
}

func (ctr *container) pickAndSend(proc *process.Process, result *vm.CallResult) (sendOver bool, err error) {
	if ctr.buf != nil {
		proc.PutBatch(ctr.buf)
		ctr.buf = nil
	}
	ctr.buf = batch.NewWithSize(ctr.batchList[0].VectorCount())
	mp := proc.Mp()

	for i := range ctr.buf.Vecs {
		ctr.buf.Vecs[i] = proc.GetVector(*ctr.batchList[0].Vecs[i].GetType())
	}

	wholeLength := 0
	for {
		choice := ctr.pickFirstRow()
		for j := range ctr.buf.Vecs {
			err = ctr.buf.Vecs[j].UnionOne(ctr.batchList[choice].Vecs[j], ctr.indexList[choice], mp)
			if err != nil {
				return false, err
			}
		}

		wholeLength++
		ctr.indexList[choice]++
		if ctr.indexList[choice] == int64(ctr.batchList[choice].RowCount()) {
			ctr.removeBatch(proc, choice)
		}

		if len(ctr.indexList) == 0 {
			sendOver = true
			break
		}
		if ctr.buf.Size() >= maxBatchSizeToSend {
			break
		}
	}
	ctr.buf.SetRowCount(wholeLength)
	result.Batch = ctr.buf
	return sendOver, nil
}

func (ctr *container) pickFirstRow() (batIndex int) {
	l := len(ctr.indexList)

	if l > 1 {
		i, j := 0, 1
		for j < l {
			for k := 0; k < len(ctr.compares); k++ {
				ctr.compares[k].Set(0, ctr.orderCols[i][k])
				ctr.compares[k].Set(1, ctr.orderCols[j][k])
				result := ctr.compares[k].Compare(0, 1, ctr.indexList[i], ctr.indexList[j])
				if result < 0 {
					break
				} else if result > 0 {
					i = j
					break
				} else if k == len(ctr.compares)-1 {
					break
				}
			}
			j++
		}
		return i
	}
	return 0
}

func (ctr *container) removeBatch(proc *process.Process, index int) {
	bat := ctr.batchList[index]
	cols := ctr.orderCols[index]

	alreadyPut := make(map[*vector.Vector]bool, len(bat.Vecs))
	for i := range bat.Vecs {
		proc.PutVector(bat.Vecs[i])
		alreadyPut[bat.Vecs[i]] = true
	}
	ctr.batchList = append(ctr.batchList[:index], ctr.batchList[index+1:]...)
	ctr.indexList = append(ctr.indexList[:index], ctr.indexList[index+1:]...)

	for i := range cols {
		if _, ok := alreadyPut[cols[i]]; ok {
			continue
		}
		proc.PutVector(cols[i])
	}
	ctr.orderCols = append(ctr.orderCols[:index], ctr.orderCols[index+1:]...)
}

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	ap := arg
	buf.WriteString(": mergeorder([")
	for i, f := range ap.OrderBySpecs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = new(container)
	ctr := arg.ctr
	arg.ctr.InitReceiver(proc, true)

	length := 2 * len(proc.Reg.MergeReceivers)
	ctr.batchList = make([]*batch.Batch, 0, length)
	ctr.orderCols = make([][]*vector.Vector, 0, length)

	arg.ctr.executors = make([]colexec.ExpressionExecutor, len(arg.OrderBySpecs))
	for i := range arg.ctr.executors {
		arg.ctr.executors[i], err = colexec.NewExpressionExecutor(proc, arg.OrderBySpecs[i].Expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ctr := arg.ctr
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	result := vm.NewCallResult()

	for {
		switch ctr.status {
		case receiving:
			bat, end, err := ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return result, err
			}
			if end {
				// if number of block is less than 2, no need to do merge sort.
				ctr.status = normalSending

				if len(ctr.batchList) > 1 {
					ctr.status = pickUpSending

					// evaluate the first batch's order column.
					if err = ctr.evaluateOrderColumn(proc, 0); err != nil {
						return result, err
					}
					ctr.generateCompares(arg.OrderBySpecs)
					ctr.indexList = make([]int64, len(ctr.batchList))
				}
				continue
			}

			if err = ctr.mergeAndEvaluateOrderColumn(proc, bat); err != nil {
				return result, err
			}

		case normalSending:
			if len(ctr.batchList) == 0 {
				result.Batch = nil
				result.Status = vm.ExecStop
				return result, nil
			}

			// If only one batch, no need to sort. just send it.
			if len(ctr.batchList) == 1 {
				ctr.buf = ctr.batchList[0]
				ctr.batchList[0] = nil
				result.Batch = ctr.buf
				result.Status = vm.ExecStop
				return result, nil
			}

		case pickUpSending:
			ok, err := ctr.pickAndSend(proc, &result)
			if ok {
				result.Status = vm.ExecStop
				return result, err
			}
			return result, err
		}
	}
}
