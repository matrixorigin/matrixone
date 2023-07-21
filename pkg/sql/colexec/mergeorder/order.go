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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxBatchSizeToSend = 64 * mpool.MB

const (
	receiving = iota
	normalSending
	pickUpSending
)

type Argument struct {
	ctr *container

	OrderBySpecs []*plan.OrderBySpec
}

type container struct {
	colexec.ReceiverOperator

	// operator status
	status int

	// batchList is the data structure to store the all the received batches
	batchList []*batch.Batch
	orderCols [][]*vector.Vector
	// indexList[i] = k means the number of rows before k in batchList[i] has been merged and send.
	indexList []int64

	// expression executors for order columns.
	executors []colexec.ExpressionExecutor
	compares  []compare.Compare
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.ctr != nil {
		mp := proc.Mp()
		ctr := arg.ctr
		for i := range ctr.batchList {
			if ctr.batchList[i] != nil {
				ctr.batchList[i].Clean(mp)
			}
		}
		for i := range ctr.orderCols {
			if ctr.orderCols[i] != nil {
				for j := range ctr.orderCols[i] {
					if ctr.orderCols[i][j] != nil {
						ctr.orderCols[i][j].Free(mp)
					}
				}
			}
		}
		for i := range ctr.executors {
			if ctr.executors[i] != nil {
				ctr.executors[i].Free()
			}
		}
	}
}

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

func (ctr *container) pickAndSend(proc *process.Process) (sendOver bool, err error) {
	bat := batch.NewWithSize(ctr.batchList[0].VectorCount())
	mp := proc.Mp()

	for i := range bat.Vecs {
		bat.Vecs[i] = proc.GetVector(*ctr.batchList[0].Vecs[i].GetType())
	}

	wholeLength := 0
	for {
		choice := ctr.pickFirstRow()
		for j := range bat.Vecs {
			err = bat.Vecs[j].UnionOne(ctr.batchList[choice].Vecs[j], ctr.indexList[choice], mp)
			if err != nil {
				bat.Clean(mp)
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
		if bat.Size() >= maxBatchSizeToSend {
			break
		}
	}
	bat.SetRowCount(wholeLength)
	proc.SetInputBatch(bat)
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

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("mergeorder([")
	for i, f := range ap.OrderBySpecs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ctr := ap.ctr
	ap.ctr.InitReceiver(proc, true)

	length := 2 * len(proc.Reg.MergeReceivers)
	ctr.batchList = make([]*batch.Batch, 0, length)
	ctr.orderCols = make([][]*vector.Vector, 0, length)

	ap.ctr.executors = make([]colexec.ExpressionExecutor, len(ap.OrderBySpecs))
	for i := range ap.ctr.executors {
		ap.ctr.executors[i], err = colexec.NewExpressionExecutor(proc, ap.OrderBySpecs[i].Expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr

	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	for {
		switch ctr.status {
		case receiving:
			bat, end, err := ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return process.ExecNext, err
			}
			if end {
				// if number of block is less than 2, no need to do merge sort.
				ctr.status = normalSending

				if len(ctr.batchList) > 1 {
					ctr.status = pickUpSending

					// evaluate the first batch's order column.
					if err = ctr.evaluateOrderColumn(proc, 0); err != nil {
						return process.ExecNext, err
					}
					ctr.generateCompares(ap.OrderBySpecs)
					ctr.indexList = make([]int64, len(ctr.batchList))
				}
				continue
			}

			if err = ctr.mergeAndEvaluateOrderColumn(proc, bat); err != nil {
				return process.ExecNext, err
			}

		case normalSending:
			if len(ctr.batchList) == 0 {
				proc.SetInputBatch(nil)
				return process.ExecStop, nil
			}

			// If only one batch, no need to sort. just send it.
			if len(ctr.batchList) == 1 {
				proc.SetInputBatch(ctr.batchList[0])
				ctr.batchList[0] = nil
				return process.ExecStop, nil
			}

		case pickUpSending:
			ok, err := ctr.pickAndSend(proc)
			if ok {
				return process.ExecStop, err
			}
			return process.ExecNext, err
		}
	}
}
