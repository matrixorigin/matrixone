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

package partition

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

const opName = "partition"

func (partition *Partition) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": partition([")
	for i, f := range partition.OrderBySpecs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (partition *Partition) OpType() vm.OpType {
	return vm.Partition
}

func (partition *Partition) Prepare(proc *process.Process) (err error) {
	partition.OpAnalyzer = process.NewAnalyzer(partition.GetIdx(), partition.IsFirst, partition.IsLast, "partition")

	if len(partition.ctr.executors) > 0 {
		return nil
	}

	ctr := &partition.ctr
	ctr.batchList = make([]*batch.Batch, 0, 16)
	ctr.orderCols = make([][]*vector.Vector, 0, 16)

	partition.ctr.executors = make([]colexec.ExpressionExecutor, len(partition.OrderBySpecs))
	for i := range partition.ctr.executors {
		partition.ctr.executors[i], err = colexec.NewExpressionExecutor(proc, partition.OrderBySpecs[i].Expr)
		if err != nil {
			return err
		}
	}
	ctr.generateCompares(partition.OrderBySpecs)
	return nil
}

func (partition *Partition) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(partition.GetIdx(), partition.GetParallelIdx(), partition.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := partition.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &partition.ctr
	for {
		switch ctr.status {
		case receive:
			//result, err := partition.GetChildren(0).Call(proc)
			result, err := vm.ChildrenCallV1(partition.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				ctr.indexList = make([]int64, len(ctr.batchList))
				ctr.status = eval
			} else {
				if len(ctr.batchList) > ctr.i {
					if ctr.batchList[ctr.i] != nil {
						ctr.batchList[ctr.i].CleanOnlyData()
					}
					ctr.batchList[ctr.i], err = ctr.batchList[ctr.i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
					if err != nil {
						return result, err
					}
				} else {
					appBat, err := result.Batch.Dup(proc.Mp())
					if err != nil {
						return result, err
					}
					analyzer.Alloc(int64(appBat.Size()))
					ctr.batchList = append(ctr.batchList, appBat)
				}

				if err = ctr.evaluateOrderColumn(proc, ctr.i); err != nil {
					return result, err
				}
				ctr.i++
			}

		case eval:
			result := vm.NewCallResult()
			if len(ctr.batchList) == 0 {
				result.Status = vm.ExecStop
				return result, nil
			}

			ok, err := ctr.pickAndSend(proc, &result)
			if ok {
				result.Status = vm.ExecStop
				//anal.Output(result.Batch, partition.IsLast)
				analyzer.Output(result.Batch)
				return result, err
			}
			//anal.Output(result.Batch, partition.IsLast)
			analyzer.Output(result.Batch)
			return result, err

		}
	}
}

func (ctr *container) evaluateOrderColumn(proc *process.Process, index int) error {
	inputs := []*batch.Batch{ctr.batchList[index]}

	if len(ctr.orderCols) < index+1 {
		ctr.orderCols = append(ctr.orderCols, make([]*vector.Vector, len(ctr.executors)))
	}
	for i := 0; i < len(ctr.executors); i++ {
		vec, err := ctr.executors[i].Eval(proc, inputs, nil)
		if err != nil {
			return err
		}
		if ctr.orderCols[index][i] != nil {
			ctr.orderCols[index][i].CleanOnlyData()
			if err = ctr.orderCols[index][i].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
				return err
			}
		} else {
			ctr.orderCols[index][i], err = vec.Dup(proc.Mp())
			if err != nil {
				return err
			}
		}
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
		ctr.buf.CleanOnlyData()
	} else {
		ctr.buf = batch.NewWithSize(ctr.batchList[0].VectorCount())
		for i := range ctr.buf.Vecs {
			ctr.buf.Vecs[i] = vector.NewVec(*ctr.batchList[0].Vecs[i].GetType())
		}
	}
	mp := proc.Mp()

	wholeLength, choice := 0, 0
	hasSame := false
	var row int64
	var cols []*vector.Vector
	for {

		if wholeLength == 0 {
			choice = ctr.pickFirstRow()
		} else {
			if choice, hasSame = ctr.pickSameRow(row, cols); !hasSame {
				break
			}
		}

		row = ctr.indexList[choice]
		cols = ctr.orderCols[choice]

		for j := range ctr.buf.Vecs {
			// here copy
			err = ctr.buf.Vecs[j].UnionOne(ctr.batchList[choice].Vecs[j], row, mp)
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
	}
	ctr.buf.SetRowCount(wholeLength)
	result.Batch = ctr.buf
	return sendOver, nil
}

func (ctr *container) pickFirstRow() (batIndex int) {
	l := len(ctr.indexList)

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

func (ctr *container) pickSameRow(row int64, cols []*vector.Vector) (batIndex int, hasSame bool) {
	l := len(ctr.indexList)

	j := 0
	for ; j < l; j++ {
		hasSame = true
		for k := 0; k < len(ctr.compares); k++ {
			ctr.compares[k].Set(0, cols[k])
			ctr.compares[k].Set(1, ctr.orderCols[j][k])
			result := ctr.compares[k].Compare(0, 1, row, ctr.indexList[j])
			if result != 0 {
				hasSame = false
				break
			}
		}
		if hasSame {
			break
		}
	}
	return j, hasSame
}

func (ctr *container) removeBatch(proc *process.Process, index int) {
	if ctr.batchList[index] != nil {
		ctr.batchList[index].Clean(proc.Mp())
	}
	ctr.batchList = append(ctr.batchList[:index], ctr.batchList[index+1:]...)
	ctr.indexList = append(ctr.indexList[:index], ctr.indexList[index+1:]...)
	for _, vec := range ctr.orderCols[index] {
		vec.Free(proc.Mp())
	}
	ctr.orderCols = append(ctr.orderCols[:index], ctr.orderCols[index+1:]...)
}
