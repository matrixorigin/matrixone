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

const opName = "merge_order"

func (ctr *container) mergeAndEvaluateOrderColumn(proc *process.Process, bat *batch.Batch) error {
	ctr.batchList = append(ctr.batchList, bat)
	ctr.orderCols = append(ctr.orderCols, nil)
	ctr.spillMemUsage += int64(bat.Size())
	return nil
}

func (ctr *container) evaluateOrderColumn(proc *process.Process, index int) error {
	cols, err := ctr.evaluateOrderColumns(proc, ctr.batchList[index])
	if err != nil {
		return err
	}
	ctr.orderCols[index] = cols
	return nil
}

func (ctr *container) prepareInMemoryMerge(proc *process.Process, fs []*plan.OrderBySpec) error {
	if len(ctr.batchList) <= 1 {
		return nil
	}
	for i := range ctr.batchList {
		if ctr.orderCols[i] != nil {
			continue
		}
		if err := ctr.evaluateOrderColumn(proc, i); err != nil {
			return err
		}
	}
	ctr.generateCompares(fs)
	ctr.indexList = make([]int64, len(ctr.batchList))
	return nil
}

func (ctr *container) generateCompares(fs []*plan.OrderBySpec) {
	if len(ctr.compares) > 0 {
		return
	}
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
	mp := proc.Mp()
	if ctr.buf == nil {
		ctr.buf = batch.NewOffHeapWithSize(ctr.batchList[0].VectorCount())
		for i := range ctr.buf.Vecs {
			ctr.buf.Vecs[i] = vector.NewOffHeapVecWithType(*ctr.batchList[0].Vecs[i].GetType())
		}
	} else {
		ctr.buf.CleanOnlyData()
	}

	wholeLength := 0
	nextSizeCheck := batchSizeCheckInterval
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
		if wholeLength >= nextSizeCheck {
			if ctr.buf.Size() >= maxBatchSizeToSend {
				break
			}
			nextSizeCheck = wholeLength + batchSizeCheckInterval
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
	ctr.spillMemUsage -= int64(bat.Size())
	ctr.batchList = append(ctr.batchList[:index], ctr.batchList[index+1:]...)
	ctr.indexList = append(ctr.indexList[:index], ctr.indexList[index+1:]...)

	for i := range cols {
		if batchContainsVector(bat, cols[i]) {
			continue
		}
		cols[i].Free(proc.GetMPool())
	}
	ctr.orderCols = append(ctr.orderCols[:index], ctr.orderCols[index+1:]...)
}

func (mergeOrder *MergeOrder) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	ap := mergeOrder
	buf.WriteString(": mergeorder([")
	for i, f := range ap.OrderBySpecs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (mergeOrder *MergeOrder) OpType() vm.OpType {
	return vm.MergeOrder
}

func (mergeOrder *MergeOrder) Prepare(proc *process.Process) (err error) {
	if mergeOrder.OpAnalyzer == nil {
		mergeOrder.OpAnalyzer = process.NewAnalyzer(mergeOrder.GetIdx(), mergeOrder.IsFirst, mergeOrder.IsLast, "merge order")
	} else {
		mergeOrder.OpAnalyzer.Reset()
	}

	ctr := &mergeOrder.ctr
	ctr.setSpillThreshold(mergeOrder.SpillThreshold)
	if len(mergeOrder.ctr.executors) == 0 {
		ctr.batchList = make([]*batch.Batch, 0, defaultCacheBatchSize)
		ctr.orderCols = make([][]*vector.Vector, 0, defaultCacheBatchSize)

		mergeOrder.ctr.executors = make([]colexec.ExpressionExecutor, len(mergeOrder.OrderBySpecs))
		for i := range mergeOrder.ctr.executors {
			mergeOrder.ctr.executors[i], err = colexec.NewExpressionExecutor(proc, mergeOrder.OrderBySpecs[i].Expr)
			if err != nil {
				return err
			}
		}
	}
	ctr.initSpillKeyMetadata(mergeOrder.OrderBySpecs)
	return nil
}

func (ctr *container) initSpillKeyMetadata(fs []*plan.OrderBySpec) {
	if len(ctr.executors) == 0 {
		return
	}
	ctr.spillKeyIndexes = ctr.spillKeyIndexes[:0]
	ctr.spillColPos = make([]int32, len(ctr.executors))
	for i := range ctr.executors {
		if ctr.executors[i].IsColumnExpr() {
			ctr.spillColPos[i] = fs[i].Expr.Expr.(*plan2.Expr_Col).Col.ColPos
			continue
		}
		ctr.spillColPos[i] = -1
		ctr.spillKeyIndexes = append(ctr.spillKeyIndexes, i)
	}
}

func (mergeOrder *MergeOrder) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := mergeOrder.OpAnalyzer

	ctr := &mergeOrder.ctr
	for {
		switch ctr.status {
		case receiving:
			input, err := vm.ChildrenCall(mergeOrder.GetChildren(0), proc, analyzer)
			if err != nil {
				return vm.CancelResult, err
			}

			if input.Batch == nil {
				if ctr.spilling {
					if err = ctr.prepareSpillFinalMerge(proc, mergeOrder.OrderBySpecs, analyzer); err != nil {
						return input, err
					}
					if len(ctr.spillReaders) == 0 {
						ctr.status = finish
					} else {
						ctr.status = spillSending
					}
					continue
				}

				// if number of block is less than 2, no need to do merge sort.
				ctr.status = normalSending
				if len(ctr.batchList) > 1 {
					if err = ctr.prepareInMemoryMerge(proc, mergeOrder.OrderBySpecs); err != nil {
						return input, err
					}
					ctr.status = pickUpSending
				}
				continue
			}

			if input.Batch.IsEmpty() {
				continue
			}

			if ctr.shouldSpill(int64(input.Batch.Size())) {
				if !ctr.spilling {
					ctr.generateCompares(mergeOrder.OrderBySpecs)
					if err = ctr.spillCachedRuns(proc, analyzer); err != nil {
						input.Batch.Clean(proc.Mp())
						return vm.CancelResult, err
					}
				}
				if err = ctr.spillInputBatch(proc, input.Batch, analyzer); err != nil {
					input.Batch.Clean(proc.Mp())
					return vm.CancelResult, err
				}
				input.Batch.Clean(proc.Mp())
				continue
			}

			bat, err := input.Batch.Dup(proc.GetMPool())
			if err != nil {
				return vm.CancelResult, err
			}
			analyzer.Alloc(int64(bat.Size()))
			if err = ctr.mergeAndEvaluateOrderColumn(proc, bat); err != nil {
				bat.Clean(proc.Mp())
				return vm.CancelResult, err
			}
			if ctr.shouldSpill(0) {
				if err = ctr.spillCachedRuns(proc, analyzer); err != nil {
					return vm.CancelResult, err
				}
			}

		case normalSending:
			if len(ctr.batchList) == 0 {
				return vm.CancelResult, nil
			}

			// If only one batch, no need to sort. just send it.
			if len(ctr.batchList) == 1 {
				if ctr.buf != nil {
					ctr.buf.Clean(proc.Mp())
					ctr.buf = nil
				}
				ctr.buf = ctr.batchList[0]
				ctr.batchList[0] = nil
				result := vm.NewCallResult()
				result.Batch = ctr.buf
				return result, nil
			}
			return vm.CancelResult, nil

		case pickUpSending:
			result := vm.NewCallResult()
			sendOver, err := ctr.pickAndSend(proc, &result)
			if sendOver {
				ctr.status = finish
				return result, err
			}
			result.Status = vm.ExecHasMore
			return result, err

		case spillSending:
			result := vm.NewCallResult()
			sendOver, err := ctr.sendSpillResult(proc, &result)
			if sendOver {
				ctr.status = finish
			} else {
				result.Status = vm.ExecHasMore
			}
			return result, err

		case finish:
			return vm.CancelResult, nil
		}
	}
}
