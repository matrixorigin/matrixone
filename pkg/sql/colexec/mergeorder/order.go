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

func calcFixedRowBytes(vecs []*vector.Vector) int {
	rowBytes := 0
	for _, vec := range vecs {
		typ := vec.GetType()
		if typ.IsVarlen() {
			return 0
		}
		rowBytes += typ.TypeSize()
	}
	if rowBytes < 1 {
		return 0
	}
	return rowBytes
}

func computeBatchDrainChunk(src *batch.Batch, start int64, currentSize int, fixedRowBytes int) int {
	remaining := src.RowCount() - int(start)
	if remaining <= 0 {
		return 0
	}
	budget := maxBatchSizeToSend - currentSize
	if budget <= 0 {
		return 0
	}

	if fixedRowBytes > 0 {
		maxByBudget := budget / fixedRowBytes
		if maxByBudget < 1 {
			maxByBudget = 1
		}
		if maxByBudget > maxDrainChunkRows {
			maxByBudget = maxDrainChunkRows
		}
		if maxByBudget < remaining {
			return maxByBudget
		}
		return remaining
	}

	avgRowBytes := src.Size() / max(1, src.RowCount())
	if avgRowBytes < 1 {
		avgRowBytes = 1
	}
	maxByBudget := budget / avgRowBytes
	if maxByBudget < 1 {
		maxByBudget = 1
	}
	if maxByBudget > maxVarlenDrainChunkRows {
		maxByBudget = maxVarlenDrainChunkRows
	}
	if maxByBudget < remaining {
		return maxByBudget
	}
	return remaining
}

func (ctr *container) compareInMemoryRows(left int, leftRow int64, right int, rightRow int64) int {
	for k := 0; k < len(ctr.compares); k++ {
		ctr.compares[k].Set(0, ctr.orderCols[left][k])
		ctr.compares[k].Set(1, ctr.orderCols[right][k])
		if r := ctr.compares[k].Compare(0, 1, leftRow, rightRow); r != 0 {
			return r
		}
	}
	return 0
}

func (ctr *container) compareInMemoryRowsWithBoundLeft(leftRow int64, right int, rightRow int64) int {
	for k := 0; k < len(ctr.compares); k++ {
		ctr.compares[k].Set(1, ctr.orderCols[right][k])
		if r := ctr.compares[k].Compare(0, 1, leftRow, rightRow); r != 0 {
			return r
		}
	}
	return 0
}

func (ctr *container) computeInMemoryWinnerChunk(winner int, loser int, budgetChunk int) int {
	if budgetChunk <= 1 {
		return budgetChunk
	}
	start := ctr.indexList[winner]
	loserRow := ctr.indexList[loser]
	remaining := ctr.batchList[winner].RowCount() - int(start)
	if remaining <= 1 {
		return remaining
	}
	limit := budgetChunk
	if limit > remaining {
		limit = remaining
	}
	if limit > maxWinnerChunkRows {
		limit = maxWinnerChunkRows
	}

	for k := 0; k < len(ctr.compares); k++ {
		ctr.compares[k].Set(0, ctr.orderCols[winner][k])
		ctr.compares[k].Set(1, ctr.orderCols[loser][k])
	}

	chunk := 1
	for chunk < limit {
		nextRow := start + int64(chunk)
		ordered := true
		for k := 0; k < len(ctr.compares); k++ {
			if r := ctr.compares[k].Compare(0, 1, nextRow, loserRow); r != 0 {
				ordered = r < 0
				break
			}
		}
		if ordered {
			chunk++
		} else {
			break
		}
	}
	return chunk
}

func (ctr *container) pickFirstSecondRows() (first int, second int) {
	l := len(ctr.indexList)
	if l < 2 {
		return 0, -1
	}
	first, second = 0, 1
	if ctr.compareInMemoryRows(second, ctr.indexList[second], first, ctr.indexList[first]) < 0 {
		first, second = second, first
	}
	for i := 2; i < l; i++ {
		row := ctr.indexList[i]
		for k := 0; k < len(ctr.compares); k++ {
			ctr.compares[k].Set(0, ctr.orderCols[i][k])
		}
		if ctr.compareInMemoryRowsWithBoundLeft(row, first, ctr.indexList[first]) < 0 {
			second = first
			first = i
			continue
		}
		if ctr.compareInMemoryRowsWithBoundLeft(row, second, ctr.indexList[second]) < 0 {
			second = i
		}
	}
	return first, second
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
	fixedRowBytes := calcFixedRowBytes(ctr.buf.Vecs)
	for {
		choice := -1
		if len(ctr.indexList) == 1 {
			src := ctr.batchList[0]
			start := ctr.indexList[0]
			chunk := computeBatchDrainChunk(src, start, ctr.buf.Size(), fixedRowBytes)
			if chunk < 1 {
				chunk = 1
			}
			for j := range ctr.buf.Vecs {
				err = ctr.buf.Vecs[j].UnionBatch(src.Vecs[j], start, chunk, nil, mp)
				if err != nil {
					return false, err
				}
			}
			wholeLength += chunk
			ctr.indexList[0] += int64(chunk)
			if ctr.indexList[0] == int64(src.RowCount()) {
				ctr.removeBatch(proc, 0)
			}
			if len(ctr.indexList) == 0 {
				sendOver = true
				break
			}
			if ctr.buf.Size() >= maxBatchSizeToSend {
				break
			}
			if wholeLength >= nextSizeCheck {
				nextSizeCheck = wholeLength + batchSizeCheckInterval
			}
			continue
		}
		if len(ctr.indexList) == 2 {
			winner := 0
			if ctr.compareInMemoryRows(1, ctr.indexList[1], 0, ctr.indexList[0]) < 0 {
				winner = 1
			}
			choice = winner
			loser := 1 - winner
			budgetChunk := computeBatchDrainChunk(ctr.batchList[winner], ctr.indexList[winner], ctr.buf.Size(), fixedRowBytes)
			if budgetChunk > 1 {
				chunk := ctr.computeInMemoryWinnerChunk(winner, loser, budgetChunk)
				if chunk > 1 {
					for j := range ctr.buf.Vecs {
						err = ctr.buf.Vecs[j].UnionBatch(ctr.batchList[winner].Vecs[j], ctr.indexList[winner], chunk, nil, mp)
						if err != nil {
							return false, err
						}
					}
					wholeLength += chunk
					ctr.indexList[winner] += int64(chunk)
					if ctr.indexList[winner] == int64(ctr.batchList[winner].RowCount()) {
						ctr.removeBatch(proc, winner)
					}
					if len(ctr.indexList) == 0 {
						sendOver = true
						break
					}
					if ctr.buf.Size() >= maxBatchSizeToSend {
						break
					}
					if wholeLength >= nextSizeCheck {
						nextSizeCheck = wholeLength + batchSizeCheckInterval
					}
					continue
				}
			}
		}
		if len(ctr.indexList) > 2 {
			first, second := ctr.pickFirstSecondRows()
			choice = first
			budgetChunk := computeBatchDrainChunk(ctr.batchList[first], ctr.indexList[first], ctr.buf.Size(), fixedRowBytes)
			if budgetChunk > 1 {
				chunk := ctr.computeInMemoryWinnerChunk(first, second, budgetChunk)
				if chunk > 1 {
					for j := range ctr.buf.Vecs {
						err = ctr.buf.Vecs[j].UnionBatch(ctr.batchList[first].Vecs[j], ctr.indexList[first], chunk, nil, mp)
						if err != nil {
							return false, err
						}
					}
					wholeLength += chunk
					ctr.indexList[first] += int64(chunk)
					if ctr.indexList[first] == int64(ctr.batchList[first].RowCount()) {
						ctr.removeBatch(proc, first)
					}
					if len(ctr.indexList) == 0 {
						sendOver = true
						break
					}
					if ctr.buf.Size() >= maxBatchSizeToSend {
						break
					}
					if wholeLength >= nextSizeCheck {
						nextSizeCheck = wholeLength + batchSizeCheckInterval
					}
					continue
				}
			}
		}

		if choice < 0 {
			choice = ctr.pickFirstRow()
		}
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
		i := 0
		for k := 0; k < len(ctr.compares); k++ {
			ctr.compares[k].Set(0, ctr.orderCols[i][k])
		}
		for j := 1; j < l; j++ {
			for k := 0; k < len(ctr.compares); k++ {
				ctr.compares[k].Set(1, ctr.orderCols[j][k])
				result := ctr.compares[k].Compare(0, 1, ctr.indexList[i], ctr.indexList[j])
				if result < 0 {
					break
				} else if result > 0 {
					i = j
					for kk := 0; kk < len(ctr.compares); kk++ {
						ctr.compares[kk].Set(0, ctr.orderCols[i][kk])
					}
					break
				} else if k == len(ctr.compares)-1 {
					break
				}
			}
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
