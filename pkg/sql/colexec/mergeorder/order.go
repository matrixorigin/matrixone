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
	"container/heap"

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
	ctr.initInMemoryHeap()
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

type inMemoryMergeHeap struct {
	ctr   *container
	items []int
}

func (h *inMemoryMergeHeap) Len() int {
	return len(h.items)
}

func (h *inMemoryMergeHeap) Less(i, j int) bool {
	left := h.items[i]
	right := h.items[j]
	return h.ctr.compareInMemoryRows(left, h.ctr.indexList[left], right, h.ctr.indexList[right]) < 0
}

func (h *inMemoryMergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.ctr.inMemoryHeapPos[h.items[i]] = i
	h.ctr.inMemoryHeapPos[h.items[j]] = j
}

func (h *inMemoryMergeHeap) Push(x any) {
	idx := x.(int)
	h.ctr.inMemoryHeapPos[idx] = len(h.items)
	h.items = append(h.items, idx)
}

func (h *inMemoryMergeHeap) Pop() any {
	old := h.items
	n := len(old)
	idx := old[n-1]
	h.items = old[:n-1]
	h.ctr.inMemoryHeapPos[idx] = -1
	return idx
}

func (ctr *container) compareInMemoryRows(left int, leftRow int64, right int, rightRow int64) int {
	compares := ctr.compares
	leftCols := ctr.orderCols[left]
	rightCols := ctr.orderCols[right]
	if len(compares) == 1 {
		compares[0].Set(0, leftCols[0])
		compares[0].Set(1, rightCols[0])
		return compares[0].Compare(0, 1, leftRow, rightRow)
	}
	for k := 0; k < len(compares); k++ {
		compares[k].Set(0, leftCols[k])
		compares[k].Set(1, rightCols[k])
		if r := compares[k].Compare(0, 1, leftRow, rightRow); r != 0 {
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

	compares := ctr.compares
	for k := 0; k < len(compares); k++ {
		compares[k].Set(0, ctr.orderCols[winner][k])
		compares[k].Set(1, ctr.orderCols[loser][k])
	}

	chunk := 1
	if len(compares) == 1 {
		cmp := compares[0]
		for chunk < limit {
			nextRow := start + int64(chunk)
			if cmp.Compare(0, 1, nextRow, loserRow) <= 0 {
				chunk++
			} else {
				break
			}
		}
		return chunk
	}
	for chunk < limit {
		nextRow := start + int64(chunk)
		ordered := true
		for k := 0; k < len(compares); k++ {
			if r := compares[k].Compare(0, 1, nextRow, loserRow); r != 0 {
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

func (ctr *container) initInMemoryHeap() {
	if len(ctr.batchList) <= 1 {
		ctr.inMemoryHeap = nil
		ctr.inMemoryHeapPos = nil
		return
	}
	ctr.inMemoryHeapPos = make([]int, len(ctr.batchList))
	for i := range ctr.inMemoryHeapPos {
		ctr.inMemoryHeapPos[i] = -1
	}
	items := make([]int, 0, len(ctr.batchList))
	for i := range ctr.batchList {
		if ctr.batchList[i] == nil {
			continue
		}
		ctr.inMemoryHeapPos[i] = len(items)
		items = append(items, i)
	}
	ctr.inMemoryHeap = &inMemoryMergeHeap{ctr: ctr, items: items}
	heap.Init(ctr.inMemoryHeap)
}

func (ctr *container) advanceInMemoryBatchByChunk(proc *process.Process, index int, chunk int) error {
	ctr.indexList[index] += int64(chunk)
	if ctr.indexList[index] < int64(ctr.batchList[index].RowCount()) {
		heap.Fix(ctr.inMemoryHeap, ctr.inMemoryHeapPos[index])
		return nil
	}
	return ctr.removeInMemoryBatch(proc, index)
}

func (ctr *container) removeInMemoryBatch(proc *process.Process, index int) error {
	bat := ctr.batchList[index]
	cols := ctr.orderCols[index]
	ctr.spillMemUsage -= int64(bat.Size())
	if ctr.inMemoryHeap != nil {
		heap.Remove(ctr.inMemoryHeap, ctr.inMemoryHeapPos[index])
	}
	for i := range cols {
		if batchContainsVector(bat, cols[i]) {
			continue
		}
		cols[i].Free(proc.GetMPool())
	}
	ctr.batchList[index] = nil
	ctr.orderCols[index] = nil
	ctr.indexList[index] = -1
	return nil
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
		if ctr.inMemoryHeap == nil || ctr.inMemoryHeap.Len() == 0 {
			sendOver = true
			break
		}
		if ctr.inMemoryHeap.Len() == 1 {
			choice := ctr.inMemoryHeap.items[0]
			src := ctr.batchList[choice]
			start := ctr.indexList[choice]
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
			if err = ctr.advanceInMemoryBatchByChunk(proc, choice, chunk); err != nil {
				return false, err
			}
			if ctr.inMemoryHeap.Len() == 0 {
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
		choice := ctr.inMemoryHeap.items[0]
		if ctr.inMemoryHeap.Len() == 2 {
			winner := choice
			loser := ctr.inMemoryHeap.items[1]
			if ctr.compareInMemoryRows(loser, ctr.indexList[loser], winner, ctr.indexList[winner]) < 0 {
				winner, loser = loser, winner
			}
			choice = winner
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
					if err = ctr.advanceInMemoryBatchByChunk(proc, winner, chunk); err != nil {
						return false, err
					}
					if ctr.inMemoryHeap.Len() == 0 {
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
		} else {
			items := ctr.inMemoryHeap.items
			second := items[1]
			if len(items) > 2 && ctr.compareInMemoryRows(items[2], ctr.indexList[items[2]], second, ctr.indexList[second]) < 0 {
				second = items[2]
			}
			budgetChunk := computeBatchDrainChunk(ctr.batchList[choice], ctr.indexList[choice], ctr.buf.Size(), fixedRowBytes)
			if budgetChunk > 1 {
				chunk := ctr.computeInMemoryWinnerChunk(choice, second, budgetChunk)
				if chunk > 1 {
					for j := range ctr.buf.Vecs {
						err = ctr.buf.Vecs[j].UnionBatch(ctr.batchList[choice].Vecs[j], ctr.indexList[choice], chunk, nil, mp)
						if err != nil {
							return false, err
						}
					}
					wholeLength += chunk
					if err = ctr.advanceInMemoryBatchByChunk(proc, choice, chunk); err != nil {
						return false, err
					}
					if ctr.inMemoryHeap.Len() == 0 {
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

		if err = appendContiguousRows(ctr.buf, ctr.batchList[choice], ctr.indexList[choice], 1, proc); err != nil {
			return false, err
		}

		wholeLength++
		if err = ctr.advanceInMemoryBatchByChunk(proc, choice, 1); err != nil {
			return false, err
		}
		if ctr.inMemoryHeap.Len() == 0 {
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
