// Copyright 2021 - 2024 Matrix Origin
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

package mergetop

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_top"

func (mergeTop *MergeTop) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": mergetop([")
	for i, f := range mergeTop.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	fmt.Fprintf(buf, "], %v)", mergeTop.Limit)
}

func (mergeTop *MergeTop) OpType() vm.OpType {
	return vm.MergeTop
}

func (mergeTop *MergeTop) Prepare(proc *process.Process) (err error) {
	defer func() {
		err = mergeTopTerminalCapacityError(proc.Ctx, err)
	}()
	if mergeTop.OpAnalyzer == nil {
		mergeTop.OpAnalyzer = process.NewAnalyzer(mergeTop.GetIdx(), mergeTop.IsFirst, mergeTop.IsLast, "mergetop")
	} else {
		mergeTop.OpAnalyzer.Reset()
	}

	// limit executor
	if mergeTop.ctr.limitExecutor == nil {
		mergeTop.ctr.limitExecutor, err = colexec.NewExpressionExecutorWithAllocation(
			proc,
			mergeTop.Limit,
			mergeTop.ctr.expressionAllocation,
		)
		if err != nil {
			return err
		}
	}
	vec, err := mergeTop.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	mergeTop.ctr.limit = vector.MustFixedColWithTypeCheck[uint64](vec)[0]
	initialSelections := int(min(mergeTop.ctr.limit, uint64(1024)))
	if initialSelections > 0 {
		selections, growErr := growMergeTopSelections(
			mergeTop.ctr.sels,
			initialSelections,
			proc,
			mergeTop.ctr.allocationAccount,
		)
		if growErr != nil {
			return growErr
		}
		mergeTop.ctr.sels = selections[:0]
	}
	mergeTop.ctr.poses = make([]int32, 0, len(mergeTop.Fs))

	// executor for order list
	if len(mergeTop.ctr.executorsForOrderList) != len(mergeTop.Fs) {
		mergeTop.ctr.executorsForOrderList = make([]colexec.ExpressionExecutor, len(mergeTop.Fs))
		for i := range mergeTop.ctr.executorsForOrderList {
			mergeTop.ctr.executorsForOrderList[i], err = colexec.NewExpressionExecutorWithAllocation(
				proc,
				mergeTop.Fs[i].Expr,
				mergeTop.ctr.expressionAllocation,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (mergeTop *MergeTop) Call(proc *process.Process) (
	result vm.CallResult,
	err error,
) {
	defer func() {
		err = mergeTopTerminalCapacityError(proc.Ctx, err)
	}()
	analyzer := mergeTop.OpAnalyzer

	result = vm.NewCallResult()
	if mergeTop.ctr.limit == 0 {
		mergeTop.ctr.state = vm.End
		result.Status = vm.ExecStop
		return result, nil
	}

	if mergeTop.ctr.state == vm.Build {
		if end, err := mergeTop.ctr.build(mergeTop, proc, analyzer); err != nil {
			return result, err
		} else if end {
			mergeTop.ctr.state = vm.End
			result.Status = vm.ExecStop
			return result, nil
		}
		mergeTop.ctr.state = vm.Eval
	}

	if mergeTop.ctr.state == vm.Eval {
		if mergeTop.ctr.bat == nil || mergeTop.ctr.bat.IsEmpty() {
			mergeTop.ctr.state = vm.End
			result.Status = vm.ExecStop
			return result, nil
		}
		err = mergeTop.ctr.eval(mergeTop.ctr.limit, proc, analyzer, &result)
		if err == nil {
			mergeTop.ctr.state = vm.End
			result.Status = vm.ExecStop
		}
		return result, err
	}

	if mergeTop.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}

func (ctr *container) build(ap *MergeTop, proc *process.Process, analyzer process.Analyzer) (bool, error) {
	for {
		result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
		if err != nil {
			return true, err
		}
		if result.Batch == nil {
			// The child can cancel after this invocation passed vm.Exec's entry
			// check and still return EOF. Do not sort and publish partial input.
			if err, canceled := vm.CancelCheck(proc); canceled {
				return true, err
			}
			return false, nil
		}
		if result.Batch.IsEmpty() {
			continue
		}
		if len(result.Batch.ExtraBuf) != 0 {
			return true, moerr.NewInternalError(proc.Ctx,
				"merge top build should not have extra buffers")
		}

		bat, err := cloneMergeTopBatch(
			proc,
			result.Batch,
			ctr.retainedAllocation,
		)
		if err != nil {
			return true, err
		}
		analyzer.Alloc(int64(bat.Size()))

		ctr.n = len(bat.Vecs)
		ctr.poses = ctr.poses[:0]
		for i := range ctr.executorsForOrderList {
			if ctr.executorsForOrderList[i].IsColumnExpr() {
				colIndex := ctr.executorsForOrderList[i].(*colexec.ColumnExpressionExecutor).GetColIndex()
				ctr.poses = append(ctr.poses, int32(colIndex))
			} else {
				vec, err := ctr.executorsForOrderList[i].EvalWithoutResultReusing(proc, []*batch.Batch{bat}, nil)
				if err != nil {
					bat.Clean(proc.Mp())
					return false, err
				}
				ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
				bat.Vecs = append(bat.Vecs, vec)
				analyzer.Alloc(int64(vec.Size()))
			}
		}

		if len(ctr.cmps) == 0 {
			mp := make(map[int]int, len(ctr.poses))
			for i, pos := range ctr.poses {
				mp[int(pos)] = i
			}

			if ctr.bat == nil {
				ctr.bat = batch.NewOffHeapWithSize(len(bat.Vecs))
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.NewOffHeapVecWithType(*vec.GetType())
				}
				if ctr.retainedAllocation != nil {
					if err := ctr.bat.SetAllocationAccount(ctr.retainedAllocation); err != nil {
						bat.Clean(proc.Mp())
						ctr.bat.Clean(proc.Mp())
						ctr.bat = nil
						return false, err
					}
				}
			}

			for i := 0; i < len(bat.Vecs); i++ {
				var desc, nullsLast bool
				if pos, ok := mp[i]; ok {
					desc = ap.Fs[pos].Flag&plan.OrderBySpec_DESC != 0
					if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
						nullsLast = false
					} else if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
						nullsLast = true
					} else {
						nullsLast = desc
					}
				}
				ctr.cmps = append(
					ctr.cmps,
					compare.NewOrder(*bat.Vecs[i].GetType(), desc, nullsLast),
				)
			}

		}

		if err := ctr.processBatch(ap.ctr.limit, bat, proc); err != nil {
			bat.Clean(proc.Mp())
			return false, err
		}
		bat.Clean(proc.Mp())
	}
}

func (ctr *container) processBatch(limit uint64, bat *batch.Batch, proc *process.Process) error {
	rowCount := bat.RowCount()
	processCount := rowsToFill(limit, len(ctr.sels), rowCount)

	if processCount > 0 {
		if processCount > math.MaxInt-len(ctr.sels) {
			return mpool.ErrAllocationAllocatorLimit
		}
		oldSelectionCount := len(ctr.sels)
		selections, err := growMergeTopSelections(
			ctr.sels,
			oldSelectionCount+processCount,
			proc,
			ctr.allocationAccount,
		)
		if err != nil {
			return err
		}
		ctr.sels = selections[:oldSelectionCount]
		var checkpoints []vector.AppendCheckpoint
		if ctr.allocationAccount != nil {
			checkpoints, err = ctr.appendCheckpoints(len(ctr.bat.Vecs), proc)
			if err != nil {
				return err
			}
			for i := range ctr.bat.Vecs {
				checkpoints[i] = ctr.bat.Vecs[i].MakeAppendCheckpoint()
			}
		}
		for j, vec := range ctr.bat.Vecs {
			if err := vec.UnionBatch(
				bat.Vecs[j],
				0,
				processCount,
				nil,
				proc.Mp(),
			); err != nil {
				for i := range checkpoints {
					ctr.bat.Vecs[i].RollbackAppend(checkpoints[i], processCount)
				}
				return err
			}
		}
		baseSel := int64(len(ctr.sels))
		for i := range processCount {
			ctr.sels = ctr.sels[:oldSelectionCount+i+1]
			ctr.sels[oldSelectionCount+i] = baseSel + int64(i)
		}
		ctr.bat.AddRowCount(processCount)

		if uint64(len(ctr.sels)) == limit {
			ctr.sort()
		}
	}

	if processCount == rowCount {
		return nil
	}

	// bat is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
	}
	for i, j := processCount, rowCount; i < j; i++ {
		rowIdx := int64(i)
		if ctr.compare(1, 0, rowIdx, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, rowIdx, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func rowsToFill(limit uint64, currentRows int, batchRows int) int {
	if uint64(currentRows) >= limit {
		return 0
	}
	remaining := limit - uint64(currentRows)
	if remaining >= uint64(batchRows) {
		return batchRows
	}
	return int(remaining)
}

func (ctr *container) eval(limit uint64, proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) error {
	if uint64(len(ctr.sels)) < limit {
		ctr.sort()
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	ordered := ctr.sels[:len(ctr.sels)]
	for range len(ordered) {
		heap.Pop(ctr)
	}
	ctr.sels = ordered
	if err := ctr.bat.Shuffle(ctr.sels, proc.Mp()); err != nil {
		return err
	}
	for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
		ctr.bat.Vecs[i].Free(proc.Mp())
	}
	ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
	result.Batch = ctr.bat
	return nil
}

func cloneMergeTopBatch(
	proc *process.Process,
	source *batch.Batch,
	selection *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	if selection == nil {
		return source.Dup(proc.Mp())
	}
	destination, err := proc.NewBatchFromSrcWithAllocation(source, 0, selection)
	if err != nil {
		return nil, err
	}
	if err = source.CloneTo(destination, proc.Mp()); err != nil {
		destination.Clean(proc.Mp())
		return nil, err
	}
	return destination, nil
}

// do sort work for heap, and result order will be set in container.sels
func (ctr *container) sort() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}
