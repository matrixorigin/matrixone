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

package order

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/ordersites"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "order"

func (ctr *container) appendBatch(proc *process.Process, bat *batch.Batch) (enoughToSend bool, err error) {
	if len(bat.ExtraBuf) != 0 {
		return false, moerr.NewInternalError(proc.Ctx,
			"order build should not have extra buffers")
	}
	s1, s2 := 0, bat.Size()
	if ctr.batWaitForSort != nil {
		s1 = ctr.batWaitForSort.Size()
	}
	all := s1 + s2

	if ctr.batWaitForSort == nil && ctr.retainedAllocation != nil {
		ctr.batWaitForSort, err = proc.NewBatchFromSrcWithAllocation(
			bat,
			0,
			ctr.retainedAllocation,
		)
		if err != nil {
			return false, err
		}
		// NewBatchFromSrcWithAllocation creates an empty destination, so copy
		// the shuffle routing metadata explicitly before appending the rows.
		// The legacy Dup path preserves the same field through CloneTo.
		ctr.batWaitForSort.ShuffleIDX = bat.ShuffleIDX
	}
	if ctr.batWaitForSort == nil {
		ctr.batWaitForSort, err = bat.Dup(proc.Mp())
	} else if ctr.allocationAccount == nil {
		ctr.batWaitForSort, err = ctr.batWaitForSort.AppendWithCopy(
			proc.Ctx, proc.Mp(), bat)
	} else {
		err = ctr.appendBatchAccounted(proc, bat)
	}
	if err != nil {
		return false, err
	}
	return all >= maxBatchSizeToSort, nil
}

func (ctr *container) appendBatchAccounted(
	proc *process.Process,
	bat *batch.Batch,
) error {
	if ctr == nil || ctr.batWaitForSort == nil || bat == nil ||
		len(ctr.batWaitForSort.Vecs) != len(bat.Vecs) {
		return mpool.ErrAllocationAccountInvalid
	}
	if len(bat.Vecs) == 0 {
		ctr.batWaitForSort.AddRowCount(bat.RowCount())
		return nil
	}
	checkpoints, err := ctr.appendCheckpoints(len(ctr.batWaitForSort.Vecs), proc)
	if err != nil {
		return err
	}
	for i := range ctr.batWaitForSort.Vecs {
		checkpoints[i] = ctr.batWaitForSort.Vecs[i].MakeAppendCheckpoint()
	}
	for i := range ctr.batWaitForSort.Vecs {
		if err = ctr.batWaitForSort.Vecs[i].UnionBatch(
			bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.Mp()); err != nil {
			for j := range ctr.batWaitForSort.Vecs {
				ctr.batWaitForSort.Vecs[j].RollbackAppend(
					checkpoints[j], bat.Vecs[j].Length())
			}
			return err
		}
		ctr.batWaitForSort.Vecs[i].SetSorted(false)
	}
	ctr.batWaitForSort.AddRowCount(bat.RowCount())
	return nil
}

func (ctr *container) sortAndSend(proc *process.Process, result *vm.CallResult) (err error) {
	if ctr.batWaitForSort != nil {
		for i := range ctr.sortExprExecutor {
			ctr.sortVectors[i], err = ctr.sortExprExecutor[i].Eval(proc, []*batch.Batch{ctr.batWaitForSort}, nil)
			if err != nil {
				return err
			}
		}

		rowCount := ctr.batWaitForSort.RowCount()
		ctr.resultOrderList, ctr.resultOrderMP, err = growOrderSlice(
			ctr.resultOrderList,
			ctr.resultOrderMP,
			rowCount,
			proc,
			ctr.allocationAccount,
			ordersites.OrderSelections,
		)
		if err != nil {
			return err
		}
		if len(ctr.sortVectors) > 1 && ctr.allocationAccount != nil {
			ctr.sortScratch.Partitions, ctr.sortPartitionsMP, err = growOrderSlice(
				ctr.sortScratch.Partitions,
				ctr.sortPartitionsMP,
				rowCount,
				proc,
				ctr.allocationAccount,
				ordersites.OrderSortPartitions,
			)
			if err != nil {
				return err
			}
			ctr.sortScratch.Partitions = ctr.sortScratch.Partitions[:0]
			ctr.sortScratch.Diffs, ctr.sortDiffsMP, err = growOrderSlice(
				ctr.sortScratch.Diffs,
				ctr.sortDiffsMP,
				rowCount,
				proc,
				ctr.allocationAccount,
				ordersites.OrderSortDiffs,
			)
			if err != nil {
				return err
			}
		}

		for i := range ctr.resultOrderList {
			ctr.resultOrderList[i] = int64(i)
		}

		if ctr.allocationAccount == nil {
			sort.SortByVectors(
				ctr.resultOrderList, ctr.sortVectors, ctr.desc, ctr.nullsLast)
		} else {
			sort.SortByVectorsWithScratch(
				ctr.resultOrderList,
				ctr.sortVectors,
				ctr.desc,
				ctr.nullsLast,
				&ctr.sortScratch,
			)
		}

		if err = ctr.batWaitForSort.Shuffle(ctr.resultOrderList, proc.Mp()); err != nil {
			return err
		}
	}
	ctr.rbat = ctr.batWaitForSort
	result.Batch = ctr.rbat
	ctr.batWaitForSort = nil
	return nil
}

func (order *Order) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	ap := order
	buf.WriteString(": τ([")
	for i, f := range ap.OrderBySpec {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (order *Order) OpType() vm.OpType {
	return vm.Order
}

func (order *Order) Prepare(proc *process.Process) (err error) {
	defer func() {
		err = orderTerminalCapacityError(proc.Ctx, err)
	}()
	if order.OpAnalyzer == nil {
		order.OpAnalyzer = process.NewAnalyzer(order.GetIdx(), order.IsFirst, order.IsLast, "order")
	} else {
		order.OpAnalyzer.Reset()
	}

	ctr := &order.ctr
	if len(ctr.desc) == 0 {
		ctr.desc = make([]bool, len(order.OrderBySpec))
		ctr.nullsLast = make([]bool, len(order.OrderBySpec))
		ctr.sortVectors = make([]*vector.Vector, len(order.OrderBySpec))
		for i, f := range order.OrderBySpec {
			ctr.desc[i] = f.Flag&pbplan.OrderBySpec_DESC != 0
			if f.Flag&pbplan.OrderBySpec_NULLS_FIRST != 0 {
				order.ctr.nullsLast[i] = false
			} else if f.Flag&pbplan.OrderBySpec_NULLS_LAST != 0 {
				order.ctr.nullsLast[i] = true
			} else {
				order.ctr.nullsLast[i] = order.ctr.desc[i]
			}
		}

		planExprs := make([]*pbplan.Expr, len(order.OrderBySpec))
		for i := range order.OrderBySpec {
			planExprs[i] = order.OrderBySpec[i].Expr
		}
		ctr.sortExprExecutor, err =
			colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(
				proc,
				planExprs,
				ctr.expressionAllocation,
			)
		if err != nil {
			ctr.releaseExpressionExecutors()
			ctr.desc = nil
			ctr.nullsLast = nil
			return err
		}
	}

	return nil
}

func (order *Order) Call(proc *process.Process) (
	result vm.CallResult,
	err error,
) {
	defer func() {
		err = orderTerminalCapacityError(proc.Ctx, err)
	}()
	analyzer := order.OpAnalyzer

	ctr := &order.ctr
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.GetMPool())
		ctr.rbat = nil
	}

	if ctr.state == vm.Build {
		for {
			input, err := vm.ChildrenCall(order.GetChildren(0), proc, analyzer)
			if err != nil {
				return vm.CancelResult, err
			}
			if input.Batch == nil {
				if err, canceled := vm.CancelCheck(proc); canceled {
					return vm.CancelResult, err
				}
				ctr.state = vm.Eval
				break
			}
			if input.Batch.IsEmpty() {
				continue
			}

			enoughToSend, err := ctr.appendBatch(proc, input.Batch)
			if err != nil {
				return vm.CancelResult, err
			}

			if enoughToSend {
				err := ctr.sortAndSend(proc, &input)
				if err != nil {
					return vm.CancelResult, err
				}
				return input, nil
			}
		}
	}

	result = vm.NewCallResult()
	if ctr.state == vm.Eval {
		err := ctr.sortAndSend(proc, &result)
		if err != nil {
			return vm.CancelResult, err
		}
		ctr.state = vm.End
		return result, nil
	}

	if ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}
