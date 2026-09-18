// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	mosort "github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// SortBatch sorts one already-materialized input batch through the same
// external merge path used by the MergeOrder operator. The input batch is
// borrowed and remains owned by the caller. The returned batch is independent
// and must be released by the caller.
func SortBatch(
	proc *process.Process,
	input *batch.Batch,
	fs []*plan.OrderBySpec,
	threshold int64,
	analyzer process.Analyzer,
) (*batch.Batch, error) {
	return sortBatch(proc, input, fs, threshold, analyzer, nil)
}

// sortBatch sorts input and invokes inputConsumed after every input row has
// been copied into sorter-owned resident batches or spill runs and before the
// final result is collected. The callback transfers ownership of the source
// input to the sorter; it is not invoked when validation, input copying, or
// empty-input duplication fails.
func sortBatch(
	proc *process.Process,
	input *batch.Batch,
	fs []*plan.OrderBySpec,
	threshold int64,
	analyzer process.Analyzer,
	inputConsumed func(),
) (*batch.Batch, error) {
	if proc == nil || input == nil || len(fs) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid merge-order sort input")
	}
	if input.RowCount() == 0 {
		result, err := input.Dup(proc.Mp())
		if err == nil && inputConsumed != nil {
			inputConsumed()
		}
		return result, err
	}

	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	ctr := &container{
		batchList: make([]*batch.Batch, 0, defaultCacheBatchSize),
		orderCols: make([][]*vector.Vector, 0, defaultCacheBatchSize),
		budget:    budget,
	}
	ctr.setSpillThreshold(threshold)
	defer func() {
		for i := range ctr.batchList {
			if ctr.batchList[i] != nil {
				var orderCols []*vector.Vector
				if i < len(ctr.orderCols) {
					orderCols = ctr.orderCols[i]
				}
				freeOrderColumns(proc.Mp(), ctr.batchList[i], orderCols)
				ctr.batchList[i].Clean(proc.Mp())
			}
		}
		ctr.cleanupSpill(proc)
		if ctr.buf != nil {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		}
		for i := range ctr.executors {
			if ctr.executors[i] != nil {
				ctr.executors[i].Free()
			}
		}
	}()

	ctr.executors = make([]colexec.ExpressionExecutor, len(fs))
	for i := range fs {
		ctr.executors[i], err = colexec.NewExpressionExecutor(proc, fs[i].Expr)
		if err != nil {
			return nil, err
		}
	}
	ctr.initSpillKeyMetadata(fs)
	ctr.generateCompares(fs)

	desc, nullsLast := orderFlags(fs)
	chunkRows := sortChunkRows(input)
	for start := 0; start < input.RowCount(); start += chunkRows {
		end := start + chunkRows
		if end > input.RowCount() {
			end = input.RowCount()
		}
		if err = ctr.appendSortedChunk(proc, input, start, end, desc, nullsLast, fs, analyzer); err != nil {
			return nil, err
		}
	}

	if inputConsumed != nil {
		inputConsumed()
	}
	return ctr.collectSortedBatch(proc, fs, analyzer)
}

// SortBatchWithExtraVectors sorts input together with row-aligned vectors that
// are evaluated before the order pass. The extra vectors are returned in the
// same order after sorting and are owned by the caller. Every extra vector must
// have one value per input row; constant vectors should be omitted because
// their value is independent of row order.
func SortBatchWithExtraVectors(
	proc *process.Process,
	input *batch.Batch,
	fs []*plan.OrderBySpec,
	threshold int64,
	analyzer process.Analyzer,
	extra []*vector.Vector,
) (*batch.Batch, []*vector.Vector, error) {
	sorted, _, sortedExtra, err := sortBatchWithColumns(
		proc, input, fs, threshold, analyzer, nil, extra, nil)
	return sorted, sortedExtra, err
}

// SortBatchWithPrecomputedOrder sorts input by orderCols, which must already
// contain the values of fs in input-row order. It returns the sorted order
// columns and extra vectors alongside the sorted input. This lets a caller
// evaluate an order expression once, spill it, and reuse the same values after
// the merge.
func SortBatchWithPrecomputedOrder(
	proc *process.Process,
	input *batch.Batch,
	fs []*plan.OrderBySpec,
	threshold int64,
	analyzer process.Analyzer,
	orderCols []*vector.Vector,
	extra []*vector.Vector,
) (*batch.Batch, []*vector.Vector, []*vector.Vector, error) {
	return sortBatchWithPrecomputedOrder(
		proc, input, fs, threshold, analyzer, orderCols, extra, nil)
}

// SortBatchWithPrecomputedOrderAndRelease is the ownership-transfer variant
// of SortBatchWithPrecomputedOrder. Once the callback runs, the sorter owns
// independent copies of input, orderCols, and extra, and the callback must
// release those caller-owned vectors. The callback runs exactly once before
// final merge collection; if sorting fails before that point, the caller keeps
// ownership and the callback is not called.
func SortBatchWithPrecomputedOrderAndRelease(
	proc *process.Process,
	input *batch.Batch,
	fs []*plan.OrderBySpec,
	threshold int64,
	analyzer process.Analyzer,
	orderCols []*vector.Vector,
	extra []*vector.Vector,
	inputConsumed func(),
) (*batch.Batch, []*vector.Vector, []*vector.Vector, error) {
	return sortBatchWithPrecomputedOrder(
		proc, input, fs, threshold, analyzer, orderCols, extra, inputConsumed)
}

func sortBatchWithPrecomputedOrder(
	proc *process.Process,
	input *batch.Batch,
	fs []*plan.OrderBySpec,
	threshold int64,
	analyzer process.Analyzer,
	orderCols []*vector.Vector,
	extra []*vector.Vector,
	inputConsumed func(),
) (*batch.Batch, []*vector.Vector, []*vector.Vector, error) {
	if len(orderCols) != len(fs) {
		return nil, nil, nil, moerr.NewInvalidInputNoCtx("merge-order key count mismatch")
	}
	dataCols := 0
	if input != nil {
		dataCols = len(input.Vecs)
	}
	precomputed := make([]*plan.OrderBySpec, len(fs))
	for i, spec := range fs {
		if spec == nil || spec.Expr == nil {
			return nil, nil, nil, moerr.NewInvalidInputNoCtx("invalid merge-order specification")
		}
		precomputed[i] = &plan.OrderBySpec{
			Expr: &plan.Expr{
				Typ: spec.Expr.Typ,
				Expr: &plan2.Expr_Col{Col: &plan2.ColRef{
					ColPos: int32(dataCols + i),
				}},
			},
			Collation: spec.Collation,
			Flag:      spec.Flag,
		}
	}
	return sortBatchWithColumns(
		proc, input, precomputed, threshold, analyzer, orderCols, extra, inputConsumed)
}

func sortBatchWithColumns(
	proc *process.Process,
	input *batch.Batch,
	fs []*plan.OrderBySpec,
	threshold int64,
	analyzer process.Analyzer,
	orderCols []*vector.Vector,
	extra []*vector.Vector,
	inputConsumed func(),
) (*batch.Batch, []*vector.Vector, []*vector.Vector, error) {
	if proc == nil || input == nil {
		return nil, nil, nil, moerr.NewInvalidInputNoCtx("invalid merge-order sort input")
	}
	for _, vec := range orderCols {
		if vec == nil || vec.Length() != input.RowCount() {
			return nil, nil, nil, moerr.NewInvalidInputNoCtx("invalid merge-order carry vector")
		}
	}
	for _, vec := range extra {
		if vec == nil || vec.Length() != input.RowCount() {
			return nil, nil, nil, moerr.NewInvalidInputNoCtx("invalid merge-order carry vector")
		}
	}
	if len(orderCols) == 0 && len(extra) == 0 {
		sorted, err := sortBatch(proc, input, fs, threshold, analyzer, inputConsumed)
		return sorted, nil, nil, err
	}

	// The combined batch borrows all source vectors. SortBatch duplicates each
	// chunk before it shuffles or spills, so the borrowed source remains owned by
	// the caller throughout this operation.
	combined := batch.NewWithSize(len(input.Vecs) + len(orderCols) + len(extra))
	combined.Vecs = append(combined.Vecs[:0], input.Vecs...)
	combined.Vecs = append(combined.Vecs, orderCols...)
	combined.Vecs = append(combined.Vecs, extra...)
	combined.Attrs = make([]string, len(combined.Vecs))
	copy(combined.Attrs, input.Attrs)
	combined.Recursive = input.Recursive
	combined.SetRowCount(input.RowCount())
	dataCols := len(input.Vecs)
	inputAttrs := append([]string(nil), input.Attrs...)

	sorted, err := sortBatch(proc, combined, fs, threshold, analyzer, inputConsumed)
	if err != nil {
		return nil, nil, nil, err
	}
	orderCount := len(orderCols)
	if sorted == nil || len(sorted.Vecs) != dataCols+orderCount+len(extra) {
		if sorted != nil {
			sorted.Clean(proc.Mp())
		}
		return nil, nil, nil, moerr.NewInternalErrorNoCtx("merge-order carry vector count mismatch")
	}

	sortedOrder := append([]*vector.Vector(nil), sorted.Vecs[dataCols:dataCols+orderCount]...)
	sortedExtra := append([]*vector.Vector(nil), sorted.Vecs[dataCols+orderCount:]...)
	sorted.Vecs = sorted.Vecs[:dataCols]
	sorted.Attrs = inputAttrs
	return sorted, sortedOrder, sortedExtra, nil
}

func orderFlags(fs []*plan.OrderBySpec) (desc, nullsLast []bool) {
	desc = make([]bool, len(fs))
	nullsLast = make([]bool, len(fs))
	for i, f := range fs {
		desc[i] = f.Flag&plan2.OrderBySpec_DESC != 0
		switch {
		case f.Flag&plan2.OrderBySpec_NULLS_FIRST != 0:
			nullsLast[i] = false
		case f.Flag&plan2.OrderBySpec_NULLS_LAST != 0:
			nullsLast[i] = true
		default:
			nullsLast[i] = desc[i]
		}
	}
	return desc, nullsLast
}

func sortChunkRows(input *batch.Batch) int {
	rows := input.RowCount()
	if rows <= 1 || input.Size() <= maxBatchSizeToSend {
		return rows
	}
	bytesPerRow := input.Size() / rows
	if bytesPerRow < 1 {
		bytesPerRow = 1
	}
	rows = maxBatchSizeToSend / bytesPerRow
	if rows < 1 {
		rows = 1
	}
	if rows > input.RowCount() {
		rows = input.RowCount()
	}
	return rows
}

func (ctr *container) appendSortedChunk(
	proc *process.Process,
	input *batch.Batch,
	start, end int,
	desc, nullsLast []bool,
	fs []*plan.OrderBySpec,
	analyzer process.Analyzer,
) error {
	window, err := input.Window(start, end)
	if err != nil {
		return err
	}
	chunk, err := window.Dup(proc.Mp())
	window.Clean(proc.Mp())
	if err != nil {
		return err
	}

	orderCols, err := ctr.evaluateOrderColumns(proc, chunk)
	if err != nil {
		freeOrderColumns(proc.Mp(), chunk, orderCols)
		chunk.Clean(proc.Mp())
		return err
	}

	sels := make([]int64, chunk.RowCount())
	for i := range sels {
		sels[i] = int64(i)
	}
	mosort.SortByVectors(sels, orderCols, desc, nullsLast)
	if err = chunk.Shuffle(sels, proc.Mp()); err != nil {
		freeOrderColumns(proc.Mp(), chunk, orderCols)
		chunk.Clean(proc.Mp())
		return err
	}
	for _, vec := range orderCols {
		if vec != nil && !batchContainsVector(chunk, vec) {
			if err = vec.Shuffle(sels, proc.Mp()); err != nil {
				freeOrderColumns(proc.Mp(), chunk, orderCols)
				chunk.Clean(proc.Mp())
				return err
			}
		}
	}

	if ctr.shouldSpill(int64(chunk.Size())) || len(ctr.batchList) >= maxResidentBatches {
		if !ctr.spilling {
			if err = ctr.spillCachedRuns(proc, analyzer); err != nil {
				freeOrderColumns(proc.Mp(), chunk, orderCols)
				chunk.Clean(proc.Mp())
				return err
			}
		}
		err = ctr.spillEvaluatedBatch(proc, chunk, orderCols, analyzer)
		freeOrderColumns(proc.Mp(), chunk, orderCols)
		chunk.Clean(proc.Mp())
		return err
	}

	ctr.batchList = append(ctr.batchList, chunk)
	ctr.orderCols = append(ctr.orderCols, orderCols)
	ctr.spillMemUsage += int64(chunk.Size())
	if ctr.shouldSpill(0) {
		return ctr.spillCachedRuns(proc, analyzer)
	}
	return nil
}

func (ctr *container) collectSortedBatch(
	proc *process.Process,
	fs []*plan.OrderBySpec,
	analyzer process.Analyzer,
) (result *batch.Batch, err error) {
	var output *batch.Batch
	defer func() {
		if err != nil && output != nil {
			output.Clean(proc.Mp())
		}
	}()
	if ctr.spilling {
		if err = ctr.prepareSpillFinalMerge(proc, fs, analyzer); err != nil {
			return nil, err
		}
		return ctr.collectSpillResult(proc)
	}

	if len(ctr.batchList) == 1 {
		result := ctr.batchList[0]
		freeOrderColumns(proc.Mp(), result, ctr.orderCols[0])
		ctr.batchList[0] = nil
		ctr.orderCols[0] = nil
		return result, nil
	}
	if len(ctr.batchList) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("merge-order produced no sorted batch")
	}
	if err = ctr.prepareInMemoryMerge(proc, fs); err != nil {
		return nil, err
	}

	for {
		result := vm.NewCallResult()
		var done bool
		done, err = ctr.pickAndSend(proc, &result)
		if err != nil {
			return nil, err
		}
		if result.Batch != nil && result.Batch.RowCount() > 0 {
			var next *batch.Batch
			next, err = appendSortedResult(proc, output, result.Batch)
			if err != nil {
				return nil, err
			}
			output = next
		}
		if done {
			return output, nil
		}
	}
}

func (ctr *container) collectSpillResult(proc *process.Process) (*batch.Batch, error) {
	var (
		output *batch.Batch
		err    error
	)
	defer func() {
		if err != nil && output != nil {
			output.Clean(proc.Mp())
		}
	}()
	for {
		result := vm.NewCallResult()
		var done bool
		done, err = ctr.sendSpillResult(proc, &result)
		if err != nil {
			return nil, err
		}
		if result.Batch != nil && result.Batch.RowCount() > 0 {
			var next *batch.Batch
			next, err = appendSortedResult(proc, output, result.Batch)
			if err != nil {
				return nil, err
			}
			output = next
		}
		if done {
			return output, nil
		}
	}
}

func appendSortedResult(
	proc *process.Process,
	destination, source *batch.Batch,
) (*batch.Batch, error) {
	if destination == nil {
		return source.Dup(proc.Mp())
	}
	return destination.AppendWithCopy(proc.Ctx, proc.Mp(), source)
}
