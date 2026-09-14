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
	if proc == nil || input == nil || len(fs) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid merge-order sort input")
	}
	if input.RowCount() == 0 {
		return input.Dup(proc.Mp())
	}

	ctr := &container{
		batchList: make([]*batch.Batch, 0, defaultCacheBatchSize),
		orderCols: make([][]*vector.Vector, 0, defaultCacheBatchSize),
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

	var err error
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

	return ctr.collectSortedBatch(proc, fs, analyzer)
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
) (*batch.Batch, error) {
	if ctr.spilling {
		if err := ctr.prepareSpillFinalMerge(proc, fs, analyzer); err != nil {
			return nil, err
		}
		return ctr.collectSpillResult(proc)
	}

	if len(ctr.batchList) == 1 {
		result := ctr.batchList[0]
		ctr.batchList[0] = nil
		ctr.orderCols[0] = nil
		return result, nil
	}
	if len(ctr.batchList) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("merge-order produced no sorted batch")
	}
	if err := ctr.prepareInMemoryMerge(proc, fs); err != nil {
		return nil, err
	}

	var output *batch.Batch
	for {
		result := vm.NewCallResult()
		done, err := ctr.pickAndSend(proc, &result)
		if err != nil {
			return nil, err
		}
		if result.Batch != nil && result.Batch.RowCount() > 0 {
			output, err = appendSortedResult(proc, output, result.Batch)
			if err != nil {
				return nil, err
			}
		}
		if done {
			return output, nil
		}
	}
}

func (ctr *container) collectSpillResult(proc *process.Process) (*batch.Batch, error) {
	var output *batch.Batch
	for {
		result := vm.NewCallResult()
		done, err := ctr.sendSpillResult(proc, &result)
		if err != nil {
			return nil, err
		}
		if result.Batch != nil && result.Batch.RowCount() > 0 {
			output, err = appendSortedResult(proc, output, result.Batch)
			if err != nil {
				return nil, err
			}
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
