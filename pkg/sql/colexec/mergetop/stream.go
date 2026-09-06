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

package mergetop

import (
	"container/heap"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	mergeTopStreamBatchRows  = 8192
	mergeTopStreamBatchBytes = 64 * mpool.MB
)

type orderedStream struct {
	receiver  *process.PipelineSignalReceiver
	bat       *batch.Batch
	orderCols []*vector.Vector
	ownedCols []bool
	row       int64
	ended     bool
}

type streamContainer struct {
	limit         uint64
	emitted       uint64
	limitExecutor colexec.ExpressionExecutor
	executors     []colexec.ExpressionExecutor
	compares      []compare.Compare
	streams       []orderedStream
	heap          streamHeap
	output        *batch.Batch
	primed        bool
	done          bool
}

type streamHeap struct {
	ctr   *streamContainer
	items []int
}

func (h *streamHeap) Len() int { return len(h.items) }
func (h *streamHeap) Less(i, j int) bool {
	left, right := h.items[i], h.items[j]
	r := h.ctr.compareRows(left, right)
	if r == 0 {
		return left < right
	}
	return r < 0
}
func (h *streamHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *streamHeap) Push(x any)    { h.items = append(h.items, x.(int)) }
func (h *streamHeap) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

func (mergeTop *MergeTop) prepareStream(proc *process.Process) (err error) {
	ctr := mergeTop.ctr.stream
	if ctr == nil {
		ctr = &streamContainer{}
		mergeTop.ctr.stream = ctr
	}
	ctr.emitted = 0
	ctr.primed = false
	ctr.done = false
	ctr.heap = streamHeap{ctr: ctr}

	if ctr.limitExecutor == nil {
		ctr.limitExecutor, err = colexec.NewExpressionExecutorWithAllocation(
			proc, mergeTop.Limit, mergeTop.ctr.expressionAllocation)
		if err != nil {
			return err
		}
	}
	vec, err := ctr.limitExecutor.Eval(
		proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	ctr.limit = vector.MustFixedColWithTypeCheck[uint64](vec)[0]

	if len(ctr.executors) != len(mergeTop.Fs) {
		for _, executor := range ctr.executors {
			if executor != nil {
				executor.Free()
			}
		}
		ctr.executors = make([]colexec.ExpressionExecutor, len(mergeTop.Fs))
	}
	for i := range mergeTop.Fs {
		if ctr.executors[i] == nil {
			ctr.executors[i], err = colexec.NewExpressionExecutorWithAllocation(
				proc, mergeTop.Fs[i].Expr, mergeTop.ctr.expressionAllocation)
			if err != nil {
				return err
			}
		}
	}
	ctr.compares = make([]compare.Compare, len(mergeTop.Fs))
	for i, spec := range mergeTop.Fs {
		desc := spec.Flag&plan2.OrderBySpec_DESC != 0
		nullsLast := desc
		if spec.Flag&plan2.OrderBySpec_NULLS_FIRST != 0 {
			nullsLast = false
		} else if spec.Flag&plan2.OrderBySpec_NULLS_LAST != 0 {
			nullsLast = true
		}
		t := spec.Expr.Typ
		typ := types.NewWithCharset(types.T(t.Id), t.Width, t.Scale, uint8(t.Charset))
		ctr.compares[i] = compare.NewOrder(typ, desc, nullsLast)
	}

	ctr.streams = make([]orderedStream, len(proc.Reg.MergeReceivers))
	for i, reg := range proc.Reg.MergeReceivers {
		ctr.streams[i].receiver = process.InitPipelineSignalReceiver(
			proc.Ctx, []*process.WaitRegister{reg})
	}
	return nil
}

func (mergeTop *MergeTop) callStream(
	proc *process.Process,
	analyzer process.Analyzer,
) (vm.CallResult, error) {
	ctr := mergeTop.ctr.stream
	if ctr == nil {
		return vm.CancelResult, nil
	}
	if ctr.output != nil {
		ctr.output.Clean(proc.Mp())
		ctr.output = nil
	}
	if ctr.done || ctr.limit == 0 {
		ctr.done = true
		return vm.CancelResult, nil
	}
	if !ctr.primed {
		for i := range ctr.streams {
			if err := ctr.loadNextBatch(proc, analyzer, i); err != nil {
				return vm.CancelResult, err
			}
			if !ctr.streams[i].ended {
				ctr.heap.items = append(ctr.heap.items, i)
			}
		}
		heap.Init(&ctr.heap)
		ctr.primed = true
	}
	if ctr.heap.Len() == 0 {
		ctr.done = true
		return vm.CancelResult, nil
	}

	first := ctr.streams[ctr.heap.items[0]].bat
	var err error
	ctr.output, err = proc.NewBatchFromSrcWithAllocation(
		first, 0, mergeTop.ctr.retainedAllocation)
	if err != nil {
		return vm.CancelResult, err
	}

	remaining := ctr.limit - ctr.emitted
	maxRows := uint64(mergeTopStreamBatchRows)
	if remaining < maxRows {
		maxRows = remaining
	}
	rows := 0
	for uint64(rows) < maxRows && ctr.heap.Len() > 0 {
		winner := ctr.heap.items[0]
		stream := &ctr.streams[winner]
		if rows > 0 && ctr.output.Size()+stream.rowBytes() > mergeTopStreamBatchBytes {
			break
		}
		chunk := ctr.winnerChunk(winner, int(maxRows)-rows)
		for col := range ctr.output.Vecs {
			if err = ctr.output.Vecs[col].UnionBatch(
				stream.bat.Vecs[col], stream.row, chunk, nil, proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
		}
		rows += chunk
		ctr.emitted += uint64(chunk)
		stream.row += int64(chunk)
		if stream.row >= int64(stream.bat.RowCount()) {
			if err = ctr.loadNextBatch(proc, analyzer, winner); err != nil {
				return vm.CancelResult, err
			}
			if stream.ended {
				heap.Pop(&ctr.heap)
			} else {
				heap.Fix(&ctr.heap, 0)
			}
		} else {
			heap.Fix(&ctr.heap, 0)
		}
		if ctr.output.Size() >= mergeTopStreamBatchBytes {
			break
		}
	}
	ctr.output.SetRowCount(rows)
	if ctr.emitted >= ctr.limit || ctr.heap.Len() == 0 {
		ctr.done = true
	}
	result := vm.NewCallResult()
	result.Batch = ctr.output
	if !ctr.done {
		result.Status = vm.ExecHasMore
	}
	return result, nil
}

func (stream *orderedStream) rowBytes() int {
	return stream.rowBytesAt(stream.row)
}

func (stream *orderedStream) rowBytesAt(row int64) int {
	bytes := 0
	for _, vec := range stream.bat.Vecs {
		typ := vec.GetType()
		bytes += typ.TypeSize()
		if typ.IsVarlen() {
			bytes += len(vec.GetBytesAt(int(row)))
		}
	}
	return bytes
}

// winnerChunk batches contiguous winner rows while preserving both the k-way
// merge boundary and the 64 MiB output budget. Varlen rows use their actual
// payload lengths rather than an average-size estimate.
func (ctr *streamContainer) winnerChunk(winner, maxRows int) int {
	stream := &ctr.streams[winner]
	remaining := stream.bat.RowCount() - int(stream.row)
	if remaining < maxRows {
		maxRows = remaining
	}
	if maxRows <= 1 {
		return 1
	}
	rowBytes := 0
	fixedWidth := true
	for _, vec := range stream.bat.Vecs {
		typ := vec.GetType()
		if typ.IsVarlen() {
			fixedWidth = false
			break
		}
		rowBytes += typ.TypeSize()
	}
	if maxRows > 256 {
		maxRows = 256
	}
	if fixedWidth {
		if rowBytes <= 0 {
			return 1
		}
		budgetRows := (mergeTopStreamBatchBytes - ctr.output.Size()) / rowBytes
		if budgetRows < 1 {
			return 1
		}
		if maxRows > budgetRows {
			maxRows = budgetRows
		}
	} else {
		budget := mergeTopStreamBatchBytes - ctr.output.Size()
		used := 0
		budgetRows := 0
		for budgetRows < maxRows {
			next := stream.rowBytesAt(stream.row + int64(budgetRows))
			if budgetRows > 0 && used+next > budget {
				break
			}
			used += next
			budgetRows++
		}
		if budgetRows < 1 {
			return 1
		}
		maxRows = budgetRows
	}
	if ctr.heap.Len() == 1 {
		return maxRows
	}

	second := ctr.heap.items[1]
	if ctr.heap.Len() > 2 && ctr.compareRows(ctr.heap.items[2], second) < 0 {
		second = ctr.heap.items[2]
	}
	start := stream.row
	chunk := 1
	for chunk < maxRows {
		stream.row = start + int64(chunk)
		if ctr.compareRows(winner, second) > 0 {
			break
		}
		chunk++
	}
	stream.row = start
	return chunk
}

func (ctr *streamContainer) loadNextBatch(
	proc *process.Process,
	analyzer process.Analyzer,
	idx int,
) error {
	stream := &ctr.streams[idx]
	ctr.freeOrderCols(proc, stream)
	for {
		bat, err := stream.receiver.GetNextBatch(analyzer)
		if err != nil {
			return err
		}
		if bat == nil {
			if cancelErr, canceled := vm.CancelCheck(proc); canceled {
				return cancelErr
			}
			stream.bat = nil
			stream.ended = true
			return nil
		}
		if bat.IsEmpty() {
			continue
		}
		stream.bat = bat
		stream.row = 0
		stream.ended = false
		stream.orderCols = make([]*vector.Vector, len(ctr.executors))
		stream.ownedCols = make([]bool, len(ctr.executors))
		for i, executor := range ctr.executors {
			if executor.IsColumnExpr() {
				col := executor.(*colexec.ColumnExpressionExecutor).GetColIndex()
				if col < 0 || col >= len(bat.Vecs) {
					return moerr.NewInternalErrorf(proc.Ctx,
						"merge top ordered stream column %d out of range [0,%d)",
						col, len(bat.Vecs))
				}
				stream.orderCols[i] = bat.Vecs[col]
				continue
			}
			vec, evalErr := executor.EvalWithoutResultReusing(
				proc, []*batch.Batch{bat}, nil)
			if evalErr != nil {
				ctr.freeOrderCols(proc, stream)
				return evalErr
			}
			stream.orderCols[i] = vec
			stream.ownedCols[i] = true
		}
		return nil
	}
}

func (ctr *streamContainer) compareRows(left, right int) int {
	l, r := &ctr.streams[left], &ctr.streams[right]
	for i, cmp := range ctr.compares {
		cmp.Set(0, l.orderCols[i])
		cmp.Set(1, r.orderCols[i])
		if result := cmp.Compare(0, 1, l.row, r.row); result != 0 {
			return result
		}
	}
	return 0
}

func (ctr *streamContainer) freeOrderCols(proc *process.Process, stream *orderedStream) {
	for i, col := range stream.orderCols {
		if i < len(stream.ownedCols) && stream.ownedCols[i] && col != nil {
			col.Free(proc.Mp())
		}
	}
	stream.orderCols = nil
	stream.ownedCols = nil
}

func (ctr *container) resetStream(
	proc *process.Process,
	pipelineFailed bool,
	err error,
) {
	if ctr.stream == nil {
		return
	}
	s := ctr.stream
	cleanupDeadline := time.Now().Add(process.PipelineCleanupWaitTimeout)
	if s.output != nil {
		s.output.Clean(proc.Mp())
		s.output = nil
	}
	for i := range s.streams {
		s.freeOrderCols(proc, &s.streams[i])
		remaining := time.Until(cleanupDeadline)
		if remaining <= 0 {
			remaining = time.Nanosecond
		}
		if receiver := s.streams[i].receiver; receiver != nil &&
			!receiver.WaitingEndWithTimeout(remaining) {
			state := receiver.State()
			process.WarnPipelineCleanupf(
				proc,
				"merge_top_stream_cleanup_wait_end_timeout",
				"merge top stream cleanup timed out: timeout=%s stream=%d alive=%d nil_batch_count=%v channel_len=%v channel_cap=%v pipeline_failed=%t err=%v",
				process.PipelineCleanupWaitTimeout,
				i,
				state.Alive,
				state.NilBatches,
				state.ChannelLen,
				state.ChannelCap,
				pipelineFailed,
				err)
		}
	}
	s.streams = nil
	s.heap.items = nil
	s.primed = false
	s.done = false
	s.emitted = 0
	for i, executor := range s.executors {
		if executor != nil {
			executor.Free()
			s.executors[i] = nil
		}
	}
	s.executors = nil
	if s.limitExecutor != nil {
		s.limitExecutor.Free()
		s.limitExecutor = nil
	}
	s.compares = nil
	ctr.stream = nil
}

func (ctr *container) freeStream(proc *process.Process) {
	ctr.resetStream(proc, false, nil)
}
