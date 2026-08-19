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

package top

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/topsites"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "top"

func (top *Top) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": top([")
	for i, f := range top.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	fmt.Fprintf(buf, "], %v)", top.Limit)
}

func (top *Top) OpType() vm.OpType {
	return vm.Top
}

func growTopSlice[T any](
	values []T,
	length int,
	proc *process.Process,
	allocation *spillutil.SpillAllocationAccount,
	site mpool.AllocationSite,
) ([]T, error) {
	if length < len(values) || proc == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if length <= cap(values) {
		return values[:length], nil
	}
	if allocation != nil {
		return spillutil.GrowAccountedSlice(
			values,
			length,
			proc.Mp(),
			allocation,
			site,
		)
	}
	values = slices.Grow(values, length-len(values))
	return values[:length], nil
}

func (top *Top) Prepare(proc *process.Process) (err error) {
	if top.OpAnalyzer == nil {
		top.OpAnalyzer = process.NewAnalyzer(top.GetIdx(), top.IsFirst, top.IsLast, "top")
	} else {
		top.OpAnalyzer.Reset()
	}

	if top.ctr.allocationAccount != nil {
		top.ctr.budget, err = proc.GetExecutionResourceBudget()
		if err != nil {
			return err
		}
	}

	// limit executor
	if top.ctr.limitExecutor == nil {
		top.ctr.limitExecutor, err = colexec.NewExpressionExecutorWithAllocation(
			proc,
			top.Limit,
			top.ctr.expressionAllocation,
		)
		if err != nil {
			return err
		}
	}
	vec, err := top.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	top.ctr.limit = vector.MustFixedColWithTypeCheck[uint64](vec)[0]

	initialSelections := int(min(top.ctr.limit, uint64(1024)))
	if initialSelections > 0 {
		top.ctr.sels, err = growTopSlice(
			top.ctr.sels,
			initialSelections,
			proc,
			top.ctr.spillAllocation,
			topsites.TopSelections,
		)
		if err != nil {
			return err
		}
		top.ctr.sels = top.ctr.sels[:0]
	}
	top.ctr.poses = make([]int32, 0, len(top.Fs))

	if len(top.ctr.executorsForOrderColumn) != len(top.Fs) {
		top.ctr.executorsForOrderColumn = make([]colexec.ExpressionExecutor, len(top.Fs))
		for i := range top.ctr.executorsForOrderColumn {
			top.ctr.executorsForOrderColumn[i], err = colexec.NewExpressionExecutorWithAllocation(
				proc,
				top.Fs[i].Expr,
				top.ctr.expressionAllocation,
			)
			if err != nil {
				return err
			}
		}
	}

	typ := top.Fs[0].Expr.Typ
	if top.TopValueTag > 0 {
		top.ctr.desc = top.Fs[0].Flag&plan.OrderBySpec_DESC != 0
		top.ctr.topValueZM = objectio.NewZM(types.T(typ.Id), typ.Scale)
	}

	if top.ctr.limit > topSpillThreshold {
		top.ctr.spilling = true
	}

	return nil
}

func (top *Top) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := top.OpAnalyzer

	if top.ctr.limit == 0 {
		result := vm.NewCallResult()
		result.Status = vm.ExecStop
		return result, nil
	}

	if top.ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(top.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			bat := result.Batch

			if bat == nil {
				// The child may cancel the process while returning EOF, after this
				// Top invocation has passed vm.Exec's entry cancellation check.
				if err, canceled := vm.CancelCheck(proc); canceled {
					return vm.CancelResult, err
				}
				top.ctr.state = vm.Eval
				break
			}
			if bat.IsEmpty() {
				continue
			}

			//because ctr.build will change input batch(append new Vector)
			if top.ctr.buildBat == nil {
				top.ctr.n = len(bat.Vecs)
				top.ctr.buildBat = batch.NewWithSize(top.ctr.n)
			} else {
				top.ctr.buildBat.Vecs = top.ctr.buildBat.Vecs[:len(bat.Vecs)]
			}
			top.ctr.buildBat.Recursive = bat.Recursive
			top.ctr.buildBat.ShuffleIDX = bat.ShuffleIDX
			top.ctr.buildBat.Attrs = bat.Attrs
			if len(bat.ExtraBuf) > 0 {
				return result, moerr.NewInternalError(proc.Ctx, "top build should not have extra buffers")
			}
			copy(top.ctr.buildBat.Vecs, bat.Vecs)
			top.ctr.buildBat.SetRowCount(bat.RowCount())

			err = top.ctr.build(top, top.ctr.buildBat, proc, analyzer)
			if err != nil {
				if _, canceled := vm.CancelCheck(proc); canceled {
					return vm.CancelResult, err
				}
				return result, err
			}
			if top.TopValueTag > 0 && top.updateTopValueZM() {
				message.SendMessage(message.TopValueMessage{TopValueZM: top.ctr.topValueZM, Tag: top.TopValueTag}, proc.GetMessageBoard())
			}
		}
	}

	result := vm.NewCallResult()
	if top.ctr.state == vm.Eval {
		if top.ctr.bat == nil && !top.ctr.spillOrdered {
			top.ctr.state = vm.End
			return result, nil
		}
		done, err := top.ctr.eval(top.ctr.limit, top.ctr.n, proc, &result)
		if err != nil {
			return result, err
		}
		if done {
			top.ctr.state = vm.End
		}
		return result, nil
	}

	if top.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}

func (ctr *container) build(ap *Top, bat *batch.Batch, proc *process.Process, analyzer process.Analyzer) error {
	ctr.poses = ctr.poses[:0]
	for i := range ap.Fs {
		vec, err := ctr.executorsForOrderColumn[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		aNewOrderColumn := true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				aNewOrderColumn = false
				ctr.poses = append(ctr.poses, int32(j))
				break
			}
		}
		if aNewOrderColumn {
			ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
			bat.Vecs = append(bat.Vecs, vec)
		}
	}

	if len(ctr.cmps) == 0 {
		mp := make(map[int]int)
		for i, pos := range ctr.poses {
			mp[int(pos)] = i
		}

		if ctr.bat == nil {
			if ctr.spilling {
				ctr.bat = batch.NewOffHeapWithSize(len(ctr.poses))
				for idx, pos := range ctr.poses {
					ctr.bat.Vecs[idx] = vector.NewOffHeapVecWithType(*bat.Vecs[pos].GetType())
				}
				if ctr.retainedAllocation != nil {
					if err := ctr.bat.SetAllocationAccount(ctr.retainedAllocation); err != nil {
						ctr.bat.Clean(proc.Mp())
						ctr.bat = nil
						return err
					}
				}
			} else {
				batNew, vecNew := batch.NewWithSize, vector.NewVec
				if ap.ctr.limit > 10240 || ctr.retainedAllocation != nil {
					batNew, vecNew = batch.NewOffHeapWithSize, vector.NewOffHeapVecWithType
				}
				ctr.bat = batNew(len(bat.Vecs))
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vecNew(*vec.GetType())
				}
				if ctr.retainedAllocation != nil {
					if err := ctr.bat.SetAllocationAccount(ctr.retainedAllocation); err != nil {
						ctr.bat.Clean(proc.Mp())
						ctr.bat = nil
						return err
					}
				}
			}
		}

		if ctr.spilling {
			for idx := range ctr.poses {
				var desc, nullsLast bool
				pos := ctr.poses[idx]
				if posIdx, ok := mp[int(pos)]; ok {
					desc = ap.Fs[posIdx].Flag&plan.OrderBySpec_DESC != 0
					if ap.Fs[posIdx].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
						nullsLast = false
					} else if ap.Fs[posIdx].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
						nullsLast = true
					} else {
						nullsLast = desc
					}
				}
				ctr.cmps = append(
					ctr.cmps,
					compare.NewOrder(*bat.Vecs[pos].GetType(), desc, nullsLast),
				)
			}
		} else {
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
	}

	if ctr.spilling {
		return ctr.processBatchSpill(ap.ctr.limit, bat, proc, analyzer)
	}
	return ctr.processBatch(ap.ctr.limit, bat, proc)
}

func (ctr *container) processBatch(limit uint64, bat *batch.Batch, proc *process.Process) error {
	rowCount := bat.RowCount()
	processCount := rowsToFill(limit, len(ctr.sels), rowCount)

	if processCount > 0 {
		if processCount > math.MaxInt-len(ctr.sels) {
			return moerr.NewInvalidInputNoCtx("top selection count exceeds platform limit")
		}
		baseSel := int64(len(ctr.sels))
		var err error
		ctr.sels, err = growTopSlice(
			ctr.sels,
			len(ctr.sels)+processCount,
			proc,
			ctr.spillAllocation,
			topsites.TopSelections,
		)
		if err != nil {
			return err
		}
		for j, vec := range ctr.bat.Vecs {
			if err := vec.UnionBatch(
				bat.Vecs[j],
				0,
				processCount,
				nil,
				proc.Mp(),
			); err != nil {
				return err
			}
		}
		for i := range processCount {
			ctr.sels[int(baseSel)+i] = baseSel + int64(i)
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

const topSpillWriteBufferSize = 64 << 10

func (ctr *container) ensureSpillWriter(proc *process.Process) error {
	if ctr.spillWriter != nil {
		return nil
	}
	if proc == nil || ctr.spillFile != nil || ctr.spillFDToken != nil ||
		ctr.spillDiskToken != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if ctr.allocationAccount != nil && ctr.budget == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	spillFS, err := proc.GetSpillFileService()
	if err != nil {
		return err
	}

	var fdToken *process.ExecutionSpillFDReservation
	var diskToken *process.ExecutionSpillDiskReservation
	if ctr.budget != nil {
		fdToken, err = ctr.budget.ReserveSpillFD(1)
		if err != nil {
			return err
		}
		diskToken, err = ctr.budget.ReserveSpillDisk(0)
		if err != nil {
			fdToken.Release()
			return err
		}
	}
	file, err := spillFS.CreateAndRemoveFile(
		proc.Ctx,
		fmt.Sprintf("top_%s", uuid.NewString()),
	)
	if err != nil {
		if diskToken != nil {
			diskToken.Release()
		}
		if fdToken != nil {
			fdToken.Release()
		}
		return err
	}
	writer, err := spillutil.NewAccountedWriter(
		proc.Ctx,
		proc.Mp(),
		ctr.allocationAccount,
		mpool.AllocationOwnerTop,
		topsites.TopSpillWriteBuffer,
		spillutil.NewDiskReservationWriter(file, diskToken),
		topSpillWriteBufferSize,
	)
	if err != nil {
		_ = file.Close()
		if diskToken != nil {
			diskToken.Release()
		}
		if fdToken != nil {
			fdToken.Release()
		}
		return err
	}
	ctr.spillFile = file
	ctr.spillWriter = writer
	ctr.spillFDToken = fdToken
	ctr.spillDiskToken = diskToken
	return nil
}

func (ctr *container) flushSpillWriter() error {
	if ctr.spillWriter == nil {
		return nil
	}
	writer := ctr.spillWriter
	ctr.spillWriter = nil
	if err := writer.Flush(); err != nil {
		writer.Free()
		return err
	}
	writer.Free()
	return nil
}

func (ctr *container) spillBatch(
	bat *batch.Batch,
	proc *process.Process,
	analyzer process.Analyzer,
) (spillRecordRef, error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return spillRecordRef{}, err
	}
	if bat == nil || ctr.n < 0 || ctr.n > len(bat.Vecs) {
		return spillRecordRef{}, moerr.NewInvalidInputNoCtx("invalid top spill batch")
	}
	if len(bat.ExtraBuf) != 0 {
		return spillRecordRef{}, moerr.NewInvalidInputNoCtx("top spill batch has extra buffers")
	}

	// Serialize only original columns. The appended order expressions are
	// retained in the bounded key batch and never duplicated on disk.
	var origBat batch.Batch
	origBat.Vecs = bat.Vecs[:ctr.n]
	origBat.Recursive = bat.Recursive
	origBat.ShuffleIDX = bat.ShuffleIDX
	origBat.SetRowCount(bat.RowCount())
	payloadSize, err := origBat.MarshalBinaryWithPrepareParamKindsSize()
	if err != nil {
		return spillRecordRef{}, err
	}
	if payloadSize <= 0 || uint64(payloadSize) > math.MaxInt64 ||
		ctr.spillOffset > math.MaxInt64-int64(payloadSize) {
		return spillRecordRef{}, moerr.NewInvalidInputNoCtx("top spill payload exceeds format")
	}
	if err = ctr.ensureSpillWriter(proc); err != nil {
		return spillRecordRef{}, err
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		return spillRecordRef{}, err
	}
	offset := ctr.spillOffset
	if err = origBat.MarshalBinaryWithPrepareParamKindsTo(ctr.spillWriter); err != nil {
		return spillRecordRef{}, err
	}
	ctr.spillOffset += int64(payloadSize)
	if err, canceled := vm.CancelCheck(proc); canceled {
		return spillRecordRef{}, err
	}
	if analyzer != nil {
		analyzer.Spill(int64(payloadSize))
		analyzer.SpillRows(int64(bat.RowCount()))
	}
	return spillRecordRef{offset: offset, size: int64(payloadSize)}, nil
}

func (ctr *container) processBatchSpill(limit uint64, bat *batch.Batch, proc *process.Process, analyzer process.Analyzer) error {
	record, err := ctr.spillBatch(bat, proc, analyzer)
	if err != nil {
		return err
	}

	rowCount := bat.RowCount()
	processCount := rowsToFill(limit, len(ctr.sels), rowCount)

	if processCount > 0 {
		if processCount > math.MaxInt-len(ctr.sels) {
			return moerr.NewInvalidInputNoCtx("top selection count exceeds platform limit")
		}
		baseSel := len(ctr.sels)
		newLength := baseSel + processCount
		ctr.sels, err = growTopSlice(
			ctr.sels,
			newLength,
			proc,
			ctr.spillAllocation,
			topsites.TopSelections,
		)
		if err != nil {
			return err
		}
		ctr.rowRefs, err = growTopSlice(
			ctr.rowRefs,
			newLength,
			proc,
			ctr.spillAllocation,
			topsites.TopRowReferences,
		)
		if err != nil {
			return err
		}
		for idx, pos := range ctr.poses {
			if err := ctr.bat.Vecs[idx].UnionBatch(
				bat.Vecs[pos],
				0,
				processCount,
				nil,
				proc.Mp(),
			); err != nil {
				return err
			}
		}
		for i := range processCount {
			position := baseSel + i
			ctr.sels[position] = int64(position)
			ctr.rowRefs[position] = rowRef{
				offset: record.offset,
				size:   record.size,
				rowIdx: int64(i),
			}
		}
		ctr.bat.AddRowCount(processCount)

		if uint64(len(ctr.sels)) == limit {
			ctr.sort()
		}
	}

	if processCount == rowCount {
		return nil
	}

	// heap is full, compare and replace
	for idx, pos := range ctr.poses {
		ctr.cmps[idx].Set(1, bat.Vecs[pos])
	}
	for i, j := processCount, rowCount; i < j; i++ {
		rowIdx := int64(i)
		if ctr.compare(1, 0, rowIdx, ctr.sels[0]) < 0 {
			for idx := range ctr.cmps {
				if err := ctr.cmps[idx].Copy(1, 0, rowIdx, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			ctr.rowRefs[ctr.sels[0]] = rowRef{
				offset: record.offset,
				size:   record.size,
				rowIdx: int64(i),
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

func (ctr *container) eval(limit uint64, n int, proc *process.Process, result *vm.CallResult) (bool, error) {
	if ctr.spilling {
		return ctr.evalSpill(limit, n, proc, result)
	}
	err := ctr.evalInMemory(limit, n, proc, result)
	return true, err
}

func (ctr *container) evalInMemory(limit uint64, n int, proc *process.Process, result *vm.CallResult) error {
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
	ctr.releaseSelectionState(proc)
	for i := n; i < len(ctr.bat.Vecs); i++ {
		ctr.bat.Vecs[i].Free(proc.Mp())
	}
	ctr.bat.Vecs = ctr.bat.Vecs[:n]
	result.Batch = ctr.bat
	return nil
}

const evalSpillChunkSize = 8192

func (ctr *container) evalSpill(limit uint64, n int, proc *process.Process, result *vm.CallResult) (bool, error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return false, err
	}
	if err := ctr.flushSpillWriter(); err != nil {
		return false, err
	}

	// First call: flush all published records, then reuse the heap's own
	// backing as the final ascending selection order. heap.Pop stores each
	// removed root in the vacated suffix, so no second data-scaled slice is
	// needed for ordered references.
	if !ctr.spillOrdered {
		if uint64(len(ctr.sels)) < limit {
			ctr.sort()
		}
		ordered := ctr.sels[:len(ctr.sels)]
		for i := range len(ordered) {
			if i%evalSpillChunkSize == 0 {
				if err, canceled := vm.CancelCheck(proc); canceled {
					return false, err
				}
			}
			heap.Pop(ctr)
		}
		ctr.sels = ordered
		ctr.spillOrdered = true
		ctr.evalCursor = 0
		if ctr.bat != nil {
			ctr.bat.Clean(proc.Mp())
			ctr.bat = nil
		}
	}

	// Free previous chunk's output batch.
	if ctr.spillOutBat != nil {
		ctr.spillOutBat.Clean(proc.Mp())
		ctr.spillOutBat = nil
	}

	if ctr.evalCursor >= len(ctr.sels) {
		ctr.releaseSelectionState(proc)
		return true, ctr.closeSpillFile()
	}
	if ctr.spillFile == nil {
		return false, moerr.NewInternalErrorNoCtx("top spill file is unavailable")
	}

	chunkStart := ctr.evalCursor
	chunkEnd := min(chunkStart+evalSpillChunkSize, len(ctr.sels))
	chunkSize := chunkEnd - chunkStart

	type batchRow struct {
		chunkPos int
		rowIdx   int64
	}
	// The map and its value slices are bounded by evalSpillChunkSize,
	// independently of total input batch count.
	batchRows := make(map[spillRecordRef][]batchRow)
	for i, selection := range ctr.sels[chunkStart:chunkEnd] {
		if selection < 0 || selection >= int64(len(ctr.rowRefs)) {
			return false, moerr.NewInternalErrorNoCtx("invalid top spill row reference")
		}
		ref := ctr.rowRefs[selection]
		if ref.offset < 0 || ref.size <= 0 || ref.offset > ctr.spillOffset-ref.size {
			return false, moerr.NewInternalErrorNoCtx("invalid top spill record reference")
		}
		record := spillRecordRef{offset: ref.offset, size: ref.size}
		batchRows[record] = append(batchRows[record], batchRow{
			chunkPos: i,
			rowIdx:   ref.rowIdx,
		})
	}

	outputBat := batch.NewOffHeapWithSize(n)
	outputTransferred := false
	defer func() {
		if !outputTransferred {
			outputBat.Clean(proc.Mp())
		}
	}()

	reuseBat := batch.NewOffHeapWithSize(0)
	if ctr.spillAllocation != nil {
		if err := ctr.spillAllocation.ConfigureDecodedBatch(reuseBat); err != nil {
			return false, err
		}
	}
	defer reuseBat.Clean(proc.Mp())

	outputInitialized := false
	for record, rows := range batchRows {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return false, err
		}
		reuseBat.CleanOnlyData()
		reader := io.NewSectionReader(ctr.spillFile, record.offset, record.size)
		if err := reuseBat.UnmarshalFromReaderWithPrepareParamKindsForSpill(
			reader,
			record.size,
			proc.Mp(),
		); err != nil {
			return false, err
		}
		if err := reuseBat.CheckLength(); err != nil {
			return false, moerr.NewInvalidInputNoCtx(
				"top spill vector length does not match row count",
			)
		}
		if len(reuseBat.Vecs) != n {
			return false, moerr.NewInternalErrorNoCtx("top spill column count mismatch")
		}

		if !outputInitialized {
			for i := 0; i < n; i++ {
				if ctr.outputAllocation == nil {
					outputBat.Vecs[i] = vector.NewOffHeapVecWithType(*reuseBat.Vecs[i].GetType())
				} else {
					var err error
					outputBat.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(
						*reuseBat.Vecs[i].GetType(),
						ctr.outputAllocation,
					)
					if err != nil {
						return false, err
					}
				}
				if err := outputBat.Vecs[i].PreExtend(chunkSize, proc.Mp()); err != nil {
					return false, err
				}
				if err := outputBat.Vecs[i].PreExtendBitmap(chunkSize, proc.Mp()); err != nil {
					return false, err
				}
				outputBat.Vecs[i].SetLength(chunkSize)
				// The output is capacity-reserved but no logical row has been
				// written yet. Marking the slots null makes Copy's row-level
				// provenance decision independent of the reserved length.
				outputBat.Vecs[i].SetAllNulls(chunkSize)
			}
			outputInitialized = true
		}

		for _, r := range rows {
			if r.rowIdx < 0 || r.rowIdx >= int64(reuseBat.RowCount()) {
				return false, moerr.NewInternalErrorNoCtx("top spill row index out of range")
			}
			for col := 0; col < n; col++ {
				if err := outputBat.Vecs[col].Copy(reuseBat.Vecs[col], int64(r.chunkPos), r.rowIdx, proc.Mp()); err != nil {
					return false, err
				}
			}
		}
	}

	if err, canceled := vm.CancelCheck(proc); canceled {
		return false, err
	}
	outputBat.SetRowCount(chunkSize)
	ctr.evalCursor = chunkEnd
	done := ctr.evalCursor >= len(ctr.sels)
	if done {
		ctr.releaseSelectionState(proc)
		if err := ctr.closeSpillFile(); err != nil {
			return false, err
		}
	}
	ctr.spillOutBat = outputBat
	outputTransferred = true
	result.Batch = outputBat
	return done, nil
}

func (ctr *container) releaseSelectionState(proc *process.Process) {
	if ctr.allocationAccount != nil && proc != nil {
		spillutil.FreeAccountedSlice(ctr.sels, proc.Mp())
		spillutil.FreeAccountedSlice(ctr.rowRefs, proc.Mp())
	}
	ctr.sels = nil
	ctr.rowRefs = nil
}

// do sort work for heap, and result order will be set in container.sels
func (ctr *container) sort() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}

func (top *Top) updateTopValueZM() bool {
	v, ok := top.getTopValue()
	if !ok {
		return false
	}
	zm := top.ctr.topValueZM
	if !zm.IsInited() {
		index.UpdateZM(zm, v)
		return true
	}
	newZM := objectio.NewZM(zm.GetType(), zm.GetScale())
	index.UpdateZM(newZM, v)
	if top.ctr.desc && newZM.CompareMax(zm) > 0 {
		top.ctr.topValueZM = newZM
		return true
	}
	if !top.ctr.desc && newZM.CompareMin(zm) < 0 {
		top.ctr.topValueZM = newZM
		return true
	}
	return false
}

func (top *Top) getTopValue() ([]byte, bool) {
	// not enough items in the heap.
	if uint64(len(top.ctr.sels)) < top.ctr.limit {
		return nil, false
	}
	x := int(top.ctr.sels[0])
	ctr := &top.ctr
	var vec *vector.Vector
	if ctr.spilling {
		vec = ctr.cmps[0].Vector()
	} else {
		vec = ctr.cmps[ctr.poses[0]].Vector()
	}
	if vec.GetType().IsVarlen() {
		return vec.GetBytesAt(x), true
	}
	switch vec.GetType().Oid {
	case types.T_int8:
		v := vector.GetFixedAtNoTypeCheck[int8](vec, x)
		return types.EncodeInt8(&v), true
	case types.T_int16:
		v := vector.GetFixedAtNoTypeCheck[int16](vec, x)
		return types.EncodeInt16(&v), true
	case types.T_int32:
		v := vector.GetFixedAtNoTypeCheck[int32](vec, x)
		return types.EncodeInt32(&v), true
	case types.T_int64:
		v := vector.GetFixedAtNoTypeCheck[int64](vec, x)
		return types.EncodeInt64(&v), true
	case types.T_uint8:
		v := vector.GetFixedAtNoTypeCheck[uint8](vec, x)
		return types.EncodeUint8(&v), true
	case types.T_uint16:
		v := vector.GetFixedAtNoTypeCheck[uint16](vec, x)
		return types.EncodeUint16(&v), true
	case types.T_uint32:
		v := vector.GetFixedAtNoTypeCheck[uint32](vec, x)
		return types.EncodeUint32(&v), true
	case types.T_uint64:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, x)
		return types.EncodeUint64(&v), true
	case types.T_float32:
		v := vector.GetFixedAtNoTypeCheck[float32](vec, x)
		return types.EncodeFloat32(&v), true
	case types.T_float64:
		v := vector.GetFixedAtNoTypeCheck[float64](vec, x)
		return types.EncodeFloat64(&v), true
	case types.T_date:
		v := vector.GetFixedAtNoTypeCheck[types.Date](vec, x)
		return types.EncodeDate(&v), true
	case types.T_datetime:
		v := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, x)
		return types.EncodeDatetime(&v), true
	case types.T_timestamp:
		v := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, x)
		return types.EncodeTimestamp(&v), true
	case types.T_time:
		v := vector.GetFixedAtNoTypeCheck[types.Time](vec, x)
		return types.EncodeTime(&v), true
	case types.T_decimal64:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, x)
		return types.EncodeDecimal64(&v), true
	case types.T_decimal128:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, x)
		return types.EncodeDecimal128(&v), true
	case types.T_enum:
		v := vector.GetFixedAtNoTypeCheck[types.Enum](vec, x)
		return types.EncodeEnum(&v), true
	case types.T_year:
		v := vector.GetFixedAtNoTypeCheck[types.MoYear](vec, x)
		return types.EncodeMoYear(&v), true
	}
	return nil, false
}
