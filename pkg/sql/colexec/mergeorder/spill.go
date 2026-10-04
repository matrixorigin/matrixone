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
	"container/heap"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/ordersites"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const spillWriteBufferSize = 64 << 10

func newMergeOrderSpillWriter(
	proc *process.Process,
	ctr *container,
	run *spillRun,
) (*spillutil.AccountedWriter, error) {
	if proc == nil || ctr == nil || run == nil || run.file == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return spillutil.NewAccountedWriter(
		proc.Ctx,
		proc.Mp(),
		ctr.allocationAccount,
		mpool.AllocationOwnerOrder,
		ordersites.MergeOrderSpillWriteBuffer,
		spillutil.NewDiskReservationWriter(run.file, run.diskToken),
		spillWriteBufferSize,
	)
}

func (ctr *container) getSpillFS(proc *process.Process) (fileservice.MutableFileService, error) {
	if ctr.spillFS != nil {
		return ctr.spillFS, nil
	}
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	ctr.spillFS = fs
	return fs, nil
}

func (ctr *container) shouldSpill(incomingBatchSize int64) bool {
	if ctr.spilling {
		return true
	}
	if ctr.spillThreshold <= 0 {
		return false
	}
	return ctr.spillMemUsage > ctr.spillThreshold-incomingBatchSize
}

func (ctr *container) evaluateOrderColumns(proc *process.Process, bat *batch.Batch) ([]*vector.Vector, error) {
	var inputs [1]*batch.Batch
	inputs[0] = bat
	cols := make([]*vector.Vector, len(ctr.executors))
	for i := 0; i < len(ctr.executors); i++ {
		if i < len(ctr.spillColPos) && ctr.spillColPos[i] >= 0 {
			colPos := int(ctr.spillColPos[i])
			if colPos >= len(bat.Vecs) {
				freeOrderColumns(proc.Mp(), bat, cols)
				return nil, moerr.NewInternalErrorf(
					proc.Ctx,
					"merge-order column index out of range: %d",
					colPos,
				)
			}
			cols[i] = bat.Vecs[colPos]
			continue
		}
		if ctr.executors[i] == nil {
			freeOrderColumns(proc.Mp(), bat, cols)
			return nil, moerr.NewInternalError(
				proc.Ctx,
				"merge-order expression executor is nil",
			)
		}
		vec, err := ctr.executors[i].EvalWithoutResultReusing(proc, inputs[:], nil)
		if err != nil {
			freeOrderColumns(proc.Mp(), bat, cols)
			return nil, err
		}
		cols[i] = vec
	}
	return cols, nil
}

func (ctr *container) spillKeyColumnsFromOrderColumns(
	orderCols []*vector.Vector,
) []*vector.Vector {
	cols := ctr.spillKeyCols[:0]
	if cap(cols) < len(ctr.spillKeyIndexes) {
		cols = make([]*vector.Vector, 0, len(ctr.spillKeyIndexes))
	}
	for _, idx := range ctr.spillKeyIndexes {
		cols = append(cols, orderCols[idx])
	}
	ctr.spillKeyCols = cols
	return cols
}

func (ctr *container) remapRetainedOrderColumns(
	retained *batch.Batch,
	orderCols []*vector.Vector,
) {
	for i, colPos := range ctr.spillColPos {
		if colPos >= 0 {
			orderCols[i] = retained.Vecs[colPos]
		}
	}
}

func writeSpillInt64(writer io.Writer, value int64) error {
	data := types.EncodeInt64(&value)
	written, err := writer.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

func writeSpillUint64(writer io.Writer, value uint64) error {
	data := types.EncodeUint64(&value)
	written, err := writer.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

func appendSpillPayload(writer io.Writer, bat *batch.Batch) (int64, error) {
	size, err := bat.MarshalBinaryWithPrepareParamKindsSize()
	if err != nil {
		return 0, err
	}
	if err = writeSpillInt64(writer, int64(size)); err != nil {
		return 0, err
	}
	if err = bat.MarshalBinaryWithPrepareParamKindsTo(writer); err != nil {
		return 0, err
	}
	return int64(size) + 8, nil
}

func makeSpillOrderBatch(orderCols []*vector.Vector, rowCount int) batch.Batch {
	var keyBatch batch.Batch
	keyBatch.Vecs = orderCols
	keyBatch.SetRowCount(rowCount)
	return keyBatch
}

func writeSpillBatch(proc *process.Process, bat *batch.Batch, keyCols []*vector.Vector, writer io.Writer, analyzer process.Analyzer) (int64, int64, error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return 0, 0, err
	}

	cnt := int64(bat.RowCount())
	if err := writeSpillInt64(writer, cnt); err != nil {
		return 0, 0, err
	}
	written := int64(8)
	payloadWritten, err := appendSpillPayload(writer, bat)
	if err != nil {
		return 0, 0, err
	}
	written += payloadWritten
	keyCount := int64(len(keyCols))
	if err = writeSpillInt64(writer, keyCount); err != nil {
		return 0, 0, err
	}
	written += 8
	if keyCount > 0 {
		keyBatch := makeSpillOrderBatch(keyCols, bat.RowCount())
		payloadWritten, err = appendSpillPayload(writer, &keyBatch)
		if err != nil {
			return 0, 0, err
		}
		written += payloadWritten
	}
	if err = writeSpillUint64(writer, uint64(spillMagic)); err != nil {
		return 0, 0, err
	}
	written += 8
	if err, canceled := vm.CancelCheck(proc); canceled {
		return 0, 0, err
	}
	if analyzer != nil {
		analyzer.Spill(written)
		analyzer.SpillRows(cnt)
	}
	return cnt, written, nil
}

func readSpillPayload(proc *process.Process, reader io.Reader, reuseBat *batch.Batch) (*batch.Batch, error) {
	var header [8]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	batchSize := types.DecodeInt64(header[:])
	if batchSize < 0 {
		return nil, moerr.NewInternalError(proc.Ctx, "negative merge-order spill payload size")
	}
	reuseBat.CleanOnlyData()
	if err := reuseBat.UnmarshalFromReaderWithPrepareParamKinds(reader, batchSize, proc.Mp()); err != nil {
		return nil, err
	}
	return reuseBat, nil
}

func readSpillBatches(proc *process.Process, reader io.Reader, reuseBat, reuseKeyBat *batch.Batch) (*batch.Batch, *batch.Batch, error) {
	var header [8]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		if err == io.EOF {
			return nil, nil, io.EOF
		}
		return nil, nil, err
	}
	cnt := types.DecodeInt64(header[:])
	if cnt < 0 {
		return nil, nil, moerr.NewInternalError(
			proc.Ctx,
			"negative merge-order spill row count",
		)
	}
	bat, err := readSpillPayload(proc, reader, reuseBat)
	if err != nil {
		return nil, nil, err
	}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, nil, err
	}
	keyCount := types.DecodeInt64(header[:])
	if keyCount < 0 {
		return nil, nil, moerr.NewInternalError(
			proc.Ctx,
			"negative merge-order spill key count",
		)
	}
	keyBat := reuseKeyBat
	if keyCount > 0 {
		keyBat, err = readSpillPayload(proc, reader, reuseKeyBat)
		if err != nil {
			return nil, nil, err
		}
		if int64(keyBat.VectorCount()) != keyCount {
			return nil, nil, moerr.NewInternalError(
				proc.Ctx,
				"merge-order spill key count mismatch",
			)
		}
	} else if keyBat != nil && keyBat.VectorCount() != 0 {
		return nil, nil, moerr.NewInternalError(
			proc.Ctx,
			"merge-order spill key count mismatch",
		)
	}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, nil, err
	}
	if types.DecodeUint64(header[:]) != spillMagic {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "corrupted merge-order spill file")
	}
	if bat.RowCount() != int(cnt) ||
		(keyCount > 0 && keyBat.RowCount() != int(cnt)) {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "merge-order spill row count mismatch")
	}
	return bat, keyBat, nil
}

func (ctr *container) createSpillRun(proc *process.Process) (*spillRun, error) {
	spillfs, err := ctr.getSpillFS(proc)
	if err != nil {
		return nil, err
	}
	var fdToken *process.ExecutionSpillFDReservation
	var diskToken *process.ExecutionSpillDiskReservation
	if ctr.budget != nil {
		fdToken, err = ctr.budget.ReserveSpillFD(1)
		if err != nil {
			return nil, err
		}
		diskToken, err = ctr.budget.ReserveSpillDisk(0)
		if err != nil {
			fdToken.Release()
			return nil, err
		}
	}
	file, err := spillfs.CreateAndRemoveFile(proc.Ctx, fmt.Sprintf("mergeorder_%s", uuid.NewString()))
	if err != nil {
		if diskToken != nil {
			diskToken.Release()
		}
		if fdToken != nil {
			fdToken.Release()
		}
		return nil, err
	}
	return &spillRun{file: file, fdToken: fdToken, diskToken: diskToken}, nil
}

func (ctr *container) ensureSpillTailColumns() {
	if cap(ctr.spillTailCols) < len(ctr.executors) {
		ctr.spillTailCols = make([]*vector.Vector, len(ctr.executors))
		return
	}
	ctr.spillTailCols = ctr.spillTailCols[:len(ctr.executors)]
}

func (ctr *container) clearSpillTailColumns(mp *mpool.MPool) {
	for i := range ctr.spillTailCols {
		if ctr.spillTailCols[i] != nil {
			ctr.spillTailCols[i].Free(mp)
			ctr.spillTailCols[i] = nil
		}
	}
	ctr.spillTailCols = nil
	ctr.spillTailReady = false
}

func (ctr *container) canAppendToActiveRun(incomingOrderCols []*vector.Vector) bool {
	if !ctr.spillTailReady {
		return true
	}
	for k := 0; k < len(ctr.compares); k++ {
		ctr.compares[k].Set(0, ctr.spillTailCols[k])
		ctr.compares[k].Set(1, incomingOrderCols[k])
		if r := ctr.compares[k].Compare(0, 1, 0, 0); r != 0 {
			return r < 0
		}
	}
	return true
}

func (ctr *container) updateActiveRunTail(proc *process.Process, incomingOrderCols []*vector.Vector, rowIdx int64) error {
	ctr.ensureSpillTailColumns()
	for i := range incomingOrderCols {
		if ctr.spillTailCols[i] == nil {
			if ctr.outputAllocation == nil {
				ctr.spillTailCols[i] = vector.NewOffHeapVecWithType(*incomingOrderCols[i].GetType())
			} else {
				var err error
				ctr.spillTailCols[i], err = vector.NewOffHeapVecWithTypeAndAllocation(
					*incomingOrderCols[i].GetType(),
					ctr.outputAllocation,
				)
				if err != nil {
					return err
				}
			}
		} else {
			ctr.spillTailCols[i].CleanOnlyData()
		}
		if err := ctr.spillTailCols[i].UnionOne(incomingOrderCols[i], rowIdx, proc.Mp()); err != nil {
			return err
		}
	}
	ctr.spillTailReady = true
	return nil
}

func computeDrainChunk(src *spillRunReader, currentSize int) int {
	remaining := src.batch.RowCount() - int(src.rowIdx)
	if remaining <= 0 {
		return 0
	}
	budget := maxBatchSizeToSend - currentSize
	if budget <= 0 {
		return 0
	}

	if src.fixedWidth && src.rowBytes > 0 {
		maxByBudget := budget / src.rowBytes
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

	avgRowBytes := src.avgRowBytes
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

func appendContiguousRows(dst *batch.Batch, src *batch.Batch, start int64, cnt int, proc *process.Process) error {
	for col := range dst.Vecs {
		if err := dst.Vecs[col].UnionBatch(src.Vecs[col], start, cnt, nil, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func appendContiguousOrderRows(dstOrder *batch.Batch, srcOrderCols []*vector.Vector, keyIdxes []int, start int64, cnt int, proc *process.Process) error {
	for col, keyIdx := range keyIdxes {
		if err := dstOrder.Vecs[col].UnionBatch(srcOrderCols[keyIdx], start, cnt, nil, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) ensureSpillRunSlot(
	proc *process.Process,
	analyzer process.Analyzer,
) error {
	needCompact := len(ctr.spillRuns) >= spillMergeFanIn-1
	if ctr.budget != nil && ctr.budget.SpillFDCap() > 0 {
		used := ctr.budget.SpillFDUsed()
		cap := ctr.budget.SpillFDCap()
		needCompact = needCompact || used >= cap-1
	}
	if !needCompact || len(ctr.spillRuns) < 2 {
		return nil
	}
	merged, err := ctr.mergeRunsToSpill(proc, ctr.spillRuns, analyzer)
	if err != nil {
		return err
	}
	clear(ctr.spillRuns)
	ctr.spillRuns = append(ctr.spillRuns[:0], merged)
	return nil
}

func (ctr *container) ensureActiveSpillRun(
	proc *process.Process,
	analyzer process.Analyzer,
) error {
	if ctr.spillActiveRun != nil {
		return nil
	}
	if err := ctr.ensureSpillRunSlot(proc, analyzer); err != nil {
		return err
	}
	run, err := ctr.createSpillRun(proc)
	if err != nil {
		return err
	}
	writer, err := newMergeOrderSpillWriter(proc, ctr, run)
	if err != nil {
		run.close()
		return err
	}
	ctr.spillActiveRun = run
	ctr.spillActiveWriter = writer
	ctr.spillActiveBytes = 0
	ctr.spillTailReady = false
	return nil
}

func (ctr *container) finalizeActiveSpillRun(proc *process.Process, keepRun bool) error {
	if ctr.spillActiveRun == nil {
		return nil
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	run := ctr.spillActiveRun
	writer := ctr.spillActiveWriter
	ctr.spillActiveRun = nil
	ctr.spillActiveWriter = nil
	ctr.spillActiveBytes = 0
	ctr.spillTailReady = false

	if writer != nil {
		if err := writer.Flush(); err != nil {
			writer.Free()
			run.close()
			return err
		}
		writer.Free()
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		run.close()
		return err
	}
	if _, err := run.file.Seek(0, io.SeekStart); err != nil {
		run.close()
		return err
	}
	if keepRun {
		ctr.spillRuns = append(ctr.spillRuns, run)
		return nil
	}
	run.close()
	return nil
}

func (ctr *container) spillBatchToNewRun(
	proc *process.Process,
	bat *batch.Batch,
	keyCols []*vector.Vector,
	analyzer process.Analyzer,
) (*spillRun, error) {
	if err := ctr.ensureSpillRunSlot(proc, analyzer); err != nil {
		return nil, err
	}
	run, err := ctr.createSpillRun(proc)
	if err != nil {
		return nil, err
	}
	writer, err := newMergeOrderSpillWriter(proc, ctr, run)
	if err != nil {
		run.close()
		return nil, err
	}
	defer writer.Free()
	rows, _, err := writeSpillBatch(proc, bat, keyCols, writer, analyzer)
	if err == nil {
		err = writer.Flush()
	}
	if err == nil {
		_, err = run.file.Seek(0, io.SeekStart)
	}
	if err != nil {
		run.close()
		return nil, err
	}
	run.batchCount = 1
	run.rowCount = rows
	return run, nil
}

func (ctr *container) spillEvaluatedBatch(
	proc *process.Process,
	bat *batch.Batch,
	orderCols []*vector.Vector,
	analyzer process.Analyzer,
) error {
	keyCols := ctr.spillKeyColumnsFromOrderColumns(orderCols)
	if ctr.spillAppendEnabled {
		return ctr.spillBatchWithAppend(
			proc,
			bat,
			keyCols,
			orderCols,
			analyzer,
		)
	}
	run, err := ctr.spillBatchToNewRun(proc, bat, keyCols, analyzer)
	if err != nil {
		return err
	}
	ctr.spillRuns = append(ctr.spillRuns, run)
	return nil
}

func (ctr *container) spillBatchWithAppend(
	proc *process.Process,
	bat *batch.Batch,
	keyCols []*vector.Vector,
	incomingOrderCols []*vector.Vector,
	analyzer process.Analyzer,
) error {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}

	if ctr.spillActiveRun != nil && ((ctr.spillAppendTarget > 0 && ctr.spillActiveBytes >= ctr.spillAppendTarget) || !ctr.canAppendToActiveRun(incomingOrderCols)) {
		if err := ctr.finalizeActiveSpillRun(proc, true); err != nil {
			return err
		}
	}
	if err := ctr.ensureActiveSpillRun(proc, analyzer); err != nil {
		return err
	}
	rows, written, err := writeSpillBatch(proc, bat, keyCols, ctr.spillActiveWriter, analyzer)
	if err != nil {
		return err
	}
	ctr.spillActiveRun.batchCount++
	ctr.spillActiveRun.rowCount += rows
	ctr.spillActiveBytes += written
	if err := ctr.updateActiveRunTail(
		proc,
		incomingOrderCols,
		int64(bat.RowCount()-1),
	); err != nil {
		if !mpool.IsRetryableAllocationCapacity(err) {
			return err
		}
		// Tail state is only an append/coalescing optimization. The record is
		// already durably owned by the active run; disable append and commit that
		// run instead of turning optional tail capacity into a query failure.
		ctr.clearSpillTailColumns(proc.Mp())
		ctr.spillAppendEnabled = false
		ctr.spillAppendTarget = 0
		return ctr.finalizeActiveSpillRun(proc, true)
	}
	return nil
}

func (ctr *container) spillCachedRuns(proc *process.Process, analyzer process.Analyzer) error {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}

	for i := range ctr.batchList {
		if ctr.batchList[i] == nil {
			continue
		}
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		bat := ctr.batchList[i]
		orderCols := ctr.orderCols[i]
		if orderCols == nil {
			var err error
			orderCols, err = ctr.evaluateOrderColumns(proc, bat)
			if err != nil {
				return err
			}
			ctr.orderCols[i] = orderCols
		}
		keyCols := ctr.spillKeyColumnsFromOrderColumns(orderCols)
		if !ctr.spillAppendEnabled {
			run, err := ctr.spillBatchToNewRun(proc, bat, keyCols, analyzer)
			if err != nil {
				return err
			}
			ctr.spillRuns = append(ctr.spillRuns, run)
			freeOrderColumns(proc.Mp(), bat, orderCols)
			ctr.batchList[i].Clean(proc.Mp())
			ctr.batchList[i] = nil
			ctr.orderCols[i] = nil
			continue
		}
		if err := ctr.spillBatchWithAppend(proc, bat, keyCols, orderCols, analyzer); err != nil {
			return err
		}
		freeOrderColumns(proc.Mp(), bat, orderCols)
		ctr.batchList[i].Clean(proc.Mp())
		ctr.batchList[i] = nil
		ctr.orderCols[i] = nil
	}
	ctr.batchList = ctr.batchList[:0]
	ctr.orderCols = ctr.orderCols[:0]
	ctr.spilling = true
	ctr.spillMemUsage = 0
	return nil
}

func (ctr *container) compareSpillReaders(i, j int) int {
	left := ctr.spillReaders[i]
	right := ctr.spillReaders[j]
	return ctr.compareSpillReaderRows(left, left.rowIdx, right, right.rowIdx)
}

func (ctr *container) compareSpillReaderRows(left *spillRunReader, leftRow int64, right *spillRunReader, rightRow int64) int {
	leftCols := left.orderCols
	rightCols := right.orderCols
	compares := ctr.compares
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

func (ctr *container) computeWinnerChunk(root *spillRunReader, second *spillRunReader, budgetChunk int) int {
	if budgetChunk <= 1 {
		return budgetChunk
	}
	remaining := root.batch.RowCount() - int(root.rowIdx)
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
		compares[k].Set(0, root.orderCols[k])
		compares[k].Set(1, second.orderCols[k])
	}

	chunk := 1
	secondRow := second.rowIdx
	if len(compares) == 1 {
		cmp := compares[0]
		for chunk < limit {
			nextRow := root.rowIdx + int64(chunk)
			if cmp.Compare(0, 1, nextRow, secondRow) <= 0 {
				chunk++
			} else {
				break
			}
		}
		return chunk
	}
	for chunk < limit {
		nextRow := root.rowIdx + int64(chunk)
		ordered := true
		for k := 0; k < len(compares); k++ {
			if r := compares[k].Compare(0, 1, nextRow, secondRow); r != 0 {
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

func (ctr *container) Len() int {
	return len(ctr.spillReaders)
}

func (ctr *container) Less(i, j int) bool {
	return ctr.compareSpillReaders(i, j) < 0
}

func (ctr *container) Swap(i, j int) {
	ctr.spillReaders[i], ctr.spillReaders[j] = ctr.spillReaders[j], ctr.spillReaders[i]
	ctr.spillReaders[i].heapIdx = i
	ctr.spillReaders[j].heapIdx = j
}

func (ctr *container) Push(x any) {
	reader := x.(*spillRunReader)
	reader.heapIdx = len(ctr.spillReaders)
	ctr.spillReaders = append(ctr.spillReaders, reader)
}

func (ctr *container) Pop() any {
	old := ctr.spillReaders
	n := len(old)
	reader := old[n-1]
	reader.heapIdx = -1
	ctr.spillReaders = old[:n-1]
	return reader
}

func (ctr *container) fixSpillHeapAfterAdvance(idx int) {
	switch len(ctr.spillReaders) {
	case 0, 1:
		return
	case 2:
		other := 1 - idx
		if other < 0 || other >= len(ctr.spillReaders) {
			other = 1
		}
		if ctr.compareSpillReaders(idx, other) > 0 {
			ctr.Swap(idx, other)
		}
		return
	default:
		heap.Fix(ctr, idx)
	}
}

func (ctr *container) advanceSpillReaderByChunk(proc *process.Process, idx int, chunk int) error {
	reader := ctr.spillReaders[idx]
	reader.rowIdx += int64(chunk)
	if reader.rowIdx < int64(reader.batch.RowCount()) {
		ctr.fixSpillHeapAfterAdvance(idx)
		return nil
	}
	ok, err := reader.readNextBatch(proc, ctr)
	if err != nil {
		return err
	}
	if !ok {
		removed := heap.Remove(ctr, idx).(*spillRunReader)
		removed.close(proc)
		return nil
	}
	ctr.fixSpillHeapAfterAdvance(idx)
	return nil
}

func (ctr *container) openSpillReaders(
	proc *process.Process,
	runs []*spillRun,
) (retErr error) {
	ctr.spillReaders = make([]*spillRunReader, 0, len(runs))
	defer func() {
		if retErr == nil {
			return
		}
		for i := range ctr.spillReaders {
			ctr.spillReaders[i].close(proc)
		}
		ctr.spillReaders = nil
	}()
	for _, run := range runs {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		if _, err := run.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		reader := &spillRunReader{}
		if err := reader.reset(proc, ctr, run.file); err != nil {
			return err
		}
		// Ownership transfers to the reader before its first read. This keeps
		// success, EOF, and read-error cleanup under the same owner.
		run.file = nil
		reader.fdToken = run.fdToken
		reader.diskToken = run.diskToken
		run.fdToken = nil
		run.diskToken = nil
		ok, err := reader.readNextBatch(proc, ctr)
		if err != nil {
			reader.close(proc)
			return err
		}
		if ok {
			ctr.spillReaders = append(ctr.spillReaders, reader)
		} else {
			reader.close(proc)
		}
	}
	for i := range ctr.spillReaders {
		ctr.spillReaders[i].heapIdx = i
	}
	if len(ctr.spillReaders) > 1 {
		heap.Init(ctr)
	}
	return nil
}

func (ctr *container) restoreSpillOrderColumns(proc *process.Process, dataBatch, keyBatch *batch.Batch, orderCols []*vector.Vector) ([]*vector.Vector, error) {
	if cap(orderCols) < len(ctr.executors) {
		orderCols = make([]*vector.Vector, len(ctr.executors))
	} else {
		orderCols = orderCols[:len(ctr.executors)]
	}
	keyIdx := 0
	for i := range ctr.executors {
		if ctr.spillColPos[i] >= 0 {
			colPos := ctr.spillColPos[i]
			if int(colPos) >= len(dataBatch.Vecs) {
				return nil, moerr.NewInternalErrorf(proc.Ctx, "merge-order spill column index out of range: %d", colPos)
			}
			orderCols[i] = dataBatch.Vecs[colPos]
			continue
		}
		if keyBatch == nil || keyIdx >= len(keyBatch.Vecs) {
			return nil, moerr.NewInternalError(proc.Ctx, "merge-order spill key batch missing")
		}
		orderCols[i] = keyBatch.Vecs[keyIdx]
		keyIdx++
	}
	if keyBatch != nil && keyIdx != len(keyBatch.Vecs) {
		return nil, moerr.NewInternalError(proc.Ctx, "merge-order spill key batch mismatch")
	}
	return orderCols, nil
}

func (ctr *container) mergeRunsToSpill(proc *process.Process, runs []*spillRun, analyzer process.Analyzer) (*spillRun, error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return nil, err
	}
	if len(runs) == 1 {
		return runs[0], nil
	}
	if err := ctr.openSpillReaders(proc, runs); err != nil {
		return nil, err
	}
	defer func() {
		for i := range ctr.spillReaders {
			ctr.spillReaders[i].close(proc)
		}
		ctr.spillReaders = nil
	}()

	run, err := ctr.createSpillRun(proc)
	if err != nil {
		return nil, err
	}
	writer, err := newMergeOrderSpillWriter(proc, ctr, run)
	if err != nil {
		run.close()
		return nil, err
	}
	complete := false
	defer func() {
		writer.Free()
		if !complete {
			run.close()
		}
	}()

	var out *batch.Batch
	var outOrder *batch.Batch
	defer func() {
		if out != nil {
			out.Clean(proc.Mp())
		}
		if outOrder != nil {
			outOrder.Clean(proc.Mp())
		}
	}()
	keyCount := len(ctr.spillKeyIndexes)
	for len(ctr.spillReaders) > 0 {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return nil, err
		}
		if out == nil {
			first := ctr.spillReaders[0].batch
			out, err = newBatchWithAllocation(
				proc,
				first,
				ctr.outputAllocation,
			)
			if err != nil {
				return nil, err
			}
			if keyCount > 0 {
				outOrder = batch.NewOffHeapWithSize(keyCount)
				for i, keyIdx := range ctr.spillKeyIndexes {
					outOrder.Vecs[i] = vector.NewOffHeapVecWithType(*ctr.spillReaders[0].orderCols[keyIdx].GetType())
				}
				if ctr.outputAllocation != nil {
					if err = outOrder.SetAllocationAccount(ctr.outputAllocation); err != nil {
						return nil, err
					}
				}
			}
		} else {
			out.CleanOnlyData()
			if outOrder != nil {
				outOrder.CleanOnlyData()
			}
		}

		rows := 0
		nextSizeCheck := batchSizeCheckInterval
		currentOutSize := 0
		getOutSize := func() int {
			if currentOutSize < 0 {
				currentOutSize = out.Size()
			}
			return currentOutSize
		}
		updateOutSize := func(reader *spillRunReader, cnt int) {
			if currentOutSize >= 0 && reader.fixedWidth {
				currentOutSize += cnt * reader.rowBytes
				return
			}
			currentOutSize = -1
		}
		for len(ctr.spillReaders) > 0 {
			if rows >= nextSizeCheck {
				if err, canceled := vm.CancelCheck(proc); canceled {
					return nil, err
				}
				if getOutSize() >= maxBatchSizeToSend {
					break
				}
				nextSizeCheck = rows + batchSizeCheckInterval
			}

			if len(ctr.spillReaders) == 1 {
				src := ctr.spillReaders[0]
				chunk := computeDrainChunk(src, getOutSize())
				if chunk < 1 {
					chunk = 1
				}
				if err := appendContiguousRows(out, src.batch, src.rowIdx, chunk, proc); err != nil {
					return nil, err
				}
				if outOrder != nil {
					if err := appendContiguousOrderRows(outOrder, src.orderCols, ctr.spillKeyIndexes, src.rowIdx, chunk, proc); err != nil {
						return nil, err
					}
				}
				rows += chunk
				updateOutSize(src, chunk)
				if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
					return nil, err
				}
				if getOutSize() >= maxBatchSizeToSend {
					break
				}
				continue
			}
			if len(ctr.spillReaders) == 2 {
				src := ctr.spillReaders[0]
				chunk := computeDrainChunk(src, getOutSize())
				if chunk < 1 {
					chunk = 1
				} else if chunk > 1 {
					if c := ctr.computeWinnerChunk(src, ctr.spillReaders[1], chunk); c > 1 {
						chunk = c
					} else {
						chunk = 1
					}
				}
				if err := appendContiguousRows(out, src.batch, src.rowIdx, chunk, proc); err != nil {
					return nil, err
				}
				if outOrder != nil {
					if err := appendContiguousOrderRows(outOrder, src.orderCols, ctr.spillKeyIndexes, src.rowIdx, chunk, proc); err != nil {
						return nil, err
					}
				}
				rows += chunk
				updateOutSize(src, chunk)
				if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
					return nil, err
				}
				if getOutSize() >= maxBatchSizeToSend {
					break
				}
				continue
			}

			src := ctr.spillReaders[0]
			budgetChunk := computeDrainChunk(src, getOutSize())
			if budgetChunk > 1 {
				secondIdx := 1
				if len(ctr.spillReaders) > 2 && ctr.compareSpillReaders(2, secondIdx) < 0 {
					secondIdx = 2
				}
				if secondIdx > 0 {
					chunk := ctr.computeWinnerChunk(src, ctr.spillReaders[secondIdx], budgetChunk)
					if chunk > 1 {
						if err := appendContiguousRows(out, src.batch, src.rowIdx, chunk, proc); err != nil {
							return nil, err
						}
						if outOrder != nil {
							if err := appendContiguousOrderRows(outOrder, src.orderCols, ctr.spillKeyIndexes, src.rowIdx, chunk, proc); err != nil {
								return nil, err
							}
						}
						rows += chunk
						updateOutSize(src, chunk)
						if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
							return nil, err
						}
						if getOutSize() >= maxBatchSizeToSend {
							break
						}
						continue
					}
				}
			}

			reader := ctr.spillReaders[0]
			if err := appendContiguousRows(out, reader.batch, reader.rowIdx, 1, proc); err != nil {
				return nil, err
			}
			if outOrder != nil {
				if err := appendContiguousOrderRows(outOrder, reader.orderCols, ctr.spillKeyIndexes, reader.rowIdx, 1, proc); err != nil {
					return nil, err
				}
			}
			rows++
			updateOutSize(reader, 1)
			if err := ctr.advanceSpillReaderByChunk(proc, 0, 1); err != nil {
				return nil, err
			}
		}
		if err, canceled := vm.CancelCheck(proc); canceled {
			return nil, err
		}
		out.SetRowCount(rows)
		if outOrder != nil {
			outOrder.SetRowCount(rows)
		}
		var keyCols []*vector.Vector
		if outOrder != nil {
			keyCols = outOrder.Vecs
		}
		if _, _, err := writeSpillBatch(proc, out, keyCols, writer, analyzer); err != nil {
			return nil, err
		}
		run.rowCount += int64(rows)
		run.batchCount++
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	if _, err := run.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	complete = true
	return run, nil
}

func (ctr *container) reduceSpillRuns(proc *process.Process, analyzer process.Analyzer) error {
	const inputLimit = spillMergeFanIn - 1
	for len(ctr.spillRuns) > inputLimit {
		nextRuns := make([]*spillRun, 0, (len(ctr.spillRuns)+inputLimit-1)/inputLimit)
		for start := 0; start < len(ctr.spillRuns); start += inputLimit {
			if err, canceled := vm.CancelCheck(proc); canceled {
				closeSpillRuns(nextRuns)
				return err
			}
			end := start + inputLimit
			if end > len(ctr.spillRuns) {
				end = len(ctr.spillRuns)
			}
			run, err := ctr.mergeRunsToSpill(proc, ctr.spillRuns[start:end], analyzer)
			if err != nil {
				closeSpillRuns(nextRuns)
				return err
			}
			nextRuns = append(nextRuns, run)
		}
		ctr.spillRuns = nextRuns
	}
	return nil
}

func (ctr *container) prepareSpillFinalMerge(proc *process.Process, fs []*plan.OrderBySpec, analyzer process.Analyzer) error {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	if err := ctr.finalizeActiveSpillRun(proc, true); err != nil {
		return err
	}
	ctr.clearSpillTailColumns(proc.Mp())
	if len(ctr.spillRuns) == 0 {
		return nil
	}
	ctr.generateCompares(fs)
	if err := ctr.reduceSpillRuns(proc, analyzer); err != nil {
		return err
	}
	return ctr.openSpillReaders(proc, ctr.spillRuns)
}

func (ctr *container) sendSpillResult(proc *process.Process, result *vm.CallResult) (bool, error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return false, err
	}
	if ctr.buf == nil {
		if len(ctr.spillReaders) == 0 {
			return true, nil
		}
		first := ctr.spillReaders[0].batch
		var err error
		ctr.buf, err = newBatchWithAllocation(
			proc,
			first,
			ctr.outputAllocation,
		)
		if err != nil {
			return false, err
		}
	} else {
		ctr.buf.CleanOnlyData()
	}

	rows := 0
	nextSizeCheck := batchSizeCheckInterval
	currentBufSize := 0
	getBufSize := func() int {
		if currentBufSize < 0 {
			currentBufSize = ctr.buf.Size()
		}
		return currentBufSize
	}
	updateBufSize := func(reader *spillRunReader, cnt int) {
		if currentBufSize >= 0 && reader.fixedWidth {
			currentBufSize += cnt * reader.rowBytes
			return
		}
		currentBufSize = -1
	}
	for len(ctr.spillReaders) > 0 {
		if rows >= nextSizeCheck {
			if err, canceled := vm.CancelCheck(proc); canceled {
				return false, err
			}
			if getBufSize() >= maxBatchSizeToSend {
				break
			}
			nextSizeCheck = rows + batchSizeCheckInterval
		}

		if len(ctr.spillReaders) == 1 {
			src := ctr.spillReaders[0]
			chunk := computeDrainChunk(src, getBufSize())
			if chunk < 1 {
				chunk = 1
			}
			if err := appendContiguousRows(ctr.buf, src.batch, src.rowIdx, chunk, proc); err != nil {
				return false, err
			}
			rows += chunk
			updateBufSize(src, chunk)
			if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
				return false, err
			}
			if getBufSize() >= maxBatchSizeToSend {
				break
			}
			continue
		}
		if len(ctr.spillReaders) == 2 {
			src := ctr.spillReaders[0]
			chunk := computeDrainChunk(src, getBufSize())
			if chunk < 1 {
				chunk = 1
			} else if chunk > 1 {
				if c := ctr.computeWinnerChunk(src, ctr.spillReaders[1], chunk); c > 1 {
					chunk = c
				} else {
					chunk = 1
				}
			}
			if err := appendContiguousRows(ctr.buf, src.batch, src.rowIdx, chunk, proc); err != nil {
				return false, err
			}
			rows += chunk
			updateBufSize(src, chunk)
			if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
				return false, err
			}
			if getBufSize() >= maxBatchSizeToSend {
				break
			}
			continue
		}

		src := ctr.spillReaders[0]
		budgetChunk := computeDrainChunk(src, getBufSize())
		if budgetChunk > 1 {
			secondIdx := 1
			if len(ctr.spillReaders) > 2 && ctr.compareSpillReaders(2, secondIdx) < 0 {
				secondIdx = 2
			}
			if secondIdx > 0 {
				chunk := ctr.computeWinnerChunk(src, ctr.spillReaders[secondIdx], budgetChunk)
				if chunk > 1 {
					if err := appendContiguousRows(ctr.buf, src.batch, src.rowIdx, chunk, proc); err != nil {
						return false, err
					}
					rows += chunk
					updateBufSize(src, chunk)
					if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
						return false, err
					}
					if getBufSize() >= maxBatchSizeToSend {
						break
					}
					continue
				}
			}
		}

		reader := ctr.spillReaders[0]
		if err := appendContiguousRows(ctr.buf, reader.batch, reader.rowIdx, 1, proc); err != nil {
			return false, err
		}
		rows++
		updateBufSize(reader, 1)
		if err := ctr.advanceSpillReaderByChunk(proc, 0, 1); err != nil {
			return false, err
		}
	}

	if rows == 0 {
		return true, nil
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		return false, err
	}
	ctr.buf.SetRowCount(rows)
	result.Batch = ctr.buf
	return len(ctr.spillReaders) == 0, nil
}
