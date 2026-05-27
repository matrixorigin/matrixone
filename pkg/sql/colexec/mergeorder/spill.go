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
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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

func (ctr *container) currentMemoryUsage() int64 {
	return ctr.spillMemUsage
}

func (ctr *container) shouldSpill(incomingBatchSize int64) bool {
	if ctr.spilling {
		return true
	}
	if ctr.spillThreshold <= 0 {
		return false
	}
	return ctr.currentMemoryUsage()+incomingBatchSize > ctr.spillThreshold
}

func (ctr *container) evaluateOrderColumns(proc *process.Process, bat *batch.Batch) ([]*vector.Vector, error) {
	var inputs [1]*batch.Batch
	inputs[0] = bat
	cols := make([]*vector.Vector, len(ctr.executors))
	for i := 0; i < len(ctr.executors); i++ {
		vec, err := ctr.executors[i].EvalWithoutResultReusing(proc, inputs[:], nil)
		if err != nil {
			freeOrderColumns(proc.Mp(), bat, cols)
			return nil, err
		}
		cols[i] = vec
	}
	return cols, nil
}

func (ctr *container) buildSpillKeyColumns(proc *process.Process, bat *batch.Batch) ([]*vector.Vector, error) {
	if len(ctr.spillKeyIndexes) == 0 {
		return nil, nil
	}
	var inputs [1]*batch.Batch
	inputs[0] = bat
	cols := ctr.spillKeyCols[:0]
	if cap(cols) < len(ctr.spillKeyIndexes) {
		cols = make([]*vector.Vector, 0, len(ctr.spillKeyIndexes))
	}
	for _, idx := range ctr.spillKeyIndexes {
		vec, err := ctr.executors[idx].EvalWithoutResultReusing(proc, inputs[:], nil)
		if err != nil {
			freeOrderColumns(proc.Mp(), bat, cols)
			return nil, err
		}
		cols = append(cols, vec)
	}
	ctr.spillKeyCols = cols
	return cols, nil
}

func appendSpillPayload(buf *bytes.Buffer, bat *batch.Batch) error {
	var zero int64
	sizePos := buf.Len()
	buf.Write(types.EncodeInt64(&zero))
	start := buf.Len()
	if _, err := bat.MarshalBinaryWithBuffer(buf, false); err != nil {
		return err
	}
	payloadSize := int64(buf.Len() - start)
	copy(buf.Bytes()[sizePos:sizePos+8], types.EncodeInt64(&payloadSize))
	return nil
}

func makeSpillOrderBatch(orderCols []*vector.Vector, rowCount int) batch.Batch {
	var keyBatch batch.Batch
	keyBatch.Vecs = orderCols
	keyBatch.SetRowCount(rowCount)
	return keyBatch
}

func writeSpillBatch(proc *process.Process, bat *batch.Batch, keyCols []*vector.Vector, writer io.Writer, buf *bytes.Buffer, analyzer process.Analyzer) (int64, int64, error) {
	cnt := int64(bat.RowCount())
	buf.Reset()
	buf.Write(types.EncodeInt64(&cnt))
	if err := appendSpillPayload(buf, bat); err != nil {
		return 0, 0, err
	}
	keyCount := int64(len(keyCols))
	buf.Write(types.EncodeInt64(&keyCount))
	if keyCount > 0 {
		keyBatch := makeSpillOrderBatch(keyCols, bat.RowCount())
		if err := appendSpillPayload(buf, &keyBatch); err != nil {
			return 0, 0, err
		}
	}
	magic := uint64(spillMagic)
	buf.Write(types.EncodeUint64(&magic))
	written, err := writer.Write(buf.Bytes())
	if err != nil {
		return 0, 0, err
	}
	analyzer.Spill(int64(written))
	analyzer.SpillRows(cnt)
	return cnt, int64(written), nil
}

func readSpillPayload(proc *process.Process, reader *bufio.Reader, reuseBat *batch.Batch) (*batch.Batch, error) {
	var header [8]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	batchSize := types.DecodeInt64(header[:])
	reuseBat.CleanOnlyData()
	limited := io.LimitReader(reader, batchSize)
	if err := reuseBat.UnmarshalFromReader(limited, proc.Mp()); err != nil {
		return nil, err
	}
	if n, _ := io.Copy(io.Discard, limited); n > 0 {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "batch unmarshal did not consume all bytes: %d remaining", n)
	}
	return reuseBat, nil
}

func readSpillBatches(proc *process.Process, reader *bufio.Reader, reuseBat, reuseKeyBat *batch.Batch) (*batch.Batch, *batch.Batch, error) {
	var header [8]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		if err == io.EOF {
			return nil, nil, io.EOF
		}
		return nil, nil, err
	}
	cnt := types.DecodeInt64(header[:])
	bat, err := readSpillPayload(proc, reader, reuseBat)
	if err != nil {
		return nil, nil, err
	}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, nil, err
	}
	keyCount := types.DecodeInt64(header[:])
	var keyBat *batch.Batch
	if keyCount > 0 {
		keyBat, err = readSpillPayload(proc, reader, reuseKeyBat)
		if err != nil {
			return nil, nil, err
		}
	} else {
		keyBat = nil
	}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, nil, err
	}
	if types.DecodeUint64(header[:]) != spillMagic {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "corrupted merge-order spill file")
	}
	if bat.RowCount() != int(cnt) || (keyBat != nil && keyBat.RowCount() != int(cnt)) {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "merge-order spill row count mismatch")
	}
	return bat, keyBat, nil
}

func (ctr *container) createSpillRun(proc *process.Process) (*spillRun, error) {
	spillfs, err := ctr.getSpillFS(proc)
	if err != nil {
		return nil, err
	}
	file, err := spillfs.CreateAndRemoveFile(proc.Ctx, fmt.Sprintf("mergeorder_%s", uuid.NewString()))
	if err != nil {
		return nil, err
	}
	return &spillRun{file: file}, nil
}

func (ctr *container) spillBatchAsRun(proc *process.Process, bat *batch.Batch, keyCols []*vector.Vector, analyzer process.Analyzer) error {
	run, err := ctr.createSpillRun(proc)
	if err != nil {
		return err
	}
	if _, _, err = writeSpillBatch(proc, bat, keyCols, run.file, &ctr.spillWriteBuf, analyzer); err != nil {
		run.file.Close()
		return err
	}
	run.batchCount = 1
	run.rowCount = int64(bat.RowCount())
	if _, err = run.file.Seek(0, io.SeekStart); err != nil {
		run.file.Close()
		return err
	}
	ctr.spillRuns = append(ctr.spillRuns, run)
	return nil
}

func (ctr *container) fillSpillIncomingOrderColumns(proc *process.Process, dataBatch *batch.Batch, keyCols []*vector.Vector) ([]*vector.Vector, error) {
	if cap(ctr.spillIncomingCols) < len(ctr.executors) {
		ctr.spillIncomingCols = make([]*vector.Vector, len(ctr.executors))
	} else {
		ctr.spillIncomingCols = ctr.spillIncomingCols[:len(ctr.executors)]
	}
	keyIdx := 0
	for i := range ctr.executors {
		if ctr.spillColPos[i] >= 0 {
			colPos := ctr.spillColPos[i]
			if int(colPos) >= len(dataBatch.Vecs) {
				return nil, moerr.NewInternalErrorf(proc.Ctx, "merge-order spill column index out of range: %d", colPos)
			}
			ctr.spillIncomingCols[i] = dataBatch.Vecs[colPos]
			continue
		}
		if keyIdx >= len(keyCols) {
			return nil, moerr.NewInternalError(proc.Ctx, "merge-order spill key batch missing")
		}
		ctr.spillIncomingCols[i] = keyCols[keyIdx]
		keyIdx++
	}
	if keyIdx != len(keyCols) {
		return nil, moerr.NewInternalError(proc.Ctx, "merge-order spill key batch mismatch")
	}
	return ctr.spillIncomingCols, nil
}

func (ctr *container) ensureSpillTailColumns() {
	if cap(ctr.spillTailCols) < len(ctr.executors) {
		ctr.spillTailCols = make([]*vector.Vector, len(ctr.executors))
		return
	}
	ctr.spillTailCols = ctr.spillTailCols[:len(ctr.executors)]
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
			ctr.spillTailCols[i] = vector.NewOffHeapVecWithType(*incomingOrderCols[i].GetType())
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

func (ctr *container) ensureActiveSpillRun(proc *process.Process) error {
	if ctr.spillActiveRun != nil {
		return nil
	}
	run, err := ctr.createSpillRun(proc)
	if err != nil {
		return err
	}
	ctr.spillActiveRun = run
	ctr.spillActiveWriter = bufio.NewWriterSize(run.file, spillIOBufferSize)
	ctr.spillActiveBytes = 0
	ctr.spillTailReady = false
	return nil
}

func (ctr *container) finalizeActiveSpillRun(proc *process.Process, keepRun bool) error {
	if ctr.spillActiveRun == nil {
		return nil
	}
	run := ctr.spillActiveRun
	writer := ctr.spillActiveWriter
	ctr.spillActiveRun = nil
	ctr.spillActiveWriter = nil
	ctr.spillActiveBytes = 0
	ctr.spillTailReady = false

	if writer != nil {
		if err := writer.Flush(); err != nil {
			run.file.Close()
			run.file = nil
			return err
		}
	}
	if _, err := run.file.Seek(0, io.SeekStart); err != nil {
		run.file.Close()
		run.file = nil
		return err
	}
	if keepRun {
		ctr.spillRuns = append(ctr.spillRuns, run)
		return nil
	}
	run.file.Close()
	run.file = nil
	return nil
}

func (ctr *container) spillBatchWithAppend(
	proc *process.Process,
	bat *batch.Batch,
	keyCols []*vector.Vector,
	incomingOrderCols []*vector.Vector,
	analyzer process.Analyzer,
) error {
	if ctr.spillActiveRun != nil && ((ctr.spillAppendTarget > 0 && ctr.spillActiveBytes >= ctr.spillAppendTarget) || !ctr.canAppendToActiveRun(incomingOrderCols)) {
		if err := ctr.finalizeActiveSpillRun(proc, true); err != nil {
			return err
		}
	}
	if err := ctr.ensureActiveSpillRun(proc); err != nil {
		return err
	}
	rows, written, err := writeSpillBatch(proc, bat, keyCols, ctr.spillActiveWriter, &ctr.spillWriteBuf, analyzer)
	if err != nil {
		return err
	}
	ctr.spillActiveRun.batchCount++
	ctr.spillActiveRun.rowCount += rows
	ctr.spillActiveBytes += written
	return ctr.updateActiveRunTail(proc, incomingOrderCols, int64(bat.RowCount()-1))
}

func (ctr *container) spillInputBatch(proc *process.Process, bat *batch.Batch, analyzer process.Analyzer) error {
	keyCols, err := ctr.buildSpillKeyColumns(proc, bat)
	if err != nil {
		return err
	}
	defer freeOrderColumns(proc.Mp(), bat, keyCols)

	if !ctr.spillAppendEnabled {
		return ctr.spillBatchAsRun(proc, bat, keyCols, analyzer)
	}

	incomingOrderCols, err := ctr.fillSpillIncomingOrderColumns(proc, bat, keyCols)
	if err != nil {
		return err
	}
	return ctr.spillBatchWithAppend(proc, bat, keyCols, incomingOrderCols, analyzer)
}

func (ctr *container) spillCachedRuns(proc *process.Process, analyzer process.Analyzer) error {
	for i := range ctr.batchList {
		if ctr.batchList[i] == nil {
			continue
		}
		if err := ctr.spillInputBatch(proc, ctr.batchList[i], analyzer); err != nil {
			return err
		}
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

func (ctr *container) secondBestReaderIndex() int {
	if len(ctr.spillReaders) < 2 {
		return -1
	}
	if len(ctr.spillReaders) == 2 {
		return 1
	}
	if ctr.compareSpillReaders(1, 2) <= 0 {
		return 1
	}
	return 2
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

func (ctr *container) pickFirstSpillReader() int {
	return 0
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

func (ctr *container) advanceSpillReader(proc *process.Process, idx int) error {
	return ctr.advanceSpillReaderByChunk(proc, idx, 1)
}

func (ctr *container) openSpillReaders(proc *process.Process, runs []*spillRun) error {
	ctr.spillReaders = make([]*spillRunReader, 0, len(runs))
	for _, run := range runs {
		if _, err := run.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		reader := &spillRunReader{}
		reader.reset(run.file)
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
		run.file = nil
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
	writer := bufio.NewWriterSize(run.file, spillIOBufferSize)
	defer writer.Flush()

	var out *batch.Batch
	var outOrder *batch.Batch
	keyCount := len(ctr.spillKeyIndexes)
	for len(ctr.spillReaders) > 0 {
		if out == nil {
			first := ctr.spillReaders[0].batch
			out = batch.NewOffHeapWithSize(first.VectorCount())
			for i := range out.Vecs {
				out.Vecs[i] = vector.NewOffHeapVecWithType(*first.Vecs[i].GetType())
			}
			if len(first.Attrs) > 0 {
				out.Attrs = append(out.Attrs[:0], first.Attrs...)
			}
			if keyCount > 0 {
				outOrder = batch.NewOffHeapWithSize(keyCount)
				for i, keyIdx := range ctr.spillKeyIndexes {
					outOrder.Vecs[i] = vector.NewOffHeapVecWithType(*ctr.spillReaders[0].orderCols[keyIdx].GetType())
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
			if len(ctr.spillReaders) == 1 {
				src := ctr.spillReaders[0]
				chunk := computeDrainChunk(src, getOutSize())
				if chunk < 1 {
					chunk = 1
				}
				if err := appendContiguousRows(out, src.batch, src.rowIdx, chunk, proc); err != nil {
					out.Clean(proc.Mp())
					if outOrder != nil {
						outOrder.Clean(proc.Mp())
					}
					run.file.Close()
					return nil, err
				}
				if outOrder != nil {
					if err := appendContiguousOrderRows(outOrder, src.orderCols, ctr.spillKeyIndexes, src.rowIdx, chunk, proc); err != nil {
						out.Clean(proc.Mp())
						outOrder.Clean(proc.Mp())
						run.file.Close()
						return nil, err
					}
				}
				rows += chunk
				updateOutSize(src, chunk)
				if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
					out.Clean(proc.Mp())
					if outOrder != nil {
						outOrder.Clean(proc.Mp())
					}
					run.file.Close()
					return nil, err
				}
				if getOutSize() >= maxBatchSizeToSend {
					break
				}
				if rows >= nextSizeCheck {
					nextSizeCheck = rows + batchSizeCheckInterval
				}
				continue
			}

			src := ctr.spillReaders[0]
			budgetChunk := computeDrainChunk(src, getOutSize())
			if budgetChunk > 1 {
				secondIdx := ctr.secondBestReaderIndex()
				if secondIdx > 0 {
					chunk := ctr.computeWinnerChunk(src, ctr.spillReaders[secondIdx], budgetChunk)
					if chunk > 1 {
						if err := appendContiguousRows(out, src.batch, src.rowIdx, chunk, proc); err != nil {
							if out != nil {
								out.Clean(proc.Mp())
							}
							if outOrder != nil {
								outOrder.Clean(proc.Mp())
							}
							run.file.Close()
							return nil, err
						}
						if outOrder != nil {
							if err := appendContiguousOrderRows(outOrder, src.orderCols, ctr.spillKeyIndexes, src.rowIdx, chunk, proc); err != nil {
								if out != nil {
									out.Clean(proc.Mp())
								}
								outOrder.Clean(proc.Mp())
								run.file.Close()
								return nil, err
							}
						}
						rows += chunk
						updateOutSize(src, chunk)
						if err := ctr.advanceSpillReaderByChunk(proc, 0, chunk); err != nil {
							if out != nil {
								out.Clean(proc.Mp())
							}
							if outOrder != nil {
								outOrder.Clean(proc.Mp())
							}
							run.file.Close()
							return nil, err
						}
						if getOutSize() >= maxBatchSizeToSend {
							break
						}
						if rows >= nextSizeCheck {
							nextSizeCheck = rows + batchSizeCheckInterval
						}
						continue
					}
				}
			}

			choice := ctr.pickFirstSpillReader()
			reader := ctr.spillReaders[choice]
			if err := appendContiguousRows(out, reader.batch, reader.rowIdx, 1, proc); err != nil {
				if out != nil {
					out.Clean(proc.Mp())
				}
				run.file.Close()
				return nil, err
			}
			if outOrder != nil {
				if err := appendContiguousOrderRows(outOrder, reader.orderCols, ctr.spillKeyIndexes, reader.rowIdx, 1, proc); err != nil {
					if out != nil {
						out.Clean(proc.Mp())
					}
					outOrder.Clean(proc.Mp())
					run.file.Close()
					return nil, err
				}
			}
			rows++
			updateOutSize(reader, 1)
			if err := ctr.advanceSpillReader(proc, choice); err != nil {
				if out != nil {
					out.Clean(proc.Mp())
				}
				if outOrder != nil {
					outOrder.Clean(proc.Mp())
				}
				run.file.Close()
				return nil, err
			}
			if rows >= nextSizeCheck {
				if getOutSize() >= maxBatchSizeToSend {
					break
				}
				nextSizeCheck = rows + batchSizeCheckInterval
			}
		}
		out.SetRowCount(rows)
		if outOrder != nil {
			outOrder.SetRowCount(rows)
		}
		var keyCols []*vector.Vector
		if outOrder != nil {
			keyCols = outOrder.Vecs
		}
		if _, _, err := writeSpillBatch(proc, out, keyCols, writer, &ctr.spillWriteBuf, analyzer); err != nil {
			out.Clean(proc.Mp())
			if outOrder != nil {
				outOrder.Clean(proc.Mp())
			}
			run.file.Close()
			return nil, err
		}
		run.rowCount += int64(rows)
		run.batchCount++
	}
	if out != nil {
		out.Clean(proc.Mp())
	}
	if outOrder != nil {
		outOrder.Clean(proc.Mp())
	}
	if err := writer.Flush(); err != nil {
		run.file.Close()
		return nil, err
	}
	if _, err := run.file.Seek(0, io.SeekStart); err != nil {
		run.file.Close()
		return nil, err
	}
	return run, nil
}

func (ctr *container) reduceSpillRuns(proc *process.Process, analyzer process.Analyzer) error {
	for len(ctr.spillRuns) > spillMergeFanIn {
		nextRuns := make([]*spillRun, 0, (len(ctr.spillRuns)+spillMergeFanIn-1)/spillMergeFanIn)
		for start := 0; start < len(ctr.spillRuns); start += spillMergeFanIn {
			end := start + spillMergeFanIn
			if end > len(ctr.spillRuns) {
				end = len(ctr.spillRuns)
			}
			run, err := ctr.mergeRunsToSpill(proc, ctr.spillRuns[start:end], analyzer)
			if err != nil {
				return err
			}
			nextRuns = append(nextRuns, run)
		}
		ctr.spillRuns = nextRuns
	}
	return nil
}

func (ctr *container) prepareSpillFinalMerge(proc *process.Process, fs []*plan.OrderBySpec, analyzer process.Analyzer) error {
	if err := ctr.finalizeActiveSpillRun(proc, true); err != nil {
		return err
	}
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
	if ctr.buf == nil {
		if len(ctr.spillReaders) == 0 {
			return true, nil
		}
		first := ctr.spillReaders[0].batch
		ctr.buf = batch.NewOffHeapWithSize(first.VectorCount())
		for i := range ctr.buf.Vecs {
			ctr.buf.Vecs[i] = vector.NewOffHeapVecWithType(*first.Vecs[i].GetType())
		}
		if len(first.Attrs) > 0 {
			ctr.buf.Attrs = append(ctr.buf.Attrs[:0], first.Attrs...)
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
			if rows >= nextSizeCheck {
				nextSizeCheck = rows + batchSizeCheckInterval
			}
			continue
		}

		src := ctr.spillReaders[0]
		budgetChunk := computeDrainChunk(src, getBufSize())
		if budgetChunk > 1 {
			secondIdx := ctr.secondBestReaderIndex()
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
					if rows >= nextSizeCheck {
						nextSizeCheck = rows + batchSizeCheckInterval
					}
					continue
				}
			}
		}

		choice := ctr.pickFirstSpillReader()
		reader := ctr.spillReaders[choice]
		if err := appendContiguousRows(ctr.buf, reader.batch, reader.rowIdx, 1, proc); err != nil {
			return false, err
		}
		rows++
		updateBufSize(reader, 1)
		if err := ctr.advanceSpillReader(proc, choice); err != nil {
			return false, err
		}
		if rows >= nextSizeCheck {
			if getBufSize() >= maxBatchSizeToSend {
				break
			}
			nextSizeCheck = rows + batchSizeCheckInterval
		}
	}

	if rows == 0 {
		return true, nil
	}
	ctr.buf.SetRowCount(rows)
	result.Batch = ctr.buf
	return len(ctr.spillReaders) == 0, nil
}
