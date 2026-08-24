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

package fill

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "fill"
const maxFillPendingBatches = 1024

func (fill *Fill) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": fill")
}

func (fill *Fill) OpType() vm.OpType {
	return vm.Fill
}

func (fill *Fill) Prepare(proc *process.Process) (err error) {
	if fill.OpAnalyzer == nil {
		fill.OpAnalyzer = process.NewAnalyzer(fill.GetIdx(), fill.IsFirst, fill.IsLast, "fill")
	} else {
		fill.OpAnalyzer.Reset()
	}

	ctr := &fill.ctr
	ctr.spillThreshold = colexec.ResolveSpillThreshold(fill.SpillThreshold)
	if ctr.allocationAccount != nil {
		ctr.budget, err = proc.GetExecutionResourceBudget()
		if err != nil {
			return err
		}
		ctr.prevPart.configure(
			proc.Mp(),
			ctr.allocationAccount,
			fillAllocationSitePartitionSnapshot,
		)
		ctr.linEntryPart.configure(
			proc.Mp(),
			ctr.allocationAccount,
			fillAllocationSitePartitionSnapshot,
		)
	}

	switch fill.FillType {
	case plan.Node_VALUE:
		// the batch just for eval const value
		b := batch.NewWithSize(1)
		defer b.Clean(proc.Mp())
		b.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
		batch.SetLength(b, 1)
		if len(ctr.exes) == 0 {
			ctr.valVecs = make([]*vector.Vector, len(fill.FillVal))
			for _, val := range fill.FillVal {
				exe, err := colexec.NewExpressionExecutorWithAllocation(
					proc, val, ctr.expressionAllocation,
				)
				if err != nil {
					return err
				}
				ctr.exes = append(ctr.exes, exe)
			}
		}
		for i := range fill.FillVal {
			ctr.valVecs[i], err = ctr.exes[i].Eval(proc, []*batch.Batch{b}, nil)
			if err != nil {
				return err
			}
		}
		ctr.process = processValue
	case plan.Node_PREV:
		if len(ctr.prevVecs) == 0 {
			ctr.prevVecs = make([]*vector.Vector, fill.ColLen)
			ctr.prevValid = make([]bool, fill.ColLen)
		}
		ctr.process = processPrev
	case plan.Node_NEXT:
		if len(ctr.nextRun) < fill.ColLen {
			ctr.nextRun = make([][]fillCoord, fill.ColLen)
		}
		ctr.process = processNext
	case plan.Node_LINEAR:
		if len(ctr.linRun) < fill.ColLen {
			ctr.linRun = make([][]fillCoord, fill.ColLen)
			ctr.linPre = make([]fillCoord, fill.ColLen)
			ctr.linSeed = make([]*vector.Vector, fill.ColLen)
			ctr.linSeedValid = make([]bool, fill.ColLen)
			ctr.linEntry = make([]*vector.Vector, fill.ColLen)
			ctr.linEntryValid = make([]bool, fill.ColLen)
			for i := range ctr.linPre {
				ctr.linPre[i] = fillCoord{seq: -1, row: -1}
			}
		}
		if len(ctr.exes) == 0 {
			ctr.valVecs = make([]*vector.Vector, len(fill.FillVal))
			for _, v := range fill.FillVal {
				resetColRef(v, 0)
				exe, err := colexec.NewExpressionExecutorWithAllocation(
					proc, v, ctr.expressionAllocation,
				)
				if err != nil {
					return err
				}
				ctr.exes = append(ctr.exes, exe)
			}
		}
		ctr.process = processLinear
	default:
		ctr.process = processDefault
	}

	if fill.ProjectList != nil {
		err := fill.PrepareProjectionWithAllocation(
			proc, ctr.expressionAllocation,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fill *Fill) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := fill.OpAnalyzer

	ctr := &fill.ctr

	result, err := ctr.process(ctr, fill, proc, analyzer)

	return result, err
}

func cloneFillBatch(
	source *batch.Batch,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	if source == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	attrs, attrTypes := source.GetSchema()
	cloned := batch.NewWithSchema(selection != nil, attrs, attrTypes)
	if selection != nil {
		if err := cloned.SetAllocationAccount(selection); err != nil {
			cloned.Clean(mp)
			return nil, err
		}
	}
	cloned.Recursive = source.Recursive
	if err := source.CloneTo(cloned, mp); err != nil {
		cloned.Clean(mp)
		return nil, err
	}
	return cloned, nil
}

func (ctr *container) copyToOutput(
	proc *process.Process,
	source *batch.Batch,
) error {
	if ctr.buf == nil {
		var err error
		ctr.buf, err = cloneFillBatch(source, proc.Mp(), ctr.outputAllocation)
		return err
	}
	ctr.buf.CleanOnlyData()
	_, err := ctr.buf.AppendWithCopy(proc.Ctx, proc.Mp(), source)
	return err
}

func resetColRef(expr *plan.Expr, idx int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		exprImpl.Col.RelPos = -1
		exprImpl.Col.ColPos = int32(idx)

	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			resetColRef(arg, i)
		}
	}
}

func processValue(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}
	if err = ctr.copyToOutput(proc, result.Batch); err != nil {
		return result, err
	}

	for i := 0; i < ap.ColLen; i++ {
		for j := 0; j < ctr.buf.Vecs[i].Length(); j++ {
			if ctr.buf.Vecs[i].IsNull(uint64(j)) {
				if err = setValue(ctr.buf.Vecs[i], ctr.valVecs[i], j, 0, proc); err != nil {
					return result, err
				}
			}
		}
	}

	result.Batch = ctr.buf
	return result, nil
}

// partKeyAt reads one partition-key cell, tolerating the constant vectors the
// time window emits when it broadcasts a key across a flushed batch.
func partKeyAt(vec *vector.Vector, row int) (val []byte, isNull bool) {
	if vec.IsConstNull() {
		return nil, true
	}
	if vec.IsConst() {
		row = 0
	}
	if vec.IsNull(uint64(row)) {
		return nil, true
	}
	return vec.GetRawBytesAt(row), false
}

// samePartitionRows reports whether two rows carry the same partition key.
// NULL keys compare equal, matching how GROUP BY folded them into one group.
func samePartitionRows(partIdx []int32, batA *batch.Batch, rowA int, batB *batch.Batch, rowB int) bool {
	for _, col := range partIdx {
		valA, nullA := partKeyAt(batA.Vecs[col], rowA)
		valB, nullB := partKeyAt(batB.Vecs[col], rowB)
		if nullA || nullB {
			if nullA != nullB {
				return false
			}
			continue
		}
		if !bytes.Equal(valA, valB) {
			return false
		}
	}
	return true
}

// snapshotPartKey copies a row's partition key out of the batch, so the next
// batch can detect a boundary after this one has been recycled.
func (ctr *container) snapshotPartKey(
	partIdx []int32,
	bat *batch.Batch,
	row int,
	proc *process.Process,
) error {
	ctr.prevPart.configure(
		proc.Mp(),
		ctr.allocationAccount,
		fillAllocationSitePartitionSnapshot,
	)
	ctr.prevPart.ensureShape(len(partIdx))
	for i, col := range partIdx {
		val, isNull := partKeyAt(bat.Vecs[col], row)
		ctr.prevPart.nulls[i] = isNull
		if err := ctr.prevPart.setKey(i, val); err != nil {
			return err
		}
	}
	ctr.prevPart.set = true
	return nil
}

// matchesSnapshot compares a row against the saved cross-batch partition key.
func (ctr *container) matchesSnapshot(partIdx []int32, bat *batch.Batch, row int) bool {
	if !ctr.prevPart.set {
		return true
	}
	for i, col := range partIdx {
		val, isNull := partKeyAt(bat.Vecs[col], row)
		if isNull || ctr.prevPart.nulls[i] {
			if isNull != ctr.prevPart.nulls[i] {
				return false
			}
			continue
		}
		if !bytes.Equal(val, ctr.prevPart.keys[i]) {
			return false
		}
	}
	return true
}

// batAt resolves an absolute batch sequence number to its still-pending batch.
func (ctr *container) batAt(seq int) *batch.Batch {
	return ctr.bats[seq-ctr.baseSeq]
}

// pullChild appends the next child batch to the pending FIFO and returns its
// absolute sequence number, or eof when the child is drained.
func (ctr *container) pullChild(
	ap *Fill,
	proc *process.Process,
	analyzer process.Analyzer,
) (seq int, eof bool, spilled bool, err error) {
	result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
	if err != nil {
		return 0, false, false, err
	}
	if result.Batch == nil {
		return 0, true, false, nil
	}
	dup, err := cloneFillBatch(result.Batch, proc.Mp(), ctr.retainedAllocation)
	if err != nil {
		if ctr.allocationAccount == nil || !mpool.IsRetryableAllocationCapacity(err) {
			return 0, false, false, err
		}
		err = ctr.spillCurrentSource(ap, proc, analyzer, result.Batch)
		return 0, false, err == nil, err
	}
	if err = addOriginalNullMarkers(
		dup, ap.ColLen, proc.Mp(), ctr.retainedAllocation,
	); err != nil {
		dup.Clean(proc.Mp())
		if ctr.allocationAccount != nil && mpool.IsRetryableAllocationCapacity(err) {
			err = ctr.spillCurrentSource(ap, proc, analyzer, result.Batch)
			return 0, false, err == nil, err
		}
		return 0, false, false, err
	}
	if analyzer != nil {
		analyzer.Alloc(int64(dup.Size()))
	}
	seq = ctr.baseSeq + len(ctr.bats)
	ctr.appendPendingBatch(dup)
	ctr.pendingBytes += int64(dup.Size())
	ctr.pendingRows += int64(dup.RowCount())
	return seq, false, false, nil
}

func (ctr *container) appendPendingBatch(bat *batch.Batch) {
	if len(ctr.bats) == cap(ctr.bats) {
		capacity := max(16, cap(ctr.bats)*2)
		capacity = min(capacity, maxFillPendingBatches)
		next := make([]*batch.Batch, len(ctr.bats), capacity)
		copy(next, ctr.bats)
		ctr.bats = next
	}
	ctr.bats = append(ctr.bats, bat)
}

func (ctr *container) spillCurrentSource(
	ap *Fill,
	proc *process.Process,
	analyzer process.Analyzer,
	source *batch.Batch,
) error {
	if err := ctr.beginSpill(ap, proc, analyzer, false); err != nil {
		return err
	}
	if err := ctr.appendSpillSource(ap, proc, analyzer, source); err != nil {
		ctr.cleanupSpill(proc)
		return err
	}
	if ctr.spill.safeWatermark > ctr.spill.segmentStart {
		if err := ctr.spill.finalizeSegment(ctr, ap, proc); err != nil {
			ctr.cleanupSpill(proc)
			return err
		}
	}
	return nil
}

func (ctr *container) appendCoordinate(
	run *[]fillCoord,
	coord fillCoord,
	proc *process.Process,
) error {
	if ctr.allocationAccount == nil {
		*run = append(*run, coord)
		return nil
	}
	values, err := spillutil.GrowAccountedSlice(
		*run,
		len(*run)+1,
		proc.Mp(),
		ctr.spillAllocation,
		fillAllocationSiteCoordinates,
	)
	if err != nil {
		return err
	}
	values[len(values)-1] = coord
	*run = values
	return nil
}

// emitResolved pops and returns the resolved prefix of the FIFO one batch at a
// time. The popped batch is handed to the caller and freed on the next Call
// (via toFree), because no unresolved coordinate can still reference it.
func (ctr *container) emitResolved(ap *Fill, proc *process.Process) (vm.CallResult, error) {
	b := ctr.bats[0]
	if ap.FillType == plan.Node_LINEAR && b != nil {
		if err := ctr.advanceLinearEntry(ap, b, proc); err != nil {
			return vm.NewCallResult(), err
		}
	}
	ctr.bats = ctr.bats[1:]
	if b != nil {
		ctr.pendingBytes -= int64(b.Size())
		ctr.pendingRows -= int64(b.RowCount())
	}
	ctr.baseSeq++
	ctr.flushable--
	ctr.toFree = b
	stripOriginalNullMarkers(b, ap.ColLen, proc.Mp())
	result := vm.NewCallResult()
	result.Batch = b
	result.Status = vm.ExecNext
	return result, nil
}

// advanceLinearEntry moves the endpoint immediately before the pending FIFO
// across one emitted batch. Original-NULL markers are authoritative here:
// interpolated cells are not endpoints, while every original non-NULL cell is.
func (ctr *container) advanceLinearEntry(ap *Fill, bat *batch.Batch, proc *process.Process) error {
	for row := 0; row < bat.RowCount(); row++ {
		if len(ap.PartitionColIdx) > 0 {
			wasSet := ctr.linEntryPart.set
			same, err := ctr.linEntryPart.sameAndSet(ap.PartitionColIdx, bat, row)
			if err != nil {
				return err
			}
			if !same && wasSet {
				clearEndpoints(ctr.linEntryValid)
			}
		}
		for col := 0; col < ap.ColLen; col++ {
			if originalNullAt(bat, ap.ColLen, col, row) {
				continue
			}
			if err := setEndpoint(
				&ctr.linEntry[col], bat.Vecs[col], row, proc, ctr.retainedAllocation,
			); err != nil {
				return err
			}
			ctr.linEntryValid[col] = true
		}
	}
	return nil
}

// isNewSegment reports whether row r of the just-arrived batch opens a new
// partition: row 0 is compared against the key snapshotted from the previous
// batch, later rows against their in-batch predecessor.
func (ctr *container) isNewSegment(ap *Fill, bat *batch.Batch, r int) bool {
	if len(ap.PartitionColIdx) == 0 {
		return false
	}
	if r == 0 {
		return !ctr.matchesSnapshot(ap.PartitionColIdx, bat, 0)
	}
	return !samePartitionRows(ap.PartitionColIdx, bat, r-1, bat, r)
}

// consumeNext folds one freshly pulled batch into the NEXT state: NULLs join
// their column's pending run, a non-NULL back-fills and clears that run, and a
// partition boundary drops the previous partition's pending NULLs (they have no
// next value and stay NULL).
func (ctr *container) consumeNext(ap *Fill, bat *batch.Batch, seq int, proc *process.Process) error {
	rows := bat.RowCount()
	for r := 0; r < rows; r++ {
		if ctr.isNewSegment(ap, bat, r) {
			for c := 0; c < ap.ColLen; c++ {
				ctr.nextRun[c] = ctr.nextRun[c][:0]
			}
		}
		for c := 0; c < ap.ColLen; c++ {
			vec := bat.Vecs[c]
			if vec.IsNull(uint64(r)) {
				if err := ctr.appendCoordinate(
					&ctr.nextRun[c], fillCoord{seq: seq, row: r}, proc,
				); err != nil {
					return err
				}
				continue
			}
			run := ctr.nextRun[c]
			src, srcRow := vec, r
			var snapshot *vector.Vector
			// A run ending in the current batch writes back into vec itself. Keep
			// one stable, mpool-accounted copy of the non-inline source while the
			// destination area grows; all rows in this run share that source.
			if len(run) > 0 && run[len(run)-1].seq == seq && vec.GetType().IsVarlen() &&
				len(vec.GetBytesAt(r)) > types.VarlenaInlineSize {
				snapshot = vector.NewOffHeapVecWithType(*vec.GetType())
				if ctr.retainedAllocation != nil {
					if err := snapshot.SetAllocationAccount(ctr.retainedAllocation); err != nil {
						snapshot.Free(proc.Mp())
						return err
					}
				}
				if err := appendValue(snapshot, vec, r, proc); err != nil {
					snapshot.Free(proc.Mp())
					return err
				}
				src, srcRow = snapshot, 0
			}
			for _, cd := range run {
				if err := setValue(ctr.batAt(cd.seq).Vecs[c], src, cd.row, srcRow, proc); err != nil {
					if snapshot != nil {
						snapshot.Free(proc.Mp())
					}
					return err
				}
			}
			if snapshot != nil {
				snapshot.Free(proc.Mp())
			}
			ctr.nextRun[c] = ctr.nextRun[c][:0]
		}
	}
	if len(ap.PartitionColIdx) > 0 && rows > 0 {
		if err := ctr.snapshotPartKey(ap.PartitionColIdx, bat, rows-1, proc); err != nil {
			return err
		}
	}
	ctr.recomputeFlushableNext(ap)
	return nil
}

// recomputeFlushableNext marks every batch before the earliest column still
// awaiting a value as emittable.
func (ctr *container) recomputeFlushableNext(ap *Fill) {
	minSeq := ctr.baseSeq + len(ctr.bats)
	for c := 0; c < ap.ColLen; c++ {
		if len(ctr.nextRun[c]) > 0 && ctr.nextRun[c][0].seq < minSeq {
			minSeq = ctr.nextRun[c][0].seq
		}
	}
	ctr.flushable = minSeq - ctr.baseSeq
}

func processPrev(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}

	if err = ctr.copyToOutput(proc, result.Batch); err != nil {
		return result, err
	}

	if len(ctr.prevValid) < ap.ColLen {
		ctr.prevValid = make([]bool, ap.ColLen)
	}

	// A previous value must not leak across a partition boundary. Processing
	// rows first removes the old row-count-sized boundary scratch.
	rowCount := ctr.buf.RowCount()
	for j := 0; j < rowCount; j++ {
		if len(ap.PartitionColIdx) > 0 &&
			((j == 0 && !ctr.matchesSnapshot(ap.PartitionColIdx, ctr.buf, 0)) ||
				(j > 0 && !samePartitionRows(
					ap.PartitionColIdx, ctr.buf, j-1, ctr.buf, j,
				))) {
			clearEndpoints(ctr.prevValid)
		}
		for i := 0; i < ap.ColLen; i++ {
			if ctr.buf.Vecs[i].IsNull(uint64(j)) {
				if ctr.prevVecs[i] != nil && ctr.prevValid[i] {
					if err = setValue(ctr.buf.Vecs[i], ctr.prevVecs[i], j, 0, proc); err != nil {
						return result, err
					}
				}
				continue
			}
			if ctr.prevVecs[i] == nil {
				ctr.prevVecs[i], err = makeEndpoint(
					ctr.buf.Vecs[i], j, proc, ctr.retainedAllocation,
				)
				if err != nil {
					return result, err
				}
			} else {
				if err = setValue(ctr.prevVecs[i], ctr.buf.Vecs[i], 0, j, proc); err != nil {
					return result, err
				}
			}
			ctr.prevValid[i] = true
		}
	}
	if len(ap.PartitionColIdx) > 0 && rowCount > 0 {
		if err = ctr.snapshotPartKey(
			ap.PartitionColIdx, ctr.buf, rowCount-1, proc,
		); err != nil {
			return result, err
		}
	}
	result.Batch = ctr.buf
	return result, nil
}

// consumeLinear folds one freshly pulled batch into the LINEAR state. A NULL
// with a known previous value of the same partition joins that column's run; a
// non-NULL interpolates every position in the pending run between linPre and
// itself, then becomes the new linPre. A run whose neighbour lies across a
// partition boundary stays NULL, because there is nothing to interpolate
// between.
func (ctr *container) consumeLinear(ap *Fill, bat *batch.Batch, seq int, proc *process.Process) error {
	rows := bat.RowCount()
	for r := 0; r < rows; r++ {
		if ctr.isNewSegment(ap, bat, r) {
			ctr.clearLinearSeeds(proc.Mp())
			for c := 0; c < ap.ColLen; c++ {
				ctr.linRun[c] = ctr.linRun[c][:0]
				ctr.linPre[c] = fillCoord{seq: -1, row: -1}
			}
		}
		for c := 0; c < ap.ColLen; c++ {
			vec := bat.Vecs[c]
			if vec.IsNull(uint64(r)) {
				seedValid := c < len(ctr.linSeedValid) && ctr.linSeedValid[c]
				if ctr.linPre[c].seq >= 0 || seedValid {
					if err := ctr.appendCoordinate(
						&ctr.linRun[c], fillCoord{seq: seq, row: r}, proc,
					); err != nil {
						return err
					}
				}
				continue
			}
			if len(ctr.linRun[c]) > 0 {
				if err := ctr.interpolateRun(c, ctr.linPre[c], seq, r, proc); err != nil {
					return err
				}
				ctr.linRun[c] = ctr.linRun[c][:0]
			}
			if c < len(ctr.linSeed) && ctr.linSeed[c] != nil {
				ctr.linSeed[c].Free(proc.Mp())
				ctr.linSeed[c] = nil
			}
			if c < len(ctr.linSeedValid) {
				ctr.linSeedValid[c] = false
			}
			ctr.linPre[c] = fillCoord{seq: seq, row: r}
		}
	}
	if len(ap.PartitionColIdx) > 0 && rows > 0 {
		if err := ctr.snapshotPartKey(ap.PartitionColIdx, bat, rows-1, proc); err != nil {
			return err
		}
	}
	ctr.recomputeFlushableLinear(ap)
	return nil
}

// interpolateRun divides the interval between pre and cur into len(run)+1
// equal steps. A single missing row keeps using the bound SQL expression so
// its historical cast semantics stay unchanged; longer runs use the same
// numeric contract at each distinct position.
func (ctr *container) interpolateRun(col int, pre fillCoord, curSeq, curRow int, proc *process.Process) error {
	var preBatch *batch.Batch
	preRow := pre.row
	if pre.seq >= 0 {
		preBatch = ctr.batAt(pre.seq)
	} else {
		preBatch = batch.NewWithSize(col + 1)
		preBatch.Vecs[col] = ctr.linSeed[col]
		preBatch.SetRowCount(1)
		preRow = 0
	}
	curBatch := ctr.batAt(curSeq)
	if len(ctr.linRun[col]) == 1 {
		valVec, owned, err := linearFillValue(ctr, proc, col, preBatch, preRow, curBatch, curRow)
		if err != nil {
			return err
		}
		cd := ctr.linRun[col][0]
		err = setValue(ctr.batAt(cd.seq).Vecs[col], valVec, cd.row, 0, proc)
		if owned {
			valVec.Free(proc.Mp())
		}
		return err
	}

	total := uint64(len(ctr.linRun[col]) + 1)
	for position, cd := range ctr.linRun[col] {
		if err := setLinearInterpolatedValue(
			ctr.batAt(cd.seq).Vecs[col], cd.row,
			preBatch.Vecs[col], preRow, curBatch.Vecs[col], curRow,
			uint64(position+1), total,
		); err != nil {
			return err
		}
	}
	return nil
}

// recomputeFlushableLinear marks batches as emittable up to the earliest one
// still needed: a pending run, or a linPre that a later non-NULL might yet pair
// with. linPre therefore pins its batch until the next non-NULL supersedes it.
func (ctr *container) recomputeFlushableLinear(ap *Fill) {
	minSeq := ctr.baseSeq + len(ctr.bats)
	for c := 0; c < ap.ColLen; c++ {
		if ctr.linPre[c].seq >= 0 && ctr.linPre[c].seq < minSeq {
			minSeq = ctr.linPre[c].seq
		}
		if len(ctr.linRun[c]) > 0 && ctr.linRun[c][0].seq < minSeq {
			minSeq = ctr.linRun[c][0].seq
		}
	}
	ctr.flushable = minSeq - ctr.baseSeq
}

func linearFillValue(ctr *container, proc *process.Process, idx int, preBatch *batch.Batch, preRow int, curBatch *batch.Batch, curRow int) (*vector.Vector, bool, error) {
	preVec := preBatch.Vecs[idx]
	curVec := curBatch.Vecs[idx]
	if preVec.GetType().Oid == types.T_decimal128 && curVec.GetType().Oid == types.T_decimal128 {
		result := vector.NewVec(*preVec.GetType())
		if ctr.expressionAllocation != nil {
			result = vector.NewOffHeapVecWithType(*preVec.GetType())
			if err := result.SetAllocationAccount(ctr.expressionAllocation); err != nil {
				result.Free(proc.Mp())
				return nil, false, err
			}
		}
		left := vector.GetFixedAtNoTypeCheck[types.Decimal128](preVec, preRow)
		right := vector.GetFixedAtNoTypeCheck[types.Decimal128](curVec, curRow)
		value, err := linearExactValue(left, right, 1, 2)
		if err != nil {
			result.Free(proc.Mp())
			return nil, false, err
		}
		if err = vector.AppendFixed(result, value, false, proc.Mp()); err != nil {
			result.Free(proc.Mp())
			return nil, false, err
		}
		return result, true, nil
	}
	if preVec.GetType().Oid == types.T_decimal256 && curVec.GetType().Oid == types.T_decimal256 {
		result := vector.NewVec(*preVec.GetType())
		left := vector.GetFixedAtNoTypeCheck[types.Decimal256](preVec, preRow)
		right := vector.GetFixedAtNoTypeCheck[types.Decimal256](curVec, curRow)
		value, err := linearExactValue256(left, right, 1, 2)
		if err != nil {
			result.Free(proc.Mp())
			return nil, false, err
		}
		if err = vector.AppendFixed(result, value, false, proc.Mp()); err != nil {
			result.Free(proc.Mp())
			return nil, false, err
		}
		return result, true, nil
	}

	b := batch.NewWithSize(2)
	if ctr.expressionAllocation != nil {
		b = batch.NewOffHeapWithSize(2)
		if err := b.SetAllocationAccount(ctr.expressionAllocation); err != nil {
			b.Clean(proc.Mp())
			return nil, false, err
		}
	}
	left, err := makeEndpoint(preVec, preRow, proc, ctr.expressionAllocation)
	if err != nil {
		b.Clean(proc.Mp())
		return nil, false, err
	}
	b.SetVector(0, left)
	right, err := makeEndpoint(curVec, curRow, proc, ctr.expressionAllocation)
	if err != nil {
		b.Clean(proc.Mp())
		return nil, false, err
	}
	b.SetVector(1, right)
	b.SetRowCount(1)
	defer b.Clean(proc.Mp())
	result, err := ctr.exes[idx].Eval(proc, []*batch.Batch{b}, nil)
	return result, false, err
}

// setLinearInterpolatedValue writes the value at step/total between the two
// endpoints. Exact numeric types use a widened weighted sum, which both avoids
// endpoint arithmetic overflow and rounds once at the destination scale.
// Floating-point types use a stable convex form when the endpoints have
// opposite signs, avoiding overflow in right-left.
func setLinearInterpolatedValue(
	dst *vector.Vector,
	dstRow int,
	leftVec *vector.Vector,
	leftRow int,
	rightVec *vector.Vector,
	rightRow int,
	step uint64,
	total uint64,
) error {
	if step == 0 || step >= total {
		return moerr.NewInternalErrorNoCtxf(
			"invalid linear interpolation position %d/%d", step, total)
	}
	if dst.GetType().Oid != leftVec.GetType().Oid || dst.GetType().Oid != rightVec.GetType().Oid {
		return moerr.NewInternalErrorNoCtxf(
			"linear interpolation type mismatch: dst=%s, left=%s, right=%s",
			dst.GetType(), leftVec.GetType(), rightVec.GetType())
	}

	var err error
	switch dst.GetType().Oid {
	case types.T_bit:
		err = setLinearUnsignedValue[uint64](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_int8:
		err = setLinearSignedValue[int8](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_int16:
		err = setLinearSignedValue[int16](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_int32:
		err = setLinearSignedValue[int32](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_int64:
		err = setLinearSignedValue[int64](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_uint8:
		err = setLinearUnsignedValue[uint8](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_uint16:
		err = setLinearUnsignedValue[uint16](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_uint32:
		err = setLinearUnsignedValue[uint32](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_uint64:
		err = setLinearUnsignedValue[uint64](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_float32:
		err = setLinearFloatValue[float32](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_float64:
		err = setLinearFloatValue[float64](dst, dstRow, leftVec, leftRow, rightVec, rightRow, step, total)
	case types.T_decimal64:
		value, calcErr := linearExactValue(
			types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[types.Decimal64](leftVec, leftRow))),
			types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[types.Decimal64](rightVec, rightRow))),
			step, total)
		if calcErr != nil {
			return calcErr
		}
		err = vector.SetFixedAtNoTypeCheck(dst, dstRow, types.Decimal64(value.B0_63))
	case types.T_decimal128:
		value, calcErr := linearExactValue(
			vector.GetFixedAtNoTypeCheck[types.Decimal128](leftVec, leftRow),
			vector.GetFixedAtNoTypeCheck[types.Decimal128](rightVec, rightRow),
			step, total)
		if calcErr != nil {
			return calcErr
		}
		err = vector.SetFixedAtNoTypeCheck(dst, dstRow, value)
	case types.T_decimal256:
		value, calcErr := linearExactValue256(
			vector.GetFixedAtNoTypeCheck[types.Decimal256](leftVec, leftRow),
			vector.GetFixedAtNoTypeCheck[types.Decimal256](rightVec, rightRow),
			step, total)
		if calcErr != nil {
			return calcErr
		}
		err = vector.SetFixedAtNoTypeCheck(dst, dstRow, value)
	default:
		return moerr.NewInternalErrorNoCtxf(
			"linear interpolation does not support result type %s", dst.GetType())
	}
	if err == nil && dst.HasNull() {
		dst.GetNulls().Del(uint64(dstRow))
	}
	return err
}

type linearSignedNumber interface {
	~int8 | ~int16 | ~int32 | ~int64
}

func setLinearSignedValue[T linearSignedNumber](
	dst *vector.Vector,
	dstRow int,
	leftVec *vector.Vector,
	leftRow int,
	rightVec *vector.Vector,
	rightRow int,
	step, total uint64,
) error {
	value, err := linearExactValue(
		types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[T](leftVec, leftRow))),
		types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[T](rightVec, rightRow))),
		step, total)
	if err != nil {
		return err
	}
	return vector.SetFixedAtNoTypeCheck(dst, dstRow, T(value.B0_63))
}

type linearUnsignedNumber interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

func setLinearUnsignedValue[T linearUnsignedNumber](
	dst *vector.Vector,
	dstRow int,
	leftVec *vector.Vector,
	leftRow int,
	rightVec *vector.Vector,
	rightRow int,
	step, total uint64,
) error {
	value, err := linearExactValue(
		unsignedDecimal128(uint64(vector.GetFixedAtNoTypeCheck[T](leftVec, leftRow))),
		unsignedDecimal128(uint64(vector.GetFixedAtNoTypeCheck[T](rightVec, rightRow))),
		step, total)
	if err != nil {
		return err
	}
	return vector.SetFixedAtNoTypeCheck(dst, dstRow, T(value.B0_63))
}

type linearFloatNumber interface {
	~float32 | ~float64
}

func setLinearFloatValue[T linearFloatNumber](
	dst *vector.Vector,
	dstRow int,
	leftVec *vector.Vector,
	leftRow int,
	rightVec *vector.Vector,
	rightRow int,
	step, total uint64,
) error {
	value := linearFloat64(
		float64(vector.GetFixedAtNoTypeCheck[T](leftVec, leftRow)),
		float64(vector.GetFixedAtNoTypeCheck[T](rightVec, rightRow)),
		step, total)
	return vector.SetFixedAtNoTypeCheck(dst, dstRow, T(value))
}

func unsignedDecimal128(value uint64) types.Decimal128 {
	return types.Decimal128{B0_63: value}
}

func linearFloat64(left, right float64, step, total uint64) float64 {
	if left == right {
		return left
	}
	ratio := float64(step) / float64(total)
	if (left < 0) != (right < 0) {
		return left*(1-ratio) + right*ratio
	}
	return left + (right-left)*ratio
}

func linearExactValue(left, right types.Decimal128, step, total uint64) (types.Decimal128, error) {
	if step == 0 || step >= total {
		return types.Decimal128{}, moerr.NewInternalErrorNoCtxf(
			"invalid exact linear interpolation position %d/%d", step, total)
	}
	leftTerm, err := multiplyDecimal128ByUint64(left, total-step)
	if err != nil {
		return types.Decimal128{}, err
	}
	rightTerm, err := multiplyDecimal128ByUint64(right, step)
	if err != nil {
		return types.Decimal128{}, err
	}
	numerator, _, err := leftTerm.Add(rightTerm, 0, 0)
	if err != nil {
		return types.Decimal128{}, err
	}
	negative := numerator.Sign()
	if negative {
		numerator = numerator.Minus()
	}
	value, err := numerator.Div256(types.Decimal256{B0_63: total})
	if err != nil {
		return types.Decimal128{}, err
	}
	if negative {
		value = value.Minus()
	}
	return decimal256ToDecimal128(value)
}

func multiplyDecimal128ByUint64(value types.Decimal128, factor uint64) (types.Decimal256, error) {
	widened := types.Decimal256FromDecimal128(value)
	negative := widened.Sign()
	if negative {
		widened = widened.Minus()
	}
	result, err := widened.Mul256(types.Decimal256{B0_63: factor})
	if err != nil {
		return types.Decimal256{}, err
	}
	if negative {
		result = result.Minus()
	}
	return result, nil
}

func linearExactValue256(left, right types.Decimal256, step, total uint64) (types.Decimal256, error) {
	if step == 0 || step >= total {
		return types.Decimal256{}, moerr.NewInternalErrorNoCtxf(
			"invalid exact linear interpolation position %d/%d", step, total)
	}

	// A Decimal256 endpoint multiplied by a uint64 position needs at most 320
	// bits. Keep that exact intermediate on the stack: using Decimal256 directly
	// can overflow before division even though the final convex value is valid.
	leftTerm, leftNegative := multiplyDecimal256Magnitude(left, total-step)
	rightTerm, rightNegative := multiplyDecimal256Magnitude(right, step)
	var numerator linearUint320
	negative := leftNegative
	if leftNegative == rightNegative {
		var overflow bool
		numerator, overflow = leftTerm.add(rightTerm)
		if overflow {
			return types.Decimal256{}, moerr.NewInternalErrorNoCtx(
				"linear interpolation intermediate exceeds 320 bits")
		}
	} else if leftTerm.compare(rightTerm) >= 0 {
		numerator = leftTerm.sub(rightTerm)
	} else {
		numerator = rightTerm.sub(leftTerm)
		negative = rightNegative
	}
	value, remainder := numerator.div(total)
	if remainder >= total-remainder {
		var overflow bool
		value, overflow = value.add(linearUint320{1})
		if overflow {
			return types.Decimal256{}, moerr.NewInternalErrorNoCtx(
				"linear interpolation rounded result exceeds 320 bits")
		}
	}
	return value.decimal256(negative)
}

// linearUint320 is an unsigned little-endian integer used only for the exact
// Decimal256 weighted sum. Five limbs are sufficient for 256 bits × uint64.
type linearUint320 [5]uint64

func multiplyDecimal256Magnitude(value types.Decimal256, factor uint64) (linearUint320, bool) {
	negative := value.Sign()
	if negative {
		value = value.Minus()
	}
	input := [...]uint64{value.B0_63, value.B64_127, value.B128_191, value.B192_255}
	var result linearUint320
	var carry uint64
	for i, limb := range input {
		hi, lo := bits.Mul64(limb, factor)
		var loCarry uint64
		result[i], loCarry = bits.Add64(lo, carry, 0)
		carry, _ = bits.Add64(hi, 0, loCarry)
	}
	result[4] = carry
	return result, negative
}

func (value linearUint320) add(other linearUint320) (linearUint320, bool) {
	var result linearUint320
	var carry uint64
	for i := range result {
		result[i], carry = bits.Add64(value[i], other[i], carry)
	}
	return result, carry != 0
}

func (value linearUint320) sub(other linearUint320) linearUint320 {
	var result linearUint320
	var borrow uint64
	for i := range result {
		result[i], borrow = bits.Sub64(value[i], other[i], borrow)
	}
	return result
}

func (value linearUint320) compare(other linearUint320) int {
	for i := len(value) - 1; i >= 0; i-- {
		if value[i] < other[i] {
			return -1
		}
		if value[i] > other[i] {
			return 1
		}
	}
	return 0
}

func (value linearUint320) div(divisor uint64) (linearUint320, uint64) {
	var quotient linearUint320
	var remainder uint64
	for i := len(value) - 1; i >= 0; i-- {
		quotient[i], remainder = bits.Div64(remainder, value[i], divisor)
	}
	return quotient, remainder
}

func (value linearUint320) decimal256(negative bool) (types.Decimal256, error) {
	if value[4] != 0 || (!negative && value[3]>>63 != 0) ||
		(negative && value[3] > uint64(1)<<63) ||
		(negative && value[3] == uint64(1)<<63 && (value[2] != 0 || value[1] != 0 || value[0] != 0)) {
		return types.Decimal256{}, moerr.NewInternalErrorNoCtx(
			"linear interpolation result exceeds decimal256")
	}
	result := types.Decimal256{
		B0_63: value[0], B64_127: value[1],
		B128_191: value[2], B192_255: value[3],
	}
	if negative && (value[0] != 0 || value[1] != 0 || value[2] != 0 || value[3] != 0) {
		result = result.Minus()
	}
	return result, nil
}

func decimal256ToDecimal128(value types.Decimal256) (types.Decimal128, error) {
	if value.Sign() {
		if value.B192_255 != ^uint64(0) || value.B128_191 != ^uint64(0) || value.B64_127>>63 == 0 {
			return types.Decimal128{}, moerr.NewInternalErrorNoCtx("linear interpolation result exceeds decimal128")
		}
	} else if value.B192_255 != 0 || value.B128_191 != 0 || value.B64_127>>63 != 0 {
		return types.Decimal128{}, moerr.NewInternalErrorNoCtx("linear interpolation result exceeds decimal128")
	}
	return types.Decimal128{B0_63: value.B0_63, B64_127: value.B64_127}, nil
}

func processNext(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	return ctr.driveFill(ap, proc, analyzer, (*container).consumeNext, (*container).flushPendingRunsNext)
}

func processLinear(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	return ctr.driveFill(ap, proc, analyzer, (*container).consumeLinear, (*container).flushPendingRunsLinear)
}

// driveFill is the shared incremental loop for NEXT and LINEAR. It emits the
// resolved batch prefix as soon as it exists — only ever buffering batches that
// still contain a row whose value depends on a not-yet-seen input — and calls
// the child again only when nothing is emittable, so a no-NULL stream flows
// through without ever materializing more than the batch in flight.
func (ctr *container) driveFill(
	ap *Fill, proc *process.Process, analyzer process.Analyzer,
	consume func(*container, *Fill, *batch.Batch, int, *process.Process) error,
	flushPendingRuns func(*container, *Fill),
) (vm.CallResult, error) {
	// The batch returned last Call has been consumed by the parent by now.
	if ctr.toFree != nil {
		ctr.toFree.Clean(proc.Mp())
		ctr.toFree = nil
	}
	for {
		if ctr.spill != nil {
			if !ctr.spill.ready {
				if err := ctr.collectSpill(ap, proc, analyzer); err != nil {
					ctr.cleanupSpill(proc)
					return vm.NewCallResult(), err
				}
			}
			bat, err := ctr.spill.replayNext(ctr, ap, proc)
			if err == io.EOF {
				if err = ctr.finishSpillReplay(ap, proc); err != nil {
					return vm.NewCallResult(), err
				}
				continue
			}
			if err != nil {
				ctr.cleanupSpill(proc)
				return vm.NewCallResult(), err
			}
			result := vm.NewCallResult()
			result.Batch = bat
			result.Status = vm.ExecNext
			return result, nil
		}
		if ctr.flushable > 0 {
			return ctr.emitResolved(ap, proc)
		}
		if ctr.childDone {
			if len(ctr.bats) == 0 {
				result := vm.NewCallResult()
				result.Batch = nil
				result.Status = vm.ExecStop
				return result, nil
			}
			// End of input: whatever is still pending has no future value to
			// wait for, so it stays NULL and the whole tail becomes emittable.
			flushPendingRuns(ctr, ap)
			ctr.flushable = len(ctr.bats)
			continue
		}
		seq, eof, spilled, err := ctr.pullChild(ap, proc, analyzer)
		if err != nil {
			return vm.NewCallResult(), err
		}
		if spilled {
			continue
		}
		if eof {
			ctr.childDone = true
			continue
		}
		if err = consume(ctr, ap, ctr.batAt(seq), seq, proc); err != nil {
			if ctr.allocationAccount != nil &&
				mpool.IsRetryableAllocationCapacity(err) {
				if spillErr := ctr.beginSpill(ap, proc, analyzer, true); spillErr != nil {
					return vm.NewCallResult(), spillErr
				}
				continue
			}
			return vm.NewCallResult(), err
		}
		if ctr.flushable == 0 &&
			(ctr.shouldSpillPending() || len(ctr.bats) >= maxFillPendingBatches) {
			if err = ctr.beginSpill(ap, proc, analyzer, true); err != nil {
				return vm.NewCallResult(), err
			}
			continue
		}
	}
}

func (ctr *container) flushPendingRunsNext(ap *Fill) {
	for c := 0; c < ap.ColLen; c++ {
		ctr.nextRun[c] = ctr.nextRun[c][:0]
	}
}

func (ctr *container) flushPendingRunsLinear(ap *Fill) {
	for c := 0; c < ap.ColLen; c++ {
		ctr.linRun[c] = ctr.linRun[c][:0]
		ctr.linPre[c] = fillCoord{seq: -1, row: -1}
	}
}

func processDefault(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	return result, nil
}

func appendValue(v, w *vector.Vector, j int, proc *process.Process) error {
	if w.IsNull(uint64(j)) {
		return vector.AppendNull(v, proc.Mp())
	}
	return v.UnionOne(w, int64(j), proc.Mp())
}

func setValue(v, w *vector.Vector, i, j int, proc *process.Process) error {
	if v.HasNull() {
		v.GetNulls().Del(uint64(i))
	}
	var err error
	switch v.GetType().Oid {
	case types.T_bool:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[bool](w, j))
	case types.T_bit:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint64](w, j))
	case types.T_int8:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int8](w, j))
	case types.T_int16:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int16](w, j))
	case types.T_int32:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int32](w, j))
	case types.T_int64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int64](w, j))
	case types.T_uint8:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint8](w, j))
	case types.T_uint16:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint16](w, j))
	case types.T_uint32:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint32](w, j))
	case types.T_uint64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint64](w, j))
	case types.T_float32:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[float32](w, j))
	case types.T_float64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[float64](w, j))
	case types.T_date:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Date](w, j))
	case types.T_datetime:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Datetime](w, j))
	case types.T_time:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Time](w, j))
	case types.T_timestamp:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Timestamp](w, j))
	case types.T_enum:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Enum](w, j))
	case types.T_decimal64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Decimal64](w, j))
	case types.T_decimal128:
		err = setDecimal128Value(v, w, i, j)
	case types.T_decimal256:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Decimal256](w, j))
	case types.T_uuid:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Uuid](w, j))
	case types.T_TS:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.TS](w, j))
	case types.T_Rowid:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Rowid](w, j))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8, types.T_datalink:
		err = vector.SetBytesAtFrom(v, i, w, j, proc.Mp())
	default:
		panic(fmt.Sprintf("unexpect type %s for function set value in fill query", v.GetType()))
	}
	return err
}

func setDecimal128Value(v, w *vector.Vector, i, j int) error {
	if v.GetType().Oid == w.GetType().Oid && v.GetType().Scale == w.GetType().Scale {
		return vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Decimal128](w, j))
	}

	var (
		value types.Decimal128
		err   error
	)
	targetScale := v.GetType().Scale
	switch w.GetType().Oid {
	case types.T_int8:
		value = types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[int8](w, j)))
	case types.T_int16:
		value = types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[int16](w, j)))
	case types.T_int32:
		value = types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[int32](w, j)))
	case types.T_int64:
		value = types.Decimal128FromInt64(vector.GetFixedAtNoTypeCheck[int64](w, j))
	case types.T_uint8:
		value = types.Decimal128{B0_63: uint64(vector.GetFixedAtNoTypeCheck[uint8](w, j))}
	case types.T_uint16:
		value = types.Decimal128{B0_63: uint64(vector.GetFixedAtNoTypeCheck[uint16](w, j))}
	case types.T_uint32:
		value = types.Decimal128{B0_63: uint64(vector.GetFixedAtNoTypeCheck[uint32](w, j))}
	case types.T_uint64:
		value = types.Decimal128{B0_63: vector.GetFixedAtNoTypeCheck[uint64](w, j)}
	case types.T_float32:
		value, err = types.Decimal128FromFloat64(float64(vector.GetFixedAtNoTypeCheck[float32](w, j)), v.GetType().Width, targetScale)
	case types.T_float64:
		value, err = types.Decimal128FromFloat64(vector.GetFixedAtNoTypeCheck[float64](w, j), v.GetType().Width, targetScale)
	case types.T_decimal64:
		value = types.Decimal128FromDecimal64(vector.GetFixedAtNoTypeCheck[types.Decimal64](w, j), w.GetType().Scale)
		value, err = value.Scale(targetScale - w.GetType().Scale)
	case types.T_decimal128:
		value = vector.GetFixedAtNoTypeCheck[types.Decimal128](w, j)
		value, err = value.Scale(targetScale - w.GetType().Scale)
	default:
		return moerr.NewInternalErrorNoCtxf("cannot set decimal128 fill value from %s", w.GetType())
	}
	if err != nil {
		return err
	}
	if w.GetType().Oid != types.T_float32 && w.GetType().Oid != types.T_float64 &&
		w.GetType().Oid != types.T_decimal64 && w.GetType().Oid != types.T_decimal128 {
		value, err = value.Scale(targetScale)
		if err != nil {
			return err
		}
	}
	return vector.SetFixedAtNoTypeCheck(v, i, value)
}
