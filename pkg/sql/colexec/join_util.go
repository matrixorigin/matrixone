// Copyright 2022 Matrix Origin
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

package colexec

import (
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Batches struct {
	Buf     []*batch.Batch
	MemSize int64
	// allocation is the accounted MPool view the retained batches allocate
	// through (nil = unaccounted). Every batch in Buf shares it.
	allocation *mpool.MPool
	// pool is the pool every vector in Buf was created with: allocation, or
	// the process pool when unaccounted. Buf is freed with it.
	pool *mpool.MPool
}

// Pool returns the pool the retained batches were created with, or nil when
// there are none.
func (bs *Batches) Pool() *mpool.MPool {
	return bs.pool
}

// cleanPool is the pool Buf is freed with: the recorded pool, else mp.
func (bs *Batches) cleanPool(mp *mpool.MPool) *mpool.MPool {
	if bs.pool != nil {
		return bs.pool
	}
	return mp
}

func (bs *Batches) RowCount() int {
	var count int
	for _, b := range bs.Buf {
		count += b.RowCount()
	}
	return count
}

// Clean frees the retained batches with the pool they were created with; mp
// is used only when no batch was ever retained.
func (bs *Batches) Clean(mp *mpool.MPool) {
	pool := bs.cleanPool(mp)
	for i := range bs.Buf {
		bs.Buf[i].Clean(pool)
	}
	bs.Buf = nil
	bs.MemSize = 0
	bs.allocation = nil
	bs.pool = nil
}

// Allocation returns the MPool view the retained batches allocate through,
// or nil when they are unaccounted.
func (bs *Batches) Allocation() *mpool.MPool {
	return bs.allocation
}

func (bs *Batches) CleanOnlyData() {
	for i := range bs.Buf {
		bs.Buf[i].CleanOnlyData()
	}
	bs.Buf = nil
	bs.MemSize = 0
	bs.pool = nil
}

// Reset forgets Buf without freeing it; its owner frees it with Pool().
func (bs *Batches) Reset() {
	if bs.Buf != nil {
		bs.Buf = nil
	}
	bs.MemSize = 0
	bs.pool = nil
}

// copy from input batch into batches
// the batches structure hold data in fix size 8192 rows, and continue to append from next batch
func (bs *Batches) CopyIntoBatches(src *batch.Batch, proc *process.Process) (err error) {
	return bs.CopyIntoBatchesWithAllocation(src, proc, nil)
}

// CopyIntoBatchesWithAllocation selects provenance for every retained vector
// destination. The Go descriptors are bounded by one Batch per 8,192 rows and
// one Vector pointer per input column; physical data, area, null, and grouping
// buffers are allocation-accounted and remain owned by the copied batches.
//
// The append is transactional. In particular, an allocation rejection while
// copying a later input must not destroy batches retained from earlier inputs:
// HashBuild needs those batches intact to recover by spilling them. Existing
// vectors are restored to logical append checkpoints on failure; successful
// capacity growth remains owned and reusable. This avoids repeatedly copying
// a partial 8,192-row tail as small input batches arrive.
func (bs *Batches) CopyIntoBatchesWithAllocation(
	src *batch.Batch,
	proc *process.Process,
	selection *mpool.MPool,
) (err error) {
	return bs.copyWithAllocation(src, proc, selection, 0)
}

// CopyIntoBoundedBatches retains ordinary vectors, opening a new batch when
// either the row limit or the physical allocation limit of a column is reached.
func (bs *Batches) CopyIntoBoundedBatches(
	src *batch.Batch,
	proc *process.Process,
	selection *mpool.MPool,
) error {
	for _, vec := range src.Vecs {
		if vec == nil {
			return mpool.ErrAllocationAccountInvalid
		}
	}
	return bs.copyWithAllocation(src, proc, selection, mpool.MaxAllocationSize())
}

func (bs *Batches) copyWithAllocation(
	src *batch.Batch,
	proc *process.Process,
	selection *mpool.MPool,
	allocationLimit int64,
) (err error) {
	if len(bs.Buf) > 0 && !mpool.SameView(bs.allocation, selection) {
		return mpool.ErrAllocationAccountMismatch
	}

	originalLen := len(bs.Buf)
	originalMemSize := bs.MemSize
	originalAllocation := bs.allocation
	originalNil := bs.Buf == nil
	var originalTail *batch.Batch
	originalTailRows := 0
	var localTailCheckpoints [16]vector.AppendCheckpoint
	var tailCheckpoints []vector.AppendCheckpoint
	if originalLen > 0 && bs.Buf[originalLen-1].RowCount() != DefaultBatchSize {
		originalTail = bs.Buf[originalLen-1]
		originalTailRows = originalTail.RowCount()
		if len(originalTail.Vecs) > len(localTailCheckpoints) {
			tailCheckpoints = make([]vector.AppendCheckpoint, len(originalTail.Vecs))
		} else {
			tailCheckpoints = localTailCheckpoints[:len(originalTail.Vecs)]
		}
		if err := originalTail.OwnReadOnlyVectors(allocationMP(proc, selection)); err != nil {
			return err
		}
		for i := range originalTail.Vecs {
			tailCheckpoints[i] = originalTail.Vecs[i].MakeAppendCheckpoint()
		}
	}
	originalPool := bs.pool
	if originalLen == 0 {
		bs.allocation = selection
		bs.pool = allocationMP(proc, selection)
	}
	if err = bs.copyIntoBatches(src, proc, selection, allocationLimit); err == nil {
		return nil
	}
	for i := originalLen; i < len(bs.Buf); i++ {
		bs.Buf[i].Clean(allocationMP(proc, selection))
	}
	clear(bs.Buf[originalLen:])
	bs.Buf = bs.Buf[:originalLen]
	if originalNil {
		bs.Buf = nil
	}
	if originalTail != nil {
		var rollbackErr error
		for i := range originalTail.Vecs {
			rollbackErr = vector.JoinAppendRollbackError(rollbackErr, originalTail.Vecs[i].RollbackAppend(tailCheckpoints[i], DefaultBatchSize-originalTailRows))
		}
		if rollbackErr == nil {
			originalTail.SetRowCount(originalTailRows)
		}
		err = vector.JoinAppendRollbackError(err, rollbackErr)
	}
	bs.MemSize = originalMemSize
	if originalLen == 0 {
		bs.allocation = originalAllocation
		bs.pool = originalPool
	}
	return err
}

func (bs *Batches) copyIntoBatches(
	src *batch.Batch,
	proc *process.Process,
	selection *mpool.MPool,
	allocationLimit int64,
) (err error) {
	if bs.Buf == nil {
		bs.Buf = make([]*batch.Batch, 0, 16)
	}
	if len(bs.Buf) > 0 && !mpool.SameView(bs.allocation, selection) {
		return mpool.ErrAllocationAccountMismatch
	}

	var tmp *batch.Batch
	if src.RowCount() == DefaultBatchSize &&
		(allocationLimit == 0 || appendableRows(nil, src, 0, DefaultBatchSize, allocationLimit) == DefaultBatchSize) {
		if selection == nil {
			tmp, err = src.Dup(proc.Mp())
		} else {
			tmp, err = proc.NewBatchFromSrcWithAllocation(src, 0, selection)
			if err == nil {
				err = src.CloneTo(tmp, selection)
			}
		}
		if err != nil {
			if tmp != nil {
				tmp.Clean(allocationMP(proc, selection))
			}
			return err
		}
		if allocationLimit != 0 {
			tmp.SetAttributes(slices.Clone(tmp.Attrs))
		}
		bs.MemSize += int64(tmp.Size())
		bs.Buf = append(bs.Buf, tmp)
		lenBuf := len(bs.Buf)
		if lenBuf > 1 && bs.Buf[lenBuf-2].RowCount() != DefaultBatchSize {
			//swap the last 2 batches to get right order
			bs.Buf[lenBuf-2], bs.Buf[lenBuf-1] = bs.Buf[lenBuf-1], bs.Buf[lenBuf-2]
		}
		return nil
	}

	offset := 0
	length := src.RowCount()

	for offset < length {
		lenBuf := len(bs.Buf)
		var rows int
		tmp = nil
		if lenBuf > 0 && bs.Buf[lenBuf-1].RowCount() != DefaultBatchSize {
			tmp = bs.Buf[lenBuf-1]
			if len(tmp.Vecs) != len(src.Vecs) {
				return moerr.NewInternalError(proc.Ctx, "unexpected error happens in batch append")
			}
			rows = min(length-offset, DefaultBatchSize-tmp.RowCount())
			if allocationLimit != 0 {
				rows = appendableRows(tmp, src, offset, rows, allocationLimit)
				if rows == 0 {
					bs.MemSize += int64(tmp.Size())
					tmp = nil
				}
			}
		}
		if tmp == nil {
			rows = min(length-offset, DefaultBatchSize)
			if allocationLimit != 0 {
				rows = appendableRows(nil, src, offset, rows, allocationLimit)
				if rows == 0 {
					return mpool.ErrAllocationAllocatorLimit
				}
			}
			tmp, err = proc.NewBatchFromSrcWithAllocation(src, rows, selection)
			if err != nil {
				return err
			}
			if allocationLimit != 0 {
				tmp.SetAttributes(slices.Clone(tmp.Attrs))
			}
			bs.Buf = append(bs.Buf, tmp)
		}
		if err = appendBatchRange(tmp, src, offset, rows, proc, allocationMP(proc, selection)); err != nil {
			return err
		}
		if tmp.RowCount() == DefaultBatchSize {
			bs.MemSize += int64(tmp.Size())
		}
		offset += rows
	}
	return nil
}

// appendableRows projects UnionBatch's physical copies, one column at a time.
// Only ranges whose area cannot be bounded in constant time inspect values.
func appendableRows(dst, src *batch.Batch, start, rows int, limit int64) int {
	var dstRows int
	if dst != nil {
		dstRows = dst.RowCount()
	}
	for i, vec := range src.Vecs {
		if size := vec.GetType().TypeSize(); int64(dstRows+rows)*int64(size) > limit {
			rows = min(rows, max(0, int(limit/int64(size))-dstRows))
		}
		if rows == 0 || !vec.GetType().IsVarlen() {
			continue
		}
		available := limit
		if dst != nil {
			available -= int64(len(dst.Vecs[i].GetArea()))
		}
		if vec.IsConst() {
			// UnionBatch copies a flat constant once and fills the descriptors.
			if vector.VarlenaPayloadSize(vec, 0) > available {
				return 0
			}
			continue
		}
		area := int64(len(vec.GetArea()))
		if area == 0 || (vec.VarlenaAreaIsDisjoint() && area <= available) || area <= available/int64(rows) {
			continue
		}
		for row := 0; row < rows; row++ {
			available -= vector.VarlenaPayloadSize(vec, start+row)
			if available < 0 {
				rows = row
				break
			}
		}
	}
	return rows
}

func (bs *Batches) Shrink(ignoreRow *bitmap.Bitmap, proc *process.Process) error {
	if ignoreRow.Count() == 0 {
		return nil
	}
	if len(bs.Buf) == 0 || bs.Buf[0] == nil {
		return mpool.ErrAllocationAccountInvalid
	}

	ignoreRow.Negate()
	// Build the replacement privately and stream the active row IDs directly
	// from the bitmap. The old implementation materialized one Go int32 per
	// row and silently dropped the copied-batch allocation provenance.
	count := ignoreRow.Count()
	n := (count + DefaultBatchSize - 1) / DefaultBatchSize
	if n == 0 {
		n = 1
	}
	selection := bs.allocation
	mp := allocationMP(proc, selection)
	newBuf := make([]*batch.Batch, n)
	cleanup := true
	defer func() {
		if cleanup {
			for _, bat := range newBuf {
				if bat != nil {
					bat.Clean(mp)
				}
			}
			// Preserve the caller's ignore-row checkpoint on failure.
			ignoreRow.Negate()
		}
	}()
	for i := range newBuf {
		newBuf[i] = batch.NewWithSize(len(bs.Buf[0].Vecs))
		for j, vec := range bs.Buf[0].Vecs {
			newBuf[i].SetVector(int32(j), vector.NewVec(*vec.GetType(), mp))
		}
	}
	itr := ignoreRow.Iterator()
	outRow := 0
	for itr.HasNext() {
		sel := int(itr.Next())
		srcBatch, srcRow := sel/DefaultBatchSize, sel%DefaultBatchSize
		dstBatch := outRow / DefaultBatchSize
		for j, vec := range bs.Buf[srcBatch].Vecs {
			if err := newBuf[dstBatch].Vecs[j].UnionOne(vec, int64(srcRow), mp); err != nil {
				return err
			}
		}
		newBuf[dstBatch].AddRowCount(1)
		outRow++
	}

	bs.Clean(proc.Mp())
	bs.Buf = newBuf
	bs.allocation = selection
	bs.pool = mp
	for _, bat := range newBuf {
		bs.MemSize += int64(bat.Size())
	}
	cleanup = false

	return nil
}

// allocationMP returns selection when it is set, otherwise the process pool.
func allocationMP(proc *process.Process, selection *mpool.MPool) *mpool.MPool {
	if selection != nil {
		return selection
	}
	return proc.Mp()
}

func appendBatchRange(dst *batch.Batch, src *batch.Batch, offset, length int, proc *process.Process, mp *mpool.MPool) error {
	var err error
	if len(dst.Vecs) != len(src.Vecs) {
		return moerr.NewInternalError(proc.Ctx, "unexpected error happens in batch append")
	}
	var localCheckpoints [16]vector.AppendCheckpoint
	var checkpoints []vector.AppendCheckpoint
	if len(dst.Vecs) > len(localCheckpoints) {
		checkpoints = make([]vector.AppendCheckpoint, len(dst.Vecs))
	} else {
		checkpoints = localCheckpoints[:len(dst.Vecs)]
	}
	if err := dst.OwnReadOnlyVectors(mp); err != nil {
		return err
	}
	for i := range dst.Vecs {
		checkpoints[i] = dst.Vecs[i].MakeAppendCheckpoint()
	}
	for i := range dst.Vecs {
		if err = dst.Vecs[i].UnionBatch(src.Vecs[i], int64(offset), length, nil, mp); err != nil {
			for j := 0; j <= i; j++ {
				err = vector.JoinAppendRollbackError(err, dst.Vecs[j].RollbackAppend(checkpoints[j], length))
			}
			return err
		}
		dst.Vecs[i].SetSorted(false)
	}
	dst.AddRowCount(length)
	return nil
}
