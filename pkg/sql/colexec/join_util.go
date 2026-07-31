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
}

func (bs *Batches) RowCount() int {
	var count int
	for _, b := range bs.Buf {
		count += b.RowCount()
	}
	return count
}

func (bs *Batches) Clean(mp *mpool.MPool) {
	for i := range bs.Buf {
		bs.Buf[i].Clean(mp)
	}
	bs.Buf = nil
	bs.MemSize = 0
}

func (bs *Batches) CleanOnlyData() {
	for i := range bs.Buf {
		bs.Buf[i].CleanOnlyData()
	}
	bs.Buf = nil
	bs.MemSize = 0
}
func (bs *Batches) Reset() {
	if bs.Buf != nil {
		bs.Buf = nil
	}
	bs.MemSize = 0
}

// copy from input batch into batches
// the batches structure hold data in fix size 8192 rows, and continue to append from next batch
// if error return , the batches will clean itself
func (bs *Batches) CopyIntoBatches(src *batch.Batch, proc *process.Process) (err error) {
	return bs.CopyIntoBatchesWithAllocation(src, proc, nil)
}

// CopyIntoBatchesWithAllocation selects provenance for every retained vector
// destination. The Go descriptors are bounded by one Batch per 8,192 rows and
// one Vector pointer per input column; physical data, area, null, and grouping
// buffers are allocation-accounted and remain owned by the copied batches.
func (bs *Batches) CopyIntoBatchesWithAllocation(
	src *batch.Batch,
	proc *process.Process,
	selection *vector.AllocationAccountSelection,
) (err error) {
	defer func() {
		if err != nil {
			bs.Clean(proc.Mp())
		}
	}()

	if bs.Buf == nil {
		bs.Buf = make([]*batch.Batch, 0, 16)
	}
	if len(bs.Buf) > 0 &&
		bs.Buf[len(bs.Buf)-1].AllocationAccountSelection() != selection {
		return mpool.ErrAllocationAccountMismatch
	}

	var tmp *batch.Batch
	if src.RowCount() == DefaultBatchSize {
		if selection == nil {
			tmp, err = src.Dup(proc.Mp())
		} else {
			tmp, err = proc.NewBatchFromSrcWithAllocation(src, 0, selection)
			if err == nil {
				err = src.CloneTo(tmp, proc.Mp())
			}
		}
		if err != nil {
			return err
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
	appendRows := 0
	length := src.RowCount()

	for offset < length {
		lenBuf := len(bs.Buf)
		if lenBuf > 0 && bs.Buf[lenBuf-1].RowCount() != DefaultBatchSize {
			tmp = bs.Buf[lenBuf-1]
			if tmp.AllocationAccountSelection() != selection {
				return mpool.ErrAllocationAccountMismatch
			}
		} else {
			preAllocSize := length - offset
			if preAllocSize > DefaultBatchSize {
				preAllocSize = DefaultBatchSize
			}
			tmp, err = proc.NewBatchFromSrcWithAllocation(
				src,
				preAllocSize,
				selection,
			)
			if err != nil {
				return err
			}
			bs.Buf = append(bs.Buf, tmp)
		}
		appendRows, err = appendToFixedSizeFromOffset(tmp, src, offset, proc)
		if err != nil {
			return err
		}
		if tmp.RowCount() == DefaultBatchSize {
			bs.MemSize += int64(tmp.Size())
		}
		offset += appendRows
	}
	return nil
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
	selection := bs.Buf[0].AllocationAccountSelection()
	newBuf := make([]*batch.Batch, n)
	cleanup := true
	defer func() {
		if cleanup {
			for _, bat := range newBuf {
				if bat != nil {
					bat.Clean(proc.Mp())
				}
			}
			// Preserve the caller's ignore-row checkpoint on failure.
			ignoreRow.Negate()
		}
	}()
	for i := range newBuf {
		newBuf[i] = batch.NewOffHeapWithSize(len(bs.Buf[0].Vecs))
		if err := newBuf[i].SetAllocationAccount(selection); err != nil {
			return err
		}
		for j, vec := range bs.Buf[0].Vecs {
			newBuf[i].SetVector(int32(j), vector.NewOffHeapVecWithType(*vec.GetType()))
		}
	}
	itr := ignoreRow.Iterator()
	outRow := 0
	for itr.HasNext() {
		sel := int(itr.Next())
		srcBatch, srcRow := sel/DefaultBatchSize, sel%DefaultBatchSize
		dstBatch := outRow / DefaultBatchSize
		for j, vec := range bs.Buf[srcBatch].Vecs {
			if err := newBuf[dstBatch].Vecs[j].UnionOne(vec, int64(srcRow), proc.Mp()); err != nil {
				return err
			}
		}
		newBuf[dstBatch].AddRowCount(1)
		outRow++
	}

	bs.Clean(proc.Mp())
	bs.Buf = newBuf
	for _, bat := range newBuf {
		bs.MemSize += int64(bat.Size())
	}
	cleanup = false

	return nil
}

func appendToFixedSizeFromOffset(dst *batch.Batch, src *batch.Batch, offset int, proc *process.Process) (int, error) {
	var err error
	if dst == nil {
		panic("should not be nil")
	}
	if dst.RowCount() >= DefaultBatchSize {
		panic("can't call AppendToFixedSizeFromOffset when batch is full!")
	}
	if len(dst.Vecs) != len(src.Vecs) {
		return 0, moerr.NewInternalError(proc.Ctx, "unexpected error happens in batch append")
	}
	length := DefaultBatchSize - dst.RowCount()
	if length+offset > src.RowCount() {
		length = src.RowCount() - offset
	}
	for i := range dst.Vecs {
		if err = dst.Vecs[i].UnionBatch(src.Vecs[i], int64(offset), length, nil, proc.Mp()); err != nil {
			return 0, err
		}
		dst.Vecs[i].SetSorted(false)
	}
	dst.AddRowCount(length)
	return length, nil
}
