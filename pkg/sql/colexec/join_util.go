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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
	"unsafe"
)

type Batches struct {
	Buf     []*batch.Batch
	MemSize int64

	// Allocation tracking is enabled only by an owner with a retained-byte
	// budget. Mutations through this type then account only the touched batch.
	trackAllocation bool
	retainedBytes   int64
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
	bs.retainedBytes = 0
}

func (bs *Batches) CleanOnlyData() {
	for i := range bs.Buf {
		bs.Buf[i].CleanOnlyData()
	}
	bs.Buf = nil
	bs.MemSize = 0
	bs.retainedBytes = 0
}
func (bs *Batches) Reset() {
	bs.trackAllocation = false
	if bs.Buf != nil {
		bs.Buf = nil
	}
	bs.MemSize = 0
	bs.retainedBytes = 0
}

// copy from input batch into batches
// the batches structure hold data in fix size 8192 rows, and continue to append from next batch
// if error return , the batches will clean itself
func (bs *Batches) CopyIntoBatches(src *batch.Batch, proc *process.Process) (err error) {
	oldCapacity := cap(bs.Buf)
	defer func() {
		if err != nil {
			bs.Clean(proc.Mp())
		} else if bs.trackAllocation {
			bs.retainedBytes += int64(cap(bs.Buf)-oldCapacity) * int64(unsafe.Sizeof((*batch.Batch)(nil)))
		}
	}()

	if bs.Buf == nil {
		bs.Buf = make([]*batch.Batch, 0, 16)
	}

	var tmp *batch.Batch
	if src.RowCount() == DefaultBatchSize {
		tmp, err = src.Dup(proc.Mp())
		if err != nil {
			return err
		}
		bs.MemSize += int64(tmp.Size())
		if bs.trackAllocation {
			bs.retainedBytes += batchAllocated(tmp)
		}
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
		previous := int64(0)
		if lenBuf > 0 && bs.Buf[lenBuf-1].RowCount() != DefaultBatchSize {
			tmp = bs.Buf[lenBuf-1]
			if bs.trackAllocation {
				previous = batchAllocated(tmp)
			}
		} else {
			preAllocSize := min(length-offset, DefaultBatchSize)
			tmp, err = proc.NewBatchFromSrc(src, preAllocSize)
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
		if bs.trackAllocation {
			bs.retainedBytes += batchAllocated(tmp) - previous
		}
		offset += appendRows
	}
	return nil
}

func (bs *Batches) Shrink(ignoreRow *bitmap.Bitmap, proc *process.Process) error {
	if ignoreRow.Count() == 0 {
		return nil
	}

	ignoreRow.Negate()
	count := int64(ignoreRow.Count())
	sels := make([]int32, 0, count)
	itr := ignoreRow.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		sels = append(sels, int32(r))
	}

	n := (len(sels)-1)/DefaultBatchSize + 1
	newBuf := make([]*batch.Batch, n)
	for i := range newBuf {
		newBuf[i] = batch.NewOffHeapWithSize(len(bs.Buf[i].Vecs))
		for j, vec := range bs.Buf[0].Vecs {
			newBuf[i].Vecs[j] = vector.NewOffHeapVecWithType(*vec.GetType())
		}
		var newsels []int32
		if (i+1)*DefaultBatchSize <= len(sels) {
			newsels = sels[i*DefaultBatchSize : (i+1)*DefaultBatchSize]
		} else {
			newsels = sels[i*DefaultBatchSize:]
		}
		for _, sel := range newsels {
			idx1, idx2 := sel/DefaultBatchSize, sel%DefaultBatchSize
			for j, vec := range bs.Buf[idx1].Vecs {
				if err := newBuf[i].Vecs[j].UnionOne(vec, int64(idx2), proc.Mp()); err != nil {
					for k := 0; k <= i; k++ {
						newBuf[k].Clean(proc.Mp())
					}
					return err
				}
			}
		}
		newBuf[i].SetRowCount(len(newsels))
	}

	bs.Clean(proc.Mp())
	bs.Buf = newBuf
	if bs.trackAllocation {
		bs.retainedBytes = bs.Allocated()
	}

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
		area := src.Vecs[i].UnionBatchAreaBytes(offset, offset+length)
		if err = dst.Vecs[i].PreExtendWithArea(length, int(area), proc.Mp()); err != nil {
			return 0, err
		}
		if err = dst.Vecs[i].UnionBatch(src.Vecs[i], int64(offset), length, nil, proc.Mp()); err != nil {
			return 0, err
		}
		dst.Vecs[i].SetSorted(false)
	}
	dst.AddRowCount(length)
	return length, nil
}

// Allocated counts owned vector storage and Go metadata retained by the batch
// list. It intentionally does not use Size(), which counts logical lengths.
func (bs *Batches) Allocated() int64 {
	total := int64(cap(bs.Buf)) * int64(unsafe.Sizeof((*batch.Batch)(nil)))
	for _, bat := range bs.Buf {
		total += batchAllocated(bat)
	}
	return total
}

// TrackAllocation starts retained-byte accounting at the budget owner. Buf and
// its vectors must subsequently be mutated through Batches until transfer/reset.
func (bs *Batches) TrackAllocation() {
	if !bs.trackAllocation {
		bs.retainedBytes = bs.Allocated()
		bs.trackAllocation = true
	}
}

// RetainedBytes is constant time; Allocated remains the independent full recount.
func (bs *Batches) RetainedBytes() int64 {
	if !bs.trackAllocation {
		panic("batch allocation tracking is not enabled")
	}
	return bs.retainedBytes
}

func batchAllocated(bat *batch.Batch) int64 {
	total := batchAllocationMetadata(bat)
	for _, vec := range bat.Vecs {
		total += int64(vec.Allocated() + nulls.Allocated(vec.GetNulls()) + nulls.Allocated(vec.GetGrouping()))
	}
	return total
}

func batchAllocationMetadata(bat *batch.Batch) int64 {
	return int64(unsafe.Sizeof(batch.Batch{})) + int64(cap(bat.Vecs))*int64(unsafe.Sizeof((*vector.Vector)(nil))) + int64(len(bat.Vecs))*int64(unsafe.Sizeof(vector.Vector{}))
}

// AppendAllocation is the complete new storage required by CopyIntoBatches,
// while existing allocations remain live. Varlen append pre-extends its area
// once, using this same payload calculation, rather than growing per row.
func (bs *Batches) AppendAllocation(src *batch.Batch) int64 {
	add := func(a, b int64) int64 {
		if b < 0 || a > math.MaxInt64-b {
			return math.MaxInt64
		}
		return a + b
	}
	total := int64(0)
	if src.RowCount() == DefaultBatchSize {
		total = batchAllocationMetadata(src)
		for _, vec := range src.Vecs {
			// Dup flattens constants and uses UnionBatch's physical-area copy
			// policy. The destination growth (including per-column capacity
			// rounding and bitmaps) is identical to an empty vector append.
			empty := vector.NewVec(*vec.GetType())
			total = add(total, empty.AppendWindowAllocation(vec, 0, vec.Length()))
		}
	} else {
		offset, oldCount := 0, len(bs.Buf)
		if oldCount > 0 && bs.Buf[oldCount-1].RowCount() < DefaultBatchSize {
			last := bs.Buf[oldCount-1]
			rows := min(src.RowCount(), DefaultBatchSize-last.RowCount())
			for i, vec := range last.Vecs {
				total = add(total, vec.AppendWindowAllocation(src.Vecs[i], 0, rows))
			}
			offset = rows
		}
		for offset < src.RowCount() {
			rows := min(src.RowCount()-offset, DefaultBatchSize)
			total = add(total, batchAllocationMetadata(src))
			for _, vec := range src.Vecs {
				empty := vector.NewVec(*vec.GetType())
				total = add(total, empty.AppendWindowAllocation(vec, offset, offset+rows))
			}
			offset += rows
		}
	}
	// A growing list keeps its old backing array alive through append. Two times
	// required entries bounds the Go append capacity policy plus allocator rounding.
	required := len(bs.Buf) + (src.RowCount()+DefaultBatchSize-1)/DefaultBatchSize + 1
	if required > cap(bs.Buf) {
		total = add(total, int64(max(required*2, 16))*int64(unsafe.Sizeof((*batch.Batch)(nil)))+8192)
	}
	return total
}
