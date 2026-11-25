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

package batch

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// BatchSet collects rows copied from input batches into batches of at most
// maxRows rows. Rows fill the last batch before a new one starts, so every
// batch but the last is full, except one closed early because the rows after
// it were copied through a different pool: a vector belongs to exactly one
// pool, so a batch never takes rows through another.
//
// The set owns its batches. What to do with them (when to hand them out, how
// much memory they may take, whether to spill) is the caller's policy.
type BatchSet struct {
	batches []*Batch
	maxRows int
}

func NewBatchSet(maxRows int) *BatchSet {
	if maxRows <= 0 {
		panic("batch set needs a positive row limit")
	}
	return &BatchSet{maxRows: maxRows}
}

func (bs *BatchSet) Length() int {
	return len(bs.batches)
}

func (bs *BatchSet) Get(idx int) *Batch {
	if idx < 0 || idx >= len(bs.batches) {
		return nil
	}
	return bs.batches[idx]
}

func (bs *BatchSet) RowCount() int {
	rows := 0
	for _, bat := range bs.batches {
		rows += bat.rowCount
	}
	return rows
}

// ReadyCount returns how many batches are complete: all but the last, and the
// last too when it is full.
func (bs *BatchSet) ReadyCount() int {
	n := len(bs.batches)
	if n == 0 || bs.batches[n-1].rowCount < bs.maxRows {
		return max(0, n-1)
	}
	return n
}

// ReadyDelta returns how many more batches become complete if count rows of
// src are appended through mp.
func (bs *BatchSet) ReadyDelta(src *Batch, mp *mpool.MPool, count int) int {
	if count <= 0 {
		return 0
	}
	tailRows := 0
	if n := len(bs.batches); n > 0 && bs.batches[n-1].rowCount < bs.maxRows {
		if bs.writableTail(src, mp) == nil {
			// The partial tail closes; the rows start new batches.
			return 1 + count/bs.maxRows
		}
		tailRows = bs.batches[n-1].rowCount
	}
	return (tailRows+count)/bs.maxRows - tailRows/bs.maxRows
}

// PopFront removes and returns the first batch, or nil.
func (bs *BatchSet) PopFront() *Batch {
	if len(bs.batches) == 0 {
		return nil
	}
	bat := bs.batches[0]
	bs.batches[0] = nil
	bs.batches = bs.batches[1:]
	return bat
}

// Pop removes and returns the last batch, or nil.
func (bs *BatchSet) Pop() *Batch {
	n := len(bs.batches)
	if n == 0 {
		return nil
	}
	bat := bs.batches[n-1]
	bs.batches[n-1] = nil
	bs.batches = bs.batches[:n-1]
	return bat
}

// TakeBatches removes and returns all batches; the caller owns them.
func (bs *BatchSet) TakeBatches() []*Batch {
	batches := bs.batches
	bs.batches = nil
	return batches
}

func (bs *BatchSet) Clean(mp *mpool.MPool) {
	for _, bat := range bs.batches {
		bat.Clean(mp)
	}
	bs.batches = nil
}

// Append copies rows [offset, offset+count) of src. When the set needs a new
// batch it takes reuse (emptied) if reuse fits src and mp, and reports whether
// it did; an untaken reuse stays the caller's.
func (bs *BatchSet) Append(mp *mpool.MPool, src *Batch, offset, count int, reuse *Batch) (bool, error) {
	if err := requireCompacted("BatchSet.Append", src); err != nil {
		return false, err
	}
	return bs.appendRows(mp, src, offset, count, nil, reuse)
}

// AppendSels copies the rows of src at sels, in that order, like Append.
// sels must not repeat a row: a selection of every row of src is copied as
// the whole batch.
func (bs *BatchSet) AppendSels(mp *mpool.MPool, src *Batch, sels []int32, reuse *Batch) (bool, error) {
	if len(sels) > src.RowCount() {
		panic("batch set selection is longer than its batch")
	}
	if len(sels) == src.RowCount() {
		return bs.Append(mp, src, 0, len(sels), reuse)
	}
	return bs.appendRows(mp, src, 0, len(sels), sels, reuse)
}

// Push gives owned to the set. It becomes a batch of its own when the last
// batch cannot take its rows; otherwise they are copied and owned is freed.
func (bs *BatchSet) Push(mp *mpool.MPool, owned *Batch) error {
	if owned.rowCount > 0 && owned.rowCount <= bs.maxRows && bs.writableTail(owned, mp) == nil {
		bs.batches = append(bs.batches, owned)
		return nil
	}
	defer owned.Clean(mp)
	_, err := bs.Append(mp, owned, 0, owned.rowCount, nil)
	return err
}

// appendRows fills the last batch, then new ones, with count rows of src:
// sels[:count] when sels is set, else the rows from offset. A batch is the
// set's before it is written, so an error leaves nothing unowned.
func (bs *BatchSet) appendRows(
	mp *mpool.MPool,
	src *Batch,
	offset, count int,
	sels []int32,
	reuse *Batch,
) (consumed bool, err error) {
	if count <= 0 {
		return false, nil
	}
	// Most appends fit in a last batch that already has rows.
	if tail := bs.writableTail(src, mp); tail != nil && tail.rowCount > 0 &&
		count <= bs.maxRows-tail.rowCount {
		return false, copyRows(tail, src, offset, sels, count, mp, false)
	}
	for done := 0; done < count; {
		dst := bs.writableTail(src, mp)
		fresh := dst == nil || dst.rowCount == 0
		if dst == nil {
			if dst = newBatchLike(src, mp, reuse); dst == reuse {
				consumed, reuse = true, nil
			}
			bs.batches = append(bs.batches, dst)
		}
		n := min(count-done, bs.maxRows-dst.rowCount)
		var part []int32
		if sels != nil {
			part = sels[done : done+n]
		}
		if err = copyRows(dst, src, offset+done, part, n, mp, fresh); err != nil {
			return
		}
		done += n
	}
	return
}

// copyRows copies n rows of src into dst: those at sels, or if sels is nil
// the run from offset. A contiguous run into an empty batch keeps each
// column's sorted flag; anything else may be unsorted.
func copyRows(dst, src *Batch, offset int, sels []int32, n int, mp *mpool.MPool, fresh bool) error {
	keepSorted := fresh && sels == nil
	for i, vec := range dst.Vecs {
		var err error
		if sels == nil {
			err = vec.UnionBatch(src.Vecs[i], int64(offset), n, nil, mp)
		} else {
			err = vec.UnionInt32(src.Vecs[i], sels, mp)
		}
		if err != nil {
			return err
		}
		vec.SetSorted(keepSorted && src.Vecs[i].GetSorted())
	}
	dst.rowCount += n
	return nil
}

// writableTail returns the last batch if it can take rows of src through mp:
// not full, same columns, and every vector from mp.
func (bs *BatchSet) writableTail(src *Batch, mp *mpool.MPool) *Batch {
	n := len(bs.batches)
	if n == 0 || bs.batches[n-1].rowCount >= bs.maxRows {
		return nil
	}
	if tail := bs.batches[n-1]; fits(tail, src, mp) {
		return tail
	}
	return nil
}

// newBatchLike returns reuse, emptied, if it fits src and mp, else a new
// empty batch with src's column types allocating from mp.
func newBatchLike(src *Batch, mp *mpool.MPool, reuse *Batch) *Batch {
	if fits(reuse, src, mp) {
		reuse.CleanOnlyData()
		return reuse
	}
	bat := NewWithSize(len(src.Vecs))
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.NewVec(*src.Vecs[i].GetType(), mp)
	}
	return bat
}

// fits reports whether rows of src can be copied into dst through mp.
func fits(dst, src *Batch, mp *mpool.MPool) bool {
	if dst == nil || len(dst.Vecs) != len(src.Vecs) {
		return false
	}
	for i, vec := range dst.Vecs {
		if vec == nil || src.Vecs[i] == nil || vec.MPool() != mp {
			return false
		}
	}
	return true
}
