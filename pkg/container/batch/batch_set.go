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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// simple batch slice
// if BatchSet.batches[lastIdx].rowCount < batchMaxRow
// just fill data to BatchSet.batches[lastIdx]
// until BatchSet.batches[lastIdx].rowCount to batchMaxRow
type BatchSet struct {
	batches     []*Batch
	batchMaxRow int
}

func NewBatchSet(batchMaxRow int) *BatchSet {
	bs := &BatchSet{
		batchMaxRow: batchMaxRow,
	}

	return bs
}

func (bs *BatchSet) Length() int {
	return len(bs.batches)
}

func (bs *BatchSet) Get(idx int) *Batch {
	if idx >= len(bs.batches) {
		return nil
	}
	return bs.batches[idx]
}

func (bs *BatchSet) PopFront() *Batch {
	batchLen := bs.Length()
	if batchLen == 0 {
		return nil
	}
	bat := bs.batches[0]
	bs.batches = bs.batches[1:]
	return bat
}

func (bs *BatchSet) Pop() *Batch {
	if len(bs.batches) == 0 {
		return nil
	}
	last := len(bs.batches) - 1
	bat := bs.batches[last]
	bs.batches = bs.batches[:last]
	return bat
}

// Push  append inBatch to BatchSet.
// BatchSet will obtain ownership of inBatch
func (bs *BatchSet) Push(mpool *mpool.MPool, inBatch *Batch) error {
	batLen := bs.Length()
	var err error

	// empty input
	if inBatch.rowCount == 0 {
		return nil
	}

	// empty bats
	if batLen == 0 {
		bs.batches = append(bs.batches, inBatch)
		return nil
	}

	// fast path 1
	lastBatRowCount := bs.batches[batLen-1].rowCount
	if lastBatRowCount == bs.batchMaxRow {
		bs.batches = append(bs.batches, inBatch)
		return nil
	}

	defer func() {
		inBatch.Clean(mpool)
	}()

	// fast path 2
	if lastBatRowCount+inBatch.RowCount() <= bs.batchMaxRow {
		bs.batches[batLen-1], err = bs.batches[batLen-1].Append(context.TODO(), mpool, inBatch)
		return err
	}

	// slow path
	_, err = bs.fillData(mpool, inBatch, nil)
	return err
}

// Extend  extend one batch'data to BatchSet, returns whether reuseBuf was consumed
// BatchSet donot obtain ownership of inBatch
func (bs *BatchSet) Extend(mpool *mpool.MPool, inBatch *Batch, reuseBuf *Batch) (bool, error) {
	batLen := bs.Length()
	var err error

	// empty input
	if inBatch.rowCount == 0 {
		return false, nil
	}

	// empty bats or last batch is full - can directly use fast path
	lastIdx := batLen - 1
	if batLen == 0 || bs.batches[lastIdx].rowCount == bs.batchMaxRow {
		if reuseBuf != nil && len(reuseBuf.Vecs) == len(inBatch.Vecs) {
			reuseBuf.CleanOnlyData()
			reuseBuf, err = reuseBuf.AppendWithCopy(context.TODO(), mpool, inBatch)
			if err != nil {
				return false, err
			}
			bs.batches = append(bs.batches, reuseBuf)
			return true, nil
		}
		copyBat, err := inBatch.Clone(mpool, true)
		if err != nil {
			return false, err
		}
		bs.batches = append(bs.batches, copyBat)
		return false, nil
	}

	// fast path 2: inBatch is full
	if inBatch.rowCount == bs.batchMaxRow {
		if reuseBuf != nil && len(reuseBuf.Vecs) == len(inBatch.Vecs) {
			reuseBuf.CleanOnlyData()
			reuseBuf, err = reuseBuf.AppendWithCopy(context.TODO(), mpool, inBatch)
			if err != nil {
				return false, err
			}
			lastBat := bs.batches[lastIdx]
			bs.batches[lastIdx] = reuseBuf
			bs.batches = append(bs.batches, lastBat)
			return true, nil
		}
		copyBat, err := inBatch.Clone(mpool, true)
		if err != nil {
			return false, err
		}
		lastBat := bs.batches[lastIdx]
		bs.batches[lastIdx] = copyBat
		bs.batches = append(bs.batches, lastBat)
		return false, nil
	}

	// slow path: need to split data
	return bs.fillData(mpool, inBatch, reuseBuf)
}

// Union  union some data from one batch to BatchSet, returns whether reuseBuf was consumed
func (bs *BatchSet) Union(mpool *mpool.MPool, inBatch *Batch, sels []int32, reuseBuf *Batch) (bool, error) {
	selsLen := len(sels)
	if selsLen == 0 {
		return false, nil
	}
	if selsLen == inBatch.RowCount() {
		return bs.Extend(mpool, inBatch, reuseBuf)
	}
	if selsLen > inBatch.RowCount() {
		panic("sels len > inBatch.RowCount()")
	}

	consumed := false
	if bs.Length() == 0 {
		// Handle large sels: split into multiple batches if selsLen > batchMaxRow
		remainingSels := sels
		for len(remainingSels) > 0 {
			tmpSize := len(remainingSels)
			if tmpSize > bs.batchMaxRow {
				tmpSize = bs.batchMaxRow
			}
			tmpSels := remainingSels[:tmpSize]
			tmpBat, err := bs.getOrCreateBatch(inBatch, reuseBuf, mpool)
			if err != nil {
				return consumed, err
			}
			if tmpBat == reuseBuf {
				consumed = true
				reuseBuf = nil
			}
			for i := range tmpBat.Vecs {
				err := tmpBat.Vecs[i].UnionInt32(inBatch.Vecs[i], tmpSels, mpool)
				if err != nil {
					return consumed, err
				}
			}
			tmpBat.rowCount = tmpBat.Vecs[0].Length()
			bs.batches = append(bs.batches, tmpBat)
			remainingSels = remainingSels[tmpSize:]
		}
		return consumed, nil
	}

	batLen := bs.Length()
	lastBat := bs.batches[batLen-1]
	firstSelsLen := bs.batchMaxRow - lastBat.rowCount
	if firstSelsLen > selsLen {
		firstSelsLen = selsLen
	}
	firstSels := sels[:firstSelsLen]
	for i := range lastBat.Vecs {
		err := lastBat.Vecs[i].UnionInt32(inBatch.Vecs[i], firstSels, mpool)
		if err != nil {
			return consumed, err
		}
	}
	lastBat.rowCount = lastBat.Vecs[0].Length()

	newSels := sels[firstSelsLen:]
	for len(newSels) > 0 {
		tmpSize := len(newSels)
		if tmpSize > bs.batchMaxRow {
			tmpSize = bs.batchMaxRow
		}
		tmpSels := newSels[:tmpSize]
		tmpBat, err := bs.getOrCreateBatch(inBatch, reuseBuf, mpool)
		if err != nil {
			return consumed, err
		}
		if tmpBat == reuseBuf {
			consumed = true
			reuseBuf = nil
		}
		for i := range tmpBat.Vecs {
			err := tmpBat.Vecs[i].UnionInt32(inBatch.Vecs[i], tmpSels, mpool)
			if err != nil {
				return consumed, err
			}
		}
		tmpBat.rowCount = tmpBat.Vecs[0].Length()
		bs.batches = append(bs.batches, tmpBat)
		newSels = newSels[tmpSize:]
	}
	return consumed, nil
}

func (bs *BatchSet) getOrCreateBatch(inBatch *Batch, reuseBuf *Batch, mpool *mpool.MPool) (*Batch, error) {
	if reuseBuf != nil && len(reuseBuf.Vecs) == len(inBatch.Vecs) {
		reuseBuf.CleanOnlyData()
		return reuseBuf, nil
	}
	tmpBat := NewWithSize(len(inBatch.Vecs))
	for i := range tmpBat.Vecs {
		tmpBat.Vecs[i] = vector.NewVec(*inBatch.Vecs[i].GetType())
		if err := tmpBat.Vecs[i].PreExtend(bs.batchMaxRow, mpool); err != nil {
			return nil, err
		}
	}
	return tmpBat, nil
}

func (bs *BatchSet) RowCount() int {
	rowCount := 0
	for _, bat := range bs.batches {
		rowCount += bat.rowCount
	}
	return rowCount
}

func (bs *BatchSet) Clean(mpool *mpool.MPool) {
	for _, bat := range bs.batches {
		bat.Clean(mpool)
	}
	bs.batches = nil
}

func (bs *BatchSet) TakeBatches() []*Batch {
	batches := bs.batches
	bs.batches = nil
	return batches
}

func (bs *BatchSet) fillData(mpool *mpool.MPool, inBatch *Batch, reuseBuf *Batch) (bool, error) {
	batLen := bs.Length()
	var (
		tmpBat *Batch
		err    error
	)
	consumed := false

	//fill data
	start, end := 0, inBatch.RowCount()
	for start < end {
		if bs.batches[batLen-1].rowCount < bs.batchMaxRow {
			tmpBat = bs.batches[batLen-1]
		} else {
			tmpBat, err = bs.getOrCreateBatch(inBatch, reuseBuf, mpool)
			if err != nil {
				return consumed, err
			}
			if tmpBat == reuseBuf {
				reuseBuf = nil
				consumed = true
			}
			bs.batches = append(bs.batches, tmpBat)
		}

		addRowCount := min(end-start, bs.batchMaxRow-tmpBat.RowCount())
		if err := tmpBat.UnionWindow(inBatch, start, addRowCount, mpool); err != nil {
			return consumed, err
		}
		start = start + addRowCount
	}

	return consumed, nil
}
