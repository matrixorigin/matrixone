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

const (
	DefaultBatchMaxRow = 8192
)

// simple batch slice
// if CompactBatchs.Batchs[lastIdx].rowCount < DefaultBatchMaxRow
// just fill data to CompactBatchs.Batchs[lastIdx]
// until bats.Batchs[lastIdx].rowCount to  DefaultBatchMaxRow
type CompactBatchs struct {
	batchs []*Batch
}

func NewCompactBatchs() *CompactBatchs {
	batchs := &CompactBatchs{}

	return batchs
}

func (bats *CompactBatchs) Length() int {
	return len(bats.batchs)
}

func (bats *CompactBatchs) Get(idx int) *Batch {
	if idx >= len(bats.batchs) {
		return nil
	}
	return bats.batchs[idx]
}

func (bats *CompactBatchs) PopFront() *Batch {
	batchLen := bats.Length()
	if batchLen == 0 {
		return nil
	}
	bat := bats.batchs[0]
	bats.batchs = bats.batchs[1:]
	return bat
}

func (bats *CompactBatchs) Pop() *Batch {
	if len(bats.batchs) == 0 {
		return nil
	}
	last := len(bats.batchs) - 1
	bat := bats.batchs[last]
	bats.batchs = bats.batchs[:last]
	return bat
}

// Push  append inBatch to CompactBatchs.
// CompactBatchs will obtain ownership of inBatch
func (bats *CompactBatchs) Push(mpool *mpool.MPool, inBatch *Batch) error {
	batLen := bats.Length()
	var err error

	// empty input
	if inBatch.rowCount == 0 {
		return nil
	}

	// empty bats
	if batLen == 0 {
		bats.batchs = append(bats.batchs, inBatch)
		return nil
	}

	// fast path 1
	lastBatRowCount := bats.batchs[batLen-1].rowCount
	if lastBatRowCount == DefaultBatchMaxRow {
		bats.batchs = append(bats.batchs, inBatch)
		return nil
	}

	defer func() {
		inBatch.Clean(mpool)
	}()

	// fast path 2
	if lastBatRowCount+inBatch.RowCount() <= DefaultBatchMaxRow {
		bats.batchs[batLen-1], err = bats.batchs[batLen-1].Append(context.TODO(), mpool, inBatch)
		return err
	}

	// slow path
	return bats.fillData(mpool, inBatch)
}

// Extend  extend one batch'data to CompactBatchs
// CompactBatchs donot obtain ownership of inBatch
func (bats *CompactBatchs) Extend(mpool *mpool.MPool, inBatch *Batch) error {
	batLen := bats.Length()
	var err error
	var copyBat *Batch

	// empty input
	if inBatch.rowCount == 0 {
		return nil
	}

	copyBat, err = inBatch.Dup(mpool)
	if err != nil {
		return err
	}

	// empty bats
	if batLen == 0 {
		bats.batchs = append(bats.batchs, copyBat)
		return nil
	}

	// fast path 1
	lastIdx := batLen - 1
	if bats.batchs[lastIdx].rowCount == DefaultBatchMaxRow {
		bats.batchs = append(bats.batchs, copyBat)
		return nil
	}

	// fast path 2
	if copyBat.rowCount == DefaultBatchMaxRow {
		lastBat := bats.batchs[lastIdx]
		bats.batchs[lastIdx] = copyBat
		bats.batchs = append(bats.batchs, lastBat)
		return nil
	}

	defer func() {
		copyBat.Clean(mpool)
	}()

	return bats.fillData(mpool, copyBat)
}

// Union  union some data from one batch to CompactBatchs
func (bats *CompactBatchs) Union(mpool *mpool.MPool, inBatch *Batch, sels []int32) error {
	selsLen := len(sels)
	if selsLen == 0 {
		return nil
	}
	if selsLen == inBatch.RowCount() {
		return bats.Extend(mpool, inBatch)
	}
	if selsLen > inBatch.RowCount() {
		panic("sels len > inBatch.RowCount()")
	}

	if bats.Length() == 0 {
		tmpBat := NewWithSize(len(inBatch.Vecs))
		for i := range tmpBat.Vecs {
			tmpBat.Vecs[i] = vector.NewVec(*inBatch.Vecs[i].GetType())
			err := tmpBat.Vecs[i].UnionInt32(inBatch.Vecs[i], sels, mpool)
			if err != nil {
				return err
			}
		}
		tmpBat.rowCount = tmpBat.Vecs[0].Length()
		bats.batchs = append(bats.batchs, tmpBat)
		return nil
	}

	batLen := bats.Length()
	lastBat := bats.batchs[batLen-1]
	firstSelsLen := DefaultBatchMaxRow - lastBat.rowCount
	if firstSelsLen > selsLen {
		firstSelsLen = selsLen
	}
	firstSels := sels[:firstSelsLen]
	for i := range lastBat.Vecs {
		err := lastBat.Vecs[i].UnionInt32(inBatch.Vecs[i], firstSels, mpool)
		if err != nil {
			return err
		}
	}
	lastBat.rowCount = lastBat.Vecs[0].Length()

	newSels := sels[firstSelsLen:]
	for len(newSels) > 0 {
		tmpSize := len(newSels)
		if tmpSize > DefaultBatchMaxRow {
			tmpSize = DefaultBatchMaxRow
		}
		tmpSels := newSels[:tmpSize]
		tmpBat := NewWithSize(len(inBatch.Vecs))
		for i := range tmpBat.Vecs {
			tmpBat.Vecs[i] = vector.NewVec(*inBatch.Vecs[i].GetType())
			err := tmpBat.Vecs[i].UnionInt32(inBatch.Vecs[i], tmpSels, mpool)
			if err != nil {
				return err
			}
		}
		tmpBat.rowCount = tmpBat.Vecs[0].Length()
		bats.batchs = append(bats.batchs, tmpBat)
		newSels = newSels[tmpSize:]
	}
	return nil
}

func (bats *CompactBatchs) RowCount() int {
	rowCount := 0
	for _, bat := range bats.batchs {
		rowCount += bat.rowCount
	}
	return rowCount
}

func (bats *CompactBatchs) Clean(mpool *mpool.MPool) {
	for _, bat := range bats.batchs {
		bat.Clean(mpool)
	}
	bats.batchs = nil
}

func (bats *CompactBatchs) TakeBatchs() []*Batch {
	batchs := bats.batchs
	bats.batchs = nil
	return batchs
}

func (bats *CompactBatchs) fillData(mpool *mpool.MPool, inBatch *Batch) error {
	batLen := bats.Length()
	var tmpBat *Batch
	var err error

	//fill data
	start, end := 0, inBatch.RowCount()
	isNewBat := false
	for start < end {

		if bats.batchs[batLen-1].rowCount < DefaultBatchMaxRow {
			tmpBat = bats.batchs[batLen-1]
			isNewBat = false
		} else {
			tmpBat = NewWithSize(len(inBatch.Vecs))
			bats.batchs = append(bats.batchs, tmpBat)
			isNewBat = true
		}

		addRowCount := end - start
		if left := DefaultBatchMaxRow - tmpBat.RowCount(); addRowCount > left {
			addRowCount = left
		}

		if isNewBat {
			for i := range tmpBat.Vecs {
				tmpBat.Vecs[i], err = inBatch.Vecs[i].CloneWindow(start, start+addRowCount, mpool)
				if err != nil {
					return err
				}
			}
			tmpBat.AddRowCount(addRowCount)
		} else {
			err := tmpBat.UnionWindow(inBatch, start, addRowCount, mpool)
			if err != nil {
				return err
			}
		}

		start = start + addRowCount
	}

	return nil
}
