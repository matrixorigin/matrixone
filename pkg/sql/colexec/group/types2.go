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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

type ResHashRelated struct {
	Hash     hashmap.HashMap
	Itr      hashmap.Iterator
	inserted []uint8
}

func (ghr *ResHashRelated) BuildHashTable(
	rebuild bool,
	isStrHash bool, keyNullable bool, preAllocated uint64) error {

	if rebuild {
		if ghr.Hash != nil {
			ghr.Hash.Free()
			ghr.Hash = nil
		}
	}

	if ghr.Hash != nil {
		return nil
	}

	if isStrHash {
		h, err := hashmap.NewStrMap(keyNullable)
		if err != nil {
			return err
		}
		ghr.Hash = h

		if ghr.Itr != nil {
			ghr.Itr = h.NewIterator()
		}
		if preAllocated > 0 {
			if err = h.PreAlloc(preAllocated); err != nil {
				return err
			}
		}
		return nil
	}

	h, err := hashmap.NewIntHashMap(keyNullable)
	if err != nil {
		return err
	}
	ghr.Hash = h

	if ghr.Itr != nil {
		ghr.Itr = h.NewIterator()
	}
	if preAllocated > 0 {
		if err = h.PreAlloc(preAllocated); err != nil {
			return err
		}
	}
	return nil
}

func (ghr *ResHashRelated) GetNewList(vals []uint64, before uint64) (insertList []uint8, insertCount uint64) {
	if cap(ghr.inserted) < len(vals) {
		ghr.inserted = make([]uint8, len(vals))
	} else {
		ghr.inserted = ghr.inserted[:len(vals)]
	}

	insertCount = ghr.Hash.GroupCount() - before
	for k, val := range vals {
		if val > before {
			ghr.inserted[k] = 1
		} else {
			ghr.inserted[k] = 0
		}
	}
	return ghr.inserted, insertCount
}

func (ghr *ResHashRelated) Free0() {
	if ghr.Hash != nil {
		ghr.Hash.Free()
		ghr.Hash = nil
	}
}

type BlockingGroupRelated struct {
	ChunkSize int
	Popped    *batch.Batch
	ToPopped  []*batch.Batch
	AggList   []aggexec.AggFuncExec
}

func (bgr *BlockingGroupRelated) IsEmpty() bool {
	return len(bgr.ToPopped) == 0
}

func (bgr *BlockingGroupRelated) Init(chunkSize int, aggList []aggexec.AggFuncExec, vecExampleBatch *batch.Batch) {
	bgr.ChunkSize = chunkSize
	bgr.AggList = aggList
	bgr.ToPopped = make([]*batch.Batch, 0, 1)
	bgr.ToPopped = append(bgr.ToPopped, getInitialBatchWithSameType(vecExampleBatch))
}

func (bgr *BlockingGroupRelated) AppendBatch(
	mp *mpool.MPool,
	bat *batch.Batch, offset int, insertList []uint8) (rowIncrease int, err error) {

	spaceNonBatchExpand := bgr.ChunkSize - bgr.ToPopped[len(bgr.ToPopped)-1].RowCount()
	toIncrease, k := countNonZeroAndFindKth(insertList, spaceNonBatchExpand)

	if toIncrease == 0 {
		return toIncrease, nil
	}

	if spaceNonBatchExpand > toIncrease {
		if err = bgr.unionToSpecificBatch(mp, len(bgr.ToPopped)-1, bat, int64(offset), insertList, toIncrease); err != nil {
			return toIncrease, err
		}
		return toIncrease, nil
	}

	if err = bgr.unionToSpecificBatch(mp, len(bgr.ToPopped)-1, bat, int64(offset), insertList[:k+1], spaceNonBatchExpand); err != nil {
		return toIncrease, err
	}

	bgr.ToPopped = append(bgr.ToPopped, getInitialBatchWithSameType(bat))
	_, err = bgr.AppendBatch(mp, bat, offset+k+1, insertList[k+1:])

	return toIncrease, err
}

func (bgr *BlockingGroupRelated) unionToSpecificBatch(
	mp *mpool.MPool,
	idx int, bat *batch.Batch, offset int64, insertList []uint8, rowIncrease int) error {
	for i, vec := range bgr.ToPopped[idx].Vecs {
		if err := vec.UnionBatch(bat.Vecs[i], offset, len(insertList), insertList, mp); err != nil {
			return err
		}
	}
	bgr.ToPopped[idx].AddRowCount(rowIncrease)
	return nil
}

func (bgr *BlockingGroupRelated) DealPartialResult(partials []any) error {
	for i, agg := range bgr.AggList {
		if len(partials) > i && partials[i] != nil {
			if err := agg.SetExtraInformation(partials[i], 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bgr *BlockingGroupRelated) PopResult(m *mpool.MPool) (*batch.Batch, error) {
	if bgr.Popped != nil {
		bgr.Popped.Clean(m)
		bgr.Popped = nil
	}

	if len(bgr.ToPopped) == 0 {
		return nil, nil
	}

	if bgr.AggList != nil {
		for i := range bgr.AggList {
			vec, err := bgr.AggList[i].Flush()
			if err != nil {
				return nil, err
			}

			for j := range vec {
				bgr.ToPopped[i].Vecs = append(bgr.ToPopped[i].Vecs, vec[j])
			}
		}

		for i := range bgr.AggList {
			bgr.AggList[i].Free()
		}
		bgr.AggList = nil
	}

	bgr.Popped = bgr.ToPopped[0]
	bgr.ToPopped = bgr.ToPopped[1:]
	return bgr.Popped, nil
}

func (bgr *BlockingGroupRelated) Free0(m *mpool.MPool) {
	for i := range bgr.ToPopped {
		if bgr.ToPopped[i] != nil {
			bgr.ToPopped[i].Clean(m)
		}
	}
	for i := range bgr.AggList {
		if bgr.AggList[i] != nil {
			bgr.AggList[i].Free()
		}
	}
	if bgr.Popped != nil {
		bgr.Popped.Clean(m)
	}

	bgr.ToPopped, bgr.Popped, bgr.AggList = nil, nil, nil
}

func getInitialBatchWithSameType(src *batch.Batch) *batch.Batch {
	b := batch.NewOffHeapWithSize(len(src.Vecs))
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewOffHeapVecWithType(*src.Vecs[i].GetType())
	}
	b.SetRowCount(0)
	return b
}

func countNonZeroAndFindKth(values []uint8, k int) (int, int) {
	count := 0
	kth := -1

	for i, v := range values {
		if v == 0 {
			continue
		}

		count++
		if count == k {
			kth = i
			break
		}
	}

	for i := kth + 1; i < len(values); i++ {
		if values[i] == 0 {
			continue
		}
		count++
	}
	return count, kth
}
