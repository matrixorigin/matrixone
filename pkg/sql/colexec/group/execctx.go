// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ResHashRelated struct {
	Hash     hashmap.HashMap
	Itr      hashmap.Iterator
	inserted []uint8
}

func (hr *ResHashRelated) IsEmpty() bool {
	return hr.Hash == nil || hr.Itr == nil
}

func (hr *ResHashRelated) BuildHashTable(
	rebuild bool,
	isStrHash bool, keyNullable bool, preAllocated uint64) error {

	if rebuild {
		if hr.Hash != nil {
			hr.Hash.Free()
			hr.Hash = nil
		}
	}

	if hr.Hash != nil {
		return nil
	}

	if isStrHash {
		h, err := hashmap.NewStrMap(keyNullable)
		if err != nil {
			return err
		}
		hr.Hash = h

		if hr.Itr == nil {
			hr.Itr = h.NewIterator()
		} else {
			hashmap.IteratorChangeOwner(hr.Itr, hr.Hash)
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
	hr.Hash = h

	if hr.Itr == nil {
		hr.Itr = h.NewIterator()
	} else {
		hashmap.IteratorChangeOwner(hr.Itr, hr.Hash)
	}
	if preAllocated > 0 {
		if err = h.PreAlloc(preAllocated); err != nil {
			return err
		}
	}
	return nil
}

func (hr *ResHashRelated) GetBinaryInsertList(vals []uint64, before uint64) (insertList []uint8, insertCount uint64) {
	if cap(hr.inserted) < len(vals) {
		hr.inserted = make([]uint8, len(vals))
	} else {
		hr.inserted = hr.inserted[:len(vals)]
	}

	insertCount = hr.Hash.GroupCount() - before

	last := before
	for k, val := range vals {
		if val > last {
			hr.inserted[k] = 1
			last++
		} else {
			hr.inserted[k] = 0
		}
	}
	return hr.inserted, insertCount
}

func (hr *ResHashRelated) Free0() {
	if hr.Hash != nil {
		hr.Hash.Free()
		hr.Hash = nil
	}
}

type GroupResultBuffer struct {
	ChunkSize int
	Popped    *batch.Batch
	ToPopped  []*batch.Batch
	AggList   []aggexec.AggFuncExec
}

func (buf *GroupResultBuffer) IsEmpty() bool {
	return len(buf.ToPopped) == 0
}

func (buf *GroupResultBuffer) InitOnlyAgg(chunkSize int, aggList []aggexec.AggFuncExec) {
	aggexec.SyncAggregatorsToChunkSize(aggList, chunkSize)

	buf.ChunkSize = chunkSize
	buf.AggList = aggList
	buf.ToPopped = make([]*batch.Batch, 0, 1)
	buf.ToPopped = append(buf.ToPopped, batch.NewOffHeapEmpty())
}

func (buf *GroupResultBuffer) InitWithGroupBy(chunkSize int, aggList []aggexec.AggFuncExec, groupByVec []*vector.Vector) {
	aggexec.SyncAggregatorsToChunkSize(aggList, chunkSize)

	buf.ChunkSize = chunkSize
	buf.AggList = aggList
	buf.ToPopped = make([]*batch.Batch, 0, 1)
	buf.ToPopped = append(buf.ToPopped, getInitialBatchWithSameTypeVecs(groupByVec))
}

func (buf *GroupResultBuffer) InitWithBatch(chunkSize int, aggList []aggexec.AggFuncExec, vecExampleBatch *batch.Batch) {
	aggexec.SyncAggregatorsToChunkSize(aggList, chunkSize)

	buf.ChunkSize = chunkSize
	buf.AggList = aggList
	buf.ToPopped = make([]*batch.Batch, 0, 1)
	buf.ToPopped = append(buf.ToPopped, getInitialBatchWithSameTypeVecs(vecExampleBatch.Vecs))
}

func (buf *GroupResultBuffer) AppendBatch(
	mp *mpool.MPool,
	vs []*vector.Vector,
	offset int, insertList []uint8) (rowIncrease int, err error) {

	spaceNonBatchExpand := buf.ChunkSize - buf.ToPopped[len(buf.ToPopped)-1].RowCount()
	toIncrease, k := countNonZeroAndFindKth(insertList, spaceNonBatchExpand)

	if toIncrease == 0 {
		return toIncrease, nil
	}

	if spaceNonBatchExpand > toIncrease {
		if err = buf.unionToSpecificBatch(mp, len(buf.ToPopped)-1, vs, int64(offset), insertList, toIncrease); err != nil {
			return toIncrease, err
		}
		return toIncrease, nil
	}

	if err = buf.unionToSpecificBatch(mp, len(buf.ToPopped)-1, vs, int64(offset), insertList[:k+1], spaceNonBatchExpand); err != nil {
		return toIncrease, err
	}

	buf.ToPopped = append(buf.ToPopped, getInitialBatchWithSameTypeVecs(vs))
	_, err = buf.AppendBatch(mp, vs, offset+k+1, insertList[k+1:])

	return toIncrease, err
}

func (buf *GroupResultBuffer) GetAggList() []aggexec.AggFuncExec {
	return buf.AggList
}

func (buf *GroupResultBuffer) unionToSpecificBatch(
	mp *mpool.MPool,
	idx int, vs []*vector.Vector, offset int64, insertList []uint8, rowIncrease int) error {
	for i, vec := range buf.ToPopped[idx].Vecs {
		if err := vec.UnionBatch(vs[i], offset, len(insertList), insertList, mp); err != nil {
			return err
		}
	}
	buf.ToPopped[idx].AddRowCount(rowIncrease)
	return nil
}

func (buf *GroupResultBuffer) DealPartialResult(partials []any) error {
	for i, agg := range buf.AggList {
		if len(partials) > i && partials[i] != nil {
			if err := agg.SetExtraInformation(partials[i], 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (buf *GroupResultBuffer) PopResult(m *mpool.MPool) (*batch.Batch, error) {
	buf.CleanLastPopped(m)

	if len(buf.ToPopped) == 0 {
		return nil, nil
	}

	if buf.AggList != nil {
		for i := range buf.AggList {
			vec, err := buf.AggList[i].Flush()
			if err != nil {
				return nil, err
			}

			for j := range vec {
				buf.ToPopped[j].Vecs = append(buf.ToPopped[j].Vecs, vec[j])
			}
		}

		for i := range buf.AggList {
			buf.AggList[i].Free()
		}
		buf.AggList = nil
	}

	buf.Popped = buf.ToPopped[0]
	buf.ToPopped = buf.ToPopped[1:]
	return buf.Popped, nil
}

func (buf *GroupResultBuffer) CleanLastPopped(m *mpool.MPool) {
	if buf.Popped != nil {
		buf.Popped.Clean(m)
		buf.Popped = nil
	}
}

func (buf *GroupResultBuffer) Free0(m *mpool.MPool) {
	for i := range buf.ToPopped {
		if buf.ToPopped[i] != nil {
			buf.ToPopped[i].Clean(m)
		}
	}
	for i := range buf.AggList {
		if buf.AggList[i] != nil {
			buf.AggList[i].Free()
		}
	}
	if buf.Popped != nil {
		buf.Popped.Clean(m)
	}

	buf.ToPopped, buf.Popped, buf.AggList = nil, nil, nil
}

type GroupResultNoneBlock struct {
	res *batch.Batch
}

func (r *GroupResultNoneBlock) resetLastPopped() {
	if r.res == nil {
		return
	}
	for _, ag := range r.res.Aggs {
		if ag != nil {
			ag.Free()
		}
	}
	for i := range r.res.Vecs {
		r.res.Vecs[i].CleanOnlyData()
	}
}

func (r *GroupResultNoneBlock) getResultBatch(
	proc *process.Process,
	gEval *ExprEvalVector,
	aEval []ExprEvalVector, aExpressions []aggexec.AggFuncExecExpression) (*batch.Batch, error) {
	var err error

	// prepare an OK result.
	if r.res == nil {
		r.res = batch.NewOffHeapWithSize(len(gEval.Vec))
		for i := range r.res.Vecs {
			r.res.Vecs[i] = vector.NewOffHeapVecWithType(*gEval.Vec[i].GetType())
		}
		r.res.Aggs = make([]aggexec.AggFuncExec, len(aExpressions))
	} else {
		if cap(r.res.Aggs) >= len(aExpressions) {
			r.res.Aggs = r.res.Aggs[:len(aExpressions)]
			for i := range r.res.Aggs {
				r.res.Aggs[i] = nil
			}
		} else {
			r.res.Aggs = make([]aggexec.AggFuncExec, len(aExpressions))
		}

		for i := range r.res.Vecs {
			r.res.Vecs[i].ResetWithSameType()
		}
		r.res.SetRowCount(0)
	}

	// set agg.
	for i := range r.res.Aggs {
		r.res.Aggs[i] = makeAggExec(
			proc, aExpressions[i].GetAggID(), aExpressions[i].IsDistinct(), aEval[i].Typ...)

		if config := aExpressions[i].GetExtraConfig(); config != nil {
			if err = r.res.Aggs[i].SetExtraInformation(config, 0); err != nil {
				return nil, err
			}
		}
	}
	return r.res, nil
}

func (r *GroupResultNoneBlock) Free0(m *mpool.MPool) {
	if r.res != nil {
		r.res.Clean(m)
		r.res = nil
	}
}

func getInitialBatchWithSameTypeVecs(src []*vector.Vector) *batch.Batch {
	b := batch.NewOffHeapWithSize(len(src))
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewOffHeapVecWithType(*src[i].GetType())
	}
	b.SetRowCount(0)
	return b
}

func countNonZeroAndFindKth(values []uint8, k int) (count int, kth int) {
	count = 0
	kth = -1
	if len(values) < k {
		for _, v := range values {
			if v == 0 {
				continue
			}
			count++
		}
		return count, kth
	}

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

	if kth != -1 {
		for i := kth + 1; i < len(values); i++ {
			if values[i] == 0 {
				continue
			}
			count++
		}
	}
	return count, kth
}
