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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

// getInputBatch return the received data from its last operator.
//
// a description about what data does this operator will receive.
// 1. batch with group-by column and agg-middle result of each group.
// 2. batch with only agg-middle result.
// 3. nil batch.
//
// all the agg-middle results take a same chunk size,
// and the row of group-by column is always not more than the size.
func (mergeGroup *MergeGroup) getInputBatch(proc *process.Process) (*batch.Batch, error) {
	r, err := vm.ChildrenCall(mergeGroup.GetChildren(0), proc, mergeGroup.OpAnalyzer)
	return r.Batch, err
}

// consumeInputBatch
// 1. put batch into hashtable, and get a group list.
// 2. use group list to union new row and do the agg merge work.
func (r *GroupResult) consumeInputBatch(
	proc *process.Process,
	bat *batch.Batch) (err error) {
	if bat.IsEmpty() {
		return nil
	}

	// first time.
	if len(r.aggList) == 0 {
		r.aggList, bat.Aggs = bat.Aggs, nil
		r.chunkSize = aggexec.GetChunkSizeOfAggregator(r.aggList[0])
		r.toNext = append(r.toNext, getInitialBatchWithSameType(bat))
	}

	// put into hash table.
	var vals []uint64
	var more int
	count := bat.RowCount()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		rowCount := r.hash.GroupCount()
		vals, _, err = r.itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		r.updateInserted(vals, rowCount)
		more, err = r.appendBatch(proc.Mp(), bat, i, r.inserted)
		if err != nil {
			return err
		}
		if err = r.updateAgg(bat, vals, i, len(r.inserted), more); err != nil {
			return err
		}
	}

	return nil
}

func (r *GroupResult) consumeInputBatchOnlyAgg(
	bat *batch.Batch) error {
	if bat.IsEmpty() {
		return nil
	}

	if len(r.aggList) == 0 {
		r.aggList, bat.Aggs = bat.Aggs, nil
		r.chunkSize = math.MaxInt32
		r.toNext = append(r.toNext, getInitialBatchWithSameType(bat))
		r.toNext[0].SetRowCount(1)
	}

	for i, agg := range r.aggList {
		if err := agg.Merge(bat.Aggs[i], 0, 0); err != nil {
			return err
		}
	}
	return nil
}

type GroupResult struct {
	chunkSize int

	// hashmap related structure.
	itr      hashmap.Iterator
	hash     hashmap.HashMap
	inserted []uint8

	// this operator's final result.
	toNext []*batch.Batch
	// the aggregator of this operator.
	aggList []aggexec.AggFuncExec
}

func (r *GroupResult) tryToInitHashTable(isStrHash bool, hashKeyNullable bool) error {
	if r.hash != nil {
		return nil
	}

	if isStrHash {
		h, err := hashmap.NewStrMap(hashKeyNullable)
		if err != nil {
			return err
		}
		r.hash = h
		r.itr = h.NewIterator()
		return nil
	}
	h, err := hashmap.NewIntHashMap(hashKeyNullable)
	if err != nil {
		return err
	}
	r.hash = h
	r.itr = h.NewIterator()
	return nil
}

func (r *GroupResult) reset(m *mpool.MPool) {
	r.free(m)
}

func (r *GroupResult) free(m *mpool.MPool) {
	if r.hash != nil {
		r.hash.Free()
		r.hash = nil
	}
	for i := range r.toNext {
		if r.toNext[i] != nil {
			r.toNext[i].Clean(m)
		}
	}
	for i := range r.aggList {
		if r.aggList[i] != nil {
			r.aggList[i].Free()
		}
	}

	r.toNext, r.aggList = nil, nil
}

func (r *GroupResult) dealPartialResult(partials []any) error {
	for i, agg := range r.aggList {
		if len(partials) > i && partials[i] != nil {
			if err := agg.SetExtraInformation(partials[i], 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *GroupResult) popOneResult() (*batch.Batch, error) {
	if len(r.toNext) == 0 {
		return nil, nil
	}

	if r.aggList != nil {
		for i := range r.aggList {
			vecs, err := r.aggList[i].Flush()
			if err != nil {
				return nil, err
			}

			for j := range vecs {
				r.toNext[j].Vecs = append(r.toNext[j].Vecs, vecs[j])
			}
		}

		for i := range r.aggList {
			r.aggList[i].Free()
		}
		r.aggList = nil
	}

	first := r.toNext[0]
	r.toNext = r.toNext[1:]
	return first, nil
}

func (r *GroupResult) hasMoreResult() bool {
	return len(r.toNext) > 0
}

func (r *GroupResult) updateInserted(vals []uint64, groupCountBefore uint64) {
	if cap(r.inserted) < len(vals) {
		r.inserted = make([]uint8, len(vals))
	} else {
		r.inserted = r.inserted[:len(vals)]
	}

	for i, v := range vals {
		if v > groupCountBefore {
			r.inserted[i] = 1
			groupCountBefore++
		} else {
			r.inserted[i] = 0
		}
	}
}

func (r *GroupResult) appendBatch(
	mp *mpool.MPool,
	bat *batch.Batch, offset int, insertList []uint8) (count int, err error) {

	space1 := r.chunkSize - r.toNext[len(r.toNext)-1].RowCount()
	more, k := countNonZeroAndFindKth(insertList, space1)

	// if there is not any new row should append, just return.
	if more == 0 {
		return 0, nil
	}

	// try to append rows to multiple batches.
	// 1. fill the last part first.
	if space1 >= more {
		if err = r.unionToSpecificBatch(mp, len(r.toNext)-1, bat, int64(offset), insertList, more); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err = r.unionToSpecificBatch(mp, len(r.toNext)-1, bat, int64(offset), insertList[:k+1], space1); err != nil {
		return 0, err
	}

	// 2. add a new batch to continue the append action.
	r.toNext = append(r.toNext, getInitialBatchWithSameType(bat))
	_, err = r.appendBatch(mp, bat, offset+k+1, insertList[k+1:])

	return more, err
}

func (r *GroupResult) updateAgg(
	bat *batch.Batch,
	vals []uint64, offset int, length int,
	moreGroup int) error {
	if len(bat.Aggs) == 0 {
		return nil
	}

	for i := range r.aggList {
		if err := r.aggList[i].GroupGrow(moreGroup); err != nil {
			return err
		}
	}

	for i := range r.aggList {
		if err := r.aggList[i].BatchMerge(bat.Aggs[i], offset, vals[:length]); err != nil {
			return err
		}
	}
	return nil
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

func (r *GroupResult) unionToSpecificBatch(
	mp *mpool.MPool,
	idx int, bat *batch.Batch, offset int64, insertList []uint8, rowIncrease int) error {
	for i, vec := range r.toNext[idx].Vecs {
		if err := vec.UnionBatch(bat.Vecs[i], offset, len(insertList), insertList, mp); err != nil {
			return err
		}
	}
	r.toNext[idx].AddRowCount(rowIncrease)
	return nil
}

func getInitialBatchWithSameType(src *batch.Batch) *batch.Batch {
	b := batch.NewOffHeapWithSize(len(src.Vecs))
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewOffHeapVecWithType(*src.Vecs[i].GetType())
	}
	b.SetRowCount(0)
	return b
}
