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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
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
	if r.BlockingGroupRelated.IsEmpty() {
		r.BlockingGroupRelated.Init(aggexec.GetChunkSizeOfAggregator(bat.Aggs[0]), bat.Aggs, bat)
		bat.Aggs = nil
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

		rowCount := r.Hash.GroupCount()
		vals, _, err = r.Itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		insertList, _ := r.GetNewList(vals, rowCount)

		more, err = r.AppendBatch(proc.Mp(), bat, i, insertList)
		if err != nil {
			return err
		}
		if err = r.updateAgg(bat, vals, i, len(insertList), more); err != nil {
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

	if r.BlockingGroupRelated.IsEmpty() {
		r.BlockingGroupRelated.Init(math.MaxInt32, bat.Aggs, bat)
		bat.Aggs = nil
		r.BlockingGroupRelated.ToPopped[0].SetRowCount(1)
	}

	for i, agg := range r.AggList {
		if err := agg.Merge(bat.Aggs[i], 0, 0); err != nil {
			return err
		}
	}
	return nil
}

type GroupResult struct {
	group.ResHashRelated
	group.BlockingGroupRelated
}

func (r *GroupResult) reset(m *mpool.MPool) {
	r.free(m)
}

func (r *GroupResult) free(m *mpool.MPool) {
	r.ResHashRelated.Free0()
	r.BlockingGroupRelated.Free0(m)
}

func (r *GroupResult) updateAgg(
	bat *batch.Batch,
	vals []uint64, offset int, length int,
	moreGroup int) error {
	if len(bat.Aggs) == 0 {
		return nil
	}

	for i := range r.AggList {
		if err := r.AggList[i].GroupGrow(moreGroup); err != nil {
			return err
		}
	}

	for i := range r.AggList {
		if err := r.AggList[i].BatchMerge(bat.Aggs[i], offset, vals[:length]); err != nil {
			return err
		}
	}
	return nil
}
