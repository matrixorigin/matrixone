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

package mergesort

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

/// merge things

func SortBlockColumns(
	cols []containers.Vector, pk int, pool *containers.VectorPool,
) ([]int64, error) {
	pkCol := cols[pk]
	sortedIdx := make([]int64, pkCol.Length())
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, pkCol.GetDownstreamVector())

	for i := 0; i < len(cols); i++ {
		// The columns are shuffled in place, through their own pool.
		vec := cols[i].GetDownstreamVector()
		err := vec.Permute(sortedIdx, vec.MPool())
		if err != nil {
			return nil, err
		}
	}
	return sortedIdx, nil
}

// ReshapeBatches copies the rows of batches that are not deleted, in order,
// into new batches of toLayout sizes. Rows past the layout are dropped. It
// also returns the old → new map: entry r is where global row r (the rows of
// all batches numbered one after another) went, or -1 if it was dropped. The
// map is valid until the returned release function is called.
func ReshapeBatches(batches []*containers.Batch, toLayout []uint32, vpool DisposableVecPool) ([]*batch.Batch, func(), []int64, error) {
	accRowCnt := make([]int64, len(batches))
	totalRowCnt := 0
	for i, blk := range batches {
		accRowCnt[i] = int64(totalRowCnt)
		totalRowCnt += blk.Length()
	}
	layoutRows := 0
	for _, size := range toLayout {
		layoutRows += int(size)
	}

	var order mergeOrder
	defer order.free()
	if err := order.init(vpool, accRowCnt, min(totalRowCnt, layoutRows)); err != nil {
		return nil, nil, nil, err
	}
collect:
	for batIdx, bat := range batches {
		for row := 0; row < bat.Length(); row++ {
			if order.rows.Len() == layoutRows {
				break collect
			}
			if bat.Deletes.Contains(uint64(row)) {
				continue
			}
			if err := order.add(batIdx, int64(row)); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	ret, rfs, err := order.gather(batches, toLayout, vpool)
	if err != nil {
		return nil, nil, nil, err
	}
	// A block is made when the one before it is full, even if no rows are
	// left for it, and the first block always.
	for b := range ret {
		if ret[b] == nil && (b == 0 || ret[b-1] != nil && uint32(ret[b-1].RowCount()) == toLayout[b-1]) {
			ret[b], rfs[b] = getSimilarBatch(containers.ToCNBatch(batches[0]), int(toLayout[b]), vpool)
		}
	}
	releaseF := func() {
		for _, rf := range rfs {
			if rf != nil {
				rf()
			}
		}
	}
	mapping, freeMapping, err := order.mapping(vpool, totalRowCnt)
	if err != nil {
		releaseF()
		return nil, nil, nil, err
	}
	return ret, func() { releaseF(); freeMapping() }, mapping, nil
}
