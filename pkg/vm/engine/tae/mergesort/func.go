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
	sort.Sort(false, false, true, sortedIdx, pkCol.GetDownstreamVector(), nil)

	for i := 0; i < len(cols); i++ {
		err := cols[i].GetDownstreamVector().Shuffle(sortedIdx, pool.GetMPool())
		if err != nil {
			return nil, err
		}
	}
	return sortedIdx, nil
}

func ReshapeBatches(batches []*containers.Batch, toLayout []uint32, vpool DisposableVecPool) ([]*batch.Batch, func()) {
	// just do reshape, keep sortedIdx nil
	ret := make([]*batch.Batch, len(toLayout))
	rfs := make([]func(), len(toLayout))
	releaseF := func() {
		for _, rf := range rfs {
			rf()
		}
	}

	retIdx := 0
	ret[0], rfs[0] = getSimilarBatch(containers.ToCNBatch(batches[0]), int(toLayout[retIdx]), vpool)
	for _, bat := range batches {
		cnBat := containers.ToCNBatch(bat)
		for row := 0; row < cnBat.RowCount(); row++ {
			if bat.Deletes.Contains(uint64(row)) {
				continue
			}

			for idx := range ret[retIdx].Vecs {
				err := ret[retIdx].Vecs[idx].UnionOne(cnBat.Vecs[idx], int64(row), vpool.GetMPool())
				if err != nil {
					return nil, nil
				}
			}
			ret[retIdx].SetRowCount(ret[retIdx].RowCount() + 1)
			if uint32(ret[retIdx].RowCount()) == toLayout[retIdx] {
				if retIdx == len(toLayout)-1 {
					return ret, releaseF
				}
				retIdx++
				ret[retIdx], rfs[retIdx] = getSimilarBatch(containers.ToCNBatch(batches[0]), int(toLayout[retIdx]), vpool)
			}
		}
	}
	return ret, releaseF
}
