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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func ReshapeBatches(
	batches []*batch.Batch,
	fromLayout, toLayout []uint32,
	vpool DisposableVecPool) ([]*batch.Batch, func()) {
	// just do reshape, keep sortedIdx nil
	ret := make([]*batch.Batch, 0, len(toLayout))
	rfs := make([]func(), 0, len(toLayout))
	for _, layout := range toLayout {
		bat, releaseF := getSimilarBatch(batches[0], int(layout), vpool)
		ret = append(ret, bat)
		rfs = append(rfs, releaseF)
	}
	releaseF := func() {
		for _, rf := range rfs {
			rf()
		}
	}

	fromIdx := 0
	fromOffset := 0
	for i := 0; i < len(toLayout); i++ {
		toOffset := 0
		for toOffset < int(toLayout[i]) {
			// find offset to fill a full block
			fromLeft := int(fromLayout[fromIdx]) - fromOffset
			if fromLeft == 0 {
				fromIdx++
				fromOffset = 0
				fromLeft = int(fromLayout[fromIdx])
			}
			length := 0
			if fromLeft < int(toLayout[i])-toOffset {
				length = fromLeft
			} else {
				length = int(toLayout[i]) - toOffset
			}

			for vecIdx, vec := range batches[fromIdx].Vecs {
				window, err := vec.Window(fromOffset, fromOffset+length)
				if err != nil {
					panic(err)
				}
				err = vector.GetUnionAllFunction(*vec.GetType(), vpool.GetMPool())(ret[i].Vecs[vecIdx], window)
				if err != nil {
					panic(err)
				}
			}

			// update offset
			fromOffset += length
			toOffset += length
		}
		ret[i].SetRowCount(int(toLayout[i]))
	}
	return ret, releaseF
}
