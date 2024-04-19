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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

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

// MergeColumn merge sorted column. It modify sortidx and mapping as merging record. After that, vector in `column` will be closed. Used by Tn only
func MergeColumn(
	column []containers.Vector,
	sortidx, mapping []uint32,
	fromLayout, toLayout []uint32,
	pool *containers.VectorPool,
) (ret []containers.Vector) {

	columns := make([]*vector.Vector, len(column))
	for i := range column {
		columns[i] = column[i].GetDownstreamVector()
	}

	ret = make([]containers.Vector, len(toLayout))
	retvec := make([]*vector.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(column[0].GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	Merge(columns, retvec, sortidx, mapping, fromLayout, toLayout, ret[0].GetAllocator())

	for _, v := range column {
		v.Close()
	}

	return
}

// ShuffleColumn shuffle column according to sortedIdx.  After that, vector in `column` will be closed. Used by Tn only
func ShuffleColumn(
	column []containers.Vector, sortedIdx []uint32, fromLayout, toLayout []uint32, pool *containers.VectorPool,
) (ret []containers.Vector) {

	columns := make([]*vector.Vector, len(column))
	for i := range column {
		columns[i] = column[i].GetDownstreamVector()
	}

	ret = make([]containers.Vector, len(toLayout))
	retvec := make([]*vector.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(column[0].GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	Multiplex(columns, retvec, sortedIdx, fromLayout, toLayout, ret[0].GetAllocator())

	for _, v := range column {
		v.Close()
	}

	return
}

// ReshapeColumn rearrange array according to toLayout. After that, vector in `column` will be closed. Used by Tn only
func ReshapeColumn(
	column []containers.Vector, fromLayout, toLayout []uint32, pool *containers.VectorPool,
) (ret []containers.Vector) {
	columns := make([]*vector.Vector, len(column))
	for i := range column {
		columns[i] = column[i].GetDownstreamVector()
	}
	ret = make([]containers.Vector, len(toLayout))
	retvec := make([]*vector.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(column[0].GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	Reshape(columns, retvec, fromLayout, toLayout, ret[0].GetAllocator())

	for _, v := range column {
		v.Close()
	}
	return
}
