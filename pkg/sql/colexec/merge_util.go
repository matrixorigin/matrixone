// Copyright 2022 Matrix Origin
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

package colexec

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

type MergeInterface interface {
	GetNextPos() (int, int, int)
}

type MixData[T any] struct {
	data     *T
	isNull   bool
	batIndex int
	rowIndex int
}

// we will sort by primary key or
// clusterby key, so we just need one
// vector of every batch.
type Merge[T any] struct {
	datas []*MixData[T]
	// the number of bacthes
	size    uint64
	cmpLess func([]T, int64, int64) bool
	// convert the vecotrs which need to sort
	// into cols data
	cols [][]T
	// pointer is used to specify
	// which postion we have gotten.
	// for example, pointers[i] means
	// we are now at the i-th row for
	// cols[i]
	pointers []int

	nulls []*nulls.Nulls
}

func NewMerge[T any](size int, compLess func([]T, int64, int64) bool, cols [][]T, nulls []*nulls.Nulls) (mergeHeap *Merge[T]) {
	mergeHeap = &Merge[T]{
		datas:    make([]*MixData[T], size),
		size:     uint64(size),
		cmpLess:  compLess,
		cols:     cols,
		pointers: make([]int, size),
		nulls:    nulls,
	}
	mergeHeap.datas = mergeHeap.datas[:0]
	return
}

func (mergeHeap *Merge[T]) GetNextPos() (batchIndex, rowIndex, size int) {
	mergeHeap.datas = mergeHeap.datas[:0]
	mergeHeap.pushNext()
	sort.Slice(mergeHeap.datas, func(i, j int) bool {
		return mergeHeap.Less(i, j)
	})
	batchIndex = mergeHeap.datas[0].batIndex
	rowIndex = mergeHeap.datas[0].rowIndex
	mergeHeap.pointers[batchIndex]++
	if mergeHeap.pointers[batchIndex] >= len(mergeHeap.cols[batchIndex]) {
		mergeHeap.pointers[batchIndex] = -1
		mergeHeap.size--
	}
	size = int(mergeHeap.size)
	return
}

func (mergeHeap *Merge[T]) Len() int {
	return int(mergeHeap.size)
}

func (mergeHeap *Merge[T]) Less(i, j int) bool {
	if mergeHeap.datas[i].isNull {
		return true
	}
	if mergeHeap.datas[j].isNull {
		return false
	}
	return mergeHeap.cmpLess([]T{*mergeHeap.datas[i].data, *mergeHeap.datas[j].data}, 0, 1)
}

func (mergeHeap *Merge[T]) pushNext() {
	for i := 0; i < len(mergeHeap.pointers); i++ {
		if mergeHeap.pointers[i] != -1 {
			mergeHeap.datas = append(mergeHeap.datas, &MixData[T]{
				data:     &mergeHeap.cols[i][mergeHeap.pointers[i]],
				isNull:   mergeHeap.nulls[i].Contains(uint64(mergeHeap.pointers[i])),
				batIndex: i,
				rowIndex: mergeHeap.pointers[i],
			})
		}
	}
}
