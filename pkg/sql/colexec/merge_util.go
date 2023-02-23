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

import "github.com/matrixorigin/matrixone/pkg/container/nulls"

type MixData[T any] struct {
	data     *T
	isNull   bool
	batIndex int
	rowIndex int
}

func (mixData *MixData[T]) GetPos() (int, int) {
	return mixData.batIndex, mixData.rowIndex
}

// we will sort by primary key or
// clusterby key, so we just need one
// vector of every batch.
type MergeHeap[T any] struct {
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

func NewMergeHeap[T any](size int, compLess func([]T, int64, int64) bool, cols [][]T, nulls []*nulls.Nulls) (mergeHeap *MergeHeap[T]) {
	mergeHeap = &MergeHeap[T]{
		datas:    make([]*MixData[T], size),
		size:     0,
		cmpLess:  compLess,
		cols:     cols,
		pointers: make([]int, size),
		nulls:    nulls,
	}
	mergeHeap.datas = mergeHeap.datas[:0]
	return
}

func (mergeHeap *MergeHeap[T]) Len() int {
	return int(mergeHeap.size)
}

func (mergeHeap *MergeHeap[T]) Less(i, j int) bool {
	if mergeHeap.datas[i].isNull {
		return true
	}
	if mergeHeap.datas[j].isNull {
		return false
	}
	return mergeHeap.cmpLess([]T{*mergeHeap.datas[i].data, *mergeHeap.datas[j].data}, 0, 1)
}

func (mergeHeap *MergeHeap[T]) Swap(i, j int) {
	mergeHeap.datas[i], mergeHeap.datas[j] = mergeHeap.datas[j], mergeHeap.datas[i]
}

func (mergeHeap *MergeHeap[T]) pushNext() {
	for i := 0; i < len(mergeHeap.pointers); i++ {
		if mergeHeap.pointers[i] != -1 {
			mergeHeap.datas = append(mergeHeap.datas, &MixData[T]{
				data:     &mergeHeap.cols[i][mergeHeap.pointers[i]],
				isNull:   mergeHeap.nulls[i].Contains(uint64(mergeHeap.pointers[i])),
				batIndex: i,
				rowIndex: mergeHeap.pointers[i],
			})
			mergeHeap.pointers[i]++
			mergeHeap.size++
			if mergeHeap.pointers[i] >= len(mergeHeap.cols[i]) {
				mergeHeap.pointers[i] = -1
			}
		}
	}
}

// x is always nil
// the data is generated in the mergeheap's inernal
func (mergeHeap *MergeHeap[T]) Push(x interface{}) {
	mergeHeap.pushNext()
}

func (mergeHeap *MergeHeap[T]) Pop() (res interface{}) {
	res = mergeHeap.datas[mergeHeap.size-1]
	mergeHeap.datas = mergeHeap.datas[0 : mergeHeap.size-1]
	mergeHeap.size--
	return
}

// MergeHeap will take null first rule
// type MergeHeap[T any] struct {
// 	cmpLess func([]T, int64, int64) bool
// 	datas   []MixData[T]
// 	size    uint64
// }

// func NewMergeHeap[T any](cap_size uint64, cmp func([]T, int64, int64) bool) *MergeHeap[T] {
// 	return &MergeHeap[T]{
// 		cmpLess: cmp,
// 		datas:   make([]MixData[T], cap_size+1),
// 		size:    0,
// 	}
// }

// func (heap *MergeHeap[T]) Push(data *T, isNull bool, batchIndex int, rowIndex int) {
// 	heap.datas[heap.size+1] = MixData[T]{
// 		isNull:   isNull,
// 		data:     data,
// 		batIndex: batchIndex,
// 		rowIndex: rowIndex,
// 	}
// 	heap.size++
// 	heap.up(int(heap.size))
// }

// func (heap *MergeHeap[T]) Pop() (batchIndex int, rowIndex int) {
// 	if heap.size < 1 {
// 		return -1, -1
// 	}
// 	batchIndex = heap.datas[1].batIndex
// 	rowIndex = heap.datas[1].rowIndex
// 	heap.datas[1], heap.datas[heap.size] = heap.datas[heap.size], heap.datas[1]
// 	heap.size--
// 	heap.down(1)
// 	return
// }

// func (heap *MergeHeap[T]) compLess(i, j int) bool {
// 	if heap.datas[i].isNull {
// 		return true
// 	}
// 	if heap.datas[j].isNull {
// 		return false
// 	}
// 	return heap.cmpLess([]T{*heap.datas[i].data, *heap.datas[j].data}, 0, 1)
// }

// func (heap *MergeHeap[T]) down(i int) {
// 	t := i
// 	if i*2 <= int(heap.size) && heap.compLess(i*2, t) {
// 		t = i * 2
// 	}
// 	if i*2+1 <= int(heap.size) && heap.compLess(i*2+1, t) {
// 		t = i*2 + 1
// 	}
// 	if t != i {
// 		heap.datas[t], heap.datas[i] = heap.datas[i], heap.datas[t]
// 		heap.down(t)
// 	}
// }

// func (heap *MergeHeap[T]) up(i int) {
// 	t := i
// 	if i/2 >= 1 && heap.compLess(t, i/2) {
// 		t = i / 2
// 	}
// 	if t != i {
// 		heap.datas[t], heap.datas[i] = heap.datas[i], heap.datas[t]
// 		heap.up(t)
// 	}
// }
