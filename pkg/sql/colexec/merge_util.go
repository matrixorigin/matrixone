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
	"container/heap"

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

	heaps *Heap[T]
}

func NewMerge[T any](size int, compLess func([]T, int64, int64) bool, cols [][]T, nulls []*nulls.Nulls) (merge *Merge[T]) {
	merge = &Merge[T]{
		size:     uint64(size),
		cmpLess:  compLess,
		cols:     cols,
		pointers: make([]int, size),
		nulls:    nulls,
	}
	merge.NewHeap()
	merge.InitHeap()
	return
}

func (merge *Merge[T]) InitHeap() {
	heap.Init(merge.heaps)
	for i := 0; i < int(merge.size); i++ {
		if len(merge.cols[i]) == 0 {
			merge.pointers[i] = -1
			merge.size--
			continue
		}
		heap.Push(merge.heaps, &MixData[T]{
			data:     &merge.cols[i][merge.pointers[i]],
			isNull:   merge.nulls[i].Contains(uint64(merge.pointers[i])),
			batIndex: i,
			rowIndex: merge.pointers[i],
		})
		if merge.pointers[i] >= len(merge.cols[i]) {
			merge.pointers[i] = -1
			merge.size--
		}
	}
}

func (merge *Merge[T]) GetNextPos() (batchIndex, rowIndex, size int) {
	data := merge.pushNext()
	if data == nil {
		// now, merge.size is 0
		return -1, -1, int(merge.size)
	}
	return data.batIndex, data.rowIndex, int(merge.size)
}

func (merge *Merge[T]) Len() int {
	return int(merge.size)
}

func (merge *Merge[T]) Less(i, j int) bool {
	if merge.heaps.datas[i].isNull {
		return true
	}
	if merge.heaps.datas[j].isNull {
		return false
	}
	return merge.cmpLess([]T{*merge.heaps.datas[i].data, *merge.heaps.datas[j].data}, 0, 1)
}

func (merge *Merge[T]) pushNext() *MixData[T] {
	if merge.size == 0 {
		return nil
	}
	data := heap.Pop(merge.heaps).(*MixData[T])

	batchIndex := data.batIndex
	merge.pointers[batchIndex]++
	if merge.pointers[batchIndex] >= len(merge.cols[batchIndex]) {
		merge.pointers[batchIndex] = -1
		merge.size--
	}
	if merge.pointers[batchIndex] != -1 {
		heap.Push(merge.heaps, &MixData[T]{
			data:     &merge.cols[batchIndex][merge.pointers[batchIndex]],
			isNull:   merge.nulls[batchIndex].Contains(uint64(merge.pointers[batchIndex])),
			batIndex: batchIndex,
			rowIndex: merge.pointers[batchIndex],
		})
	}
	return data
}

type Heap[T any] struct {
	datas []*MixData[T]
	less  func(int, int) bool
}

func (merge *Merge[T]) NewHeap() {
	merge.heaps = &Heap[T]{
		datas: make([]*MixData[T], merge.size),
		less:  merge.Less,
	}
	merge.heaps.datas = merge.heaps.datas[:0]
}

func (h *Heap[T]) Less(i, j int) bool {
	return h.less(i, j)
}

func (h *Heap[T]) Push(x any) {
	h.datas = append(h.datas, x.(*MixData[T]))
}

func (h *Heap[T]) Len() int {
	return len(h.datas)
}

func (h *Heap[T]) Swap(i, j int) {
	h.datas[i], h.datas[j] = h.datas[j], h.datas[i]
}

func (h *Heap[T]) Pop() any {
	if h.Len() == 0 {
		return nil
	}
	x := h.datas[len(h.datas)-1]
	h.datas = h.datas[:len(h.datas)-1]
	return x
}
