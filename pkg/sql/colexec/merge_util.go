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

	heaps *MergeHeap[T]
}

func NewMerge[T any](size int, compLess func([]T, int64, int64) bool, cols [][]T, nulls []*nulls.Nulls) (merge *Merge[T]) {
	merge = &Merge[T]{
		size:     uint64(size),
		cmpLess:  compLess,
		cols:     cols,
		pointers: make([]int, size),
		nulls:    nulls,
	}
	merge.heaps = NewMergeHeap(uint64(size), merge.cmpLess)
	merge.InitHeap()
	return
}

func (merge *Merge[T]) InitHeap() {
	for i := 0; i < int(merge.size); i++ {
		if len(merge.cols[i]) == 0 {
			merge.pointers[i] = -1
			merge.size--
			continue
		}
		merge.heaps.Push(&MixData[T]{
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
	data := merge.heaps.Pop()
	batchIndex := data.batIndex
	merge.pointers[batchIndex]++
	if merge.pointers[batchIndex] >= len(merge.cols[batchIndex]) {
		merge.pointers[batchIndex] = -1
		merge.size--
	}
	if merge.pointers[batchIndex] != -1 {
		merge.heaps.Push(&MixData[T]{
			data:     &merge.cols[batchIndex][merge.pointers[batchIndex]],
			isNull:   merge.nulls[batchIndex].Contains(uint64(merge.pointers[batchIndex])),
			batIndex: batchIndex,
			rowIndex: merge.pointers[batchIndex],
		})
	}
	return data
}

// MergeHeap will take null first rule
type MergeHeap[T any] struct {
	cmpLess func([]T, int64, int64) bool
	datas   []*MixData[T]
	size    uint64
}

func NewMergeHeap[T any](cap_size uint64, cmp func([]T, int64, int64) bool) *MergeHeap[T] {
	return &MergeHeap[T]{
		cmpLess: cmp,
		datas:   make([]*MixData[T], cap_size+1),
		size:    0,
	}
}

func (heap *MergeHeap[T]) Push(data *MixData[T]) {
	heap.datas[heap.size+1] = data
	heap.size++
	heap.up(int(heap.size))
}

func (heap *MergeHeap[T]) Pop() (data *MixData[T]) {
	if heap.size < 1 {
		return nil
	}
	data = heap.datas[1]
	heap.datas[1], heap.datas[heap.size] = heap.datas[heap.size], heap.datas[1]
	heap.size--
	heap.down(1)
	return
}

func (heap *MergeHeap[T]) compLess(i, j int) bool {
	if heap.datas[i].isNull {
		return true
	}
	if heap.datas[j].isNull {
		return false
	}
	return heap.cmpLess([]T{*heap.datas[i].data, *heap.datas[j].data}, 0, 1)
}

func (heap *MergeHeap[T]) down(i int) {
	t := i
	if i*2 <= int(heap.size) && heap.compLess(i*2, t) {
		t = i * 2
	}
	if i*2+1 <= int(heap.size) && heap.compLess(i*2+1, t) {
		t = i*2 + 1
	}
	if t != i {
		heap.datas[t], heap.datas[i] = heap.datas[i], heap.datas[t]
		heap.down(t)
	}
}

func (heap *MergeHeap[T]) up(i int) {
	t := i
	if i/2 >= 1 && heap.compLess(t, i/2) {
		t = i / 2
	}
	if t != i {
		heap.datas[t], heap.datas[i] = heap.datas[i], heap.datas[t]
		heap.up(t)
	}
}
