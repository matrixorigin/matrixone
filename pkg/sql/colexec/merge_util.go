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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sort"
)

type MergeInterface interface {
	getNextPos() (int, int, int)
}

type heapElem[T any] struct {
	data     T
	isNull   bool
	batIndex int
	rowIndex int
}

// we will sort by primary key or
// clusterby key, so we just need one
// vector of every batch.
type Merge[T any] struct {
	// the number of bacthes
	size uint64
	// vectors
	cols []*vector.Vector
	// pointer is used to specify
	// which postion we have gotten.
	// for example, pointers[i] means
	// we are now at the i-th row for
	// cols[i]
	pointers []int

	nulls []*nulls.Nulls

	heaps *mergeHeap[T]

	// get i-th data from vector.
	// this function wraps vector.GetFixedAt or vector.UnsafeGetStringAt
	// this enables Merge struct handling fixed and varlen vectors.
	getFunc func(*vector.Vector, int) T
}

func newMerge[T any](size int, compLess sort.LessFunc[T], cols []*vector.Vector, nulls []*nulls.Nulls, getFunc func(*vector.Vector, int) T) (merge *Merge[T]) {
	merge = &Merge[T]{
		size:     uint64(size),
		cols:     cols,
		pointers: make([]int, size),
		nulls:    nulls,
		getFunc:  getFunc,
	}
	merge.heaps = newMergeHeap(uint64(size), compLess)
	merge.initHeap()
	return
}

func (m *Merge[T]) initHeap() {
	for i := 0; i < int(m.size); i++ {
		if m.cols[i].Length() == 0 {
			m.pointers[i] = -1
			m.size--
			continue
		}
		m.heaps.push(&heapElem[T]{
			data:     m.getFunc(m.cols[i], m.pointers[i]),
			isNull:   m.nulls[i].Contains(uint64(m.pointers[i])),
			batIndex: i,
			rowIndex: m.pointers[i],
		})
		if m.pointers[i] >= m.cols[i].Length() {
			m.pointers[i] = -1
			m.size--
		}
	}
}

func (m *Merge[T]) getNextPos() (batchIndex, rowIndex, size int) {
	data := m.pushNext()
	if data == nil {
		// now, m.size is 0
		return -1, -1, int(m.size)
	}
	return data.batIndex, data.rowIndex, int(m.size)
}

func (m *Merge[T]) pushNext() *heapElem[T] {
	if m.size == 0 {
		return nil
	}
	data := m.heaps.pop()
	batchIndex := data.batIndex
	m.pointers[batchIndex]++
	if m.pointers[batchIndex] >= m.cols[batchIndex].Length() {
		m.pointers[batchIndex] = -1
		m.size--
	}
	if m.pointers[batchIndex] != -1 {
		m.heaps.push(&heapElem[T]{
			data:     m.getFunc(m.cols[batchIndex], m.pointers[batchIndex]),
			isNull:   m.nulls[batchIndex].Contains(uint64(m.pointers[batchIndex])),
			batIndex: batchIndex,
			rowIndex: m.pointers[batchIndex],
		})
	}
	return data
}

// mergeHeap will take null first rule
type mergeHeap[T any] struct {
	cmpLess sort.LessFunc[T]
	datas   []*heapElem[T]
	size    uint64
}

func newMergeHeap[T any](cap_size uint64, cmp sort.LessFunc[T]) *mergeHeap[T] {
	return &mergeHeap[T]{
		cmpLess: cmp,
		datas:   make([]*heapElem[T], cap_size+1),
		size:    0,
	}
}

func (heap *mergeHeap[T]) push(data *heapElem[T]) {
	heap.datas[heap.size+1] = data
	heap.size++
	heap.up(int(heap.size))
}

func (heap *mergeHeap[T]) pop() (data *heapElem[T]) {
	if heap.size < 1 {
		return nil
	}
	data = heap.datas[1]
	heap.datas[1], heap.datas[heap.size] = heap.datas[heap.size], heap.datas[1]
	heap.size--
	heap.down(1)
	return
}

func (heap *mergeHeap[T]) compLess(i, j int) bool {
	if heap.datas[i].isNull {
		return true
	}
	if heap.datas[j].isNull {
		return false
	}
	return heap.cmpLess(heap.datas[i].data, heap.datas[j].data)
}

func (heap *mergeHeap[T]) down(i int) {
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

func (heap *mergeHeap[T]) up(i int) {
	t := i
	if i/2 >= 1 && heap.compLess(t, i/2) {
		t = i / 2
	}
	if t != i {
		heap.datas[t], heap.datas[i] = heap.datas[i], heap.datas[t]
		heap.up(t)
	}
}
