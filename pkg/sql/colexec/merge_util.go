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

// Merge we will sort by primary key or
// clusterby key, so we just need one
// vector of every batch.
type Merge[T any] struct {
	// the number of bacthes
	size int
	// convert the vecotrs which need to sort
	// into cols data
	cols [][]T
	// pointer is used to specify
	// which position we have gotten.
	// for example, rowIdx[i] means
	// we are now at the rowIdx[i]-th row for
	// cols[i]
	rowIdx []int

	nulls []*nulls.Nulls

	heap *heapSlice[T]
}

func newMerge[T any](compLess sort.LessFunc[T], cols [][]T, nulls []*nulls.Nulls) *Merge[T] {
	m := &Merge[T]{
		size:   len(cols),
		cols:   cols,
		rowIdx: make([]int, len(cols)),
		nulls:  nulls,
		heap:   newHeapSlice(len(cols), compLess),
	}
	m.initHeap()
	return m
}

func (m *Merge[T]) initHeap() {
	for i := 0; i < len(m.cols); i++ {
		if len(m.cols[i]) == 0 {
			m.rowIdx[i] = -1
			m.size--
			continue
		}
		heapPush(m.heap, heapElem[T]{
			data:     m.cols[i][m.rowIdx[i]],
			isNull:   m.nulls[i].Contains(uint64(m.rowIdx[i])),
			batIndex: i,
			rowIndex: m.rowIdx[i],
		})
		if m.rowIdx[i] >= len(m.cols[i]) {
			m.rowIdx[i] = -1
			m.size--
		}
	}
}

func (m *Merge[T]) getNextPos() (batchIndex, rowIndex, size int) {
	data := m.pushNext()
	if data == nil {
		// now, m.size is 0
		return -1, -1, m.size
	}
	return data.batIndex, data.rowIndex, m.size
}

func (m *Merge[T]) pushNext() *heapElem[T] {
	if m.size == 0 {
		return nil
	}
	data := heapPop(m.heap)
	batchIndex := data.batIndex
	m.rowIdx[batchIndex]++
	if m.rowIdx[batchIndex] >= len(m.cols[batchIndex]) {
		m.rowIdx[batchIndex] = -1
		m.size--
	}
	if m.rowIdx[batchIndex] != -1 {
		heapPush(m.heap, heapElem[T]{
			data:     m.cols[batchIndex][m.rowIdx[batchIndex]],
			isNull:   m.nulls[batchIndex].Contains(uint64(m.rowIdx[batchIndex])),
			batIndex: batchIndex,
			rowIndex: m.rowIdx[batchIndex],
		})
	}
	return &data
}

type heapSlice[T any] struct {
	lessFunc sort.LessFunc[T]
	s        []heapElem[T]
}

func newHeapSlice[T any](n int, lessFunc sort.LessFunc[T]) *heapSlice[T] {
	return &heapSlice[T]{
		lessFunc: lessFunc,
		s:        make([]heapElem[T], 0, n),
	}
}

// Push pushes the element x onto the heap.
// The complexity is Operator(log n) where n = len(h).
func heapPush[T any](h *heapSlice[T], x heapElem[T]) {
	h.s = append(h.s, x)
	up(h, len(h.s)-1)
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is Operator(log n) where n = len(h).
// Pop is equivalent to Remove(h, 0).
func heapPop[T any](h *heapSlice[T]) heapElem[T] {
	n := len(h.s) - 1
	(h.s)[0], (h.s)[n] = (h.s)[n], (h.s)[0]
	down(h, 0, n)
	res := (h.s)[n]
	h.s = (h.s)[:n]
	return res
}

func up[T any](h *heapSlice[T], j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func down[T any](h *heapSlice[T], i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

func (x *heapSlice[T]) Less(i, j int) bool {
	if x.s[i].isNull {
		return true
	}
	if x.s[j].isNull {
		return false
	}
	return x.lessFunc(x.s[i].data, x.s[j].data)
}
func (x *heapSlice[T]) Swap(i, j int) { x.s[i], x.s[j] = x.s[j], x.s[i] }
func (x *heapSlice[T]) Len() int      { return len(x.s) }
