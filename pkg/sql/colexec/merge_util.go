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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sort"
)

type dataSlice[T any] interface {
	at(i, j int) T
	length(i int) int
	size() int
}

type fixedDataSlice[T any] struct {
	cols [][]T
}

func (f *fixedDataSlice[T]) at(i, j int) T {
	return f.cols[i][j]
}
func (f *fixedDataSlice[T]) length(i int) int {
	return len(f.cols[i])
}

func (f *fixedDataSlice[T]) size() int {
	return len(f.cols)
}

type varlenaDataSlice struct {
	cols []struct {
		data []types.Varlena
		area []byte
	}
}

func (v *varlenaDataSlice) at(i, j int) string {
	return v.cols[i].data[j].UnsafeGetString(v.cols[i].area)
}

func (v *varlenaDataSlice) length(i int) int {
	return len(v.cols[i].data)
}

func (v *varlenaDataSlice) size() int {
	return len(v.cols)
}

type mergeInterface interface {
	getNextPos() (int, int, int)
}

type heapElem[T any] struct {
	data     T
	isNull   bool
	batIndex int
	rowIndex int
}

// merge we will sort by primary key or
// clusterby key, so we just need one
// vector of every batch.
type merge[T comparable] struct {
	// the number of bacthes
	size int

	// convert the vectors which need to sort
	// into ds data
	ds dataSlice[T]

	// pointer is used to specify
	// which position we have gotten.
	// for example, rowIdx[i] means
	// we are now at the rowIdx[i]-th row for
	// cols[i]
	rowIdx []int

	nulls []*nulls.Nulls

	heap *heapSlice[T]
}

func newMerge[T comparable](compLess sort.LessFunc[T], ds dataSlice[T], nulls []*nulls.Nulls) mergeInterface {
	m := &merge[T]{
		size:   ds.size(),
		ds:     ds,
		rowIdx: make([]int, ds.size()),
		nulls:  nulls,
		heap:   newHeapSlice(ds.size(), compLess),
	}
	m.initHeap()
	return m
}

func (m *merge[T]) initHeap() {
	for i := 0; i < m.ds.size(); i++ {
		if m.ds.length(i) == 0 {
			m.rowIdx[i] = -1
			m.size--
			continue
		}
		heapPush(m.heap, heapElem[T]{
			data:     m.ds.at(i, m.rowIdx[i]),
			isNull:   m.nulls[i].Contains(uint64(m.rowIdx[i])),
			batIndex: i,
			rowIndex: m.rowIdx[i],
		})
		if m.rowIdx[i] >= m.ds.length(i) {
			m.rowIdx[i] = -1
			m.size--
		}
	}
}

func (m *merge[T]) getNextPos() (batchIndex, rowIndex, size int) {
	data := m.pushNext()
	if data == nil {
		// now, m.size is 0
		return -1, -1, m.size
	}
	return data.batIndex, data.rowIndex, m.size
}

func (m *merge[T]) pushNext() *heapElem[T] {
	if m.size == 0 {
		return nil
	}
	data := heapPop(m.heap)
	batchIndex := data.batIndex
	m.rowIdx[batchIndex]++
	if m.rowIdx[batchIndex] >= m.ds.length(batchIndex) {
		m.rowIdx[batchIndex] = -1
		m.size--
	}
	if m.rowIdx[batchIndex] != -1 {
		heapPush(m.heap, heapElem[T]{
			data:     m.ds.at(batchIndex, m.rowIdx[batchIndex]),
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

type SinkerT func(*batch.Batch) error

func MergeSortBatches(
	batches []*batch.Batch,
	sortKeyIdx int,
	buffer *batch.Batch,
	sinker SinkerT,
	mp *mpool.MPool,
) error {
	var merge mergeInterface
	nulls := make([]*nulls.Nulls, len(batches))
	for i, b := range batches {
		nulls[i] = b.Vecs[sortKeyIdx].GetNulls()
	}
	switch batches[0].Vecs[sortKeyIdx].GetType().Oid {
	case types.T_bool:
		ds := &fixedDataSlice[bool]{getFixedCols[bool](batches, sortKeyIdx)}
		merge = newMerge(sort.BoolLess, ds, nulls)
	case types.T_bit:
		ds := &fixedDataSlice[uint64]{getFixedCols[uint64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint64], ds, nulls)
	case types.T_int8:
		ds := &fixedDataSlice[int8]{getFixedCols[int8](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int8], ds, nulls)
	case types.T_int16:
		ds := &fixedDataSlice[int16]{getFixedCols[int16](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int16], ds, nulls)
	case types.T_int32:
		ds := &fixedDataSlice[int32]{getFixedCols[int32](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int32], ds, nulls)
	case types.T_int64:
		ds := &fixedDataSlice[int64]{getFixedCols[int64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[int64], ds, nulls)
	case types.T_uint8:
		ds := &fixedDataSlice[uint8]{getFixedCols[uint8](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint8], ds, nulls)
	case types.T_uint16:
		ds := &fixedDataSlice[uint16]{getFixedCols[uint16](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint16], ds, nulls)
	case types.T_uint32:
		ds := &fixedDataSlice[uint32]{getFixedCols[uint32](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint32], ds, nulls)
	case types.T_uint64:
		ds := &fixedDataSlice[uint64]{getFixedCols[uint64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[uint64], ds, nulls)
	case types.T_float32:
		ds := &fixedDataSlice[float32]{getFixedCols[float32](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[float32], ds, nulls)
	case types.T_float64:
		ds := &fixedDataSlice[float64]{getFixedCols[float64](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[float64], ds, nulls)
	case types.T_date:
		ds := &fixedDataSlice[types.Date]{getFixedCols[types.Date](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Date], ds, nulls)
	case types.T_datetime:
		ds := &fixedDataSlice[types.Datetime]{getFixedCols[types.Datetime](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Datetime], ds, nulls)
	case types.T_time:
		ds := &fixedDataSlice[types.Time]{getFixedCols[types.Time](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Time], ds, nulls)
	case types.T_timestamp:
		ds := &fixedDataSlice[types.Timestamp]{getFixedCols[types.Timestamp](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Timestamp], ds, nulls)
	case types.T_enum:
		ds := &fixedDataSlice[types.Enum]{getFixedCols[types.Enum](batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[types.Enum], ds, nulls)
	case types.T_decimal64:
		ds := &fixedDataSlice[types.Decimal64]{getFixedCols[types.Decimal64](batches, sortKeyIdx)}
		merge = newMerge(sort.Decimal64Less, ds, nulls)
	case types.T_decimal128:
		ds := &fixedDataSlice[types.Decimal128]{getFixedCols[types.Decimal128](batches, sortKeyIdx)}
		merge = newMerge(sort.Decimal128Less, ds, nulls)
	case types.T_uuid:
		ds := &fixedDataSlice[types.Uuid]{getFixedCols[types.Uuid](batches, sortKeyIdx)}
		merge = newMerge(sort.UuidLess, ds, nulls)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		ds := &varlenaDataSlice{getVarlenaCols(batches, sortKeyIdx)}
		merge = newMerge(sort.GenericLess[string], ds, nulls)
	case types.T_Rowid:
		ds := &fixedDataSlice[types.Rowid]{getFixedCols[types.Rowid](batches, sortKeyIdx)}
		merge = newMerge(sort.RowidLess, ds, nulls)
	default:
		panic(fmt.Sprintf("invalid type: %s", batches[0].Vecs[sortKeyIdx].GetType()))
	}
	var (
		batchIndex int
		rowIndex   int
		lens       int
	)
	size := len(batches)
	buffer.CleanOnlyData()
	for size > 0 {
		batchIndex, rowIndex, size = merge.getNextPos()
		for i := range buffer.Vecs {
			err := buffer.Vecs[i].UnionOne(batches[batchIndex].Vecs[i], int64(rowIndex), mp)
			if err != nil {
				return err
			}
		}
		// all data in batches[batchIndex] are used. Clean it.
		if rowIndex+1 == batches[batchIndex].RowCount() {
			batches[batchIndex].Clean(mp)
		}
		lens++
		if lens == objectio.BlockMaxRows {
			lens = 0
			buffer.SetRowCount(objectio.BlockMaxRows)
			if err := sinker(buffer); err != nil {
				return err
			}
			// force clean
			buffer.CleanOnlyData()
		}
	}
	if lens > 0 {
		buffer.SetRowCount(lens)
		if err := sinker(buffer); err != nil {
			return err
		}
		buffer.CleanOnlyData()
	}
	return nil
}
