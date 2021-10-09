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
	"matrixone/pkg/container/vector"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func numericSort[T numeric](col *vector.Vector, idx []uint32) {
	data := col.Col.([]T)
	n := len(idx)
	dataWithIdx := make(numericSortSlice[T], n)

	for i := 0; i < n; i++ {
		dataWithIdx[i] = sortElem[T]{data: data[i], idx: uint32(i)}
	}

	sortUnstable(dataWithIdx)

	for i, v := range dataWithIdx {
		data[i], idx[i] = v.data, v.idx
	}
}

func numericShuffle[T numeric](col *vector.Vector, idx []uint32) {
	if !col.Nsp.Any() {
		numericShuffleBlock[T](col, idx)
	} else {
		numericShuffleNullableBlock[T](col, idx)
	}
}

func numericShuffleBlock[T numeric](col *vector.Vector, idx []uint32) {
	data := col.Col.([]T)
	newData := make([]T, len(idx))

	for i, j := range idx {
		newData[i] = data[j]
	}

	col.Col = newData
}

func numericShuffleNullableBlock[T numeric](col *vector.Vector, idx []uint32) {
	data := col.Col.([]T)
	nulls := col.Nsp.Np
	newData := make([]T, len(idx))
	newNulls := roaring.New()

	for i, j := range idx {
		if nulls.Contains(uint64(j)) {
			newNulls.AddInt(i)
		} else {
			newData[i] = data[j]
		}
	}

	col.Col = newData
	newNulls.RunOptimize()
	col.Nsp.Np = newNulls
}

func numericMerge[T numeric](col []*vector.Vector, src []uint16) {
	data := make([][]T, len(col))

	for i, v := range col {
		data[i] = v.Col.([]T)
	}

	nElem := len(data[0])
	nBlk := len(data)
	heap := make(numericHeapSlice[T], nBlk)
	merged := make([][]T, nBlk)

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem[T]{data: data[i][0], src: uint16(i), next: 1}
		merged[i] = make([]T, nElem)
	}
	heapInit[heapElem[T]](&heap)

	k := 0
	for i := 0; i < nBlk; i++ {
		for j := 0; j < nElem; j++ {
			top := heapPop[heapElem[T]](&heap)
			merged[i][j], src[k] = top.data, top.src
			k++
			if int(top.next) < nElem {
				heapPush(&heap, heapElem[T]{data: data[top.src][top.next], src: top.src, next: top.next + 1})
			}
		}
	}

	for i := 0; i < nBlk; i++ {
		col[i].Col = merged[i]
	}
}

func numericMultiplex[T numeric](col []*vector.Vector, src []uint16) {
	if col[0].Nsp == nil {
		numericMultiplexBlocks[T](col, src)
	} else {
		numericMultiplexNullableBlocks[T](col, src)
	}
}

func numericMultiplexBlocks[T numeric](col []*vector.Vector, src []uint16) {
	data := make([][]T, len(col))
	for i, v := range col {
		data[i] = v.Col.([]T)
	}

	nElem := len(data[0])
	nBlk := len(data)
	cursors := make([]int, nBlk)
	merged := make([][]T, nBlk)

	for i := 0; i < nBlk; i++ {
		merged[i] = make([]T, nElem)
	}

	k := 0
	for i := 0; i < nBlk; i++ {
		for j := 0; j < nElem; j++ {
			s := src[k]
			merged[i][j] = data[s][cursors[s]]
			cursors[s]++
			k++
		}
	}

	for i := 0; i < nBlk; i++ {
		col[i].Col = merged[i]
	}
}

func numericMultiplexNullableBlocks[T numeric](col []*vector.Vector, src []uint16) {
	data := make([][]T, len(col))
	nElem := len(data[0])
	nBlk := len(data)

	nulls := make([]*roaring.Bitmap, nBlk)
	nullIters := make([]roaring.IntIterable64, nBlk)
	nextNulls := make([]int, nBlk)

	for i, v := range col {
		data[i] = v.Col.([]T)
		nulls[i] = v.Nsp.Np
		nullIters[i] = nulls[i].Iterator()

		if nullIters[i].HasNext() {
			nextNulls[i] = int(nullIters[i].Next())
		} else {
			nextNulls[i] = -1
		}
	}

	cursors := make([]int, nBlk)
	merged := make([][]T, nBlk)
	newNulls := make([]*roaring.Bitmap, nBlk)

	for i := 0; i < nBlk; i++ {
		merged[i] = make([]T, nElem)
	}

	k := 0
	for i := 0; i < nBlk; i++ {
		newNulls[i] = roaring.New()
		for j := 0; j < nElem; j++ {
			s := src[k]
			if cursors[s] == nextNulls[s] {
				newNulls[i].AddInt(j)

				if nullIters[s].HasNext() {
					nextNulls[s] = int(nullIters[s].Next())
				} else {
					nextNulls[s] = -1
				}
			} else {
				merged[i][j] = data[s][cursors[s]]
			}

			cursors[s]++
			k++
		}
	}

	for i := 0; i < nBlk; i++ {
		col[i].Col = merged[i]
		col[i].Nsp.Np = newNulls[i]
		col[i].Nsp.Np.RunOptimize()
	}
}
