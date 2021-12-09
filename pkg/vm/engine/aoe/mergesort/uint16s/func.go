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

package uint16s

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func Sort(col *vector.Vector, idx []uint32) {
	data := col.Col.([]uint16)
	n := len(idx)
	dataWithIdx := make(sortSlice, n)

	for i := 0; i < n; i++ {
		dataWithIdx[i] = sortElem{data: data[i], idx: uint32(i)}
	}

	sortUnstable(dataWithIdx)

	for i, v := range dataWithIdx {
		data[i], idx[i] = v.data, v.idx
	}
}

func Shuffle(col *vector.Vector, idx []uint32) {
	if !nulls.Any(col.Nsp) {
		shuffleBlock(col, idx)
	} else {
		shuffleNullableBlock(col, idx)
	}
}

func shuffleBlock(col *vector.Vector, idx []uint32) {
	data := col.Col.([]uint16)
	newData := make([]uint16, len(idx))

	for i, j := range idx {
		newData[i] = data[j]
	}

	col.Col = newData
}

func shuffleNullableBlock(col *vector.Vector, idx []uint32) {
	data := col.Col.([]uint16)
	nulls := col.Nsp.Np
	newData := make([]uint16, len(idx))
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

func Merge(col []*vector.Vector, src *[]uint16) {
	data := make([][]uint16, len(col))

	for i, v := range col {
		data[i] = v.Col.([]uint16)
	}

	nElem := len(data[0])
	nBlk := len(data)
	heap := make(heapSlice, nBlk)
	merged := make([][]uint16, nBlk)

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem{data: data[i][0], src: uint16(i), next: 1}
		merged[i] = make([]uint16, nElem)
	}
	heapInit(heap)

	k := 0
	for i := 0; i < nBlk; i++ {
		for j := 0; j < nElem; j++ {
			top := heapPop(&heap)
			merged[i][j], (*src)[k] = top.data, top.src
			k++
			if int(top.next) < nElem {
				heapPush(&heap, heapElem{data: data[top.src][top.next], src: top.src, next: top.next + 1})
			}
		}
	}

	for i := 0; i < nBlk; i++ {
		col[i].Col = merged[i]
	}
}

func Multiplex(col []*vector.Vector, src []uint16) {
	for i, _ := range col{
		if nulls.Any(col[i].Nsp) {
			multiplexNullableBlocks(col, src)
			return
		}
	}
	multiplexBlocks(col, src)
}

func multiplexBlocks(col []*vector.Vector, src []uint16) {
	data := make([][]uint16, len(col))
	for i, v := range col {
		data[i] = v.Col.([]uint16)
	}

	nElem := len(data[0])
	nBlk := len(data)
	cursors := make([]int, nBlk)
	merged := make([][]uint16, nBlk)

	for i := 0; i < nBlk; i++ {
		merged[i] = make([]uint16, nElem)
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

func multiplexNullableBlocks(col []*vector.Vector, src []uint16) {
	data := make([][]uint16, len(col))
	for i, v := range col {
		data[i] = v.Col.([]uint16)
	}
	nElem := len(data[0])
	nBlk := len(data)

	nulls := make([]*roaring.Bitmap, nBlk)
	nullIters := make([]roaring.IntIterable64, nBlk)
	nextNulls := make([]int, nBlk)

	for i, v := range col {
		data[i] = v.Col.([]uint16)
		if v.Nsp.Np == nil {
			nextNulls[i] = -1
			continue
		}
		nulls[i] = v.Nsp.Np
		nullIters[i] = nulls[i].Iterator()

		if nullIters[i].HasNext() {
			nextNulls[i] = int(nullIters[i].Next())
		} else {
			nextNulls[i] = -1
		}
	}

	cursors := make([]int, nBlk)
	merged := make([][]uint16, nBlk)
	newNulls := make([]*roaring.Bitmap, nBlk)

	for i := 0; i < nBlk; i++ {
		merged[i] = make([]uint16, nElem)
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
