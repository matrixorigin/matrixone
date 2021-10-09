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
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func strSort(col *vector.Vector, idx []uint32) {
	data := col.Col.(*types.Bytes)
	n := len(idx)
	dataWithIdx := make(stringSortSlice, n)

	for i := 0; i < n; i++ {
		dataWithIdx[i] = sortElem[[]byte]{data: data.Get(int64(i)), idx: uint32(i)}
	}

	sortUnstable(dataWithIdx)

	newData := make([]byte, len(data.Data))
	newOffsets := make([]uint32, n)
	newLengths := make([]uint32, n)

	var offset uint32

	for i, v := range dataWithIdx {
		copy(newData[offset:], v.data)
		newOffsets[i] = offset
		l := uint32(len(v.data))
		newLengths[i] = l
		offset += l
		idx[i] = v.idx
	}

	data.Data = newData[:offset]
	data.Offsets = newOffsets
	data.Lengths = newLengths
}

func strShuffle(col *vector.Vector, idx []uint32) {
	if !col.Nsp.Any() {
		strShuffleBlock(col, idx)
	} else {
		strShuffleNullableBlock(col, idx)
	}
}

func strShuffleBlock(col *vector.Vector, idx []uint32) {
	data := col.Col.(*types.Bytes)
	n := len(idx)
	newData := make([]byte, len(data.Data))
	newOffsets := make([]uint32, n)
	newLengths := make([]uint32, n)

	var offset uint32

	for i, j := range idx {
		copy(newData[offset:], data.Get(int64(j)))
		newOffsets[i] = offset
		newLengths[i] = data.Lengths[j]
		offset += newLengths[i]
	}

	data.Data = newData[:offset]
	data.Offsets = newOffsets
	data.Lengths = newLengths
}

func strShuffleNullableBlock(col *vector.Vector, idx []uint32) {
	data := col.Col.(*types.Bytes)
	nulls := col.Nsp.Np
	n := len(idx)
	newData := make([]byte, len(data.Data))
	newOffsets := make([]uint32, n)
	newLengths := make([]uint32, n)
	newNulls := roaring.New()

	var offset uint32

	for i, j := range idx {
		if nulls.Contains(uint64(j)) {
			newNulls.AddInt(i)
		} else {
			copy(newData[offset:], data.Get(int64(j)))
			newOffsets[i] = offset
			newLengths[i] = data.Lengths[j]
			offset += newLengths[i]
		}
	}

	data.Data = newData[:offset]
	data.Offsets = newOffsets
	data.Lengths = newLengths
	newNulls.RunOptimize()
	col.Nsp.Np = newNulls
}

func strMerge(col []*vector.Vector, src []uint16) {
	data := make([]*types.Bytes, len(col))

	for i, v := range col {
		data[i] = v.Col.(*types.Bytes)
	}

	nElem := len(data[0].Offsets)
	nBlk := len(data)
	heap := make(stringHeapSlice, nBlk)
	strings := make([][]byte, nElem)
	merged := make([]*types.Bytes, nBlk)

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem[[]byte]{data: data[i].Get(0), src: uint16(i), next: 1}
	}
	heapInit[heapElem[[]byte]](&heap)

	k := 0
	var offset uint32
	for i := 0; i < nBlk; i++ {
		offset = 0
		for j := 0; j < nElem; j++ {
			top := heapPop[heapElem[[]byte]](&heap)
			offset += uint32(len(top.data))
			strings[j] = top.data
			src[k] = top.src
			k++
			if int(top.next) < nElem {
				heapPush(&heap, heapElem[[]byte]{data: data[top.src].Get(int64(top.next)), src: top.src, next: top.next + 1})
			}
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, nElem)
		newLengths := make([]uint32, nElem)
		offset = 0

		for j := 0; j < nElem; j++ {
			newOffsets[j] = offset
			l := uint32(len(strings[j]))
			newLengths[j] = l
			copy(newData[offset:], strings[j])
			offset += l
		}

		merged[i] = &types.Bytes{
			Data:    newData,
			Offsets: newOffsets,
			Lengths: newLengths,
		}
	}

	for i := 0; i < nBlk; i++ {
		col[i].Col = merged[i]
	}
}

func strMultiplex(col []*vector.Vector, src []uint16) {
	if col[0].Nsp == nil {
		strMultiplexBlocks(col, src)
	} else {
		strMultiplexNullableBlocks(col, src)
	}
}

func strMultiplexBlocks(col []*vector.Vector, src []uint16) {
	data := make([]*types.Bytes, len(col))
	for i, v := range col {
		data[i] = v.Col.(*types.Bytes)
	}

	nElem := len(data[0].Offsets)
	nBlk := len(data)
	cursors := make([]uint32, nBlk)
	strings := make([][]byte, nElem)
	merged := make([]*types.Bytes, nBlk)

	k := 0
	var offset uint32
	for i := 0; i < nBlk; i++ {
		offset = 0
		for j := 0; j < nElem; j++ {
			d, cur := data[src[k]], cursors[src[k]]
			strings[j] = d.Get(int64(cur))
			offset += d.Lengths[cur]
			cursors[src[k]]++
			k++
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, nElem)
		newLengths := make([]uint32, nElem)
		for j := 0; j < nElem; j++ {
			newOffsets[j] = offset
			l := uint32(len(strings[j]))
			newLengths[j] = l
			copy(newData[offset:], strings[j])
			offset += l
		}

		merged[i] = &types.Bytes{
			Data:    newData,
			Offsets: newOffsets,
			Lengths: newLengths,
		}
	}

	for i := 0; i < nBlk; i++ {
		col[i].Col = merged[i]
	}
}

func strMultiplexNullableBlocks(col []*vector.Vector, src []uint16) {
	data := make([]*types.Bytes, len(col))
	nElem := len(data[0].Offsets)
	nBlk := len(data)

	nulls := make([]*roaring.Bitmap, nBlk)
	nullIters := make([]roaring.IntIterable64, nBlk)
	nextNulls := make([]int, nBlk)

	for i, v := range col {
		data[i] = v.Col.(*types.Bytes)
		nulls[i] = v.Nsp.Np
		nullIters[i] = nulls[i].Iterator()

		if nullIters[i].HasNext() {
			nextNulls[i] = int(nullIters[i].Next())
		} else {
			nextNulls[i] = -1
		}
	}

	cursors := make([]int, nBlk)
	strings := make([][]byte, nElem)
	merged := make([]*types.Bytes, nBlk)
	newNulls := make([]*roaring.Bitmap, nBlk)

	emptySlice := make([]byte, 0)

	k := 0
	var offset uint32
	for i := 0; i < nBlk; i++ {
		newNulls[i] = roaring.New()
		offset = 0
		for j := 0; j < nElem; j++ {
			s := src[k]
			if cursors[s] == nextNulls[s] {
				newNulls[i].AddInt(j)
				strings[j] = emptySlice

				if nullIters[s].HasNext() {
					nextNulls[s] = int(nullIters[s].Next())
				} else {
					nextNulls[s] = -1
				}
			} else {
				d, cur := data[s], cursors[s]
				strings[j] = d.Get(int64(s))
				offset += d.Lengths[cur]
			}

			cursors[s]++
			k++
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, nElem)
		newLengths := make([]uint32, nElem)

		for j := 0; j < nElem; j++ {
			newOffsets[j] = offset
			l := uint32(len(strings[j]))
			newLengths[j] = l
			copy(newData[offset:], strings[j])
			offset += l
		}

		merged[i] = &types.Bytes{
			Data:    newData,
			Offsets: newOffsets,
			Lengths: newLengths,
		}
	}

	for i := 0; i < nBlk; i++ {
		col[i].Col = merged[i]
		col[i].Nsp.Np = newNulls[i]
		col[i].Nsp.Np.RunOptimize()
	}
}
