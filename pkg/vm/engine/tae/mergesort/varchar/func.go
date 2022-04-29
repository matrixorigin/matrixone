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

package varchar

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func Sort(col *vector.Vector, idx []uint32) {
	data := col.Col.(*types.Bytes)
	n := len(idx)
	dataWithIdx := make(sortSlice, n)

	for i := 0; i < n; i++ {
		dataWithIdx[i] = sortElem{data: data.Get(int64(i)), idx: uint32(i)}
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

func Shuffle(col *vector.Vector, idx []uint32) {
	if !nulls.Any(col.Nsp) {
		shuffleBlock(col, idx)
	} else {
		shuffleNullableBlock(col, idx)
	}
}

func shuffleBlock(col *vector.Vector, idx []uint32) {
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

func shuffleNullableBlock(col *vector.Vector, idx []uint32) {
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

func Merge(col []*vector.Vector, src *[]uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector, mapping []uint32) {
	data := make([]*types.Bytes, len(col))
	ret = make([]*vector.Vector, len(toLayout))
	mapping = make([]uint32, len(*src))

	colOffset := make([]uint32, len(fromLayout))
	colOffset[0] = 0
	for i := 1; i < len(fromLayout); i++ {
		colOffset[i] = colOffset[i-1] + fromLayout[i-1]
	}

	for i, v := range col {
		data[i] = v.Col.(*types.Bytes)
	}

	from := len(fromLayout)
	to := len(toLayout)
	maxTo := uint32(0)
	for _, i := range toLayout {
		if i > maxTo {
			maxTo = i
		}
	}
	heap := make(heapSlice, from)
	strings := make([][]byte, maxTo)
	merged := make([]*types.Bytes, to)

	for i := 0; i < from; i++ {
		heap[i] = heapElem{data: data[i].Get(0), src: uint32(i), next: 1}
	}
	heapInit(heap)

	k := 0
	var offset uint32
	for i := 0; i < to; i++ {
		offset = 0
		for j := 0; j < int(toLayout[i]); j++ {
			top := heapPop(&heap)
			offset += uint32(len(top.data))
			strings[j] = top.data
			(*src)[k] = top.src
			mapping[colOffset[top.src]+top.next-1] = uint32(k)
			k++
			if int(top.next) < int(fromLayout[top.src]) {
				heapPush(&heap, heapElem{data: data[top.src].Get(int64(top.next)), src: top.src, next: top.next + 1})
			}
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, toLayout[i])
		newLengths := make([]uint32, toLayout[i])
		offset = 0

		for j := 0; j < int(toLayout[i]); j++ {
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

	for i := 0; i < to; i++ {
		ret[i] = vector.New(col[0].Typ)
		ret[i].Col = merged[i]
	}
	return
}

func Multiplex(col []*vector.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	for i, _ := range col {
		if nulls.Any(col[i].Nsp) {
			ret = multiplexNullableBlocks(col, src, fromLayout, toLayout)
			return
		}
	}
	ret = multiplexBlocks(col, src, fromLayout, toLayout)
	return
}

func multiplexBlocks(col []*vector.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	data := make([]*types.Bytes, len(col))
	ret = make([]*vector.Vector, len(toLayout))

	for i, v := range col {
		data[i] = v.Col.(*types.Bytes)
	}

	from := len(data)
	to := len(toLayout)
	maxTo := uint32(0)
	for _, i := range toLayout {
		if i > maxTo {
			maxTo = i
		}
	}
	cursors := make([]uint32, from)
	strings := make([][]byte, maxTo)
	merged := make([]*types.Bytes, to)

	k := 0
	var offset uint32
	for i := 0; i < to; i++ {
		offset = 0
		for j := 0; j < int(toLayout[i]); j++ {
			d, cur := data[src[k]], cursors[src[k]]
			strings[j] = d.Get(int64(cur))
			offset += d.Lengths[cur]
			cursors[src[k]]++
			k++
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, toLayout[i])
		newLengths := make([]uint32, toLayout[i])
		offset = 0
		for j := 0; j < int(toLayout[i]); j++ {
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

	for i := 0; i < from; i++ {
		ret[i] = vector.New(col[0].Typ)
		ret[i].Col = merged[i]
	}
	return
}

func multiplexNullableBlocks(col []*vector.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	data := make([]*types.Bytes, len(col))
	for i, v := range col {
		data[i] = v.Col.(*types.Bytes)
	}
	from := len(fromLayout)
	to := len(toLayout)
	maxTo := uint32(0)
	for _, i := range toLayout {
		if i > maxTo {
			maxTo = i
		}
	}

	nulls := make([]*roaring.Bitmap, from)
	nullIters := make([]roaring.IntIterable64, from)
	nextNulls := make([]int, from)

	for i, v := range col {
		data[i] = v.Col.(*types.Bytes)
		nulls[i] = v.Nsp.Np
		if v.Nsp.Np == nil {
			nextNulls[i] = -1
			continue
		}
		nullIters[i] = nulls[i].Iterator()
		if nullIters[i].HasNext() {
			nextNulls[i] = int(nullIters[i].Next())
		} else {
			nextNulls[i] = -1
		}
	}

	cursors := make([]int, from)
	strings := make([][]byte, maxTo)
	merged := make([]*types.Bytes, to)
	newNulls := make([]*roaring.Bitmap, to)

	emptySlice := make([]byte, 0)

	k := 0
	var offset uint32
	for i := 0; i < to; i++ {
		newNulls[i] = roaring.New()
		offset = 0
		for j := 0; j < int(toLayout[i]); j++ {
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
				d, _ := data[s], cursors[s]
				strings[j] = d.Get(int64(s))
				offset += uint32(len(strings[j]))
			}

			cursors[s]++
			k++
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, toLayout[i])
		newLengths := make([]uint32, toLayout[i])
		offset = 0
		for j := 0; j < int(toLayout[i]); j++ {
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

	for i := 0; i < to; i++ {
		ret[i] = vector.New(col[0].Typ)
		ret[i].Col = merged[i]
		ret[i].Nsp.Np = newNulls[i]
		ret[i].Nsp.Np.RunOptimize()
	}
	return
}
