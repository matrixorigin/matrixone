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

package datetimes

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func Sort(col *vector.Vector, idx []uint32) {
	data := col.Col.([]types.Datetime)
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
	data := col.Col.([]types.Datetime)
	newData := make([]types.Datetime, len(idx))

	for i, j := range idx {
		newData[i] = data[j]
	}

	col.Col = newData
}

func shuffleNullableBlock(col *vector.Vector, idx []uint32) {
	data := col.Col.([]types.Datetime)
	nulls := col.Nsp.Np
	newData := make([]types.Datetime, len(idx))
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

func Merge(col []*vector.Vector, src *[]uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector, mapping []uint32) {
	data := make([][]types.Datetime, len(col))
	ret = make([]*vector.Vector, len(toLayout))
	mapping = make([]uint32, len(*src))

	offset := make([]uint32, len(fromLayout))
	offset[0] = 0
	for i := 1; i < len(fromLayout); i++ {
		offset[i] = offset[i-1] + fromLayout[i-1]
	}

	for i, v := range col {
		data[i] = v.Col.([]types.Datetime)
	}

	nBlk := len(data)
	heap := make(heapSlice, nBlk)
	merged := make([][]types.Datetime, len(toLayout))

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem{data: data[i][0], src: uint32(i), next: 1}
		merged[i] = make([]types.Datetime, toLayout[i])
	}
	heapInit(heap)

	k := 0
	for i := 0; i < len(toLayout); i++ {
		for j := 0; j < int(toLayout[i]); j++ {
			top := heapPop(&heap)
			merged[i][j], (*src)[k] = top.data, top.src
			mapping[offset[top.src]+top.next-1] = uint32(k)
			k++
			if int(top.next) < int(fromLayout[top.src]) {
				heapPush(&heap, heapElem{data: data[top.src][top.next], src: top.src, next: top.next + 1})
			}
		}
	}
	for i := 0; i < len(toLayout); i++ {
		ret[i] = vector.New(col[0].Typ)
		ret[i].Col = merged[i]
	}
	return
}
func Reshape(col []*vector.Vector, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	ret = make([]*vector.Vector, len(toLayout))
	fromIdx := 0
	fromOffset := 0
	for i := 0; i < len(toLayout); i++ {
		ret[i] = vector.New(col[0].Typ)
		merged := make([]types.Datetime, toLayout[i])
		toOffset := 0
		for toOffset < int(toLayout[i]) {
			fromLeft := fromLayout[fromIdx] - uint32(fromOffset)
			if fromLeft == 0 {
				fromIdx++
				fromOffset = 0
				fromLeft = fromLayout[fromIdx]
			}
			length := 0
			if fromLeft < toLayout[i]-uint32(toOffset) {
				length = int(fromLeft)
			} else {
				length = int(toLayout[i]) - toOffset
			}
			copy(merged[toOffset:toOffset+length], col[fromIdx].Col.([]types.Datetime)[fromOffset:fromOffset+length])
			if col[fromIdx].Nsp.Np != nil {
				if ret[i].Nsp.Np == nil {
					ret[i].Nsp.Np = roaring.New()
				}
				iterator := col[fromIdx].Nsp.Np.Iterator()
				for iterator.HasNext() {
					row := iterator.Next()
					if row < uint64(fromOffset) {
						continue
					}
					if row >= uint64(fromOffset)+uint64(length) {
						break
					}
					ret[i].Nsp.Np.Add(row + uint64(toOffset) - uint64(fromOffset))
				}
			}
			fromOffset += length
			toOffset += length
		}
		ret[i].Col = merged
	}
	return
}
func Multiplex(col []*vector.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	for i := range col {
		if nulls.Any(col[i].Nsp) {
			ret = multiplexNullableBlocks(col, src, fromLayout, toLayout)
			return
		}
	}
	ret = multiplexBlocks(col, src, fromLayout, toLayout)
	return
}

func multiplexBlocks(col []*vector.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	data := make([][]types.Datetime, len(col))
	ret = make([]*vector.Vector, len(toLayout))

	for i, v := range col {
		data[i] = v.Col.([]types.Datetime)
	}

	from := len(data)
	to := len(toLayout)
	cursors := make([]int, from)
	merged := make([][]types.Datetime, to)

	for i := 0; i < to; i++ {
		merged[i] = make([]types.Datetime, toLayout[i])
	}

	k := 0
	for i := 0; i < to; i++ {
		for j := 0; j < int(toLayout[i]); j++ {
			s := src[k]
			merged[i][j] = data[s][cursors[s]]
			cursors[s]++
			k++
		}
	}

	for i := 0; i < to; i++ {
		ret[i] = vector.New(col[0].Typ)
		ret[i].Col = merged[i]
	}
	return
}

func multiplexNullableBlocks(col []*vector.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []*vector.Vector) {
	data := make([][]types.Datetime, len(col))
	for i, v := range col {
		data[i] = v.Col.([]types.Datetime)
	}
	from := len(fromLayout)
	to := len(toLayout)

	nulls := make([]*roaring.Bitmap, from)
	nullIters := make([]roaring.IntIterable64, from)
	nextNulls := make([]int, from)

	for i, v := range col {
		data[i] = v.Col.([]types.Datetime)
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

	cursors := make([]int, from)
	merged := make([][]types.Datetime, to)
	newNulls := make([]*roaring.Bitmap, to)
	ret = make([]*vector.Vector, to)

	for i := 0; i < to; i++ {
		merged[i] = make([]types.Datetime, toLayout[i])
	}

	k := 0
	for i := 0; i < to; i++ {
		newNulls[i] = roaring.New()
		for j := 0; j < int(toLayout[i]); j++ {
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

	for i := 0; i < to; i++ {
		ret[i] = vector.New(col[0].Typ)
		ret[i].Col = merged[i]
		ret[i].Nsp.Np = newNulls[i]
		ret[i].Nsp.Np.RunOptimize()
	}
	return
}
