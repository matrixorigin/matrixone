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

package int16s

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func Sort(col containers.Vector, idx []uint32) (ret containers.Vector) {
	n := len(idx)
	dataWithIdx := make(sortSlice, n)

	for i := 0; i < n; i++ {
		dataWithIdx[i] = sortElem{data: col.Get(i).(int16), idx: uint32(i)}
	}

	sortUnstable(dataWithIdx)

	for i, v := range dataWithIdx {
		idx[i] = v.idx
		col.Update(i, v.data)
	}
	ret = col
	return
}

func Shuffle(col containers.Vector, idx []uint32) {
	ret := containers.MakeVector(col.GetType(), col.Nullable())
	for _, j := range idx {
		ret.Append(col.Get(int(j)))
	}

	col.ResetWithData(ret.Bytes(), ret.NullMask())
	ret.Close()
}

func Merge(col []containers.Vector, src *[]uint32, fromLayout, toLayout []uint32) (ret []containers.Vector, mapping []uint32) {
	ret = make([]containers.Vector, len(toLayout))
	mapping = make([]uint32, len(*src))

	offset := make([]uint32, len(fromLayout))
	offset[0] = 0
	for i := 1; i < len(fromLayout); i++ {
		offset[i] = offset[i-1] + fromLayout[i-1]
	}

	for i := range toLayout {
		ret[i] = containers.MakeVector(col[0].GetType(), col[0].Nullable())
	}

	nBlk := len(col)
	heap := make(heapSlice, nBlk)

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem{data: col[i].Get(0).(int16), src: uint32(i), next: 1}
	}
	heapInit(heap)

	k := 0
	for i := 0; i < len(toLayout); i++ {
		for j := 0; j < int(toLayout[i]); j++ {
			top := heapPop(&heap)
			(*src)[k] = top.src
			ret[i].Append(top.data)
			mapping[offset[top.src]+top.next-1] = uint32(k)
			k++
			if int(top.next) < int(fromLayout[top.src]) {
				heapPush(&heap, heapElem{data: col[top.src].Get(int(top.next)).(int16), src: top.src, next: top.next + 1})
			}
		}
	}
	return
}

func Reshape(col []containers.Vector, fromLayout, toLayout []uint32) (ret []containers.Vector) {
	ret = make([]containers.Vector, len(toLayout))
	fromIdx := 0
	fromOffset := 0
	for i := 0; i < len(toLayout); i++ {
		ret[i] = containers.MakeVector(col[0].GetType(), col[0].Nullable())
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
			ret[i].Extend(col[fromIdx].CloneWindow(fromOffset, length))
			fromOffset += length
			toOffset += length
		}
	}
	for _, v := range col {
		v.Close()
	}
	return
}

func Multiplex(col []containers.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []containers.Vector) {
	ret = make([]containers.Vector, len(toLayout))
	to := len(toLayout)
	cursors := make([]int, len(fromLayout))

	for i := 0; i < to; i++ {
		ret[i] = containers.MakeVector(col[0].GetType(), col[0].Nullable())
	}

	k := 0
	for i := 0; i < to; i++ {
		for j := 0; j < int(toLayout[i]); j++ {
			s := src[k]
			ret[i].Append(col[s].Get(cursors[s]))
			cursors[s]++
			k++
		}
	}

	for _, v := range col {
		v.Close()
	}
	return
}
