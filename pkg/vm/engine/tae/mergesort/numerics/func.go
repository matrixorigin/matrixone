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

package numerics

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func Sort[T types.OrderedT](col containers.Vector, idx []uint32) (ret containers.Vector) {
	n := len(idx)
	dataWithIdx := make(sortSlice[T], n)

	for i := 0; i < n; i++ {
		dataWithIdx[i] = sortElem[T]{data: col.Get(i).(T), idx: uint32(i)}
	}

	sortUnstable(dataWithIdx)

	for i, v := range dataWithIdx {
		idx[i] = v.idx
		col.Update(i, v.data)
	}
	ret = col
	return
}

func Merge[T types.OrderedT](col []containers.Vector, src *[]uint32, fromLayout, toLayout []uint32) (ret []containers.Vector, mapping []uint32) {
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
	heap := make(heapSlice[T], nBlk)

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem[T]{data: col[i].Get(0).(T), src: uint32(i), next: 1}
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
				heapPush(&heap, heapElem[T]{data: col[top.src].Get(int(top.next)).(T), src: top.src, next: top.next + 1})
			}
		}
	}
	return
}
