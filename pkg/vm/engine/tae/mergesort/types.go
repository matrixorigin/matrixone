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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

const nullFirst = true

type dataFetcher[T any] interface {
	mustToCol(*vector.Vector, uint32)
	at(i, j uint32) T
	length(i uint32) int
}

type heapElem[T any] struct {
	data   T
	isNull bool
	src    uint32
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

func (x *heapSlice[T]) Less(i, j int) bool {
	a, b := x.s[i], x.s[j]
	if !a.isNull && !b.isNull {
		return x.lessFunc(a.data, b.data)
	}
	if a.isNull && b.isNull {
		return false
	} else if a.isNull {
		return nullFirst
	} else {
		return !nullFirst
	}
}
func (x *heapSlice[T]) Swap(i, j int) { x.s[i], x.s[j] = x.s[j], x.s[i] }
func (x *heapSlice[T]) Len() int      { return len(x.s) }

type mergeStats struct {
	totalRowCnt, rowSize, targetObjSize uint32
	blkPerObj                           uint16

	blkRowCnt, objRowCnt, objBlkCnt int
	mergedRowCnt, objCnt            int
}

func (s *mergeStats) needNewObject() bool {
	if s.targetObjSize == 0 {
		if s.blkPerObj == 0 {
			return s.objBlkCnt == int(options.DefaultBlocksPerObject)
		}
		return s.objBlkCnt == int(s.blkPerObj)
	}

	if uint32(s.objRowCnt)*s.rowSize > s.targetObjSize {
		return (s.totalRowCnt-uint32(s.mergedRowCnt))*s.rowSize > s.targetObjSize
	}
	return false
}
