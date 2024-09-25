// Copyright 2024 Matrix Origin
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

package indexbuild

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type VectorIntersection struct {
	len    int
	num    int
	cnt    []int
	maxcnt []int
	col    [][]byte
	val    [][]byte
}

func (vi *VectorIntersection) build(k, left, right int) {
	if left > right {
		return
	}
	mid := (left + right) >> 1
	vi.val[k] = vi.col[mid]
	vi.build(k*2, left, mid-1)
	vi.build(k*2+1, mid+1, right)
}

func (vi *VectorIntersection) insert(i, k int) {
	if vi.maxcnt[k] < vi.num {
		return
	}
	c := bytes.Compare(vi.col[i], vi.val[k])
	if c == 0 {
		vi.cnt[k]++
		if vi.cnt[k] > vi.maxcnt[k] {
			vi.maxcnt[k]++
		}
	} else if c < 0 {
		next := k * 2
		if next < vi.len {
			vi.insert(i, next)
			if vi.maxcnt[next] > vi.maxcnt[k] {
				vi.maxcnt[k]++
			}
		}
	} else {
		next := k*2 + 1
		if next < vi.len {
			vi.insert(i, next)
			if vi.maxcnt[next] > vi.maxcnt[k] {
				vi.maxcnt[k]++
			}
		}
	}
}

func IntersectVectors(vec []*vector.Vector, proc *process.Process) *vector.Vector {
	if len(vec) == 1 {
		return vec[0]
	}
	minpos := 0
	minlen := vec[0].Length()
	typ := *vec[0].GetType()
	for i := 1; i < len(vec); i++ {
		leni := vec[i].Length()
		if leni < minlen {
			minpos = i
			minlen = leni
		}
	}

	result := vector.NewVec(typ)
	if minlen == 0 {
		return result
	}

	vi := &VectorIntersection{}
	vi.len = minlen | (minlen >> 1)
	vi.len = vi.len | (vi.len >> 2)
	vi.len = vi.len | (vi.len >> 4)
	vi.len = vi.len | (vi.len >> 8)
	vi.len = vi.len | (vi.len >> 16)
	vi.len++ //up to power of 2
	vi.cnt = make([]int, vi.len)
	vi.maxcnt = make([]int, vi.len)
	vi.val = make([][]byte, vi.len)

	if typ.Oid.FixedLength() > 0 {
		vi.col = vector.UnsafeToBytesSlice(vec[minpos])
		sort.Slice(vi.col, func(i, j int) bool {
			return bytes.Compare(vi.col[i], vi.col[j]) < 0
		})
		vi.build(1, 0, minlen-1)
		for i := range vec {
			if i == minpos {
				continue
			}
			vi.col = vector.UnsafeToBytesSlice(vec[i])
			for j := range vi.col {
				vi.insert(j, 1)
			}
			vi.num++
		}
		resultBytes := []byte{}
		for i := range vi.cnt {
			if vi.cnt[i] == vi.num {
				resultBytes = append(resultBytes, vi.val[i]...)
			}
		}
		vector.UnsafeSetFromBytesSlice(result, resultBytes)
		result.InplaceSort()
	} else {
		vi.col = make([][]byte, vec[minpos].Length())
		for i := range vi.col {
			vi.col[i] = vec[minpos].GetBytesAt(i)
		}
		sort.Slice(vi.col, func(i, j int) bool {
			return bytes.Compare(vi.col[i], vi.col[j]) < 0
		})
		vi.build(1, 0, minlen-1)
		for i := range vec {
			if i == minpos {
				continue
			}
			for j := 0; j < vec[i].Length(); j++ {
				vi.col[0] = vec[i].GetBytesAt(j)
				vi.insert(0, 1)
			}
			vi.num++
		}
		for i := range vi.cnt {
			if vi.cnt[i] == vi.num {
				vector.AppendBytes(result, vi.val[i], false, proc.Mp())
			}
		}
		result.InplaceSort()
	}
	return result
}
