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

package bit_and

import "github.com/matrixorigin/matrixone/pkg/container/types"

func New[T1 types.Ints | types.UInts | types.Floats]() *BitAnd[T1] {
	return &BitAnd[T1]{}
}

func (ba *BitAnd[T1]) Grows(size int) {
	if len(ba.elemNums) == 0 {
		ba.elemNums = make([]int64, size)
	} else {
		for i := 0; i < size; i++ {
			var a int64
			ba.elemNums = append(ba.elemNums, a)
		}
	}
}

func (ba *BitAnd[T1]) Eval(vs []uint64) []uint64 {
	for idx := range vs {
		if ba.elemNums[idx] == 1 {
			vs[idx] = 0
		}
	}
	return vs
}

func (ba *BitAnd[T1]) Merge(groupIndex1, groupIndex2 int64, x, y uint64, IsEmpty1 bool, IsEmpty2 bool, agg any) (uint64, bool) {
	ba2 := agg.(*BitAnd[T1])
	if IsEmpty1 && !IsEmpty2 {
		ba.elemNums[groupIndex1] = ba2.elemNums[groupIndex2]
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		ba.elemNums[groupIndex1] += ba2.elemNums[groupIndex2]
		return x & y, false
	}
}

func (ba *BitAnd[T1]) Fill(groupIndex int64, v1 T1, v2 uint64, z int64, IsEmpty bool, hasNull bool) (uint64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		ba.elemNums[groupIndex] = 1
		return uint64(v1), false
	}
	ba.elemNums[groupIndex] += 1
	return uint64(v1) & v2, false
}
