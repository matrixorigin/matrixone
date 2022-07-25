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

package bit_xor

import "github.com/matrixorigin/matrixone/pkg/container/types"

func New[T1 types.Ints | types.UInts | types.Floats]() *BitXor[T1] {
	return &BitXor[T1]{}
}

func (bx *BitXor[T1]) Grows(_ int) {
}

func (bx *BitXor[T1]) Eval(vs []int64) []int64 {
	return vs
}

func (bx *BitXor[T1]) Merge(_, _ int64, x, y int64, IsEmpty1 bool, IsEmpty2 bool, _ any) (int64, bool) {
	if IsEmpty1 && !IsEmpty2 {
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		return x ^ y, false
	}
}

func (bx *BitXor[T1]) Fill(_ int64, v1 T1, v2 int64, z int64, IsEmpty bool, hasNull bool) (int64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		if z%2 == 0 {
			return int64(0), false
		} else {
			return int64(v1), false
		}
	}
	if z%2 == 0 {
		return int64(v2), false
	} else {
		return int64(v1) ^ v2, false
	}
}
