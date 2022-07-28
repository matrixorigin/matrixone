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

package bit_or

import "github.com/matrixorigin/matrixone/pkg/container/types"

func ReturnType(_ []types.Type) types.Type {
	return types.New(types.T_uint64, 0, 0, 0)
}

func New[T1 types.Ints | types.UInts | types.Floats]() *BitOr[T1] {
	return &BitOr[T1]{}
}

func (bo *BitOr[T1]) Grows(_ int) {
}

func (bo *BitOr[T1]) Eval(vs []uint64) []uint64 {
	return vs
}

func (bo *BitOr[T1]) Merge(_, _ int64, x, y uint64, IsEmpty1 bool, IsEmpty2 bool, _ any) (uint64, bool) {
	if IsEmpty1 && !IsEmpty2 {
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		return x | y, false
	}
}

func (bo *BitOr[T1]) Fill(_ int64, v1 T1, v2 uint64, z int64, IsEmpty bool, hasNull bool) (uint64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		return uint64(v1), false
	} else {
		return uint64(v1) | v2, false
	}
}
