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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
)

type BitXor[T1 types.Ints | types.UInts | types.Floats] struct {
}

func BitXorReturnType(_ []types.Type) types.Type {
	return types.New(types.T_uint64, 0, 0, 0)
}

func NewBitXor[T1 types.Ints | types.UInts | types.Floats]() *BitXor[T1] {
	return &BitXor[T1]{}
}

func (bx *BitXor[T1]) Grows(_ int) {
}

func (bx *BitXor[T1]) Eval(vs []uint64) []uint64 {
	return vs
}

func (bx *BitXor[T1]) Merge(_, _ int64, x, y uint64, IsEmpty1 bool, IsEmpty2 bool, _ any) (uint64, bool) {
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

func (bx *BitXor[T1]) Fill(_ int64, v1 T1, v2 uint64, z int64, IsEmpty bool, hasNull bool) (uint64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		if z%2 == 0 {
			return uint64(0), false
		} else {
			if float64(v1) > math.MaxUint64 {
				return math.MaxInt64, false
			}
			return uint64(v1), false
		}
	}
	if z%2 == 0 {
		return v2, false
	} else {
		if float64(v1) > math.MaxUint64 {
			return math.MaxInt64 ^ v2, false
		}
		return uint64(v1) ^ v2, false
	}
}

func (bx *BitXor[T1]) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (bx *BitXor[T1]) UnmarshalBinary(data []byte) error {
	return nil
}
