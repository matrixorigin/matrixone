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

type BitAnd[T1 types.Ints | types.UInts | types.Floats] struct {
}

func BitAndReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_float32, types.T_float64:
		return types.New(types.T_uint64, 0, 0, 0)
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return types.New(types.T_uint64, 0, 0, 0)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return types.New(types.T_uint64, 0, 0, 0)
	}
	return types.Type{}
}

func NewBitAnd[T1 types.Ints | types.UInts | types.Floats]() *BitAnd[T1] {
	return &BitAnd[T1]{}
}

func (ba *BitAnd[T1]) Grows(_ int) {
}

func (ba *BitAnd[T1]) Eval(vs []uint64) []uint64 {
	return vs
}

func (ba *BitAnd[T1]) Merge(groupIndex1, groupIndex2 int64, x, y uint64, isEmpty1 bool, isEmpty2 bool, agg any) (uint64, bool) {
	if isEmpty1 {
		x = ^uint64(0)
	}
	if isEmpty2 {
		y = ^uint64(0)
	}
	return x & y, isEmpty1 && isEmpty2
}

func (ba *BitAnd[T1]) Fill(groupIndex int64, v1 T1, v2 uint64, z int64, isEmpty bool, hasNull bool) (uint64, bool) {
	if hasNull {
		return v2, isEmpty
	}
	if isEmpty {
		v2 = ^uint64(0)
	}
	if float64(v1) > math.MaxUint64 {
		return math.MaxInt64 & v2, false
	}
	return uint64(v1) & v2, false
}

func (ba *BitAnd[T1]) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (ba *BitAnd[T1]) UnmarshalBinary(data []byte) error {
	return nil
}
