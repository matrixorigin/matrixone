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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type BitAnd[T1 types.Ints | types.UInts | types.Floats] struct {
}

var BitAndSupported = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
	types.T_binary, types.T_varbinary,
}

func BitAndReturnType(typs []types.Type) types.Type {
	if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
		return typs[0]
	}
	return types.New(types.T_uint64, 0, 0)
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
	if float64(v1) < 0 {
		return uint64(int64(v1)) & v2, false
	}
	return uint64(v1) & v2, false
}

func (ba *BitAnd[T1]) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (ba *BitAnd[T1]) UnmarshalBinary(data []byte) error {
	return nil
}

type BitAndBinary struct {
}

func NewBitAndBinary() *BitAndBinary {
	return &BitAndBinary{}
}

func (bab *BitAndBinary) Grows(_ int) {
}

func (bab *BitAndBinary) Eval(vs [][]byte) [][]byte {
	return vs
}

func (bab *BitAndBinary) Merge(gNum1, gNum2 int64, v1, v2 []byte, empty1, empty2 bool, _ any) ([]byte, bool) {
	if empty1 {
		return v2, empty2
	}
	if empty2 {
		return v1, empty1
	}

	result := make([]byte, len(v1))
	types.BitAnd(result, v1, v2)

	return result, false
}

func (bab *BitAndBinary) Fill(gNum int64, v1, v2 []byte, _ int64, isNew, isNull bool) ([]byte, bool) {
	if isNull {
		return v2, isNew
	}
	if isNew {
		return v1, !isNew
	}

	result := make([]byte, len(v1))
	types.BitAnd(result, v1, v2)

	return result, false
}

func (bab *BitAndBinary) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (bab *BitAndBinary) UnmarshalBinary(data []byte) error {
	return nil
}
