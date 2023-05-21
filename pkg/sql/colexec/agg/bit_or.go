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

type BitOr[T1 types.Ints | types.UInts | types.Floats] struct {
}

var BitOrSupported = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
	types.T_binary, types.T_varbinary,
}

func BitOrReturnType(typs []types.Type) types.Type {
	if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
		return typs[0]
	}
	return types.New(types.T_uint64, 0, 0)
}

func NewBitOr[T1 types.Ints | types.UInts | types.Floats]() *BitOr[T1] {
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

func (bo *BitOr[T1]) Fill(_ int64, v1 T1, v2 uint64, _ int64, IsEmpty bool, hasNull bool) (uint64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		if float64(v1) > math.MaxUint64 {
			return math.MaxInt64, false
		}
		if float64(v1) < 0 {
			return uint64(int64(v1)), false
		}
		return uint64(v1), false
	} else {
		if float64(v1) > math.MaxUint64 {
			return math.MaxInt64 | v2, false
		}
		if float64(v1) < 0 {
			return uint64(int64(v1)) | v2, false
		}
		return uint64(v1) | v2, false
	}
}

func (bo *BitOr[T1]) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (bo *BitOr[T1]) UnmarshalBinary(data []byte) error {
	return nil
}

type BitOrBinary struct {
}

func NewBitOrBinary() *BitOrBinary {
	return &BitOrBinary{}
}

func (bab *BitOrBinary) Grows(_ int) {
}

func (bab *BitOrBinary) Eval(vs [][]byte) [][]byte {
	return vs
}

func (bab *BitOrBinary) Merge(gNum1, gNum2 int64, v1, v2 []byte, empty1, empty2 bool, _ any) ([]byte, bool) {
	if empty1 {
		return v2, empty2
	}
	if empty2 {
		return v1, empty1
	}

	result := make([]byte, len(v1))
	types.BitOr(result, v1, v2)

	return result, false
}

func (bab *BitOrBinary) Fill(gNum int64, v1, v2 []byte, _ int64, isNew, isNull bool) ([]byte, bool) {
	if isNull {
		return v2, isNew
	}
	if isNew {
		return v1, !isNew
	}

	result := make([]byte, len(v1))
	types.BitOr(result, v1, v2)

	return result, false
}

func (bab *BitOrBinary) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (bab *BitOrBinary) UnmarshalBinary(data []byte) error {
	return nil
}
