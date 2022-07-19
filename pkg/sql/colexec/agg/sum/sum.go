// Copyright 2022 Matrix Origin
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

package sum

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func NewSum[T1, T2 any](t types.Type) *Sum[T1, T2] {
	return &Sum[T1, T2]{
		typ: t,
	}
}

func (s *Sum[T1, T2]) Grows(_ int) {
}

func (s *Sum[T1, T2]) Eval(vs []T2) []T2 {
	return vs
}

func (s *Sum[T1, T2]) Fill(_ int64, value T1, ov T2, z int64, isEmpty bool, isNull bool) (T2, bool) {
	if !isNull {
		switch s.typ.Oid {
		case types.T_decimal64:
			tmp1 := (any)(types.Decimal64Int64Mul((any)(value).(types.Decimal64), z)).(types.Decimal64)
			tmp2 := (any)(ov).(types.Decimal64)
			return (any)(tmp1 + tmp2).(T2), false
		case types.T_decimal128:
			tmp1 := types.Decimal128Int64Mul((any)(value).(types.Decimal128), z)
			tmp2 := (any)(ov).(types.Decimal128)
			return (any)(types.Decimal128AddAligned(tmp1, tmp2)).(T2), false
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			tmp1 := TransInt(value) * z
			tmp2 := (any)(ov).(int64)
			return (any)(tmp1 + tmp2).(T2), false
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			tmp1 := TransUint(value) * uint64(z)
			tmp2 := (any)(ov).(uint64)
			return (any)(tmp1 + tmp2).(T2), false
		case types.T_float32, types.T_float64:
			tmp1 := TransFloat(value) * float64(z)
			tmp2 := (any)(ov).(float64)
			return (any)(tmp1 + tmp2).(T2), false
		}
	}
	return ov, isEmpty
}

func (s *Sum[T1, T2]) Merge(_ int64, _ int64, x T2, y T2, xEmpty bool, yEmpty bool, _ any) (T2, bool) {
	if !xEmpty && yEmpty {
		return x, false
	} else if xEmpty && !yEmpty {
		return y, false
	} else if !xEmpty && !yEmpty {
		switch s.typ.Oid {
		case types.T_decimal64:
			tmp := (any)(x).(types.Decimal64) + (any)(y).(types.Decimal64)
			return (any)(tmp).(T2), false
		case types.T_decimal128:
			tmp := types.Decimal128AddAligned((any)(x).(types.Decimal128), (any)(y).(types.Decimal128))
			return (any)(tmp).(T2), false
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			tmp := TransInt(x) + TransInt(y)
			return (any)(tmp).(T2), false
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			tmp := TransUint(x) + TransUint(y)
			return (any)(tmp).(T2), false
		case types.T_float32, types.T_float64:
			tmp := TransFloat(x) + TransFloat(y)
			return (any)(tmp).(T2), false
		}
	}
	return x, true
}

func TransInt(a any) int64 {
	switch v := a.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	default:
		return 0
	}
}

func TransUint(a any) uint64 {
	switch v := a.(type) {
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)
	default:
		return 0
	}
}
func TransFloat(a any) float64 {
	switch v := a.(type) {
	case float32:
		return float64(v)
	case float64:
		return float64(v)
	default:
		return 0
	}
}
