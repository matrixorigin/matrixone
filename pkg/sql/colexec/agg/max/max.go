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

package max

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func NewMax[T any]() *Max[T] {
	return &Max[T]{}
}

func (m *Max[T]) Grows(_ int) {
}

func (m *Max[T]) Eval(vs []T) []T {
	return vs
}

func (m *Max[T]) Fill(_ int64, value T, ov T, _ int64, isEmpty bool, isNull bool) (T, bool) {
	if !isNull {
		switch m.typ.Oid {
		case types.T_decimal128:
			tmp1 := (any)(value).(types.Decimal128)
			tmp2 := (any)(ov).(types.Decimal128)
			if types.CompareDecimal128Decimal128Aligned(tmp1, tmp2) == 1 {
				return value, false
			}
		case types.T_bool:
			tmp1 := (any)(value).(bool)
			tmp2 := (any)(ov).(bool)
			if tmp1 && !tmp2 {
				return value, false
			}
		case types.T_char, types.T_varchar:
			tmp1 := (any)(value).(types.Bytes).Data
			tmp2 := (any)(ov).(types.Bytes).Data
			if bytes.Compare(tmp1, tmp2) > 0 {
				return value, false
			}
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_date,
			types.T_datetime, types.T_timestamp, types.T_decimal64:
			tmp1 := (any)(value).(int64)
			tmp2 := (any)(ov).(int64)
			if tmp1 > tmp2 {
				return value, false
			}
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			tmp1 := (any)(value).(uint64)
			tmp2 := (any)(ov).(uint64)
			if tmp1 > tmp2 {
				return value, false
			}
		case types.T_float32, types.T_float64:
			tmp1 := (any)(value).(float64)
			tmp2 := (any)(ov).(float64)
			if tmp1 > tmp2 {
				return value, false
			}
		}
	}
	return ov, isEmpty
}

func (m *Max[T]) Merge(_ int64, _ int64, x T, y T, xEmpty bool, yEmpty bool, _ any) (T, bool) {
	if !xEmpty && yEmpty {
		return x, false
	} else if xEmpty && !yEmpty {
		return y, false
	} else if !xEmpty && !yEmpty {
		switch m.typ.Oid {
		case types.T_decimal128:
			tmp1 := (any)(x).(types.Decimal128)
			tmp2 := (any)(y).(types.Decimal128)
			if types.CompareDecimal128Decimal128Aligned(tmp1, tmp2) == 1 {
				return x, false
			}
		case types.T_bool:
			tmp1 := (any)(x).(bool)
			tmp2 := (any)(y).(bool)
			if tmp1 && !tmp2 {
				return x, false
			}
		case types.T_char, types.T_varchar:
			tmp1 := (any)(x).(types.Bytes).Data
			tmp2 := (any)(y).(types.Bytes).Data
			if bytes.Compare(tmp1, tmp2) > 0 {
				return x, false
			}
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_date,
			types.T_datetime, types.T_timestamp, types.T_decimal64:
			tmp1 := (any)(x).(int64)
			tmp2 := (any)(y).(int64)
			if tmp1 > tmp2 {
				return x, false
			}
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			tmp1 := (any)(x).(uint64)
			tmp2 := (any)(y).(uint64)
			if tmp1 > tmp2 {
				return x, false
			}
		case types.T_float32, types.T_float64:
			tmp1 := (any)(x).(float64)
			tmp2 := (any)(y).(float64)
			if tmp1 > tmp2 {
				return x, false
			}
		}
		return y, false
	}

	return x, true
}
