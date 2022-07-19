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

package avg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func NewAvg[T1, T2 any](t types.Type) *Avg[T1, T2] {
	return &Avg[T1, T2]{
		typ: t,
		cnt: int64(0),
	}
}

func (a *Avg[T1, T2]) Grows(_ int) {
}

func (a *Avg[T1, T2]) Eval(vs []T2) []T2 {
	switch a.typ.Oid {
	case types.T_decimal64:
		for i := range vs {
			tmp := types.Decimal64ToDecimal128((any)(vs[i]).(types.Decimal64))
			vs[i] = (any)(types.Decimal128Int64Div(tmp, a.cnt)).(T2)
		}
	case types.T_decimal128:
		for i := range vs {
			vs[i] = (any)(types.Decimal128Int64Div((any)(vs[i]).(types.Decimal128), a.cnt)).(T2)
		}
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8,
		types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64:
		for i := range vs {
			vs[i] = (any)(TransToFloat64(vs[i]) / float64(a.cnt)).(T2)
		}
	}
	return vs
}

func (a *Avg[T1, T2]) Fill(_ int64, value T1, ov T2, z int64, isEmpty bool, isNull bool) (T2, bool) {
	if !isNull {
		a.cnt += z
		switch a.typ.Oid {
		case types.T_decimal64:
			tmp1 := types.Decimal64Decimal64Mul((any)(value).(types.Decimal64), types.Decimal64(z))
			tmp2 := (any)(ov).(types.Decimal128)
			return (any)(types.Decimal128AddAligned(tmp1, tmp2)).(T2), false
		case types.T_decimal128:
			tmp1 := types.Decimal128Int64Mul((any)(value).(types.Decimal128), z)
			tmp2 := (any)(ov).(types.Decimal128)
			return (any)(types.Decimal128AddAligned(tmp1, tmp2)).(T2), false
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8,
			types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64:
			tmp1 := TransToFloat64(value) * float64(z)
			tmp2 := (any)(ov).(float64)
			return (any)(tmp1 + tmp2).(T2), false
		}
	}
	return ov, isEmpty
}

func (a *Avg[T1, T2]) Merge(xIndex int64, yIndex int64, x T2, y T2, xEmpty bool, yEmpty bool, yAvg any) (T2, bool) {
	if !xEmpty && yEmpty {
		return x, false
	} else if xEmpty && !yEmpty {
		ya := yAvg.(*Avg[T1, T2])
		a.cnt += ya.cnt
		return y, false
	} else if !xEmpty && !yEmpty {
		ya := yAvg.(*Avg[T1, T2])
		a.cnt += ya.cnt
		switch a.typ.Oid {
		case types.T_decimal64:
			tmp := (any)(x).(types.Decimal64) + (any)(y).(types.Decimal64)
			return (any)(tmp).(T2), false
		case types.T_decimal128:
			tmp := types.Decimal128AddAligned((any)(x).(types.Decimal128), (any)(y).(types.Decimal128))
			return (any)(tmp).(T2), false
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8,
			types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64:
			tmp := TransToFloat64(x) + TransToFloat64(y)
			return (any)(tmp).(T2), false
		}
	}
	return x, true
}

func TransToFloat64(a any) float64 {
	switch v := a.(type) {
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return float64(v)
	default:
		return 0 //can't reach
	}
}
