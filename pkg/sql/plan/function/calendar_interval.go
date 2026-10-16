// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// normalizeCalendarInterval returns DAY counts or SECOND counts in microseconds.
// Its identity is separate from the released elapsed-interval normalizers.
// Keep their permissive digit-field string grammar, but round only DECIMAL DAY;
// SECOND always discards sub-microsecond digits before applying the sign.
func normalizeCalendarInterval(text string, unit types.IntervalType, decimal bool) timeIntervalValue {
	if unit != types.Day && unit != types.Second {
		return timeIntervalValue{state: timeIntervalInvalid}
	}
	text = strings.TrimSpace(text)
	var starts, ends [2]int
	count := 0
	inDigits := false
	for i := 0; i < len(text); i++ {
		if text[i] >= '0' && text[i] <= '9' {
			if !inDigits {
				if count == len(starts) {
					return timeIntervalValue{state: timeIntervalInvalid}
				}
				starts[count] = i
				count++
				inDigits = true
			}
			ends[count-1] = i + 1
		} else {
			inDigits = false
		}
	}
	if count == 0 {
		return timeIntervalValue{state: timeIntervalInvalid}
	}
	fraction := ""
	if count == 2 {
		dot := strings.LastIndexByte(text, '.')
		if dot < 0 || starts[1] != dot+1 || ends[1] != len(text) {
			return timeIntervalValue{state: timeIntervalInvalid}
		}
		fraction = text[starts[1]:ends[1]]
	}
	negative := strings.HasPrefix(text, "-")
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	whole, err := strconv.ParseUint(text[starts[0]:ends[0]], 10, 64)
	if err != nil {
		return timeIntervalValue{state: timeIntervalOverflow}
	}
	factor, part := uint64(1), uint64(0)
	if unit == types.Second {
		factor = types.MicroSecsPerSec
		for i := 0; i < 6; i++ {
			part *= 10
			if i < len(fraction) {
				part += uint64(fraction[i] - '0')
			}
		}
	} else if decimal && len(fraction) > 0 && fraction[0] >= '5' {
		part = 1
	}
	if whole > (limit-part)/factor {
		return timeIntervalValue{state: timeIntervalOverflow}
	}
	value := int64(whole*factor + part)
	if negative {
		value = -value
	}
	return timeIntervalValue{value: value, unit: unit}
}

func calendarIntervalOverloads() []overload {
	// Append-only ordinals for this new identity, including integer prepared values.
	sources := []types.T{types.T_varchar, types.T_char, types.T_text,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64}
	result := make([]overload, len(sources))
	for i, source := range sources {
		result[i] = overload{overloadId: i, args: []types.T{source, types.T_int64},
			retType: func([]types.Type) types.Type { return types.T_int64.ToType() },
			newOp:   func() executeLogicOfOverload { return ToCalendarInterval }}
	}
	return result
}

func calendarIntervalGetter(vec *vector.Vector) (func(uint64) (string, bool), error) {
	scale := vec.GetType().Scale
	switch vec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_text:
		values := vector.GenerateFunctionStrParameter(vec)
		return func(i uint64) (string, bool) {
			value, null := values.GetStrValue(i)
			return functionUtil.QuickBytesToStr(value), null
		}, nil
	// FLOAT is promoted before MySQL decimal conversion; preserve its full value.
	case types.T_float32:
		return typedCalendarIntervalGetter(vec, scale, func(v float32, _ int32) string { return strconv.FormatFloat(float64(v), 'f', -1, 64) }), nil
	case types.T_float64:
		return typedCalendarIntervalGetter(vec, scale, func(v float64, _ int32) string { return strconv.FormatFloat(v, 'f', -1, 64) }), nil
	case types.T_decimal64:
		return typedCalendarIntervalGetter(vec, scale, func(v types.Decimal64, s int32) string { return v.Format(s) }), nil
	case types.T_decimal128:
		return typedCalendarIntervalGetter(vec, scale, func(v types.Decimal128, s int32) string { return v.Format(s) }), nil
	case types.T_decimal256:
		return typedCalendarIntervalGetter(vec, scale, func(v types.Decimal256, s int32) string { return v.Format(s) }), nil
	case types.T_int8:
		return signedCalendarIntervalGetter[int8](vec), nil
	case types.T_int16:
		return signedCalendarIntervalGetter[int16](vec), nil
	case types.T_int32:
		return signedCalendarIntervalGetter[int32](vec), nil
	case types.T_int64:
		return signedCalendarIntervalGetter[int64](vec), nil
	case types.T_uint8:
		return unsignedCalendarIntervalGetter[uint8](vec), nil
	case types.T_uint16:
		return unsignedCalendarIntervalGetter[uint16](vec), nil
	case types.T_uint32:
		return unsignedCalendarIntervalGetter[uint32](vec), nil
	case types.T_uint64:
		return unsignedCalendarIntervalGetter[uint64](vec), nil
	}
	return nil, moerr.NewInvalidArgNoCtx("calendar interval source", vec.GetType().Oid)
}

func typedCalendarIntervalGetter[T types.FixedSizeTExceptStrType](vec *vector.Vector, scale int32, format func(T, int32) string) func(uint64) (string, bool) {
	values := vector.GenerateFunctionFixedTypeParameter[T](vec)
	return func(i uint64) (string, bool) {
		value, null := values.GetValue(i)
		if null {
			return "", true
		}
		return format(value, scale), false
	}
}
func signedCalendarIntervalGetter[T interface {
	~int8 | ~int16 | ~int32 | ~int64
}](vec *vector.Vector) func(uint64) (string, bool) {
	return typedCalendarIntervalGetter(vec, 0, func(v T, _ int32) string { return strconv.FormatInt(int64(v), 10) })
}
func unsignedCalendarIntervalGetter[T interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}](vec *vector.Vector) func(uint64) (string, bool) {
	return typedCalendarIntervalGetter(vec, 0, func(v T, _ int32) string { return strconv.FormatUint(uint64(v), 10) })
}

func ToCalendarInterval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	get, err := calendarIntervalGetter(ivecs[0])
	if err != nil {
		return err
	}
	decimal := ivecs[0].GetType().Oid.IsDecimal()
	units := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		if selectList.IgnoreAllRow() || (!selectList.ShouldEvalAllRow() && selectList.Contains(i)) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		text, null := get(i)
		unit, unitNull := units.GetValue(i)
		if null || unitNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if types.IntervalType(unit) != types.Day && types.IntervalType(unit) != types.Second {
			return moerr.NewInvalidArgNoCtx("calendar interval unit", unit)
		}
		if err := appendTimeIntervalValue(rs, normalizeCalendarInterval(text, types.IntervalType(unit), decimal)); err != nil {
			return err
		}
	}
	return nil
}
