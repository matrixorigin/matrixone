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
)

type timeIntervalState uint8

const (
	timeIntervalValid timeIntervalState = iota
	timeIntervalNull
	timeIntervalInvalid
	timeIntervalOverflow
)

type timeIntervalValue struct {
	value int64
	unit  types.IntervalType
	state timeIntervalState
}

// normalizeRawTimeInterval retains the reason a value cannot be represented.
// An INT64 intermediate vector would collapse input NULL, syntax errors and
// overflow before TIME arithmetic can decide whether a warning is due.
func normalizeRawTimeInterval(text string, unit types.IntervalType) timeIntervalValue {
	value, normalizedUnit, overflow, err := types.NormalizeIntervalWithOverflow(text, unit)
	if err != nil {
		if overflow {
			return timeIntervalValue{state: timeIntervalOverflow}
		}
		return timeIntervalValue{state: timeIntervalInvalid}
	}
	if normalizedUnit != types.MicroSecond {
		factor := int64(0)
		switch unit {
		case types.Second, types.Minute_Second, types.Hour_Second, types.Day_Second:
			factor = types.MicroSecsPerSec
		case types.Minute:
			factor = types.MicroSecsPerSec * types.SecsPerMinute
		case types.Hour:
			factor = types.MicroSecsPerSec * types.SecsPerHour
		case types.Day:
			factor = types.MicroSecsPerSec * types.SecsPerDay
		}
		if factor != 0 {
			if value > math.MaxInt64/factor || value < math.MinInt64/factor {
				return timeIntervalValue{state: timeIntervalOverflow}
			}
			value *= factor
			normalizedUnit = types.MicroSecond
		}
	}
	return timeIntervalValue{value: value, unit: normalizedUnit}
}

// Numeric MICROSECOND values are rounded to the nearest integral microsecond.
// Parse the decimal spelling directly so DECIMAL256 does not lose its low
// digits (or overflow classification) through float64.
func roundedRawMicroseconds(text string) timeIntervalValue {
	negative := strings.HasPrefix(text, "-")
	digits := strings.TrimPrefix(strings.TrimPrefix(text, "-"), "+")
	whole, fraction, _ := strings.Cut(digits, ".")
	if whole == "" {
		return timeIntervalValue{state: timeIntervalInvalid}
	}
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	number, err := strconv.ParseUint(whole, 10, 64)
	if err != nil || number > limit {
		return timeIntervalValue{state: timeIntervalOverflow}
	}
	if len(fraction) > 0 && fraction[0] >= '5' {
		if number == limit {
			return timeIntervalValue{state: timeIntervalOverflow}
		}
		number++
	}
	value := int64(number)
	if negative {
		value = -value
	}
	return timeIntervalValue{value: value, unit: types.MicroSecond}
}

func rawTimeIntervalGetter(vec *vector.Vector, unit types.IntervalType) (func(uint64) timeIntervalValue, error) {
	scale := vec.GetType().Scale
	switch vec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_text:
		values := vector.GenerateFunctionStrParameter(vec)
		return func(i uint64) timeIntervalValue {
			value, isNull := values.GetStrValue(i)
			if isNull {
				return timeIntervalValue{state: timeIntervalNull}
			}
			return normalizeRawTimeInterval(functionUtil.QuickBytesToStr(value), unit)
		}, nil
	case types.T_float32:
		return typedRawTimeIntervalGetter[float32](vec, unit, scale,
			func(v float32, _ int32) string { return strconv.FormatFloat(float64(v), 'f', -1, 32) },
			func(v float32) float64 { return float64(v) }), nil
	case types.T_float64:
		return typedRawTimeIntervalGetter[float64](vec, unit, scale,
			func(v float64, _ int32) string { return strconv.FormatFloat(v, 'f', -1, 64) },
			func(v float64) float64 { return v }), nil
	case types.T_decimal64:
		return typedRawTimeIntervalGetter[types.Decimal64](vec, unit, scale,
			func(v types.Decimal64, scale int32) string { return canonicalIntervalDecimal(v.Format(scale), scale) }, nil), nil
	case types.T_decimal128:
		return typedRawTimeIntervalGetter[types.Decimal128](vec, unit, scale,
			func(v types.Decimal128, scale int32) string { return canonicalIntervalDecimal(v.Format(scale), scale) }, nil), nil
	case types.T_decimal256:
		return typedRawTimeIntervalGetter[types.Decimal256](vec, unit, scale,
			func(v types.Decimal256, scale int32) string { return canonicalIntervalDecimal(v.Format(scale), scale) }, nil), nil
	default:
		return nil, moerr.NewInvalidArgNoCtx("time interval source type", vec.GetType().Oid)
	}
}

func typedRawTimeIntervalGetter[T types.FixedSizeTExceptStrType](
	vec *vector.Vector, unit types.IntervalType, scale int32,
	format func(T, int32) string, floatValue func(T) float64,
) func(uint64) timeIntervalValue {
	values := vector.GenerateFunctionFixedTypeParameter[T](vec)
	return func(i uint64) timeIntervalValue {
		value, isNull := values.GetValue(i)
		if isNull {
			return timeIntervalValue{state: timeIntervalNull}
		}
		if floatValue != nil {
			number := floatValue(value)
			if math.IsNaN(number) || math.IsInf(number, 0) {
				return timeIntervalValue{state: timeIntervalInvalid}
			}
			if microseconds, valid, handled := roundedScalarFloatInterval(number, unit); handled {
				if !valid {
					return timeIntervalValue{state: timeIntervalOverflow}
				}
				return timeIntervalValue{value: microseconds, unit: types.MicroSecond}
			}
		}
		text := format(value, scale)
		if unit == types.MicroSecond {
			return roundedRawMicroseconds(text)
		}
		return normalizeRawTimeInterval(text, unit)
	}
}
