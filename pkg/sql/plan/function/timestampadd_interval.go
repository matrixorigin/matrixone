// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// timestampAddSecondMicroseconds uses the existing numeric-prefix scanner, but
// truncates the exact decimal coefficient at six places. Neither storage nor
// work grows with an exponent. Overflow remains an arithmetic-consumer decision.
func timestampAddSecondMicroseconds(text string) int64 {
	var facts decimalPrefixFacts
	_, negative, ok := scanDecimalPrefix(text, &facts)
	if !ok || facts.first < 0 {
		return 0
	}
	limit := min(int64(len(text)), int64(math.MaxInt64-152)) + 152
	cut := saturatingDecimalPosition(facts.integral, facts.exponent, limit)
	cut = saturatingDecimalPosition(cut, 6, limit)
	if cut <= facts.first {
		return 0
	}
	if cut-facts.first > 19 {
		return calendarIntervalOverflow
	}
	bound := uint64(math.MaxInt64)
	if negative {
		bound++
	}
	var value uint64
	for i := facts.first; i < cut; i++ {
		digit := uint64(0)
		if i < facts.digits {
			position := i
			if int64(len(facts.mantissa)) > facts.digits && i >= facts.integral {
				position++
			}
			digit = uint64(facts.mantissa[position] - '0')
		}
		if value > (bound-digit)/10 {
			return calendarIntervalOverflow
		}
		value = value*10 + digit
	}
	if negative {
		return -int64(value)
	}
	return int64(value)
}

// TimestampAddSecondInterval is separate from DATE_ADD's interval grammar and
// rounding contract. All sources follow one decimal truncation rule.
func TimestampAddSecondInterval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	switch ivecs[0].GetType().Oid {
	case types.T_decimal64:
		return timestampAddTypedSeconds[types.Decimal64](ivecs[0], result, length, selectList, func(v types.Decimal64, scale int32) (int64, bool) {
			return timestampAddDecimal64Seconds(v, scale), false
		})
	case types.T_decimal128:
		return timestampAddTypedSeconds[types.Decimal128](ivecs[0], result, length, selectList, func(v types.Decimal128, scale int32) (int64, bool) {
			return timestampAddDecimal128Seconds(v, scale), false
		})
	case types.T_decimal256:
		return timestampAddTypedSeconds[types.Decimal256](ivecs[0], result, length, selectList, func(v types.Decimal256, scale int32) (int64, bool) {
			return timestampAddDecimal256Seconds(v, scale), false
		})
	case types.T_float32:
		return timestampAddTypedSeconds[float32](ivecs[0], result, length, selectList, func(v float32, _ int32) (int64, bool) { return timestampAddFloatSeconds(float64(v)) })
	case types.T_float64:
		return timestampAddTypedSeconds[float64](ivecs[0], result, length, selectList, func(v float64, _ int32) (int64, bool) { return timestampAddFloatSeconds(v) })
	default:
		values := vector.GenerateFunctionStrParameter(ivecs[0])
		rs := vector.MustFunctionResult[int64](result)
		for i := uint64(0); i < uint64(length); i++ {
			if functionRowSkipped(selectList, i) {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			value, null := values.GetStrValue(i)
			if err := rs.Append(timestampAddSecondMicroseconds(functionUtil.QuickBytesToStr(value)), null); err != nil {
				return err
			}
		}
		return nil
	}
}

func timestampAddTypedSeconds[T types.FixedSizeTExceptStrType](vec *vector.Vector, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList, convert func(T, int32) (int64, bool)) error {
	values := vector.GenerateFunctionFixedTypeParameter[T](vec)
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, null := values.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		microseconds, invalid := convert(value, vec.GetType().Scale)
		if err := rs.Append(microseconds, invalid); err != nil {
			return err
		}
	}
	return nil
}

// Typed DECIMAL coefficients already encode the exact value. Scale before
// narrowing, truncating toward zero without allocating a decimal spelling.
func timestampAddDecimal64Seconds(value types.Decimal64, scale int32) int64 {
	coefficient := int64(value)
	shift := 6 - scale
	if shift < 0 {
		if shift < -18 {
			return 0
		}
		return coefficient / int64(types.Pow10[-shift])
	}
	if coefficient == 0 {
		return 0
	}
	if shift > 18 {
		return calendarIntervalOverflow
	}
	factor := int64(types.Pow10[shift])
	if coefficient > math.MaxInt64/factor || coefficient < math.MinInt64/factor {
		return calendarIntervalOverflow
	}
	return coefficient * factor
}

func timestampAddDecimal128Seconds(value types.Decimal128, scale int32) int64 {
	if scale > 6 {
		value, _ = value.ScaleTruncate(6 - scale)
	}
	if (value.B64_127 != 0 || value.B0_63 > math.MaxInt64) && (value.B64_127 != math.MaxUint64 || value.B0_63 < uint64(1)<<63) {
		return calendarIntervalOverflow
	}
	return timestampAddDecimal64Seconds(types.Decimal64(value.B0_63), min(scale, 6))
}

func timestampAddDecimal256Seconds(value types.Decimal256, scale int32) int64 {
	if scale > 6 {
		value, _ = value.ScaleTruncate(6 - scale)
	}
	if (value.B192_255 != 0 || value.B128_191 != 0 || value.B64_127 != 0 || value.B0_63 > math.MaxInt64) &&
		(value.B192_255 != math.MaxUint64 || value.B128_191 != math.MaxUint64 || value.B64_127 != math.MaxUint64 || value.B0_63 < uint64(1)<<63) {
		return calendarIntervalOverflow
	}
	return timestampAddDecimal64Seconds(types.Decimal64(value.B0_63), min(scale, 6))
}

func timestampAddFloatSeconds(value float64) (int64, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, true
	}
	// FLOAT is promoted before decimal conversion, matching calendar intervals.
	return timestampAddSecondMicroseconds(strconv.FormatFloat(value, 'f', -1, 64)), false
}
