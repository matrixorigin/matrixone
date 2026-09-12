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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// NewBitwiseAggregateCast is a binder-only CAST overload. It implements the
// numeric conversion used by MySQL's BIT_AND/OR/XOR aggregates without changing
// ordinary CAST or scalar bitwise operator semantics.
func NewBitwiseAggregateCast(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if len(parameters) != 2 {
		return moerr.NewInternalErrorNoCtx("bitwise aggregate cast expects value and target type")
	}
	if parameters[1].GetType().Oid != types.T_int64 {
		return moerr.NewInternalErrorNoCtx("bitwise aggregate cast target must be INT64")
	}

	from := parameters[0]
	fromType := from.GetType()
	to := vector.MustFunctionResult[int64](result)
	switch fromType.Oid {
	case types.T_any:
		for i := uint64(0); i < uint64(length); i++ {
			if bitwiseAggregateSelectedRow(selectList, i) &&
				!from.IsConstNull() && !from.GetNulls().Contains(i) {
				return moerr.NewInternalErrorNoCtx("non-NULL value reached untyped bitwise aggregate cast")
			}
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
		}
		return nil
	case types.T_decimal64:
		return bitwiseAggregateDecimal64ToInt64(from, to, length, fromType.Scale, selectList)
	case types.T_decimal128:
		return bitwiseAggregateDecimal128ToInt64(from, to, length, fromType.Scale, selectList)
	case types.T_decimal256:
		return bitwiseAggregateDecimal256ToInt64(from, to, length, fromType.Scale, selectList)
	case types.T_float32:
		return bitwiseAggregateFloat32ToInt64(from, to, proc, length, selectList)
	case types.T_float64:
		return bitwiseAggregateFloat64ToInt64(from, to, proc, length, selectList)
	case types.T_char, types.T_varchar, types.T_text:
		// A stale prepared plan must not reinterpret a binary packet as a numeric
		// string. The prepared visitor is responsible for rebinding the aggregate
		// to the binary domain before this overload can execute.
		if from.GetIsBin() {
			return moerr.NewInternalErrorNoCtx("binary value reached numeric bitwise aggregate cast")
		}
		return bitwiseAggregateStringToInt64(from, to, proc, length, selectList)
	case types.T_date:
		return bitwiseAggregateDateToInt64(from, to, length, selectList)
	case types.T_year:
		return bitwiseAggregateYearToInt64(from, to, length, selectList)
	case types.T_datetime:
		return bitwiseAggregateDatetimeToInt64(from, to, length, fromType.Scale, selectList)
	case types.T_timestamp:
		zone := time.Local
		if proc != nil && proc.GetSessionInfo() != nil && proc.GetSessionInfo().TimeZone != nil {
			zone = proc.GetSessionInfo().TimeZone
		}
		return bitwiseAggregateTimestampToInt64(from, to, length, fromType.Scale, zone, selectList)
	case types.T_time:
		return bitwiseAggregateTimeToInt64(from, to, length, fromType.Scale, selectList)
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported bitwise aggregate conversion from %s", fromType)
	}
}

func bitwiseAggregateSelectedRow(selectList *FunctionSelectList, row uint64) bool {
	return selectList == nil || selectList.ShouldEvalAllRow() ||
		(!selectList.IgnoreAllRow() && !selectList.Contains(row))
}

func appendBitwiseAggregateNull(to *vector.FunctionResult[int64]) error {
	return to.Append(0, true)
}

func bitwiseAggregateStringToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], proc *process.Process,
	length int, selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionStrParameter(from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetStrValue(i)
		if null {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		text := strings.TrimSpace(convertByteSliceToString(value))
		converted, prefix, hasPrefix, outOfRange, err := parseSignedNumericPrefixCastString(text, 64)
		if err != nil {
			if strings.Contains(err.Error(), "value out of range") {
				return moerr.NewOutOfRangeNoCtxf("int64", "value '%s'", text)
			}
			return moerr.NewInvalidArgNoCtx("cast to int", text)
		}
		appendIntegerNumericCoercionWarning(proc, text, prefix, hasPrefix, outOfRange)
		if err := to.Append(converted, false); err != nil {
			return err
		}
	}
	return nil
}

func bitwiseAggregateDateToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], length int,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.Date](from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		if err := to.Append(packedDateInt64(value), false); err != nil {
			return err
		}
	}
	return nil
}

func bitwiseAggregateYearToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], length int,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.MoYear](from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		if err := to.Append(int64(value), false); err != nil {
			return err
		}
	}
	return nil
}

func bitwiseAggregateFloat32ToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], proc *process.Process, length int,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[float32](from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		rounded := math.RoundToEven(float64(value))
		if math.IsNaN(float64(value)) {
			return moerr.NewInvalidInputNoCtx("NaN cannot be converted to an integer")
		}
		if rounded < -math.Exp2(63) {
			appendBitwiseAggregateFloatOverflowWarning(proc, float64(value))
			if err := to.Append(math.MinInt64, false); err != nil {
				return err
			}
			continue
		}
		if rounded >= math.Exp2(63) {
			appendBitwiseAggregateFloatOverflowWarning(proc, float64(value))
			if err := to.Append(math.MaxInt64, false); err != nil {
				return err
			}
			continue
		}
		if err := to.Append(int64(rounded), false); err != nil {
			return err
		}
	}
	return nil
}

func bitwiseAggregateFloat64ToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], proc *process.Process, length int,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[float64](from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if math.IsNaN(value) {
			return moerr.NewInvalidInputNoCtx("NaN cannot be converted to an integer")
		}
		if value < -math.Exp2(63) {
			appendBitwiseAggregateFloatOverflowWarning(proc, value)
			if err := to.Append(math.MinInt64, false); err != nil {
				return err
			}
			continue
		}
		if value >= math.Exp2(63) {
			appendBitwiseAggregateFloatOverflowWarning(proc, value)
			if err := to.Append(math.MaxInt64, false); err != nil {
				return err
			}
			continue
		}
		if err := to.Append(int64(math.RoundToEven(value)), false); err != nil {
			return err
		}
	}
	return nil
}

func appendBitwiseAggregateFloatOverflowWarning(proc *process.Process, value float64) {
	if proc == nil {
		return
	}
	appender, ok := proc.GetWarningSink().(warningDiagnosticAppender)
	if !ok {
		return
	}
	appender.AppendWarningDiagnostic(
		moerr.ER_TRUNCATED_WRONG_VALUE,
		fmt.Sprintf("Truncated incorrect INTEGER value: '%s'", strconv.FormatFloat(value, 'g', -1, 64)),
	)
}

func bitwiseAggregateDatetimeToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], length int, scale int32,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.Datetime](from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if scale > 0 && value != types.ZeroDatetime && value.MicroSec() >= 500000 {
			value += types.Datetime(types.MicroSecsPerSec)
			if value.Year() > types.MaxDatetimeYear {
				return moerr.NewOutOfRangeNoCtxf("datetime", "value '%s'", value.String())
			}
		}
		if err := to.Append(packedDatetimeInt64(value), false); err != nil {
			return err
		}
	}
	return nil
}

func bitwiseAggregateTimestampToInt64(
	from *vector.Vector,
	to *vector.FunctionResult[int64],
	length int,
	scale int32,
	zone *time.Location,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		datetime := value.ToDatetime(zone)
		if scale > 0 && datetime != types.ZeroDatetime && datetime.MicroSec() >= 500000 {
			datetime += types.Datetime(types.MicroSecsPerSec)
			if datetime.Year() > types.MaxDatetimeYear {
				return moerr.NewOutOfRangeNoCtxf("datetime", "value '%s'", datetime.String())
			}
		}
		if err := to.Append(packedDatetimeInt64(datetime), false); err != nil {
			return err
		}
	}
	return nil
}

func bitwiseAggregateTimeToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], length int, scale int32,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.Time](from)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		packed := mysqlTimeInt64(value, scale > 0)
		if err := to.Append(packed, false); err != nil {
			return err
		}
	}
	return nil
}

func mysqlTimeInt64(value types.Time, roundFraction bool) int64 {
	negative := value < 0
	magnitude := uint64(value)
	if negative {
		magnitude = uint64(-(int64(value) + 1)) + 1
	}
	seconds := magnitude / types.MicroSecsPerSec
	if roundFraction && magnitude%types.MicroSecsPerSec >= types.MicroSecsPerSec/2 {
		seconds++
	}
	hour := seconds / types.SecsPerHour
	minute := seconds % types.SecsPerHour / types.SecsPerMinute
	second := seconds % types.SecsPerMinute
	packed := int64(hour*10000 + minute*100 + second)
	if negative {
		return -packed
	}
	return packed
}

func bitwiseAggregateDecimal64ToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], length int, scale int32,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](from)
	divisor, roundsToZero := decimal64RoundingDivisor(scale)
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		rounded, err := decimal64ToInt64(value, scale, divisor, roundsToZero)
		if err != nil {
			return err
		}
		if err := to.Append(rounded, false); err != nil {
			return err
		}
	}
	return nil
}

func decimal64RoundingDivisor(scale int32) (uint64, bool) {
	if scale < 0 || scale > 19 {
		return 0, scale > 19
	}
	divisor := uint64(1)
	for i := int32(0); i < scale; i++ {
		divisor *= 10
	}
	return divisor, false
}

func decimal64ToInt64(value types.Decimal64, scale int32, divisor uint64, roundsToZero bool) (int64, error) {
	if scale < 0 {
		return 0, moerr.NewInvalidInputNoCtxf("invalid DECIMAL scale %d", scale)
	}
	negative := value.Sign()
	magnitude := uint64(value)
	if negative {
		magnitude = 0 - magnitude
	}
	if roundsToZero {
		return 0, nil
	}
	quotient, remainder := magnitude/divisor, magnitude%divisor
	if remainder >= divisor/2+divisor%2 {
		quotient++
	}
	return int64FromSignedMagnitude(quotient, negative)
}

func bitwiseAggregateDecimal128ToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], length int, scale int32,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](from)
	divisor, roundsToZero, err := decimal128RoundingDivisor(scale)
	if err != nil {
		return err
	}
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		rounded, err := bitwiseAggregateDecimal128ToInt64Value(value, divisor, roundsToZero)
		if err != nil {
			return err
		}
		if err := to.Append(rounded, false); err != nil {
			return err
		}
	}
	return nil
}

func decimal128RoundingDivisor(scale int32) (types.Decimal128, bool, error) {
	if scale < 0 {
		return types.Decimal128{}, false, moerr.NewInvalidInputNoCtxf("invalid DECIMAL scale %d", scale)
	}
	if scale > 38 {
		return types.Decimal128{}, true, nil
	}
	divisor := types.Decimal128{B0_63: 1}
	ten := types.Decimal128{B0_63: 10}
	for i := int32(0); i < scale; i++ {
		var err error
		divisor, err = divisor.Mul128(ten)
		if err != nil {
			return types.Decimal128{}, false, err
		}
	}
	return divisor, false, nil
}

func bitwiseAggregateDecimal128ToInt64Value(value, divisor types.Decimal128, roundsToZero bool) (int64, error) {
	negative := value.Sign()
	if negative {
		value = value.Minus()
	}
	if roundsToZero {
		return 0, nil
	}
	quotient, err := value.Div128Trunc(divisor)
	if err != nil {
		return 0, err
	}
	remainder, err := value.Mod128(divisor)
	if err != nil {
		return 0, err
	}
	otherHalf, err := divisor.Sub128(remainder)
	if err != nil {
		return 0, err
	}
	if remainder.Compare(otherHalf) >= 0 {
		quotient, err = quotient.Add128(types.Decimal128{B0_63: 1})
		if err != nil {
			return 0, err
		}
	}
	if quotient.B64_127 != 0 {
		return 0, moerr.NewOutOfRangeNoCtx("int64", "")
	}
	return int64FromSignedMagnitude(quotient.B0_63, negative)
}

func bitwiseAggregateDecimal256ToInt64(
	from *vector.Vector, to *vector.FunctionResult[int64], length int, scale int32,
	selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](from)
	divisor, roundsToZero, err := decimal256RoundingDivisor(scale)
	if err != nil {
		return err
	}
	for i := uint64(0); i < uint64(length); i++ {
		if !bitwiseAggregateSelectedRow(selectList, i) {
			if err := appendBitwiseAggregateNull(to); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetValue(i)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		rounded, err := bitwiseAggregateDecimal256ToInt64Value(value, divisor, roundsToZero)
		if err != nil {
			return err
		}
		if err := to.Append(rounded, false); err != nil {
			return err
		}
	}
	return nil
}

func decimal256RoundingDivisor(scale int32) (types.Decimal256, bool, error) {
	if scale < 0 {
		return types.Decimal256{}, false, moerr.NewInvalidInputNoCtxf("invalid DECIMAL scale %d", scale)
	}
	if scale > 65 {
		return types.Decimal256{}, true, nil
	}
	divisor := types.Decimal256{B0_63: 1}
	ten := types.Decimal256{B0_63: 10}
	for i := int32(0); i < scale; i++ {
		var err error
		divisor, err = divisor.Mul256(ten)
		if err != nil {
			return types.Decimal256{}, false, err
		}
	}
	return divisor, false, nil
}

func bitwiseAggregateDecimal256ToInt64Value(value, divisor types.Decimal256, roundsToZero bool) (int64, error) {
	negative := value.Sign()
	if negative {
		value = value.Minus()
	}
	if roundsToZero {
		return 0, nil
	}
	quotient, err := value.Div256Trunc(divisor)
	if err != nil {
		return 0, err
	}
	// Decimal256.Mod256 performs another full-width long division. Recovering
	// the remainder from q*d is exact here (0 <= q*d <= value) and keeps the
	// per-row hot path to one division.
	product, err := quotient.Mul256(divisor)
	if err != nil {
		return 0, err
	}
	remainder, err := value.Sub256(product)
	if err != nil {
		return 0, err
	}
	otherHalf, err := divisor.Sub256(remainder)
	if err != nil {
		return 0, err
	}
	if remainder.Compare(otherHalf) >= 0 {
		quotient, err = quotient.Add256(types.Decimal256{B0_63: 1})
		if err != nil {
			return 0, err
		}
	}
	if quotient.B64_127|quotient.B128_191|quotient.B192_255 != 0 {
		return 0, moerr.NewOutOfRangeNoCtx("int64", "")
	}
	return int64FromSignedMagnitude(quotient.B0_63, negative)
}

func int64FromSignedMagnitude(magnitude uint64, negative bool) (int64, error) {
	if negative {
		if magnitude > uint64(1)<<63 {
			return 0, moerr.NewOutOfRangeNoCtx("int64", "")
		}
		if magnitude == uint64(1)<<63 {
			return math.MinInt64, nil
		}
		return -int64(magnitude), nil
	}
	if magnitude > math.MaxInt64 {
		return 0, moerr.NewOutOfRangeNoCtx("int64", "")
	}
	return int64(magnitude), nil
}
