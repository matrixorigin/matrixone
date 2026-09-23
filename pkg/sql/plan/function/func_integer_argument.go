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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// These append-only CAST identities are reserved for binder-owned integer
// evaluation. Neither explicit CAST nor assignment selects them.
const (
	IntegerArgumentCastOverload          int32 = 5
	TruncatedIntegerArgumentCastOverload int32 = 6
	TextIntegerBitsCastOverload          int32 = 7
	TemporalIntegerArgumentCastOverload  int32 = 8
)

// integerArgumentValue preserves the full signed and unsigned input domains
// until the declared parameter target has been checked. In particular, neither
// uint64 -> int64 wrapping nor an intermediate float64 is permissible.
type integerArgumentValue struct {
	magnitude uint64
	negative  bool
	overflow  bool
}

func signedIntegerArgument[T constraints.Signed](value T) (integerArgumentValue, error) {
	v := int64(value)
	if v < 0 {
		return integerArgumentValue{magnitude: uint64(-(v + 1)) + 1, negative: true}, nil
	}
	return integerArgumentValue{magnitude: uint64(v)}, nil
}

func unsignedIntegerArgument[T constraints.Unsigned](value T) (integerArgumentValue, error) {
	return integerArgumentValue{magnitude: uint64(value)}, nil
}

func realIntegerArgument(value float64, truncate bool) integerArgumentValue {
	if truncate {
		value = math.Trunc(value)
	} else {
		value = math.RoundToEven(value)
	}
	magnitude := math.Abs(value)
	if math.IsNaN(value) || math.IsInf(value, 0) || magnitude >= 0x1p64 {
		return integerArgumentValue{overflow: true}
	}
	return integerArgumentValue{magnitude: uint64(magnitude), negative: value < 0}
}

func decimal64IntegerArgument(value types.Decimal64, scale int32) (integerArgumentValue, error) {
	v, err := value.Scale(-scale)
	if err != nil {
		return integerArgumentValue{}, err
	}
	return signedIntegerArgument(int64(v))
}

func decimal128IntegerArgument(value types.Decimal128, scale int32) (integerArgumentValue, error) {
	v, err := value.Scale(-scale)
	if err != nil {
		return integerArgumentValue{}, err
	}
	negative := v.Sign()
	if negative {
		v = v.Minus()
	}
	return integerArgumentValue{magnitude: v.B0_63, negative: negative, overflow: v.B64_127 != 0}, nil
}

func decimal256IntegerArgument(value types.Decimal256, scale int32) (integerArgumentValue, error) {
	v, err := value.Scale(-scale)
	if err != nil {
		return integerArgumentValue{}, err
	}
	negative := v.Sign()
	if negative {
		v = v.Minus()
	}
	return integerArgumentValue{magnitude: v.B0_63, negative: negative, overflow: v.B64_127 != 0 || v.B128_191 != 0 || v.B192_255 != 0}, nil
}

func textIntegerArgument(value []byte, binaryLiteral bool) integerArgumentValue {
	if binaryLiteral {
		for len(value) > 0 && value[0] == 0 {
			value = value[1:]
		}
		if len(value) > 8 {
			return integerArgumentValue{overflow: true}
		}
		var magnitude uint64
		for _, b := range value {
			magnitude = magnitude<<8 | uint64(b)
		}
		return integerArgumentValue{magnitude: magnitude}
	}
	prefix := leadingDecimalIntegerPrefix(string(value))
	if prefix == "" {
		return integerArgumentValue{}
	}
	negative := prefix[0] == '-'
	if negative || prefix[0] == '+' {
		prefix = prefix[1:]
	}
	magnitude, err := strconv.ParseUint(prefix, 10, 64)
	return integerArgumentValue{magnitude: magnitude, negative: negative, overflow: err != nil}
}

func checkedIntegerArgument[R int64 | uint64](value integerArgumentValue, proc *process.Process) (R, error) {
	var zero R
	unsigned := ^zero > 0
	invalid := value.overflow
	if unsigned {
		invalid = invalid || (value.negative && value.magnitude != 0)
	} else {
		limit := uint64(math.MaxInt64)
		if value.negative {
			limit++
		}
		invalid = invalid || value.magnitude > limit
	}
	if invalid {
		name := "int64"
		if unsigned {
			name = "uint64"
		}
		return 0, moerr.NewOutOfRangef(proc.Ctx, name, "integer argument")
	}
	if value.negative {
		return -R(value.magnitude), nil
	}
	return R(value.magnitude), nil
}

func integerArgumentRows[T types.FixedSizeTExceptStrType, R int64 | uint64](
	source *vector.Vector, result *vector.FunctionResult[R], proc *process.Process, length int,
	selectList *FunctionSelectList, convert func(T) (integerArgumentValue, error),
) error {
	input := vector.GenerateFunctionFixedTypeParameter[T](source)
	for i := uint64(0); i < uint64(length); i++ {
		// Do not read, round, parse, or range-check an unselected row.
		if functionRowSkipped(selectList, i) {
			if err := result.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, isNull := input.GetValue(i)
		if isNull {
			if err := result.Append(0, true); err != nil {
				return err
			}
			continue
		}
		normalized, err := convert(value)
		if err != nil {
			return err
		}
		integer, err := checkedIntegerArgument[R](normalized, proc)
		if err != nil {
			return err
		}
		if err = result.Append(integer, false); err != nil {
			return err
		}
	}
	return nil
}

func integerArgumentTextRows[R int64 | uint64](source *vector.Vector, result *vector.FunctionResult[R], proc *process.Process, length int, selectList *FunctionSelectList, textBits bool) error {
	input := vector.GenerateFunctionStrParameter(source)
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := result.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, isNull := input.GetStrValue(i)
		if isNull {
			if err := result.Append(0, true); err != nil {
				return err
			}
			continue
		}
		normalized := textIntegerArgument(value, source.GetIsBinAt(int(i)))
		var integer R
		var err error
		if textBits && normalized.negative {
			// Only this text policy permits the valid signed pattern in an
			// unsigned physical vector. Ordinary UINT64 coercion stays strict.
			var signed int64
			signed, err = checkedIntegerArgument[int64](normalized, proc)
			integer = R(signed)
		} else {
			integer, err = checkedIntegerArgument[R](normalized, proc)
		}
		if err != nil {
			return err
		}
		if err = result.Append(integer, false); err != nil {
			return err
		}
	}
	return nil
}

func integerArgumentCast[R int64 | uint64](source *vector.Vector, result *vector.FunctionResult[R], proc *process.Process, length int, selectList *FunctionSelectList, truncate bool) error {
	if source.IsConstNull() || (selectList != nil && selectList.IgnoreAllRow()) {
		for i := 0; i < length; i++ {
			if err := result.Append(0, true); err != nil {
				return err
			}
		}
		return nil
	}
	switch source.GetType().Oid {
	case types.T_int8:
		return integerArgumentRows(source, result, proc, length, selectList, signedIntegerArgument[int8])
	case types.T_int16:
		return integerArgumentRows(source, result, proc, length, selectList, signedIntegerArgument[int16])
	case types.T_year:
		return integerArgumentRows(source, result, proc, length, selectList, signedIntegerArgument[types.MoYear])
	case types.T_int32:
		return integerArgumentRows(source, result, proc, length, selectList, signedIntegerArgument[int32])
	case types.T_int64:
		return integerArgumentRows(source, result, proc, length, selectList, signedIntegerArgument[int64])
	case types.T_uint8:
		return integerArgumentRows(source, result, proc, length, selectList, unsignedIntegerArgument[uint8])
	case types.T_uint16:
		return integerArgumentRows(source, result, proc, length, selectList, unsignedIntegerArgument[uint16])
	case types.T_enum:
		return integerArgumentRows(source, result, proc, length, selectList, unsignedIntegerArgument[types.Enum])
	case types.T_uint32:
		return integerArgumentRows(source, result, proc, length, selectList, unsignedIntegerArgument[uint32])
	case types.T_uint64, types.T_bit:
		return integerArgumentRows(source, result, proc, length, selectList, unsignedIntegerArgument[uint64])
	case types.T_bool:
		return integerArgumentRows(source, result, proc, length, selectList, func(v bool) (integerArgumentValue, error) {
			if v {
				return integerArgumentValue{magnitude: 1}, nil
			}
			return integerArgumentValue{}, nil
		})
	case types.T_float32:
		return integerArgumentRows(source, result, proc, length, selectList, func(v float32) (integerArgumentValue, error) { return realIntegerArgument(float64(v), truncate), nil })
	case types.T_float64:
		return integerArgumentRows(source, result, proc, length, selectList, func(v float64) (integerArgumentValue, error) { return realIntegerArgument(v, truncate), nil })
	case types.T_decimal64:
		return integerArgumentRows(source, result, proc, length, selectList, func(v types.Decimal64) (integerArgumentValue, error) {
			return decimal64IntegerArgument(v, source.GetType().Scale)
		})
	case types.T_decimal128:
		return integerArgumentRows(source, result, proc, length, selectList, func(v types.Decimal128) (integerArgumentValue, error) {
			return decimal128IntegerArgument(v, source.GetType().Scale)
		})
	case types.T_decimal256:
		return integerArgumentRows(source, result, proc, length, selectList, func(v types.Decimal256) (integerArgumentValue, error) {
			return decimal256IntegerArgument(v, source.GetType().Scale)
		})
	default:
		if source.GetType().Oid.IsMySQLString() {
			return integerArgumentTextRows(source, result, proc, length, selectList, false)
		}
		return moerr.NewInvalidInputf(proc.Ctx, "unsupported integer argument source %s", source.GetType().Oid)
	}
}

func newIntegerArgumentCast(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, truncate bool) error {
	if len(parameters) != 2 {
		return moerr.NewInvalidInput(proc.Ctx, "integer argument cast requires a value and target")
	}
	switch result.GetResultVector().GetType().Oid {
	case types.T_int64:
		return integerArgumentCast(parameters[0], vector.MustFunctionResult[int64](result), proc, length, selectList, truncate)
	case types.T_uint64:
		return integerArgumentCast(parameters[0], vector.MustFunctionResult[uint64](result), proc, length, selectList, truncate)
	default:
		return moerr.NewInvalidInput(proc.Ctx, "integer argument cast requires int64 or uint64 target")
	}
}

// NewTextIntegerBitsCast is shared by text-valued bit parameters, not numeric
// sources, public CAST, assignment, or signed count parameters.
func NewTextIntegerBitsCast(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) != 2 || result.GetResultVector().GetType().Oid != types.T_uint64 {
		return moerr.NewInvalidInput(proc.Ctx, "text integer bits cast requires a value and uint64 target")
	}
	source := parameters[0]
	if source.IsConstNull() || (selectList != nil && selectList.IgnoreAllRow()) {
		return integerArgumentCast(source, vector.MustFunctionResult[uint64](result), proc, length, selectList, false)
	}
	if !source.GetType().Oid.IsMySQLString() {
		return moerr.NewInvalidInput(proc.Ctx, "text integer bits cast requires a text source")
	}
	return integerArgumentTextRows(source, vector.MustFunctionResult[uint64](result), proc, length, selectList, true)
}

// Temporal values are rounded in their clock/calendar domain before decimal
// packing. Rounding HHMMSS directly would produce invalid minute/hour carries.
func timeIntegerArgument(value types.Time) (integerArgumentValue, error) {
	n, _ := signedIntegerArgument(value)
	seconds := (n.magnitude + 500000) / 1000000
	// Preserve MySQL's TIME ceiling when fractional rounding crosses it, but
	// do not saturate MatrixOne's independently supported extended TIME domain.
	const mysqlLastSecond = 838*3600 + 59*60 + 59
	if n.magnitude/1000000 == mysqlLastSecond && seconds > mysqlLastSecond {
		seconds = mysqlLastSecond
	}
	n.magnitude = seconds/3600*10000 + (seconds/60)%60*100 + seconds%60
	return n, nil
}

func datetimeIntegerArgument(value types.Datetime) (integerArgumentValue, error) {
	rounded := value.TruncateToScale(0)
	if rounded.Year() > types.MaxDatetimeYear {
		// MySQL val_int retains the original date with a zero clock when
		// fractional carry cannot advance the maximum calendar date.
		rounded = value.ToDate().ToDatetime()
	}
	return signedIntegerArgument(packedDatetimeInt64(rounded))
}

// NewTemporalIntegerArgumentCast is selected only by temporal-capable roles.
// Its identity retains the permission across prepared source changes; CAST 5/6
// must continue rejecting these sources in ordinary integer contexts.
func NewTemporalIntegerArgumentCast(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) != 2 || result.GetResultVector().GetType().Oid != types.T_int64 {
		return moerr.NewInvalidInput(proc.Ctx, "temporal integer argument cast requires a value and int64 target")
	}
	source := parameters[0]
	rs := vector.MustFunctionResult[int64](result)
	if source.IsConstNull() || (selectList != nil && selectList.IgnoreAllRow()) || (!integerArgumentTemporalSource(source.GetType().Oid) && source.GetType().Oid != types.T_uuid) {
		return integerArgumentCast(source, rs, proc, length, selectList, false)
	}
	switch source.GetType().Oid {
	case types.T_uuid:
		return integerArgumentRows(source, rs, proc, length, selectList, func(v types.Uuid) (integerArgumentValue, error) {
			return textIntegerArgument([]byte(v.String()), false), nil
		})
	case types.T_date:
		return integerArgumentRows(source, rs, proc, length, selectList, func(v types.Date) (integerArgumentValue, error) {
			return signedIntegerArgument(packedDateInt64(v))
		})
	case types.T_time:
		return integerArgumentRows(source, rs, proc, length, selectList, timeIntegerArgument)
	case types.T_datetime:
		return integerArgumentRows(source, rs, proc, length, selectList, datetimeIntegerArgument)
	default:
		return integerArgumentRows(source, rs, proc, length, selectList, func(v types.Timestamp) (integerArgumentValue, error) {
			return datetimeIntegerArgument(v.ToDatetime(proc.GetSessionInfo().TimeZone))
		})
	}
}

func NewIntegerArgumentCast(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return newIntegerArgumentCast(parameters, result, proc, length, selectList, false)
}

func NewTruncatedIntegerArgumentCast(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return newIntegerArgumentCast(parameters, result, proc, length, selectList, true)
}
