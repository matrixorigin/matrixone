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
	"context"
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func isStorageIntegerOid(oid types.T) bool {
	switch oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	}
	return false
}

// Integer assignment adjusts a typed source to the target storage boundary
// only when SQL mode or IGNORE permits it. Use an exact integer domain so a
// UINT64 source never passes through FLOAT64 before it is clipped.
func clampIntegerAssignmentSource(proc *process.Process, source *vector.Vector, target types.T,
	result vector.FunctionResultWrapper, length int, selection *FunctionSelectList) error {
	switch source.GetType().Oid {
	case types.T_int8:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[int8](source), target, result, length, selection)
	case types.T_int16:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[int16](source), target, result, length, selection)
	case types.T_int32:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[int32](source), target, result, length, selection)
	case types.T_int64:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[int64](source), target, result, length, selection)
	case types.T_uint8:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[uint8](source), target, result, length, selection)
	case types.T_uint16:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[uint16](source), target, result, length, selection)
	case types.T_uint32:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[uint32](source), target, result, length, selection)
	case types.T_uint64:
		return clampIntegerAssignmentTarget(proc, vector.GenerateFunctionFixedTypeParameter[uint64](source), target, result, length, selection)
	}
	return moerr.NewInternalError(proc.Ctx, "unsupported integer assignment source")
}

func clampIntegerAssignmentTarget[S constraints.Integer](proc *process.Process,
	source vector.FunctionParameterWrapper[S], target types.T, result vector.FunctionResultWrapper,
	length int, selection *FunctionSelectList) error {
	switch target {
	case types.T_int8:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[int8](result), length, selection)
	case types.T_int16:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[int16](result), length, selection)
	case types.T_int32:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[int32](result), length, selection)
	case types.T_int64:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[int64](result), length, selection)
	case types.T_uint8:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[uint8](result), length, selection)
	case types.T_uint16:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[uint16](result), length, selection)
	case types.T_uint32:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[uint32](result), length, selection)
	case types.T_uint64:
		return appendClampedIntegerAssignment(proc, source, vector.MustFunctionResult[uint64](result), length, selection)
	}
	return moerr.NewInternalError(proc.Ctx, "unsupported integer assignment target")
}

func appendClampedIntegerAssignment[S, D constraints.Integer](proc *process.Process,
	source vector.FunctionParameterWrapper[S], result *vector.FunctionResult[D], length int,
	selection *FunctionSelectList) error {
	var zero D
	bits := uint(unsafe.Sizeof(zero) * 8)
	signed := ^zero < zero
	maximum := uint64(math.MaxUint64)
	minimumMagnitude := uint64(0)
	if signed {
		minimumMagnitude = uint64(1) << (bits - 1)
		maximum = minimumMagnitude - 1
	} else if bits < 64 {
		maximum = (uint64(1) << bits) - 1
	}
	for row := 0; row < length; row++ {
		value, isNull := source.GetValue(uint64(row))
		if isNull || functionRowSkipped(selection, uint64(row)) {
			if err := result.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		negative, magnitude := integerAssignmentMagnitude(value)
		clipped := negative && (!signed || magnitude > minimumMagnitude) ||
			!negative && magnitude > maximum
		var converted D
		switch {
		case negative && !signed:
			converted = 0
		case negative && magnitude > minimumMagnitude:
			converted = D(-int64(minimumMagnitude))
		case negative:
			converted = D(-int64(magnitude))
		case magnitude > maximum:
			converted = D(maximum)
		default:
			converted = D(magnitude)
		}
		if clipped {
			appendIntegerAssignmentRangeWarning(proc, result.GetType().Oid, uint64(row))
		}
		if err := result.Append(converted, false); err != nil {
			return err
		}
	}
	return nil
}

func appendIntegerAssignmentRangeWarning(proc *process.Process, target types.T, row uint64) {
	if proc == nil {
		return
	}
	if appender, ok := proc.GetWarningSink().(warningDiagnosticAppender); ok {
		appender.AppendWarningDiagnostic(moerr.ER_WARN_DATA_OUT_OF_RANGE,
			fmt.Sprintf("Out of range value for %s assignment at row %d", target, row+1))
	}
}

func integerAssignmentMagnitude[S constraints.Integer](value S) (negative bool, magnitude uint64) {
	switch v := any(value).(type) {
	case int8:
		return signedIntegerMagnitude(int64(v))
	case int16:
		return signedIntegerMagnitude(int64(v))
	case int32:
		return signedIntegerMagnitude(int64(v))
	case int64:
		return signedIntegerMagnitude(v)
	case uint8:
		return false, uint64(v)
	case uint16:
		return false, uint64(v)
	case uint32:
		return false, uint64(v)
	case uint64:
		return false, v
	}
	panic("unsupported integer assignment source")
}

func signedIntegerMagnitude(value int64) (bool, uint64) {
	if value < 0 {
		return true, uint64(-(value + 1)) + 1
	}
	return false, uint64(value)
}

// Keep ordinary decimal casts unchanged: only assignment converts the exact
// rounded integer, and ParseUint checks the destination range after rounding.
func decimalToUnsignedAssignment[D types.Decimal64 | types.Decimal128 | types.Decimal256](
	ctx context.Context, source vector.FunctionParameterWrapper[D], target types.T,
	result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList,
	round func(D, int32) string,
) error {
	switch target {
	case types.T_uint8:
		return roundDecimalAssignment(ctx, source, vector.MustFunctionResult[uint8](result), length, selectList, round)
	case types.T_uint16:
		return roundDecimalAssignment(ctx, source, vector.MustFunctionResult[uint16](result), length, selectList, round)
	case types.T_uint32:
		return roundDecimalAssignment(ctx, source, vector.MustFunctionResult[uint32](result), length, selectList, round)
	case types.T_uint64:
		return roundDecimalAssignment(ctx, source, vector.MustFunctionResult[uint64](result), length, selectList, round)
	default:
		return moerr.NewInternalError(ctx, "non-unsigned assignment destination")
	}
}

func roundDecimalAssignment[D types.Decimal64 | types.Decimal128 | types.Decimal256, I constraints.Unsigned](
	ctx context.Context, source vector.FunctionParameterWrapper[D], result *vector.FunctionResult[I],
	length int, selectList *FunctionSelectList, round func(D, int32) string,
) error {
	var zero I
	for i := 0; i < length; i++ {
		value, isNull := source.GetValue(uint64(i))
		if isNull || functionRowSkipped(selectList, uint64(i)) {
			if err := result.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		rounded := round(value, source.GetType().Scale)
		integer, err := strconv.ParseUint(rounded, 10, int(unsafe.Sizeof(zero))*8)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, result.GetType().String(), "value %s", rounded)
		}
		if err := result.Append(I(integer), false); err != nil {
			return err
		}
	}
	return nil
}

func floatToIntegerAssignment[F constraints.Float](proc *process.Process, source vector.FunctionParameterWrapper[F], target types.T, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList, mode castMode) error {
	switch target {
	case types.T_int8:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[int8](result), length, selectList, mode)
	case types.T_int16:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[int16](result), length, selectList, mode)
	case types.T_int32:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[int32](result), length, selectList, mode)
	case types.T_int64:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[int64](result), length, selectList, mode)
	case types.T_uint8:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[uint8](result), length, selectList, mode)
	case types.T_uint16:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[uint16](result), length, selectList, mode)
	case types.T_uint32:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[uint32](result), length, selectList, mode)
	case types.T_uint64:
		return roundFloatAssignment(proc, source, vector.MustFunctionResult[uint64](result), length, selectList, mode)
	default:
		return moerr.NewInternalError(proc.Ctx, "non-integer assignment destination")
	}
}

func roundFloatAssignment[F constraints.Float, I constraints.Integer](proc *process.Process, source vector.FunctionParameterWrapper[F], result *vector.FunctionResult[I], length int, selectList *FunctionSelectList, mode castMode) error {
	var zero I
	bits := int(unsafe.Sizeof(zero)) * 8
	lower, upper := 0.0, math.Ldexp(1, bits)
	signed := ^zero < zero
	if signed {
		upper = math.Ldexp(1, bits-1)
		lower = -upper
	}
	for i := 0; i < length; i++ {
		value, isNull := source.GetValue(uint64(i))
		if isNull || functionRowSkipped(selectList, uint64(i)) {
			if err := result.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		rounded := math.RoundToEven(float64(value))
		// Exclusive powers-of-two bounds avoid rounding MaxInt64/MaxUint64 up
		// to the first unrepresentable integer during a float comparison.
		if math.IsNaN(rounded) {
			return moerr.NewOutOfRangef(proc.Ctx, result.GetType().String(), "value %v", value)
		}
		if rounded < lower || rounded >= upper {
			if mode == castModeStrictStringWidth {
				return moerr.NewOutOfRangef(proc.Ctx, result.GetType().String(), "value %v", value)
			}
			appendIntegerAssignmentRangeWarning(proc, result.GetType().Oid, uint64(i))
			var clipped I
			if rounded < lower {
				if signed {
					clipped = I(int64(lower))
				}
			} else if signed {
				clipped = I(uint64(upper) - 1)
			} else if bits == 64 {
				maximum := uint64(math.MaxUint64)
				clipped = I(maximum)
			} else {
				clipped = I(uint64(upper) - 1)
			}
			if err := result.Append(clipped, false); err != nil {
				return err
			}
			continue
		}
		if err := result.Append(I(rounded), false); err != nil {
			return err
		}
	}
	return nil
}
