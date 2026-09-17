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
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"golang.org/x/exp/constraints"
)

func floatToIntegerAssignment[F constraints.Float](ctx context.Context, source vector.FunctionParameterWrapper[F], target types.T, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	switch target {
	case types.T_int8:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[int8](result), length, selectList)
	case types.T_int16:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[int16](result), length, selectList)
	case types.T_int32:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[int32](result), length, selectList)
	case types.T_int64:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[int64](result), length, selectList)
	case types.T_uint8:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[uint8](result), length, selectList)
	case types.T_uint16:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[uint16](result), length, selectList)
	case types.T_uint32:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[uint32](result), length, selectList)
	case types.T_uint64:
		return roundFloatAssignment(ctx, source, vector.MustFunctionResult[uint64](result), length, selectList)
	default:
		return moerr.NewInternalError(ctx, "non-integer assignment destination")
	}
}

func roundFloatAssignment[F constraints.Float, I constraints.Integer](ctx context.Context, source vector.FunctionParameterWrapper[F], result *vector.FunctionResult[I], length int, selectList *FunctionSelectList) error {
	var zero I
	bits := int(unsafe.Sizeof(zero)) * 8
	lower, upper := 0.0, math.Ldexp(1, bits)
	if ^zero < zero {
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
		if math.IsNaN(rounded) || rounded < lower || rounded >= upper {
			return moerr.NewOutOfRangef(ctx, result.GetType().String(), "value %v", value)
		}
		if err := result.Append(I(rounded), false); err != nil {
			return err
		}
	}
	return nil
}
