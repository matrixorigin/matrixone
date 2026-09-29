// Copyright 2021 Matrix Origin
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

package function

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The scalar low-precision float types (bf16/float16/float8/float4) are cast by
// bridging through float32: every source widens to float32 then rounds to the
// target format via its FromFloat32 constructor, and every low-precision source
// widens to float32 then reuses the float32 target machinery. This keeps one
// conversion path and inherits float32's exact integer-rounding, decimal, and
// string-formatting semantics instead of duplicating them per format.

func init() {
	lowPrec := []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4}

	// Sources that may cast TO a low-precision float.
	toLowPrecSources := []types.T{
		types.T_any,
		types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
	}
	for _, s := range toLowPrecSources {
		supportedTypeCast[s] = append(supportedTypeCast[s], lowPrec...)
	}

	// Targets a low-precision float may cast to (numerics, strings, each other).
	fromLowPrecTargets := []types.T{
		types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
	}
	fromLowPrecTargets = append(fromLowPrecTargets, lowPrec...)
	for _, s := range lowPrec {
		supportedTypeCast[s] = append(supportedTypeCast[s], fromLowPrecTargets...)
	}
}

// isLowPrecFloat reports whether oid is one of the scalar low-precision float types.
func isLowPrecFloat(oid types.T) bool {
	switch oid {
	case types.T_bf16, types.T_float16, types.T_float8, types.T_float4:
		return true
	}
	return false
}

// castToLowPrecFloat converts any supported source to a low-precision float target.
func castToLowPrecFloat(parameters []*vector.Vector, toType types.Type, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	switch toType.Oid {
	case types.T_bf16:
		return anyToLowPrecFloat(parameters, result, proc, length, selectList, types.BF16FromFloat32)
	case types.T_float16:
		return anyToLowPrecFloat(parameters, result, proc, length, selectList, types.Float16FromFloat32)
	case types.T_float8:
		return anyToLowPrecFloat(parameters, result, proc, length, selectList, types.Float8FromFloat32)
	case types.T_float4:
		return anyToLowPrecFloat(parameters, result, proc, length, selectList, types.Float4FromFloat32)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast to %s", toType))
}

// lowPrecFloatConstraint is a low-precision float value type: a fixed-size ordered
// type that widens to float32.
type lowPrecFloatConstraint interface {
	types.FixedSizeTExceptStrType
	ToFloat32() float32
}

// anyToLowPrecFloat widens each source value to float32 and rounds it to Tr via ctor.
func anyToLowPrecFloat[Tr lowPrecFloatConstraint](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, ctor func(float32) Tr,
) error {
	from := parameters[0]
	switch from.GetType().Oid {
	case types.T_bool:
		return opUnaryFixedToFixed[bool, Tr](parameters, result, proc, length, func(v bool) Tr {
			if v {
				return ctor(1)
			}
			return ctor(0)
		}, selectList)
	case types.T_bit:
		return opUnaryFixedToFixed[uint64, Tr](parameters, result, proc, length, func(v uint64) Tr { return ctor(float32(v)) }, selectList)
	case types.T_int8:
		return opUnaryFixedToFixed[int8, Tr](parameters, result, proc, length, func(v int8) Tr { return ctor(float32(v)) }, selectList)
	case types.T_int16:
		return opUnaryFixedToFixed[int16, Tr](parameters, result, proc, length, func(v int16) Tr { return ctor(float32(v)) }, selectList)
	case types.T_int32:
		return opUnaryFixedToFixed[int32, Tr](parameters, result, proc, length, func(v int32) Tr { return ctor(float32(v)) }, selectList)
	case types.T_int64:
		return opUnaryFixedToFixed[int64, Tr](parameters, result, proc, length, func(v int64) Tr { return ctor(float32(v)) }, selectList)
	case types.T_uint8:
		return opUnaryFixedToFixed[uint8, Tr](parameters, result, proc, length, func(v uint8) Tr { return ctor(float32(v)) }, selectList)
	case types.T_uint16:
		return opUnaryFixedToFixed[uint16, Tr](parameters, result, proc, length, func(v uint16) Tr { return ctor(float32(v)) }, selectList)
	case types.T_uint32:
		return opUnaryFixedToFixed[uint32, Tr](parameters, result, proc, length, func(v uint32) Tr { return ctor(float32(v)) }, selectList)
	case types.T_uint64:
		return opUnaryFixedToFixed[uint64, Tr](parameters, result, proc, length, func(v uint64) Tr { return ctor(float32(v)) }, selectList)
	case types.T_float32:
		return opUnaryFixedToFixed[float32, Tr](parameters, result, proc, length, func(v float32) Tr { return ctor(v) }, selectList)
	case types.T_float64:
		return opUnaryFixedToFixed[float64, Tr](parameters, result, proc, length, func(v float64) Tr { return ctor(float32(v)) }, selectList)
	case types.T_bf16:
		return opUnaryFixedToFixed[types.BF16, Tr](parameters, result, proc, length, func(v types.BF16) Tr { return ctor(v.ToFloat32()) }, selectList)
	case types.T_float16:
		return opUnaryFixedToFixed[types.Float16, Tr](parameters, result, proc, length, func(v types.Float16) Tr { return ctor(v.ToFloat32()) }, selectList)
	case types.T_float8:
		return opUnaryFixedToFixed[types.Float8, Tr](parameters, result, proc, length, func(v types.Float8) Tr { return ctor(v.ToFloat32()) }, selectList)
	case types.T_float4:
		return opUnaryFixedToFixed[types.Float4, Tr](parameters, result, proc, length, func(v types.Float4) Tr { return ctor(v.ToFloat32()) }, selectList)
	case types.T_decimal64:
		scale := from.GetType().Scale
		return opUnaryFixedToFixed[types.Decimal64, Tr](parameters, result, proc, length, func(v types.Decimal64) Tr {
			return ctor(float32(types.Decimal64ToFloat64(v, scale)))
		}, selectList)
	case types.T_decimal128:
		scale := from.GetType().Scale
		return opUnaryFixedToFixed[types.Decimal128, Tr](parameters, result, proc, length, func(v types.Decimal128) Tr {
			return ctor(float32(types.Decimal128ToFloat64(v, scale)))
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_datalink:
		return strToLowPrecFloat(proc.Ctx, from, result, length, ctor)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from %s to %s", from.GetType(), result.GetResultVector().GetType()))
}

// strToLowPrecFloat parses each string to a float then rounds it to Tr. A malformed
// value errors, matching a strict numeric cast.
func strToLowPrecFloat[Tr lowPrecFloatConstraint](
	ctx context.Context, from *vector.Vector, result vector.FunctionResultWrapper, length int, ctor func(float32) Tr,
) error {
	src := vector.GenerateFunctionStrParameter(from)
	rs := vector.MustFunctionResult[Tr](result)
	var zero Tr
	for i := 0; i < length; i++ {
		bs, isnull := src.GetStrValue(uint64(i))
		if isnull {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(string(bs)), 32)
		if err != nil {
			return moerr.NewInvalidInput(ctx, fmt.Sprintf("invalid float value %q", string(bs)))
		}
		if err := rs.Append(ctor(float32(f)), false); err != nil {
			return err
		}
	}
	return nil
}

// lowPrecFloatToOthers widens a low-precision float source to a float32 temp vector,
// then reuses the float32 target machinery for every non-low-precision target. The
// decimal256 target is not handled by float32ToOthers, so it is routed to
// castToDecimal256 directly.
func lowPrecFloatToOthers(proc *process.Process, from *vector.Vector, toType types.Type, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList, mode castMode) error {
	tmp := vector.NewVec(types.T_float32.ToType())
	defer tmp.Free(proc.Mp())
	if err := materializeLowPrecAsFloat32(proc.Ctx, from, tmp, length, proc.Mp()); err != nil {
		return err
	}
	if toType.Oid == types.T_decimal256 {
		return castToDecimal256(proc, tmp, toType, result, length, selectList, mode)
	}
	src := vector.GenerateFunctionFixedTypeParameter[float32](tmp)
	return float32ToOthers(proc, src, toType, result, length, selectList, mode.strictStringWidth())
}

// materializeLowPrecAsFloat32 appends every source value, widened to float32, to tmp.
func materializeLowPrecAsFloat32(ctx context.Context, from, tmp *vector.Vector, length int, mp *mpool.MPool) error {
	switch from.GetType().Oid {
	case types.T_bf16:
		return appendLowPrecAsFloat32[types.BF16](from, tmp, length, mp)
	case types.T_float16:
		return appendLowPrecAsFloat32[types.Float16](from, tmp, length, mp)
	case types.T_float8:
		return appendLowPrecAsFloat32[types.Float8](from, tmp, length, mp)
	case types.T_float4:
		return appendLowPrecAsFloat32[types.Float4](from, tmp, length, mp)
	}
	return moerr.NewInternalError(ctx, fmt.Sprintf("not a low-precision float: %s", from.GetType()))
}

func appendLowPrecAsFloat32[Ts lowPrecFloatConstraint](from, tmp *vector.Vector, length int, mp *mpool.MPool) error {
	src := vector.GenerateFunctionFixedTypeParameter[Ts](from)
	for i := 0; i < length; i++ {
		v, isnull := src.GetValue(uint64(i))
		if isnull {
			if err := vector.AppendFixed(tmp, float32(0), true, mp); err != nil {
				return err
			}
			continue
		}
		if err := vector.AppendFixed(tmp, v.ToFloat32(), false, mp); err != nil {
			return err
		}
	}
	return nil
}
