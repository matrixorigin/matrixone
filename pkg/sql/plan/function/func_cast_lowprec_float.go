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

// anyToLowPrecFloat rounds each source value to Tr via ctor, once: a source wider than
// float32 reaches ctor through Float32RoundToOdd.
// Every value is finite-checked (RejectNonFiniteNarrowFloat) before rounding, so a NaN,
// Inf, or out-of-range source errors instead of persisting a non-finite / saturated
// value -- this keeps the numeric path consistent with the string path and upholds the
// repo-wide "never persist non-finite float" invariant (#29084); bf16/float16 otherwise
// overflow to Inf and float8 otherwise maps NaN to its NaN slot.
func anyToLowPrecFloat[Tr types.LowPrecFloat](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, ctor func(float32) Tr,
) error {
	from := parameters[0]
	oid := result.GetResultVector().GetType().Oid
	conv := func(f float32) (Tr, error) {
		var zero Tr
		if err := types.RejectNonFiniteNarrowFloat(f, oid); err != nil {
			return zero, err
		}
		return types.CanonicalLowPrecFloat(ctor(f)), nil
	}
	// a source wider than float32 rounds once to Tr
	conv64 := func(f float64) (Tr, error) { return conv(types.Float32RoundToOdd(f)) }
	switch from.GetType().Oid {
	case types.T_bool:
		return opUnaryFixedToFixedWithErrorCheck[bool, Tr](parameters, result, proc, length, func(v bool) (Tr, error) {
			if v {
				return conv(1)
			}
			return conv(0)
		}, selectList)
	case types.T_bit:
		return opUnaryFixedToFixedWithErrorCheck[uint64, Tr](parameters, result, proc, length, func(v uint64) (Tr, error) { return conv64(float64(v)) }, selectList)
	case types.T_int8:
		return opUnaryFixedToFixedWithErrorCheck[int8, Tr](parameters, result, proc, length, func(v int8) (Tr, error) { return conv(float32(v)) }, selectList)
	case types.T_int16:
		return opUnaryFixedToFixedWithErrorCheck[int16, Tr](parameters, result, proc, length, func(v int16) (Tr, error) { return conv(float32(v)) }, selectList)
	case types.T_int32:
		return opUnaryFixedToFixedWithErrorCheck[int32, Tr](parameters, result, proc, length, func(v int32) (Tr, error) { return conv64(float64(v)) }, selectList)
	case types.T_int64:
		return opUnaryFixedToFixedWithErrorCheck[int64, Tr](parameters, result, proc, length, func(v int64) (Tr, error) { return conv64(float64(v)) }, selectList)
	case types.T_uint8:
		return opUnaryFixedToFixedWithErrorCheck[uint8, Tr](parameters, result, proc, length, func(v uint8) (Tr, error) { return conv(float32(v)) }, selectList)
	case types.T_uint16:
		return opUnaryFixedToFixedWithErrorCheck[uint16, Tr](parameters, result, proc, length, func(v uint16) (Tr, error) { return conv(float32(v)) }, selectList)
	case types.T_uint32:
		return opUnaryFixedToFixedWithErrorCheck[uint32, Tr](parameters, result, proc, length, func(v uint32) (Tr, error) { return conv64(float64(v)) }, selectList)
	case types.T_uint64:
		return opUnaryFixedToFixedWithErrorCheck[uint64, Tr](parameters, result, proc, length, func(v uint64) (Tr, error) { return conv64(float64(v)) }, selectList)
	case types.T_float32:
		return opUnaryFixedToFixedWithErrorCheck[float32, Tr](parameters, result, proc, length, func(v float32) (Tr, error) { return conv(v) }, selectList)
	case types.T_float64:
		return opUnaryFixedToFixedWithErrorCheck[float64, Tr](parameters, result, proc, length, func(v float64) (Tr, error) { return conv64(v) }, selectList)
	case types.T_bf16:
		return opUnaryFixedToFixedWithErrorCheck[types.BF16, Tr](parameters, result, proc, length, func(v types.BF16) (Tr, error) { return conv(v.ToFloat32()) }, selectList)
	case types.T_float16:
		return opUnaryFixedToFixedWithErrorCheck[types.Float16, Tr](parameters, result, proc, length, func(v types.Float16) (Tr, error) { return conv(v.ToFloat32()) }, selectList)
	case types.T_float8:
		return opUnaryFixedToFixedWithErrorCheck[types.Float8, Tr](parameters, result, proc, length, func(v types.Float8) (Tr, error) { return conv(v.ToFloat32()) }, selectList)
	case types.T_float4:
		return opUnaryFixedToFixedWithErrorCheck[types.Float4, Tr](parameters, result, proc, length, func(v types.Float4) (Tr, error) { return conv(v.ToFloat32()) }, selectList)
	case types.T_decimal64:
		scale := from.GetType().Scale
		return opUnaryFixedToFixedWithErrorCheck[types.Decimal64, Tr](parameters, result, proc, length, func(v types.Decimal64) (Tr, error) {
			return conv64(types.Decimal64ToFloat64(v, scale))
		}, selectList)
	case types.T_decimal128:
		scale := from.GetType().Scale
		return opUnaryFixedToFixedWithErrorCheck[types.Decimal128, Tr](parameters, result, proc, length, func(v types.Decimal128) (Tr, error) {
			return conv64(types.Decimal128ToFloat64(v, scale))
		}, selectList)
	case types.T_decimal256:
		scale := from.GetType().Scale
		return opUnaryFixedToFixedWithErrorCheck[types.Decimal256, Tr](parameters, result, proc, length, func(v types.Decimal256) (Tr, error) {
			return conv64(types.Decimal256ToFloat64(v, scale))
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_datalink:
		return strToLowPrecFloat(proc.Ctx, from, result, length, ctor, selectList)
	}
	return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from %s to %s", from.GetType(), result.GetResultVector().GetType()))
}

// strToLowPrecFloat parses each selected string to a float then rounds it to Tr. A
// malformed value errors, matching a strict numeric cast.
func strToLowPrecFloat[Tr types.LowPrecFloat](
	ctx context.Context, from *vector.Vector, result vector.FunctionResultWrapper, length int, ctor func(float32) Tr,
	selectList *FunctionSelectList,
) error {
	src := vector.GenerateFunctionStrParameter(from)
	rs := vector.MustFunctionResult[Tr](result)
	if selectList != nil && selectList.IgnoreAllRow() {
		rs.SetNullResult(uint64(length))
		return nil
	}
	var zero Tr
	for i := 0; i < length; i++ {
		if castRowSkipped(selectList, uint64(i)) {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		bs, isnull := src.GetStrValue(uint64(i))
		if isnull {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		f64, err := strconv.ParseFloat(strings.TrimSpace(string(bs)), 64)
		if err != nil {
			return moerr.NewInvalidInput(ctx, fmt.Sprintf("invalid float value %q", string(bs)))
		}
		f := types.Float32RoundToOdd(f64)
		if err := types.RejectNonFiniteNarrowFloat(f, rs.GetType().Oid); err != nil {
			return err
		}
		if err := rs.Append(types.CanonicalLowPrecFloat(ctor(f)), false); err != nil {
			return err
		}
	}
	return nil
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

func appendLowPrecAsFloat32[Ts types.LowPrecFloat](from, tmp *vector.Vector, length int, mp *mpool.MPool) error {
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
