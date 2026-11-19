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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// lowPrecFloatOids is the set of scalar low-precision float types.
var lowPrecFloatOids = []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4}

// TestLowPrecFloatBinaryOpsCoerceToFloat32 verifies that arithmetic and comparison
// operators coerce a low-precision float operand to float32 (#20567): the operator
// resolves and asks for an implicit cast of the low-precision operand, so the value is
// widened losslessly and the existing float operator runs.
func TestLowPrecFloatBinaryOpsCoerceToFloat32(t *testing.T) {
	ctx := context.Background()
	f32 := types.T_float32.ToType()

	for _, oid := range lowPrecFloatOids {
		lp := oid.ToType()

		// lowprec <op> lowprec: both operands coerce to float32.
		for _, op := range []string{"+", "-", "*", "=", "<", ">", "/"} {
			got, err := GetFunctionByName(ctx, op, []types.Type{lp, lp})
			require.NoError(t, err, "%s(%s,%s)", op, oid, oid)
			casts, shouldCast := got.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast, "%s(%s,%s) should cast", op, oid, oid)
			require.Equal(t, f32, casts[0], "%s left cast", op)
			require.Equal(t, f32, casts[1], "%s right cast", op)
		}

		// lowprec + int32 resolves (mixed operands): the low-precision side coerces to
		// a float domain rather than failing to find an overload.
		got, err := GetFunctionByName(ctx, "+", []types.Type{lp, types.T_int32.ToType()})
		require.NoError(t, err, "+(%s,int32)", oid)
		_, shouldCast := got.ShouldDoImplicitTypeCast()
		require.True(t, shouldCast)

		// Unary minus / plus coerce a low-precision operand to float32 (inserting a
		// negative literal into such a column depends on this).
		for _, op := range []string{"unary_minus", "unary_plus"} {
			u, err := GetFunctionByName(ctx, op, []types.Type{lp})
			require.NoError(t, err, "%s(%s)", op, oid)
			casts, cast := u.ShouldDoImplicitTypeCast()
			require.True(t, cast, "%s(%s) should cast", op, oid)
			require.Equal(t, f32, casts[0], "%s(%s) cast target", op, oid)
		}
	}
}

// TestLowPrecFloatAggregatesWidenToFloat32 verifies MIN/MAX/SUM/AVG accept the
// low-precision float types by widening to float32, matching the float32 aggregate's
// return type (#20567).
func TestLowPrecFloatAggregatesWidenToFloat32(t *testing.T) {
	ctx := context.Background()
	f32 := types.T_float32.ToType()

	for _, name := range []string{"min", "max", "sum", "avg"} {
		// The float32 aggregate is the reference for the widened return type.
		ref, err := GetFunctionByName(ctx, name, []types.Type{f32})
		require.NoError(t, err, name)

		for _, oid := range lowPrecFloatOids {
			got, err := GetFunctionByName(ctx, name, []types.Type{oid.ToType()})
			require.NoError(t, err, "%s(%s)", name, oid)
			casts, shouldCast := got.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast, "%s(%s) should cast", name, oid)
			require.Equal(t, []types.Type{f32}, casts, "%s(%s) cast target", name, oid)
			require.Equal(t, ref.GetReturnType(), got.GetReturnType(),
				"%s(%s) return type must match float32", name, oid)
		}
	}
}
