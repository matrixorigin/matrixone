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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNullabilityRequiresPositiveProof(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []types.Type
		want bool
	}{
		{"abs", []types.Type{types.T_int64.ToType()}, true},
		{"ceil", []types.Type{types.T_float64.ToType()}, true},
		{"floor", []types.Type{types.T_float64.ToType()}, true},
		{"round", []types.Type{types.T_float64.ToType()}, true},
		{"length", []types.Type{types.T_varchar.ToType()}, true},
		{"bit_length", []types.Type{types.T_varchar.ToType()}, true},
		{"lower", []types.Type{types.T_varchar.ToType()}, true},
		{"upper", []types.Type{types.T_varchar.ToType()}, true},
		{"regexp_substr", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, false},
		{"sqrt", []types.Type{types.T_float64.ToType()}, false},
		{"ln", []types.Type{types.T_float64.ToType()}, false},
		{"unhex", []types.Type{types.T_varchar.ToType()}, false},
		{"=", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, true},
		{"+", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, true},
		{"count", []types.Type{types.T_int32.ToType()}, true},
		{"case", []types.Type{types.T_bool.ToType(), types.T_int32.ToType()}, false},
		{"case", []types.Type{types.T_bool.ToType(), types.T_int32.ToType(), types.T_int32.ToType()}, true},
		{"cast", []types.Type{types.T_varchar.ToType(), types.T_date.ToType()}, false},
		{"cast", []types.Type{types.T_int32.ToType(), types.T_int64.ToType()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			args := make([]*plan.Expr, len(tc.args))
			for i, tp := range tc.args {
				args[i] = &plan.Expr{Typ: plan.Type{Id: int32(tp.Oid), NotNullable: true}}
			}
			require.Equal(t, tc.want, DeduceNotNullable(fn.GetEncodedOverloadID(), args))
		})
	}
}

func TestCommonFunctionNonNullOverloads(t *testing.T) {
	for _, id := range []int{ABS, SIGN, CEIL, FLOOR, ROUND, TRUNCATE, LENGTH, LENGTH_UTF8, BIT_LENGTH, LOWER, UPPER, CONCAT, CONCAT_WS, LTRIM, RTRIM, REVERSE, SUBSTRING, SUBSTRING_INDEX, TRIM} {
		fn := allSupportedFunctions[id]
		require.NotEmpty(t, fn.Overloads)
		for _, ov := range fn.Overloads {
			signature := ov.args
			if id == CONCAT || id == CONCAT_WS {
				signature = []types.T{types.T_varchar, types.T_varchar, types.T_varchar}
			}
			args := make([]*plan.Expr, len(signature))
			for i, typ := range signature {
				args[i] = &plan.Expr{Typ: plan.Type{Id: int32(typ), NotNullable: true}}
			}
			encoded := encodeOverloadID(int32(id), int32(ov.overloadId))
			require.True(t, DeduceNotNullable(encoded, args), "function %d overload %d", id, ov.overloadId)
			for _, arg := range args {
				arg.Typ.NotNullable = false
				require.False(t, DeduceNotNullable(encoded, args), "nullable argument: function %d overload %d", id, ov.overloadId)
				arg.Typ.NotNullable = true
			}
		}
	}
}

func TestLosslessIntegerCastProof(t *testing.T) {
	for _, tc := range []struct {
		from, to types.T
		safe     bool
	}{
		{types.T_int8, types.T_int64, true}, {types.T_uint8, types.T_uint64, true},
		{types.T_int64, types.T_int64, true}, {types.T_int64, types.T_int8, false},
		{types.T_int32, types.T_uint64, false}, {types.T_uint64, types.T_int64, false},
		{types.T_float64, types.T_int64, false}, {types.T_varchar, types.T_int64, false},
	} {
		fn, err := GetFunctionByName(context.Background(), "cast", []types.Type{tc.from.ToType(), tc.to.ToType()})
		require.NoError(t, err)
		expr := &plan.Expr{Typ: plan.Type{Id: int32(tc.to)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID()}, Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(tc.from)}}, {Typ: plan.Type{Id: int32(tc.to)}},
			},
		}}}
		require.Equal(t, tc.safe, IsLosslessIntegerCast(expr), "%v -> %v", tc.from, tc.to)
	}
	require.False(t, IsLosslessIntegerCast(&plan.Expr{}))
}

func TestRandomBytesVolatileMetadata(t *testing.T) {
	for _, tp := range []types.T{types.T_int64, types.T_uint64} {
		fr, err := GetFunctionByName(context.Background(), "random_bytes", []types.Type{tp.ToType()})
		require.NoError(t, err)
		ov, ok := GetFunctionByIdWithoutError(fr.GetEncodedOverloadID())
		require.True(t, ok)
		require.True(t, ov.CannotFold(), "random bytes must not be folded or hoisted out of pairwise evaluation")
	}
}
