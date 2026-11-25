// Copyright 2026 Matrix Origin
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

var allVectorOids = []types.T{
	types.T_array_float32, types.T_array_float64,
	types.T_array_bf16, types.T_array_float16,
	types.T_array_int8, types.T_array_uint8,
	types.T_array_float8, types.T_array_float4,
}

// wantVectorCommonType is the lattice of docs/sql/vector-common-type.md.
func wantVectorCommonType(a, b types.T, width int32) types.Type {
	switch {
	case a == b:
		return types.New(a, width, 0)
	case a == types.T_array_float64 || b == types.T_array_float64:
		return types.New(types.T_array_float64, width, 0)
	default:
		return types.New(types.T_array_float32, width, 0)
	}
}

// vectorValueOperator builds the arguments of one value-returning operator
// from its two value arguments.
type vectorValueOperator struct {
	name string
	args func(a, b types.Type) []types.Type
}

var vectorValueOperators = []vectorValueOperator{
	{"case", func(a, b types.Type) []types.Type { return []types.Type{types.T_bool.ToType(), a, b} }},
	{"case", func(a, b types.Type) []types.Type {
		return []types.Type{types.T_bool.ToType(), a, types.T_bool.ToType(), b, types.T_any.ToType()}
	}},
	{"iff", func(a, b types.Type) []types.Type { return []types.Type{types.T_bool.ToType(), a, b} }},
	{"coalesce", func(a, b types.Type) []types.Type { return []types.Type{a, b} }},
	{"coalesce", func(a, b types.Type) []types.Type { return []types.Type{types.T_any.ToType(), a, b} }},
	{"greatest", func(a, b types.Type) []types.Type { return []types.Type{a, b} }},
	{"least", func(a, b types.Type) []types.Type { return []types.Type{a, b} }},
}

// BETWEEN over vectors binds as >= and <=; see TestBindVectorBetween.
var vectorComparisonOperators = []string{"=", "<>", "<", "<=", ">", ">=", "<=>"}

func comparisonArgs(_ string, a, b types.Type) []types.Type {
	return []types.Type{a, b}
}

// requireVectorArgsCastTo checks that every vector or string argument of a
// resolution executes as want.
func requireVectorArgsCastTo(t *testing.T, resolved FuncGetResult, inputs []types.Type, want types.Type) {
	t.Helper()
	casts, should := resolved.ShouldDoImplicitTypeCast()
	for i, input := range inputs {
		if !input.Oid.IsArray() && !input.Oid.IsMySQLString() {
			continue
		}
		got := input
		if should {
			got = casts[i]
		}
		require.Equal(t, want.Oid, got.Oid, "argument %d (%s)", i, input.Oid)
		require.Equal(t, want.Width, got.Width, "argument %d (%s)", i, input.Oid)
	}
}

// vectorTypeModes are the MySQL-compatible and the MATRIXONE_NATIVE binding
// contexts; CASE and IF resolve through separate checkers in each.
func vectorTypeModes(t *testing.T) map[string]context.Context {
	return map[string]context.Context{
		"mysql":  WithMySQLConditionalBooleanContracts(t.Context(), true),
		"native": WithMySQLConditionalBooleanContracts(t.Context(), false),
	}
}

func TestVectorCommonTypeContract(t *testing.T) {
	for mode, ctx := range vectorTypeModes(t) {
		t.Run(mode, func(t *testing.T) { testVectorCommonTypeContract(t, ctx) })
	}
}

func testVectorCommonTypeContract(t *testing.T, ctx context.Context) {
	const width = 4
	for _, a := range allVectorOids {
		for _, b := range allVectorOids {
			left, right := types.New(a, width, 0), types.New(b, width, 0)
			want := wantVectorCommonType(a, b, width)
			for _, op := range vectorValueOperators {
				inputs := op.args(left, right)
				resolved, err := GetFunctionByName(ctx, op.name, inputs)
				require.NoError(t, err, "%s(%s, %s)", op.name, a, b)
				ret := want
				if (op.name == "greatest" || op.name == "least") && ret.Oid.IsBlockScaledArray() {
					ret = types.New(types.T_array_float32, width, 0)
				}
				require.Equal(t, ret, resolved.GetReturnType(), "%s(%s, %s)", op.name, a, b)
				requireVectorArgsCastTo(t, resolved, inputs, ret)
			}
			for _, name := range vectorComparisonOperators {
				inputs := comparisonArgs(name, left, right)
				resolved, err := GetFunctionByName(ctx, name, inputs)
				require.NoError(t, err, "%s %s %s", a, name, b)
				require.Equal(t, types.T_bool, resolved.GetReturnType().Oid)
				if a != b {
					requireVectorArgsCastTo(t, resolved, inputs, want)
				}
			}
		}
	}
}

func TestVectorCommonTypeStringAndNull(t *testing.T) {
	for mode, ctx := range vectorTypeModes(t) {
		t.Run(mode, func(t *testing.T) { testVectorCommonTypeStringAndNull(t, ctx) })
	}
}

func testVectorCommonTypeStringAndNull(t *testing.T, ctx context.Context) {
	const width = 4
	for _, a := range allVectorOids {
		vec := types.New(a, width, 0)
		for _, other := range []types.Type{types.T_varchar.ToType(), types.T_text.ToType(),
			types.T_blob.ToType(), types.T_any.ToType()} {
			for _, op := range vectorValueOperators {
				for _, inputs := range [][]types.Type{op.args(vec, other), op.args(other, vec)} {
					resolved, err := GetFunctionByName(ctx, op.name, inputs)
					require.NoError(t, err, "%s(%s, %s)", op.name, a, other.Oid)
					ret := vec
					if (op.name == "greatest" || op.name == "least") && a.IsBlockScaledArray() {
						ret = types.New(types.T_array_float32, width, 0)
					}
					require.Equal(t, ret, resolved.GetReturnType(), "%s(%s, %s)", op.name, a, other.Oid)
					requireVectorArgsCastTo(t, resolved, inputs, ret)
				}
			}
		}
	}
}

func TestVectorCommonTypeRejections(t *testing.T) {
	for mode, ctx := range vectorTypeModes(t) {
		t.Run(mode, func(t *testing.T) { testVectorCommonTypeRejections(t, ctx) })
	}
}

func testVectorCommonTypeRejections(t *testing.T, ctx context.Context) {
	for _, a := range allVectorOids {
		for _, b := range allVectorOids {
			narrow, wide := types.New(a, 3, 0), types.New(b, 4, 0)
			for _, op := range vectorValueOperators {
				_, err := GetFunctionByName(ctx, op.name, op.args(narrow, wide))
				require.Error(t, err, "%s(%s(3), %s(4))", op.name, a, b)
			}
			if a != b {
				for _, name := range vectorComparisonOperators {
					_, err := GetFunctionByName(ctx, name, comparisonArgs(name, narrow, wide))
					require.Error(t, err, "%s(3) %s %s(4)", a, name, b)
				}
			}
		}
		for _, scalar := range []types.Type{types.T_int64.ToType(), types.T_float64.ToType(), types.T_json.ToType(),
			types.T_binary.ToType(), types.T_varbinary.ToType()} {
			for _, op := range vectorValueOperators {
				_, err := GetFunctionByName(ctx, op.name, op.args(types.New(a, 4, 0), scalar))
				require.Error(t, err, "%s(%s, %s)", op.name, a, scalar.Oid)
			}
		}
	}
}

func TestCommonVectorType(t *testing.T) {
	f32, f64 := types.New(types.T_array_float32, 2, 0), types.New(types.T_array_float64, 2, 0)
	i8, f8 := types.New(types.T_array_int8, 2, 0), types.New(types.T_array_float8, 2, 0)
	for _, tc := range []struct {
		name      string
		inputs    []types.Type
		target    types.Type
		hasVector bool
		ok        bool
	}{
		{"no vector", []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, types.Type{}, false, true},
		{"same", []types.Type{i8, types.T_any.ToType(), i8}, i8, true, true},
		{"mixed narrow", []types.Type{i8, f8}, f32, true, true},
		{"mixed with f64 last", []types.Type{i8, f8, f64}, f64, true, true},
		{"mixed with f64 first", []types.Type{f64, i8}, f64, true, true},
		{"string", []types.Type{types.T_varchar.ToType(), f8}, f8, true, true},
		{"dimension", []types.Type{f32, types.New(types.T_array_float32, 3, 0)}, f32, true, false},
		{"scalar", []types.Type{f32, types.T_int64.ToType()}, f32, true, false},
		{"binary string", []types.Type{f32, types.T_varbinary.ToType()}, f32, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, hasVector, ok := commonVectorType(tc.inputs)
			require.Equal(t, tc.hasVector, hasVector)
			require.Equal(t, tc.ok, ok)
			if tc.ok && tc.hasVector {
				require.Equal(t, tc.target, target)
			}
		})
	}
}
