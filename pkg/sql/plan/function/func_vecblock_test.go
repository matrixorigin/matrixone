// Copyright 2021 - 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/stretchr/testify/require"
)

var vecBlockOids = []types.T{types.T_array_float8, types.T_array_float4}

func TestVecBlockFunctionResolution(t *testing.T) {
	ctx := context.Background()
	f32 := types.New(types.T_array_float32, 4, 0)
	for _, oid := range vecBlockOids {
		vb := types.New(oid, 4, 0)

		// Arithmetic promotes vecf8/vecf4 operands to vecf32 and returns vecf32.
		for _, op := range []string{"+", "-", "*", "/"} {
			for _, args := range [][]types.Type{{vb, vb}, {vb, f32}, {f32, vb}} {
				r, err := GetFunctionByName(ctx, op, args)
				require.NoError(t, err, "%s %v", op, args)
				targets, cast := r.ShouldDoImplicitTypeCast()
				require.True(t, cast, "%s %v", op, args)
				require.Equal(t, types.T_array_float32, targets[0].Oid, "%s %v", op, args)
				require.Equal(t, types.T_array_float32, targets[1].Oid, "%s %v", op, args)
				require.Equal(t, int32(4), targets[0].Width)
				require.Equal(t, types.T_array_float32, r.GetReturnType().Oid, "%s %v", op, args)
			}
		}
		for _, op := range []string{"+", "-", "*", "/"} {
			r, err := GetFunctionByName(ctx, op, []types.Type{vb, types.T_float64.ToType()})
			require.NoError(t, err, op)
			targets, cast := r.ShouldDoImplicitTypeCast()
			require.True(t, cast, op)
			require.Equal(t, types.T_array_float32, targets[0].Oid, op)
			require.Equal(t, types.T_array_float32, r.GetReturnType().Oid, op)
		}

		// inner_product: against itself, the other format and vecf32; a text literal
		// becomes vecf32 so the query side is not quantized.
		for _, other := range []types.Type{vb, f32, types.New(types.T_array_float8, 4, 0), types.New(types.T_array_float4, 4, 0)} {
			for _, args := range [][]types.Type{{vb, other}, {other, vb}} {
				r, err := GetFunctionByName(ctx, "inner_product", args)
				require.NoError(t, err, "%v", args)
				_, cast := r.ShouldDoImplicitTypeCast()
				require.False(t, cast, "%v", args)
				require.Equal(t, types.T_float64, r.GetReturnType().Oid)
			}
		}
		r, err := GetFunctionByName(ctx, "inner_product", []types.Type{vb, types.T_varchar.ToType()})
		require.NoError(t, err)
		targets, cast := r.ShouldDoImplicitTypeCast()
		require.True(t, cast)
		require.Equal(t, oid, targets[0].Oid)
		require.Equal(t, types.T_array_float32, targets[1].Oid)

		for _, name := range []string{"any_value", "group_concat"} {
			_, err := GetFunctionByName(ctx, name, []types.Type{vb})
			require.NoError(t, err, name)
		}

		// Not supported: other distances, comparison, SUM/AVG, vector-only functions.
		for _, tc := range []struct {
			name string
			args []types.Type
		}{
			{"l2_distance", []types.Type{vb, vb}},
			{"l2_distance", []types.Type{vb, f32}},
			{"l2_distance_sq", []types.Type{vb, vb}},
			{"cosine_distance", []types.Type{vb, vb}},
			{"cosine_similarity", []types.Type{vb, vb}},
			{"l1_distance", []types.Type{vb, vb}},
			{"normalize_l2", []types.Type{vb}},
			{"vector_dims", []types.Type{vb}},
			{"=", []types.Type{vb, vb}},
			{"<", []types.Type{vb, vb}},
			{"sum", []types.Type{vb}},
			{"avg", []types.Type{vb}},
			{"max", []types.Type{vb}},
		} {
			_, err := GetFunctionByName(ctx, tc.name, tc.args)
			require.Error(t, err, "%s %v", tc.name, tc.args)
		}
	}
}

func vecBlockCellVector(t *testing.T, oid types.T, dim int32, rows [][]float32, nulls []bool) *vector.Vector {
	t.Helper()
	proc := testutil.NewProcess(t)
	f, _ := oid.BlockScaledFormat()
	vec := vector.NewVec(types.New(oid, dim, 0))
	for i, r := range rows {
		if nulls != nil && nulls[i] {
			require.NoError(t, vector.AppendBytes(vec, nil, true, proc.Mp()))
			continue
		}
		cell, err := types.AppendBlockScaled(nil, f, r)
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vec, cell, false, proc.Mp()))
	}
	return vec
}

func runInnerProductVecBlock(t *testing.T, a, b *vector.Vector) (*vector.Vector, error) {
	t.Helper()
	proc := testutil.NewProcess(t)
	result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
	require.NoError(t, result.PreExtendAndReset(a.Length()))
	err := InnerProductVecBlock([]*vector.Vector{a, b}, result, proc, a.Length(), nil)
	return result.GetResultVector(), err
}

func TestInnerProductVecBlock(t *testing.T) {
	proc := testutil.NewProcess(t)
	rows := [][]float32{{1, -3, 0, 6}, {0.25, 0.5, -0.75, 1}, {9, 9, 9, 9}}
	queries := [][]float32{{2, 1, -1, 0.5}, {1, 1, 1, 1}, {0, 0, 0, 0}}
	nulls := []bool{false, false, true}
	f32 := vector.NewVec(types.New(types.T_array_float32, 4, 0))
	for _, q := range queries {
		require.NoError(t, vector.AppendArray(f32, q, false, proc.Mp()))
	}
	for _, oid := range vecBlockOids {
		a := vecBlockCellVector(t, oid, 4, rows, nulls)
		for name, b := range map[string]*vector.Vector{
			"vecf32": f32,
			"vecf8":  vecBlockCellVector(t, types.T_array_float8, 4, queries, nil),
			"vecf4":  vecBlockCellVector(t, types.T_array_float4, 4, queries, nil),
		} {
			out, err := runInnerProductVecBlock(t, a, b)
			require.NoError(t, err, "%s x %s", oid, name)
			col := vector.MustFixedColNoTypeCheck[float64](out)
			for i := range rows {
				if nulls[i] {
					require.True(t, out.GetNulls().Contains(uint64(i)))
					continue
				}
				da, err := types.BlockScaledToFloat32(a.GetBytesAt(i))
				require.NoError(t, err)
				db := vector.GetArrayAt[float32](f32, i)
				if b.GetType().Oid.IsBlockScaledVector() {
					db, err = types.BlockScaledToFloat32(b.GetBytesAt(i))
					require.NoError(t, err)
				}
				want, err := moarray.InnerProduct[float32](da, db)
				require.NoError(t, err)
				require.Equal(t, want, col[i], "%s x %s row %d", oid, name, i)
			}
		}

		// dimension mismatch and a malformed cell are errors
		short := vecBlockCellVector(t, oid, 3, [][]float32{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}}, nil)
		_, err := runInnerProductVecBlock(t, a, short)
		require.Error(t, err)
		bad := vector.NewVec(types.New(oid, 4, 0))
		for range rows {
			require.NoError(t, vector.AppendBytes(bad, []byte{0x7f, 1, 0, 0, 4, 0, 0, 0}, false, proc.Mp()))
		}
		_, err = runInnerProductVecBlock(t, bad, f32)
		require.Error(t, err)
	}
}

func TestVecBlockArithmeticCastRules(t *testing.T) {
	f32 := types.New(types.T_array_float32, 4, 0)
	for _, oid := range vecBlockOids {
		vb := types.New(oid, 4, 0)
		cast, l, r := arithmeticTypeCastRule1(vb, vb)
		require.True(t, cast)
		require.Equal(t, f32, l)
		require.Equal(t, f32, r)
		cast, l, r = fixedTypeCastRule2(vb, f32)
		require.True(t, cast)
		require.Equal(t, types.T_array_float32, l.Oid)
		require.Equal(t, types.T_array_float32, r.Oid)
		p, ok := promoteBlockScaledVector(types.T_float8.ToType())
		require.False(t, ok)
		require.Equal(t, types.T_float8, p.Oid)
	}
}
