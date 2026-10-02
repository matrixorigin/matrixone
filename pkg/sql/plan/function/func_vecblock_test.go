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
	"math"
	"math/rand"
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

		// Distances: against itself, the other format and vecf32; a text literal becomes
		// vecf32 so the query side is not quantized.
		for _, name := range vecBlockDistanceNames {
			for _, other := range []types.Type{vb, f32, types.New(types.T_array_float8, 4, 0), types.New(types.T_array_float4, 4, 0)} {
				for _, args := range [][]types.Type{{vb, other}, {other, vb}} {
					r, err := GetFunctionByName(ctx, name, args)
					require.NoError(t, err, "%s %v", name, args)
					_, cast := r.ShouldDoImplicitTypeCast()
					require.False(t, cast, "%s %v", name, args)
					require.Equal(t, types.T_float64, r.GetReturnType().Oid)
				}
			}
			r, err := GetFunctionByName(ctx, name, []types.Type{vb, types.T_varchar.ToType()})
			require.NoError(t, err, name)
			targets, cast := r.ShouldDoImplicitTypeCast()
			require.True(t, cast, name)
			require.Equal(t, oid, targets[0].Oid, name)
			require.Equal(t, types.T_array_float32, targets[1].Oid, name)
		}
		r, err := GetFunctionByName(ctx, "vector_dims", []types.Type{vb})
		require.NoError(t, err)
		require.Equal(t, types.T_int64, r.GetReturnType().Oid)
		r, err = GetFunctionByName(ctx, "normalize_l2", []types.Type{vb})
		require.NoError(t, err)
		require.Equal(t, vb, r.GetReturnType())

		for _, name := range []string{"any_value", "group_concat"} {
			_, err := GetFunctionByName(ctx, name, []types.Type{vb})
			require.NoError(t, err, name)
		}

		// Not supported: comparison, SUM/AVG, functions vecf32-only among the vector types.
		for _, tc := range []struct {
			name string
			args []types.Type
		}{
			{"hex", []types.Type{vb}},
			{"to_base64", []types.Type{vb}},
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

var vecBlockDistanceNames = []string{"inner_product", "l2_distance", "l2_distance_sq", "l1_distance", "cosine_distance", "cosine_similarity"}

var vecBlockDistances = []struct {
	name string
	op   executeLogicOfOverload
	ref  func(a, b []float32) (float64, error)
}{
	{"inner_product", InnerProductVecBlock, moarray.InnerProduct[float32]},
	{"l2_distance", L2DistanceVecBlock, moarray.L2Distance[float32]},
	{"l2_distance_sq", L2DistanceSqVecBlock, moarray.L2DistanceSq[float32]},
	{"l1_distance", L1DistanceVecBlock, moarray.L1Distance[float32]},
	{"cosine_distance", CosineDistanceVecBlock, moarray.CosineDistance[float32]},
	{"cosine_similarity", CosineSimilarityVecBlock, moarray.CosineSimilarity[float32]},
}

func runVecBlockFn(t *testing.T, op executeLogicOfOverload, rt types.Type, args ...*vector.Vector) (*vector.Vector, error) {
	t.Helper()
	proc := testutil.NewProcess(t)
	result := vector.NewFunctionResultWrapper(rt, proc.Mp())
	require.NoError(t, result.PreExtendAndReset(args[0].Length()))
	err := op(args, result, proc, args[0].Length(), nil)
	return result.GetResultVector(), err
}

func vecBlockRowFloat32(t *testing.T, v *vector.Vector, i int) []float32 {
	t.Helper()
	if !v.GetType().Oid.IsBlockScaledArray() {
		return vector.GetArrayAt[float32](v, i)
	}
	d, err := types.BlockScaledToFloat32(v.GetBytesAt(i))
	require.NoError(t, err)
	return d
}

func TestVecBlockDistances(t *testing.T) {
	proc := testutil.NewProcess(t)
	r := rand.New(rand.NewSource(3))
	random := func(n, dim int) [][]float32 {
		rows := make([][]float32, n)
		for i := range rows {
			rows[i] = make([]float32, dim)
			for j := range rows[i] {
				rows[i][j] = float32(r.NormFloat64())
			}
		}
		return rows
	}
	for _, tc := range []struct {
		dim     int32
		rows    [][]float32
		queries [][]float32
		nulls   []bool
	}{
		{4, [][]float32{{1, -3, 0, 6}, {0.25, 0.5, -0.75, 1}, {9, 9, 9, 9}}, [][]float32{{2, 1, -1, 0.5}, {1, 1, 1, 1}, {0, 0, 0, 0}}, []bool{false, false, true}},
		{40, random(5, 40), random(5, 40), nil},
	} {
		f32 := vector.NewVec(types.New(types.T_array_float32, tc.dim, 0))
		for _, q := range tc.queries {
			require.NoError(t, vector.AppendArray(f32, q, false, proc.Mp()))
		}
		for _, oid := range vecBlockOids {
			a := vecBlockCellVector(t, oid, tc.dim, tc.rows, tc.nulls)
			for name, b := range map[string]*vector.Vector{
				"vecf32": f32,
				"vecf8":  vecBlockCellVector(t, types.T_array_float8, tc.dim, tc.queries, nil),
				"vecf4":  vecBlockCellVector(t, types.T_array_float4, tc.dim, tc.queries, nil),
			} {
				for _, d := range vecBlockDistances {
					for _, args := range [][]*vector.Vector{{a, b}, {b, a}} {
						out, err := runVecBlockFn(t, d.op, types.T_float64.ToType(), args...)
						require.NoError(t, err, "%s %s x %s", d.name, oid, name)
						col := vector.MustFixedColNoTypeCheck[float64](out)
						for i := range tc.rows {
							if tc.nulls != nil && tc.nulls[i] {
								require.True(t, out.GetNulls().Contains(uint64(i)))
								continue
							}
							want, err := d.ref(vecBlockRowFloat32(t, args[0], i), vecBlockRowFloat32(t, args[1], i))
							require.NoError(t, err)
							require.InDelta(t, want, col[i], 1e-5*math.Max(1, math.Abs(want)), "%s %s x %s row %d", d.name, oid, name, i)
						}
					}
				}
			}

			// dimension mismatch and a malformed cell are errors
			short := vecBlockCellVector(t, oid, 3, [][]float32{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}}, nil)
			bad := vector.NewVec(types.New(oid, tc.dim, 0))
			for range tc.rows {
				require.NoError(t, vector.AppendBytes(bad, []byte{0x7f, 1, 0, 0, 4, 0, 0, 0}, false, proc.Mp()))
			}
			for _, d := range vecBlockDistances {
				_, err := runVecBlockFn(t, d.op, types.T_float64.ToType(), a, short)
				require.Error(t, err, d.name)
				_, err = runVecBlockFn(t, d.op, types.T_float64.ToType(), bad, f32)
				require.Error(t, err, d.name)
			}
		}
	}
}

func TestVecBlockCosineZeroVector(t *testing.T) {
	for _, oid := range vecBlockOids {
		a := vecBlockCellVector(t, oid, 3, [][]float32{{1, 2, 3}}, nil)
		zero := vecBlockCellVector(t, oid, 3, [][]float32{{0, 0, 0}}, nil)
		out, err := runVecBlockFn(t, CosineDistanceVecBlock, types.T_float64.ToType(), a, zero)
		require.NoError(t, err)
		require.Equal(t, []float64{1}, vector.MustFixedColNoTypeCheck[float64](out))
		_, err = runVecBlockFn(t, CosineSimilarityVecBlock, types.T_float64.ToType(), a, zero)
		require.Error(t, err)
	}
}

func TestVectorDimsAndNormalizeL2VecBlock(t *testing.T) {
	proc := testutil.NewProcess(t)
	rows := [][]float32{{3, 0, -4}, {0, 0, 0}, {1, 1, 1}}
	nulls := []bool{false, false, true}
	for _, oid := range vecBlockOids {
		a := vecBlockCellVector(t, oid, 3, rows, nulls)
		out, err := runVecBlockFn(t, VectorDimsVecBlock, types.T_int64.ToType(), a)
		require.NoError(t, err)
		require.Equal(t, []int64{3, 3}, vector.MustFixedColNoTypeCheck[int64](out)[:2])
		require.True(t, out.GetNulls().Contains(2))

		out, err = runVecBlockFn(t, NormalizeL2VecBlock, *a.GetType(), a)
		require.NoError(t, err)
		require.True(t, out.GetNulls().Contains(2))
		got, err := types.BlockScaledToFloat32(out.GetBytesAt(0))
		require.NoError(t, err)
		require.InDeltaSlice(t, []float32{0.6, 0, -0.8}, got, 0.1)
		in, err := types.BlockScaledToFloat32(a.GetBytesAt(0))
		require.NoError(t, err)
		norm := make([]float32, len(in))
		require.NoError(t, moarray.NormalizeL2(in, norm))
		f, _ := oid.BlockScaledFormat()
		want, err := types.AppendBlockScaled(nil, f, norm)
		require.NoError(t, err)
		require.Equal(t, want, out.GetBytesAt(0))
		got, err = types.BlockScaledToFloat32(out.GetBytesAt(1))
		require.NoError(t, err)
		require.Equal(t, []float32{0, 0, 0}, got)

		bad := vector.NewVec(types.New(oid, 3, 0))
		require.NoError(t, vector.AppendBytes(bad, []byte{0x7f, 1, 0, 0, 3, 0, 0, 0}, false, proc.Mp()))
		_, err = runVecBlockFn(t, VectorDimsVecBlock, types.T_int64.ToType(), bad)
		require.Error(t, err)
		_, err = runVecBlockFn(t, NormalizeL2VecBlock, *bad.GetType(), bad)
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

func TestVectorMatmulResolution(t *testing.T) {
	ctx := context.Background()
	vc := types.T_varchar.ToType()
	i64 := types.T_int64.ToType()
	for _, oid := range vecBlockOids {
		vb := types.New(oid, 4, 0)
		for _, id := range []types.T{types.T_int32, types.T_int64, types.T_uint64, types.T_varchar, types.T_char, types.T_text, types.T_uuid} {
			r, err := GetFunctionByName(ctx, "vector_matmul", []types.Type{i64, id.ToType(), vb, vc})
			require.NoError(t, err, id.String())
			require.Equal(t, types.T_json, r.GetReturnType().Oid)
		}
		_, err := GetFunctionByName(ctx, "vector_matmul", []types.Type{i64, i64, vb, types.T_json.ToType(), vc})
		require.NoError(t, err)
		// a non-int64 topk, untyped queries and options are cast
		r, err := GetFunctionByName(ctx, "vector_matmul", []types.Type{types.T_uint8.ToType(), i64, vb, types.T_any.ToType(), types.T_any.ToType()})
		require.NoError(t, err)
		targets, cast := r.ShouldDoImplicitTypeCast()
		require.True(t, cast)
		require.Equal(t, types.T_int64, targets[0].Oid)
		require.Equal(t, types.T_varchar, targets[3].Oid)
		require.Equal(t, types.T_varchar, targets[4].Oid)
		r, err = GetFunctionByName(ctx, "vector_matmul", []types.Type{types.T_text.ToType(), i64, vb, vc})
		require.NoError(t, err)
		targets, _ = r.ShouldDoImplicitTypeCast()
		require.Equal(t, types.T_int64, targets[0].Oid)
		require.True(t, GetFunctionIsAggregateByName("vector_matmul"))
		for _, plain := range []types.T{types.T_array_float32, types.T_array_float16, types.T_array_bf16, types.T_array_int8, types.T_array_uint8} {
			_, err := GetFunctionByName(ctx, "vector_matmul", []types.Type{i64, i64, types.New(plain, 4, 0), vc})
			require.NoError(t, err, plain.String())
		}

		for _, args := range [][]types.Type{
			{i64, i64, vb},
			{types.T_float64.ToType(), i64, vb, vc},
			{i64, i64, vb, i64},
			{i64, i64, vb, vc, types.T_json.ToType()},
			{i64, types.T_float64.ToType(), vb, vc},
			{i64, i64, types.New(types.T_array_float64, 4, 0), vc},
			{i64, i64, vb, vc, vc, vc},
		} {
			_, err := GetFunctionByName(ctx, "vector_matmul", args)
			require.Error(t, err, "%v", args)
		}
	}
}

func TestVecBlockDistanceOverflowIsAnError(t *testing.T) {
	const m = 3e38
	x, y := make([]float32, 32), make([]float32, 32)
	for i := range x {
		x[i], y[i] = m, m
		if i%2 == 1 {
			y[i] = -m
		}
	}
	for _, oid := range vecBlockOids {
		a := vecBlockCellVector(t, oid, 32, [][]float32{x}, nil)
		b := vecBlockCellVector(t, oid, 32, [][]float32{y}, nil)
		for _, d := range []struct {
			name string
			op   executeLogicOfOverload
		}{{"inner_product", InnerProductVecBlock}, {"cosine_distance", CosineDistanceVecBlock}, {"cosine_similarity", CosineSimilarityVecBlock}} {
			_, err := runVecBlockFn(t, d.op, types.T_float64.ToType(), a, b)
			require.Error(t, err, "%s %s", d.name, oid)
		}
	}
}
