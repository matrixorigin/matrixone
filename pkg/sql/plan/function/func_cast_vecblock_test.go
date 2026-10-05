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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func runVecBlockCast(t *testing.T, proc *process.Process, src *vector.Vector, to types.Type) (*vector.Vector, error) {
	t.Helper()
	result := vector.NewFunctionResultWrapper(to, proc.Mp())
	require.NoError(t, result.PreExtendAndReset(src.Length()))
	err := NewCast([]*vector.Vector{src, vector.NewVec(to)}, result, proc, src.Length(), nil)
	return result.GetResultVector(), err
}

func vecBlockStrVector(t *testing.T, proc *process.Process, values []string, nulls []bool) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.T_varchar.ToType())
	for i, s := range values {
		require.NoError(t, vector.AppendBytes(v, []byte(s), nulls != nil && nulls[i], proc.Mp()))
	}
	return v
}

func vecBlockF32Vector(t *testing.T, proc *process.Process, dim int32, values [][]float32) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.New(types.T_array_float32, dim, 0))
	for _, a := range values {
		require.NoError(t, vector.AppendArray(v, a, false, proc.Mp()))
	}
	return v
}

func TestCastToVecBlock(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		to := types.New(oid, 4, 0)
		t.Run(oid.String(), func(t *testing.T) {
			// text -> vecf8/vecf4, with NULL and empty string -> NULL
			out, err := runVecBlockCast(t, proc,
				vecBlockStrVector(t, proc, []string{"[1, -3, 0, 6]", "", "x"}, []bool{false, false, true}), to)
			require.NoError(t, err)
			require.Equal(t, "[1, -3, 0, 6]", out.RowToString(0))
			require.Equal(t, "null", out.RowToString(1))
			require.Equal(t, "null", out.RowToString(2))

			// vecf32 -> vecf8/vecf4
			out, err = runVecBlockCast(t, proc, vecBlockF32Vector(t, proc, 4, [][]float32{{1, -3, 0, 6}}), to)
			require.NoError(t, err)
			require.Equal(t, "[1, -3, 0, 6]", out.RowToString(0))
			cell := append([]byte(nil), out.GetBytesAt(0)...)

			// same format keeps the cell bytes; other format re-quantizes
			for _, target := range []types.T{types.T_array_float8, types.T_array_float4} {
				out2, err := runVecBlockCast(t, proc, out, types.New(target, 4, 0))
				require.NoError(t, err)
				require.Equal(t, "[1, -3, 0, 6]", out2.RowToString(0))
				if target == oid {
					require.Equal(t, cell, out2.GetBytesAt(0))
				}
			}

			// vecf8/vecf4 -> vecf32
			back, err := runVecBlockCast(t, proc, out, types.New(types.T_array_float32, 4, 0))
			require.NoError(t, err)
			require.Equal(t, []float32{1, -3, 0, 6}, vector.GetArrayAt[float32](back, 0))

			// unsized target accepts any dimension
			out, err = runVecBlockCast(t, proc, vecBlockStrVector(t, proc, []string{"[1,2,3]"}, nil), oid.ToType())
			require.NoError(t, err)
			c, err := types.ParseBlockScaledCell(out.GetBytesAt(0))
			require.NoError(t, err)
			require.Equal(t, f, c.Format)
			require.Equal(t, 3, c.Dim)

			// rejections: dimension mismatch from every source, malformed text, non-finite
			for name, src := range map[string]*vector.Vector{
				"text dim":      vecBlockStrVector(t, proc, []string{"[1,2,3]"}, nil),
				"vecf32 dim":    vecBlockF32Vector(t, proc, 3, [][]float32{{1, 2, 3}}),
				"malformed":     vecBlockStrVector(t, proc, []string{"[1,2"}, nil),
				"non-finite":    vecBlockStrVector(t, proc, []string{"[1,2,3,nan]"}, nil),
				"vecblock dim":  out,
				"infinite text": vecBlockStrVector(t, proc, []string{"[1,2,3,inf]"}, nil),
			} {
				_, err := runVecBlockCast(t, proc, src, to)
				require.Error(t, err, name)
			}
			_, err = runVecBlockCast(t, proc, out, types.New(types.T_array_float32, 4, 0))
			require.Error(t, err, "vecblock -> vecf32 dim")

			// unsupported target
			_, err = runVecBlockCast(t, proc, out, types.New(types.T_array_float64, 3, 0))
			require.Error(t, err)
		})
	}
}

// TestCastBlobToVecBlock checks the binary vector input: a BLOB of little-endian float32
// elements quantizes as the same vecf32 value does.
func TestCastBlobToVecBlock(t *testing.T) {
	proc := testutil.NewProcess(t)
	blob := func(values ...[]byte) *vector.Vector {
		v := vector.NewVec(types.T_blob.ToType())
		for _, b := range values {
			require.NoError(t, vector.AppendBytes(v, b, false, proc.Mp()))
		}
		return v
	}
	values := []float32{1, -3, 0.1, 6}
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		to := types.New(oid, 4, 0)
		out, err := runVecBlockCast(t, proc, blob(types.ArrayToBytes(values), nil), to)
		require.NoError(t, err, oid.String())
		want, err := runVecBlockCast(t, proc, vecBlockF32Vector(t, proc, 4, [][]float32{values}), to)
		require.NoError(t, err)
		require.Equal(t, want.GetBytesAt(0), out.GetBytesAt(0), oid.String())
		require.True(t, out.IsNull(1), "an empty BLOB is NULL")

		for name, src := range map[string]*vector.Vector{
			"misaligned": blob([]byte{0, 0, 128}),
			"dimension":  blob(types.ArrayToBytes([]float32{1, 2, 3})),
			"non-finite": blob(types.ArrayToBytes([]float32{1, 2, 3, float32(math.Inf(1))})),
		} {
			_, err := runVecBlockCast(t, proc, src, to)
			require.Error(t, err, "%s %s", oid, name)
		}
	}
}

func TestCastVecBlockRegistered(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		for _, src := range []types.T{types.T_any, types.T_char, types.T_varchar, types.T_text, types.T_blob,
			types.T_array_float32, types.T_array_float8, types.T_array_float4} {
			require.Contains(t, supportedTypeCast[src], oid, "%s -> %s", src, oid)
		}
		require.Contains(t, supportedTypeCast[oid], types.T_array_float32)
		require.NotContains(t, supportedTypeCast[oid], types.T_varchar)
		require.NotContains(t, supportedTypeCast[oid], types.T_array_float64)
	}
}

func TestCastNullToVecBlock(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		src := vector.NewConstNull(types.T_any.ToType(), 2, proc.Mp())
		out, err := runVecBlockCast(t, proc, src, types.New(oid, 4, 0))
		require.NoError(t, err)
		require.Equal(t, "null", out.RowToString(0))
		require.Equal(t, "null", out.RowToString(1))
	}
}
