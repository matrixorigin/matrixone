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

// TestCastVecBlockExactText checks that the exact text (vecblock_json) casts back to the
// same cell for both formats, enforces the declared dimension, and that a malformed text
// is an error rather than a rounded value.
func TestCastVecBlockExactText(t *testing.T) {
	proc := testutil.NewProcess(t)
	review := make([]float32, 17)
	review[0], review[16] = 8.7649145, 5.7432985
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		cell, err := types.AppendBlockScaled(nil, f, review)
		require.NoError(t, err)
		src := vector.NewVec(types.New(oid, 17, 0))
		require.NoError(t, vector.AppendBytes(src, cell, false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(src, nil, true, proc.Mp()))

		// vecblock_json
		result := vector.NewFunctionResultWrapper(types.T_text.ToType(), proc.Mp())
		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, VecBlockJSON([]*vector.Vector{src}, result, proc, 2, nil))
		text := result.GetResultVector().GetStringAt(0)
		require.True(t, result.GetResultVector().IsNull(1))

		for _, to := range []types.Type{types.New(oid, 17, 0), oid.ToType()} {
			out, err := runVecBlockCast(t, proc, vecBlockStrVector(t, proc, []string{text}, nil), to)
			require.NoError(t, err, to.String())
			require.Equal(t, cell, out.GetBytesAt(0), to.String())
		}
		_, err = runVecBlockCast(t, proc, vecBlockStrVector(t, proc, []string{text}, nil), types.New(oid, 16, 0))
		require.Error(t, err, "dimension")
		_, err = runVecBlockCast(t, proc, vecBlockStrVector(t, proc, []string{`{"b":[{"s":3,"v":[1]}]}`}, nil), oid.ToType())
		require.Error(t, err, "malformed")
	}
}

// TestVecBlockBinary checks the binary exact form: vecblock_binary returns the stored cell,
// a BLOB of the cell casts back to the same bytes with or without a declared dimension, and
// a BLOB of cell length that is not a valid cell of the target is an error.
func TestVecBlockBinary(t *testing.T) {
	proc := testutil.NewProcess(t)
	blob := func(values ...[]byte) *vector.Vector {
		v := vector.NewVec(types.T_blob.ToType())
		for _, b := range values {
			require.NoError(t, vector.AppendBytes(v, b, false, proc.Mp()))
		}
		return v
	}
	values := []float32{0.44547153, 1.7, -3.1, 0.02, 5.5}
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		cells := vecBlockCellVector(t, oid, 5, [][]float32{values}, []bool{false})
		require.NoError(t, vector.AppendBytes(cells, nil, true, proc.Mp()))
		cell := cells.GetBytesAt(0)

		bin := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
		require.NoError(t, bin.PreExtendAndReset(2))
		require.NoError(t, VecBlockBinary([]*vector.Vector{cells}, bin, proc, 2, nil))
		require.Equal(t, cell, bin.GetResultVector().GetBytesAt(0), oid.String())
		require.True(t, bin.GetResultVector().IsNull(1))

		for _, to := range []types.Type{types.New(oid, 5, 0), types.New(oid, types.MaxArrayDimension, 0)} {
			out, err := runVecBlockCast(t, proc, blob(cell), to)
			require.NoError(t, err, "%s width %d", oid, to.Width)
			require.Equal(t, cell, out.GetBytesAt(0), "%s width %d", oid, to.Width)
		}
		// float32 elements of the same dimension still quantize
		out, err := runVecBlockCast(t, proc, blob(types.ArrayToBytes(values)), types.New(oid, 5, 0))
		require.NoError(t, err)
		require.Equal(t, cell, out.GetBytesAt(0))
		require.NotEqual(t, len(cell), 4*len(values))

		other := types.T_array_float4
		if oid == types.T_array_float4 {
			other = types.T_array_float8
		}
		otherCell := vecBlockCellVector(t, other, 5, [][]float32{values}, []bool{false}).GetBytesAt(0)
		bad := func(mutate func(b []byte)) []byte {
			b := append([]byte(nil), cell...)
			mutate(b)
			return b
		}
		for name, c := range map[string]struct {
			src []byte
			to  types.Type
		}{
			"version":  {bad(func(b []byte) { b[0] = 2 }), types.New(oid, 5, 0)},
			"format":   {bad(func(b []byte) { b[1] = 3 - b[1] }), types.New(oid, 5, 0)},
			"reserved": {bad(func(b []byte) { b[2] = 1 }), types.New(oid, 5, 0)},
			"dim":      {bad(func(b []byte) { b[4] = 4 }), types.New(oid, 5, 0)},
			"global":   {bad(func(b []byte) { b[11] = 0xff }), types.New(oid, 5, 0)},
			"declared": {cell, types.New(oid, 6, 0)},
		} {
			_, err := runVecBlockCast(t, proc, blob(c.src), c.to)
			require.Error(t, err, "%s %s", oid, name)
		}
		if types.BlockScaledCellSize(f, 5) == len(otherCell) {
			_, err := runVecBlockCast(t, proc, blob(otherCell), types.New(oid, 5, 0))
			require.Error(t, err, "%s from the other format", oid)
		}
	}
	// a float32 BLOB never has the cell size of its dimension
	for d := 1; d <= types.MaxArrayDimension; d++ {
		require.NotEqual(t, 4*d, types.BlockScaledCellSize(types.BlockScaledMXFP8, d), d)
		require.NotEqual(t, 4*d, types.BlockScaledCellSize(types.BlockScaledNVFP4, d), d)
	}
}
