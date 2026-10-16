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

package vector

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

const blockScaledTestDim = 40

// blockScaledTestVector returns a vector of rows 0..n-1 with row 1 NULL; row i holds
// the vector [i, -i, i, ...] (exact in both formats) and its expected text.
func blockScaledTestVector(t *testing.T, oid types.T, n int, mp *mpool.MPool) (*Vector, [][]byte, []string) {
	t.Helper()
	f, ok := oid.BlockScaledFormat()
	require.True(t, ok)
	vec := NewVec(types.New(oid, blockScaledTestDim, 0))
	cells := make([][]byte, n)
	texts := make([]string, n)
	for i := 0; i < n; i++ {
		if i == 1 {
			require.NoError(t, AppendBytes(vec, nil, true, mp))
			texts[i] = "null"
			continue
		}
		v := make([]float32, blockScaledTestDim)
		for j := range v {
			v[j] = float32(i)
			if j%2 == 1 {
				v[j] = -float32(i)
			}
		}
		cell, err := types.AppendBlockScaled(nil, f, v)
		require.NoError(t, err)
		require.NoError(t, AppendBytes(vec, cell, false, mp))
		cells[i] = cell
		texts[i], err = types.BlockScaledToString(cell)
		require.NoError(t, err)
	}
	return vec, cells, texts
}

func requireBlockScaledRows(t *testing.T, vec *Vector, want []string) {
	t.Helper()
	require.Equal(t, len(want), vec.Length())
	for i, w := range want {
		require.Equal(t, w, vec.RowToString(i), "row %d", i)
	}
}

func TestBlockScaledVectorOps(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec, cells, texts := blockScaledTestVector(t, oid, 4, mp)
			defer vec.Free(mp)

			require.Equal(t, "[2, -2, 2, -2", texts[2][:len("[2, -2, 2, -2")])
			requireBlockScaledRows(t, vec, texts)
			for i, cell := range cells {
				if cell != nil {
					require.Equal(t, cell, vec.GetBytesAt(i))
					require.Equal(t, cell, GetAny(vec, i, true))
				}
			}
			require.Contains(t, vec.String(), texts[3])

			// Marshal round trip validates every live cell.
			data, err := vec.MarshalBinary()
			require.NoError(t, err)
			decoded := NewVecFromReuse()
			require.NoError(t, decoded.UnmarshalBinaryWithCopy(data, mp))
			requireBlockScaledRows(t, decoded, texts)
			decoded.Free(mp)

			dup, err := vec.Dup(mp)
			require.NoError(t, err)
			requireBlockScaledRows(t, dup, texts)

			dup.Shrink([]int64{0, 3}, false)
			requireBlockScaledRows(t, dup, []string{texts[0], texts[3]})
			dup.Free(mp)

			shuffled, err := vec.Dup(mp)
			require.NoError(t, err)
			require.NoError(t, shuffled.Shuffle([]int64{3, 1, 2}, mp))
			requireBlockScaledRows(t, shuffled, []string{texts[3], texts[1], texts[2]})
			shuffled.Free(mp)

			union := NewVec(*vec.GetType())
			require.NoError(t, union.UnionOne(vec, 2, mp))
			require.NoError(t, union.UnionBatch(vec, 0, 2, nil, mp))
			requireBlockScaledRows(t, union, []string{texts[2], texts[0], texts[1]})
			union.Free(mp)

			anyVec := NewVec(*vec.GetType())
			require.NoError(t, AppendAny(anyVec, cells[3], false, mp))
			requireBlockScaledRows(t, anyVec, []string{texts[3]})
			anyVec.Free(mp)

			constVec, err := NewConstBytes(*vec.GetType(), cells[2], 5, mp)
			require.NoError(t, err)
			require.Equal(t, texts[2], constVec.String())
			require.Equal(t, texts[2], constVec.RowToString(4))
			constVec.Free(mp)

			nullConst := NewConstNull(*vec.GetType(), 3, mp)
			require.Equal(t, "null", nullConst.String())
			require.Equal(t, "null", nullConst.RowToString(0))
			nullConst.Free(mp)
		})
	}
}

func TestBlockScaledVectorUnmarshalRejectsMalformedCell(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec, cells, _ := blockScaledTestVector(t, oid, 3, mp)
			data, err := vec.MarshalBinary()
			require.NoError(t, err)
			vec.Free(mp)

			pos := bytes.Index(data, cells[2])
			require.GreaterOrEqual(t, pos, 0)
			corrupted := append([]byte(nil), data...)
			corrupted[pos] = 0x7f // version byte

			target := NewVecFromReuse()
			require.Error(t, target.UnmarshalBinaryWithCopy(corrupted, mp))
			target.Free(mp)

			// A malformed cell in a live vector renders its error instead of panicking.
			bad := append([]byte(nil), cells[2]...)
			bad[0] = 0x7f
			require.Contains(t, blockScaledCellString(bad), "version")
		})
	}
}

// Flush computes zonemap bounds for every column; vecf8/vecf4 must not panic.
func TestBlockScaledVectorGetMinMaxValue(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		mp := mpool.MustNewZero()
		vec, _, _ := blockScaledTestVector(t, oid, 4, mp)
		var ok bool
		var minv, maxv []byte
		require.NotPanics(t, func() { ok, minv, maxv = vec.GetMinMaxValue() })
		require.True(t, ok)
		require.NotEmpty(t, minv)
		require.NotEmpty(t, maxv)
		vec.Free(mp)
	}
}
