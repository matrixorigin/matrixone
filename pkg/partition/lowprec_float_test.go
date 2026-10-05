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

package partition

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// lpVec builds a vector of oid (bf16, float16, float8 or float4) from vals.
func lpVec(t *testing.T, oid types.T, mp *mpool.MPool, vals ...float32) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(oid.ToType())
	for _, v := range vals {
		var err error
		switch oid {
		case types.T_bf16:
			err = vector.AppendFixed(vec, types.BF16FromFloat32(v), false, mp)
		case types.T_float16:
			err = vector.AppendFixed(vec, types.Float16FromFloat32(v), false, mp)
		case types.T_float8:
			err = vector.AppendFixed(vec, types.Float8FromFloat32(v), false, mp)
		case types.T_float4:
			err = vector.AppendFixed(vec, types.Float4FromFloat32(v), false, mp)
		}
		require.NoError(t, err)
	}
	return vec
}

var lpTypes = []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4}

// TestLowPrecisionFloatPartition checks peer groups of bf16, float16, float8 and float4
// keys: equal values are peers, including -0 and +0 stored with different bits.
func TestLowPrecisionFloatPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range lpTypes {
		vec := lpVec(t, oid, mp, 1, 1, 0, 2)
		// a -0 stored by a source that does not canonicalize zero
		switch oid {
		case types.T_bf16:
			require.NoError(t, vector.AppendFixed(vec, types.BF16(0x8000), false, mp))
		case types.T_float16:
			require.NoError(t, vector.AppendFixed(vec, types.Float16(0x8000), false, mp))
		case types.T_float8:
			require.NoError(t, vector.AppendFixed(vec, types.Float8(0x80), false, mp))
		case types.T_float4:
			require.NoError(t, vector.AppendFixed(vec, types.Float4(0x08), false, mp))
		}
		// rows sorted by value: 0 (row 2), -0 (row 4), 1, 1, 2
		sels := []int64{2, 4, 0, 1, 3}
		for _, fn := range []func([]int64, []bool, []int64, *vector.Vector) []int64{Partition, PartitionForOrder} {
			diffs := make([]bool, len(sels))
			got := fn(sels, diffs, nil, vec)
			require.Equal(t, []int64{0, 2, 4}, got, oid.String())
		}
		vec.Free(mp)
	}
}

// TestBlockScaledPartition checks peer groups of vecf8/vecf4 keys: cells with equal decoded
// values and different bytes are peers, as = compares them.
func TestBlockScaledPartition(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		g := ""
		if f == types.BlockScaledNVFP4 {
			g = `"g":1,`
		}
		cell := func(s string) []byte {
			c, err := types.BlockScaledFromJSON(f, s)
			require.NoError(t, err)
			return c
		}
		x := cell(`{` + g + `"b":[{"s":1,"v":[1]}]}`)
		y := cell(`{` + g + `"b":[{"s":2,"v":[0.5]}]}`)
		z := cell(`{` + g + `"b":[{"s":1,"v":[2]}]}`)
		require.NotEqual(t, x, y)

		vec := vector.NewVec(types.New(oid, 1, 0))
		for _, c := range [][]byte{x, y, x, z} {
			require.NoError(t, vector.AppendBytes(vec, c, false, mp))
		}
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		sels := []int64{0, 1, 2, 3, 4, 5}
		for _, part := range []func([]int64, []bool, []int64, *vector.Vector) []int64{Partition, PartitionForOrder} {
			got := part(sels, make([]bool, len(sels)), nil, vec)
			require.Equal(t, []int64{0, 3, 4}, got, oid.String())
		}
		vec.Free(mp)
	}
}
