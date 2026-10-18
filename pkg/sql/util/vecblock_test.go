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

package util

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestArrayUserVariableValueToBytesVecBlock(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		typ := types.New(oid, 3, 0)

		cell, err := arrayUserVariableValueToBytes(typ, "[1, -3, 6]")
		require.NoError(t, err)
		s, err := types.BlockScaledToString(cell)
		require.NoError(t, err)
		require.Equal(t, "[1, -3, 6]", s)

		fromFloats, err := arrayUserVariableValueToBytes(typ, []float32{1, -3, 6})
		require.NoError(t, err)
		require.Equal(t, cell, fromFloats)

		fromCell, err := arrayUserVariableValueToBytes(typ, cell)
		require.NoError(t, err)
		require.Equal(t, cell, fromCell)

		// unsized target accepts any dimension
		_, err = arrayUserVariableValueToBytes(oid.ToType(), "[1, 2, 3, 4, 5]")
		require.NoError(t, err)

		other := types.BlockScaledMXFP8
		if f == types.BlockScaledMXFP8 {
			other = types.BlockScaledNVFP4
		}
		otherCell, err := types.AppendBlockScaled(nil, other, []float32{1, 2, 3})
		require.NoError(t, err)
		for name, val := range map[string]any{
			"dimension":    "[1, 2]",
			"malformed":    "[1, 2",
			"non-finite":   "[1, 2, inf]",
			"bad cell":     []byte{0x7f, 0, 0},
			"wrong format": otherCell,
			"wrong type":   42,
		} {
			_, err := arrayUserVariableValueToBytes(typ, val)
			require.Error(t, err, "%s %s", oid, name)
		}
	}
}
