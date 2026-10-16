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

package cdc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func vecBlockTestVector(t *testing.T, oid types.T, values []float32, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	f, _ := oid.BlockScaledFormat()
	cell, err := types.AppendBlockScaled(nil, f, values)
	require.NoError(t, err)
	vec := vector.NewVec(types.New(oid, int32(len(values)), 0))
	require.NoError(t, vector.AppendBytes(vec, cell, false, mp))
	require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
	return vec
}

// TestCDCVecBlock checks that the replicated SQL carries a vecf8/vecf4 cell exactly: the
// statement's text parses back to the same cell bytes, including vectors whose decoded
// values would quantize to other values (a vecf4 global scale follows the decoded maximum).
func TestCDCVecBlock(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	review := make([]float32, 17)
	review[0], review[16] = 8.7649145, 5.7432985
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		for _, values := range [][]float32{{1, -3, 0, 6}, review, {1e-30, 3e20, -2, 0.5}} {
			vec := vecBlockTestVector(t, oid, values, mp)
			row := make([]any, 1)
			require.NoError(t, extractRowFromVector(ctx, vec, 0, row, 0))
			sql, err := convertColIntoSql(ctx, row[0], vec.GetType(), nil)
			require.NoError(t, err)
			text, err := types.BlockScaledToJSON(vec.GetBytesAt(0))
			require.NoError(t, err)
			require.Equal(t, "'"+text+"'", string(sql))
			replayed, err := types.StringToBlockScaled(f, text)
			require.NoError(t, err)
			require.Equal(t, vec.GetBytesAt(0), replayed, "%s %v", oid, values)
			vec.Free(mp)
		}
		typ := types.New(oid, 4, 0)
		_, err := convertColIntoSql(ctx, []byte{0x7f}, &typ, nil)
		require.Error(t, err)
	}
}
