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

package frontend

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func vecBlockTestVector(t *testing.T, oid types.T, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	f, _ := oid.BlockScaledFormat()
	cell, err := types.AppendBlockScaled(nil, f, []float32{1, -3, 0, 6})
	require.NoError(t, err)
	vec := vector.NewVec(types.New(oid, 4, 0))
	require.NoError(t, vector.AppendBytes(vec, cell, false, mp))
	require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
	return vec
}

func TestFrontendVecBlockOutput(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		vec := vecBlockTestVector(t, oid, mp)
		row := make([]any, 1)
		require.NoError(t, extractRowFromVector(ctx, nil, vec, 0, row, 0, false))
		require.Equal(t, []float32{1, -3, 0, 6}, row[0])
		require.NoError(t, extractRowFromVector(ctx, nil, vec, 0, row, 1, false))
		require.Nil(t, row[0])

		col := new(MysqlColumn)
		require.NoError(t, convertEngineTypeToMysqlType(ctx, oid, col))
		require.Equal(t, defines.MYSQL_TYPE_VARCHAR, col.ColumnType())
	}
}

// TestDataBranchFormatVecBlock checks that the data branch SQL text renders vecf8/vecf4
// values as extracted from a row (dequantized) and as raw cells.
func TestDataBranchFormatVecBlock(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		vec := vecBlockTestVector(t, oid, mp)
		row := make([]any, 1)
		require.NoError(t, extractRowFromVector(ctx, nil, vec, 0, row, 0, false))
		for _, val := range []any{row[0], vec.GetBytesAt(0)} {
			var buf bytes.Buffer
			require.NoError(t, formatValIntoString(nil, val, *vec.GetType(), &buf))
			require.Equal(t, "'[1, -3, 0, 6]'", buf.String())
		}
		var buf bytes.Buffer
		require.Error(t, formatValIntoString(nil, "x", *vec.GetType(), &buf))
		vec.Free(mp)
	}
}
