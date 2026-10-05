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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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

// TestDataBranchFormatVecBlock checks that the data branch SQL text carries a vecf8/vecf4
// cell exactly: the extracted row keeps the cell and its text parses back to the same bytes.
func TestDataBranchFormatVecBlock(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		vec := vecBlockTestVector(t, oid, mp)
		row := make([]any, 1)
		require.NoError(t, extractDataBranchSQLRowValue(ctx, nil, vec, 0, row, 0))
		require.Equal(t, vec.GetBytesAt(0), row[0])
		var buf bytes.Buffer
		require.NoError(t, formatValIntoString(nil, row[0], *vec.GetType(), &buf))
		text := buf.String()
		require.True(t, len(text) > 2 && text[0] == '\'' && text[len(text)-1] == '\'')
		f, _ := oid.BlockScaledFormat()
		cell, err := types.StringToBlockScaled(f, text[1:len(text)-1])
		require.NoError(t, err)
		require.Equal(t, vec.GetBytesAt(0), cell)
		// a decoded row (the display path) renders its values
		require.NoError(t, extractRowFromVector(ctx, nil, vec, 0, row, 0, false))
		buf.Reset()
		require.NoError(t, formatValIntoString(nil, row[0], *vec.GetType(), &buf))
		require.Equal(t, "'[1, -3, 0, 6]'", buf.String())
		buf.Reset()
		require.Error(t, formatValIntoString(nil, "x", *vec.GetType(), &buf))
		vec.Free(mp)
	}
}

// TestQueryResultDumpVecBlockExactText checks that a dumped query result row carries
// vecf8/vecf4 cells as their exact text, which parses back to the same bytes.
func TestQueryResultDumpVecBlockExactText(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		ids := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(ids, []int64{1, 2}, nil, mp))
		vec := vecBlockTestVector(t, oid, mp)
		bat := batch.NewWithSize(2)
		bat.Vecs[0], bat.Vecs[1] = ids, vec
		bat.SetRowCount(2)

		row := make([]any, 2)
		for j := 0; j < 2; j++ {
			require.NoError(t, extractRowFromEveryVector(ctx, nil, bat, j, row, false))
			require.NoError(t, setVecBlockExactText(bat, j, row))
			if j == 1 {
				require.Nil(t, row[1])
				continue
			}
			text, ok := row[1].([]byte)
			require.True(t, ok)
			f, _ := oid.BlockScaledFormat()
			cell, err := types.StringToBlockScaled(f, string(text))
			require.NoError(t, err)
			require.Equal(t, vec.GetBytesAt(0), cell)
		}
		bat.Clean(mp)
	}
}
