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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func buildQuantized(t *testing.T, colType types.T, quantization string) ([]*planpb.IndexDef, []*planpb.TableDef, error) {
	t.Helper()
	cols := vecColMap("id", "vec")
	cols["vec"].Typ.Id = int32(colType)
	idx := indexOn("vec")
	idx.IndexOption = &tree.IndexOption{Quantization: quantization}
	return Hooks{}.BuildSecondaryIndexDefs(gateCtx{proc: testutil.NewProcess(t)}, idx, cols, nil, "id")
}

// Only an int8/uint8 index has a dict table, a key/value table keyed by its key column.
func TestBuildSecondaryIndexDefsDict(t *testing.T) {
	for _, q := range []string{"int8", "UINT8"} {
		indexDefs, tableDefs, err := buildQuantized(t, types.T_array_float32, q)
		require.NoError(t, err, q)
		require.Len(t, indexDefs, 4, q)
		require.Len(t, tableDefs, 4, q)
		dict := tableDefs[2]
		require.Equal(t, catalog.Hnsw_TblType_Dict, dict.TableType)
		require.Equal(t, catalog.Hnsw_TblType_Dict, indexDefs[2].IndexAlgoTableType)
		require.Equal(t, dict.Name, indexDefs[2].IndexTableName)
		require.Len(t, dict.Cols, 2)
		require.Equal(t, catalog.Hnsw_TblCol_Dict_Key, dict.Cols[0].Name)
		require.True(t, dict.Cols[0].Primary)
		require.Equal(t, catalog.Hnsw_TblCol_Dict_Val, dict.Cols[1].Name)
		require.False(t, dict.Cols[1].Primary)
		require.Equal(t, catalog.Hnsw_TblCol_Dict_Key, dict.Pkey.PkeyColName)
	}
	for _, q := range []string{"", "float32", "float16", "bf16"} {
		indexDefs, tableDefs, err := buildQuantized(t, types.T_array_float32, q)
		require.NoError(t, err, q)
		require.Len(t, indexDefs, 3, q)
		require.Len(t, tableDefs, 3, q)
		require.Equal(t, catalog.IndexIgnored_TblType, tableDefs[2].TableType, q)
	}
}

// A narrow column is indexed; a quantization wider than the column element is rejected.
func TestBuildSecondaryIndexDefsQuantizationBase(t *testing.T) {
	for _, typ := range []types.T{types.T_array_float16, types.T_array_bf16, types.T_array_int8, types.T_array_uint8} {
		_, _, err := buildQuantized(t, typ, "")
		require.NoError(t, err, typ.String())
	}
	for _, c := range []struct {
		typ types.T
		q   string
	}{
		{types.T_array_float32, "float64"},
		{types.T_array_float16, "float32"},
		{types.T_array_int8, "float16"},
	} {
		_, _, err := buildQuantized(t, c.typ, c.q)
		require.Error(t, err, "%s on %s", c.q, c.typ.String())
	}
}

func TestQuantizationParam(t *testing.T) {
	q, err := Quantization(`{"op_type":"vector_l2_ops","quantization":"int8"}`)
	require.NoError(t, err)
	require.Equal(t, "int8", q)
	q, err = Quantization("")
	require.NoError(t, err)
	require.Empty(t, q, "an index without QUANTIZATION stores the base type")
	_, err = Quantization("{")
	require.Error(t, err)
}
