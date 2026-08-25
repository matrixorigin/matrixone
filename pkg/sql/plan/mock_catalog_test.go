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
	"github.com/stretchr/testify/require"
)

func TestMockCompilerContextTableIDs(t *testing.T) {
	first := NewMockCompilerContext(true)
	second := NewMockCompilerContext(true)
	require.Equal(t, len(first.tablesByQualifiedName), len(second.tablesByQualifiedName))
	require.Equal(t, len(first.tablesByQualifiedName), len(first.id2name),
		"every schema-qualified table must have exactly one reverse ID mapping")

	seen := make(map[uint64]string, len(first.tablesByQualifiedName))
	for qualifiedName, tableDef := range first.tablesByQualifiedName {
		require.Greater(t, tableDef.TblId, uint64(catalog.MO_RESERVED_MAX),
			"ordinary mock table %s must not use a reserved catalog ID", qualifiedName)
		if previous, exists := seen[tableDef.TblId]; exists {
			require.Failf(t, "duplicate mock table ID", "tables %s and %s use ID %d",
				previous, qualifiedName, tableDef.TblId)
		}
		seen[tableDef.TblId] = qualifiedName

		require.Equal(t, tableDef.TblId, second.tablesByQualifiedName[qualifiedName].TblId,
			"table IDs must be deterministic")
		require.Equal(t, qualifiedName, first.id2name[tableDef.TblId])
		objRef, resolved, err := first.ResolveById(tableDef.TblId, nil)
		require.NoError(t, err)
		require.NotNil(t, objRef)
		require.NotNil(t, resolved)
		require.Equal(t, tableDef.TblId, uint64(objRef.Obj))
		require.Equal(t, tableDef.TblId, resolved.TblId)
		require.Equal(t, tableDef.Name, resolved.Name)
	}
}

func TestMockCompilerContextResolvesSchemaQualifiedTables(t *testing.T) {
	mock := NewMockCompilerContext(true)

	bvtObj, bvtT1, err := mock.Resolve("bvt_test1", "t1", nil)
	require.NoError(t, err)
	require.NotNil(t, bvtObj)
	require.NotNil(t, bvtT1)
	require.Equal(t, "bvt_test1", bvtObj.SchemaName)
	require.Equal(t, []string{"a", "b", "c", catalog.Row_ID}, tableColumnNames(bvtT1))

	cteObj, cteT1, err := mock.Resolve("cte_test", "t1", nil)
	require.NoError(t, err)
	require.NotNil(t, cteObj)
	require.NotNil(t, cteT1)
	require.Equal(t, "cte_test", cteObj.SchemaName)
	require.Equal(t, []string{"a", "b", catalog.Row_ID}, tableColumnNames(cteT1))
	require.NotEqual(t, bvtT1.TblId, cteT1.TblId)

	for _, expected := range []*TableDef{bvtT1, cteT1} {
		objRef, resolved, resolveErr := mock.ResolveById(expected.TblId, nil)
		require.NoError(t, resolveErr)
		require.NotNil(t, objRef)
		require.Equal(t, expected.TblId, resolved.TblId)
		require.Equal(t, tableColumnNames(expected), tableColumnNames(resolved))
	}

	require.NotNil(t, mock.tables["t1"], "unqualified compatibility view must remain available")
}

func tableColumnNames(tableDef *TableDef) []string {
	names := make([]string, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		names[i] = col.Name
	}
	return names
}
