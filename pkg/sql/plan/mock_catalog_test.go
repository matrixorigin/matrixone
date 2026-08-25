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

func TestMockCompilerContextLegacyTableOverlay(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		mock := NewMockCompilerContext(true)
		const tableID = uint64(990001)
		mock.tables["runtime_table"] = &TableDef{Name: "runtime_table", TblId: tableID}
		mock.objects["runtime_table"] = &ObjectRef{
			Obj: int64(tableID), SchemaName: "tpch", ObjName: "runtime_table",
		}
		mock.id2name[tableID] = "runtime_table"

		objRef, tableDef, err := mock.Resolve("tpch", "runtime_table", nil)
		require.NoError(t, err)
		require.Equal(t, tableID, tableDef.TblId)
		require.Equal(t, tableID, uint64(objRef.Obj))
		objRef, tableDef, err = mock.ResolveById(tableID, nil)
		require.NoError(t, err)
		require.Equal(t, tableID, tableDef.TblId)
		require.Equal(t, tableID, uint64(objRef.Obj))
	})

	t.Run("replace", func(t *testing.T) {
		mock := NewMockCompilerContext(true)
		original := mock.tables["nation"]
		replacement := DeepCopyTableDef(original, true)
		replacement.Cols[0].Typ.Width = 1234
		mock.tables["nation"] = replacement

		_, tableDef, err := mock.Resolve("tpch", "nation", nil)
		require.NoError(t, err)
		require.Equal(t, int32(1234), tableDef.Cols[0].Typ.Width)
		_, tableDef, err = mock.ResolveById(original.TblId, nil)
		require.NoError(t, err)
		require.Equal(t, int32(1234), tableDef.Cols[0].Typ.Width)
	})

	t.Run("delete", func(t *testing.T) {
		mock := NewMockCompilerContext(true)
		tableID := mock.tables["nation"].TblId
		delete(mock.tables, "nation")
		delete(mock.objects, "nation")

		objRef, tableDef, err := mock.Resolve("tpch", "nation", nil)
		require.NoError(t, err)
		require.Nil(t, objRef)
		require.Nil(t, tableDef)
		objRef, tableDef, err = mock.ResolveById(tableID, nil)
		require.NoError(t, err)
		require.Nil(t, objRef)
		require.Nil(t, tableDef)
	})
}

func TestMockCompilerContextAmbiguousLegacyOverlayUsesOwner(t *testing.T) {
	testCases := []struct {
		name          string
		replaceTable  bool
		replaceObject bool
		deleteTable   bool
		deleteObject  bool
	}{
		{name: "table replacement", replaceTable: true},
		{name: "object replacement", replaceObject: true},
		{name: "both replacement", replaceTable: true, replaceObject: true},
		{name: "table deletion", deleteTable: true},
		{name: "object deletion", deleteObject: true},
		{name: "both deletion", deleteTable: true, deleteObject: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockCompilerContext(true)
			ownerKey := mock.legacyTableOwners["t1"]
			require.Equal(t, mockQualifiedTableName("cte_test", "t1"), ownerKey)
			ownerID := mock.tablesByQualifiedName[ownerKey].TblId

			if tc.replaceTable {
				replacement := DeepCopyTableDef(mock.tables["t1"], true)
				replacement.Version = 777
				mock.tables["t1"] = replacement
			}
			if tc.replaceObject {
				replacement := *mock.objects["t1"]
				replacement.ServerName = "replacement"
				mock.objects["t1"] = &replacement
			}
			if tc.deleteTable {
				delete(mock.tables, "t1")
			}
			if tc.deleteObject {
				delete(mock.objects, "t1")
			}

			assertOwner := func(objRef *ObjectRef, tableDef *TableDef) {
				if tc.deleteTable {
					require.Nil(t, tableDef)
				} else {
					require.NotNil(t, tableDef)
					if tc.replaceTable {
						require.Equal(t, uint32(777), tableDef.Version)
					}
				}
				if tc.deleteObject {
					require.Nil(t, objRef)
				} else {
					require.NotNil(t, objRef)
					if tc.replaceObject {
						require.Equal(t, "replacement", objRef.ServerName)
					}
				}
			}
			objRef, tableDef, err := mock.Resolve("cte_test", "t1", nil)
			require.NoError(t, err)
			assertOwner(objRef, tableDef)
			objRef, tableDef, err = mock.ResolveById(ownerID, nil)
			require.NoError(t, err)
			assertOwner(objRef, tableDef)

			for _, dbName := range []string{"bvt_test1", "constraint_test"} {
				objRef, tableDef, err = mock.Resolve(dbName, "t1", nil)
				require.NoError(t, err)
				require.NotNil(t, objRef)
				require.NotNil(t, tableDef)
				require.NotEqual(t, ownerID, tableDef.TblId)
				objRef, tableDef, err = mock.ResolveById(tableDef.TblId, nil)
				require.NoError(t, err)
				require.NotNil(t, objRef)
				require.NotNil(t, tableDef)
			}
		})
	}
}

func tableColumnNames(tableDef *TableDef) []string {
	names := make([]string, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		names[i] = col.Name
	}
	return names
}
