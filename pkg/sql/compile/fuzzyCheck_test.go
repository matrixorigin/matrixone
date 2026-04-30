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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestNewFuzzyCheckAttrUsesParentUniqueColName verifies that when a hidden
// unique-index table dispatches fuzzy check via Fuzzymessage.ParentUniqueCols,
// the duplicate-entry error reports the original user-visible column name
// (e.g. "a") in displayAttr while attr stays as the hidden index column so the
// background dedup SQL keeps hitting a real column of the hidden table.
func TestNewFuzzyCheckAttrUsesParentUniqueColName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_a_index",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName, // "__mo_index_idx_col"
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{}},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "decimal15",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{}},
			},
		},
	}

	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()

	require.Equal(t, catalog.IndexTableIndexColName, f.attr,
		"attr should stay as the hidden index column name so backgroundSQLCheck queries a real column")
	require.Equal(t, "a", f.displayAttr,
		"displayAttr should be the user-visible column name used in duplicate-entry errors")
	require.NotNil(t, f.col)
	require.Equal(t, "a", f.col.Name)
}

// TestNewFuzzyCheckAttrFallsBackToPkeyColName verifies the non-hidden-index
// insertion path still uses the primary-key column name for both the SQL and
// the error message.
func TestNewFuzzyCheckAttrFallsBackToPkeyColName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{}},
			},
		},
	}

	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()

	require.Equal(t, "id", f.attr)
	require.Equal(t, "id", f.displayAttr)
}

// TestNewFuzzyCheckCompositeUniqueIndexDisplayAttr verifies that when the
// hidden unique-index plan node carries a multi-column ParentUniqueCols,
// displayAttr is set to a "(c1,c2)" tuple so the duplicate-entry error
// reports a user-recognizable key name instead of __mo_index_idx_col.
func TestNewFuzzyCheckCompositeUniqueIndexDisplayAttr(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_composite",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{}},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "t1",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "col1", Typ: plan.Type{}},
				{Name: "col2", Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.isCompound)
	require.Equal(t, "(col1,col2)", f.displayAttr,
		"composite hidden unique must surface a user-visible tuple key, not the internal hidden column")
}

// TestNewFuzzyCheckCompoundKeyPath covers the compound primary key path —
// when the hidden PkeyColName is CPrimaryKeyColName, the helper must flip
// isCompound and populate compoundCols from Pkey.Names.
func TestNewFuzzyCheckCompoundKeyPath(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.CPrimaryKeyColName,
				Names:       []string{"a", "b"},
			},
			Cols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{}},
				{Name: "b", Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.isCompound)
	require.Len(t, f.compoundCols, 2)
	require.Equal(t, "a", f.compoundCols[0].Name)
	require.Equal(t, "b", f.compoundCols[1].Name)
}

// TestNewFuzzyCheckHiddenOnlyPath covers the "ALTER TABLE add unique index on
// existing table" shortcut: hidden index table with no Fuzzymessage ->
// onlyInsertHidden should be true.
func TestNewFuzzyCheckHiddenOnlyPath(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: catalog.PrefixIndexTableName + "unique_hash",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.onlyInsertHidden)
	// Hidden-only fallback must not leak the internal PK column name —
	// firstlyCheck / backgroundSQLCheck use displayAttr directly in
	// NewDuplicateEntry without going through RewriteHiddenIndexDupEntry.
	require.NotEqual(t, catalog.IndexTableIndexColName, f.displayAttr,
		"hidden-only fallback must not surface the internal index column name")
	require.NotContains(t, f.displayAttr, "__mo_",
		"hidden-only fallback must not surface any __mo_ internal name")
}

// Composite CREATE UNIQUE INDEX on an existing table hits the same
// onlyInsertHidden fallback, but the hidden table's PK is __mo_cpkey_col.
// That name must also not leak into the user-visible error.
func TestNewFuzzyCheckHiddenOnlyCompositeDoesNotLeakInternalName(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: catalog.PrefixIndexTableName + "unique_composite",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.CPrimaryKeyColName,
				Names:       []string{"a", "b"},
			},
			Cols: []*plan.ColDef{
				{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{}},
				{Name: "a", Typ: plan.Type{}},
				{Name: "b", Typ: plan.Type{}},
			},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	defer f.release()
	require.True(t, f.onlyInsertHidden)
	require.NotEqual(t, catalog.CPrimaryKeyColName, f.displayAttr)
	require.NotContains(t, f.displayAttr, "__mo_")
}

// TestNewFuzzyCheckRejectsEmptyNames covers the error branch when tblName or
// dbName is missing.
func TestNewFuzzyCheckRejectsEmptyNames(t *testing.T) {
	n := &plan.Node{
		ObjRef:   &plan.ObjectRef{SchemaName: ""},
		TableDef: &plan.TableDef{Name: ""},
	}
	_, err := newFuzzyCheck(n)
	require.Error(t, err)
}

// TestFuzzyCheckResetAndClear exercises the lifecycle helpers so the reuse
// pool path is also covered.
func TestFuzzyCheckResetAndClear(t *testing.T) {
	n := &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{}}},
		},
	}
	f, err := newFuzzyCheck(n)
	require.NoError(t, err)
	f.condition = "x=1"
	f.cnt = 3
	f.reset()
	require.Equal(t, "", f.condition)
	require.Equal(t, 0, f.cnt)
	require.Equal(t, "id", f.attr)

	f.clear()
	require.Equal(t, "", f.attr)
	require.Equal(t, "", f.displayAttr)
	require.Equal(t, "", f.db)
	require.Equal(t, "", f.tbl)
	require.False(t, f.isCompound)
	require.False(t, f.onlyInsertHidden)
	require.Nil(t, f.col)
	require.Nil(t, f.compoundCols)

	// TypeName returns a stable identifier used by the reuse pool.
	require.Equal(t, "compile.fuzzyCheck", f.TypeName())
}

// TestConstructFuzzyFilterUsesParentUniqueColName verifies the fuzzy filter
// operator carries the user-visible column name for unique-index hidden tables
// so runtime duplicate errors report "for key 'a'" not "for key
// '__mo_index_idx_col'".
func TestConstructFuzzyFilterUsesParentUniqueColName(t *testing.T) {
	idxColType := plan.Type{Id: 27}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_a_index",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: idxColType},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "decimal15",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "a", Typ: idxColType},
			},
		},
	}
	tableScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}
	sinkScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}

	op := constructFuzzyFilter(n, tableScan, sinkScan)
	require.NotNil(t, op)
	require.Equal(t, "a", op.PkName)
}

// TestConstructFuzzyFilterFallsBackToPkeyColName verifies non-hidden-index
// targets keep using the primary-key column name.
func TestConstructFuzzyFilterFallsBackToPkeyColName(t *testing.T) {
	pkType := plan.Type{Id: 23}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "t1",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: pkType},
			},
		},
	}
	tableScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}
	sinkScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}

	op := constructFuzzyFilter(n, tableScan, sinkScan)
	require.NotNil(t, op)
	require.Equal(t, "id", op.PkName)
}

// TestConstructFuzzyFilterCompositeParentUniqueCols verifies the composite
// unique-index path surfaces a "(col1,col2)" tuple as PkName so runtime
// duplicate errors do not leak __mo_index_idx_col.
func TestConstructFuzzyFilterCompositeParentUniqueCols(t *testing.T) {
	idxColType := plan.Type{Id: 27}
	n := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "__mo_index_unique_composite",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.IndexTableIndexColName,
			},
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: idxColType},
			},
		},
		Fuzzymessage: &plan.OriginTableMessageForFuzzy{
			ParentTableName: "t1",
			ParentUniqueCols: []*plan.ColDef{
				{Name: "col1", Typ: idxColType},
				{Name: "col2", Typ: idxColType},
			},
		},
	}
	tableScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}
	sinkScan := &plan.Node{Stats: &plan.Stats{Cost: 100}}

	op := constructFuzzyFilter(n, tableScan, sinkScan)
	require.NotNil(t, op)
	require.Equal(t, "(col1,col2)", op.PkName)
}
