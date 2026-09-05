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

// Unit coverage for alter.go and schema.go. Mirrors ivfpq/plugin/plan/plan_test.go.
package plan

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// init wires the planplugin helpers BuildSecondaryIndexDefs calls. Production wires
// them in pkg/sql/plan's init, which cannot be imported here; these are shape-only.
func init() {
	if planplugin.CreateIndexDef == nil {
		planplugin.CreateIndexDef = func(_ planplugin.CompilerContext, _ *tree.Index, indexTableName, indexAlgoTableType string, indexParts []string, _ bool) (*planpb.IndexDef, error) {
			return &planpb.IndexDef{
				IndexTableName:     indexTableName,
				IndexAlgoTableType: indexAlgoTableType,
				Parts:              indexParts,
			}, nil
		}
	}
	if planplugin.MakeHiddenColDefByName == nil {
		planplugin.MakeHiddenColDefByName = func(name string) *planpb.ColDef {
			return &planpb.ColDef{Name: name, Typ: planpb.Type{Id: int32(types.T_varchar)}}
		}
	}
	// The alter hooks delegate to these shared helpers. The tests below assert the
	// delegation, not what the helper decides.
	if planplugin.IncludedColumnAffected == nil {
		planplugin.IncludedColumnAffected = func(indexDef *planpb.IndexDef, colName string) (bool, error) {
			for _, c := range indexDef.GetParts() {
				if c == colName {
					return true, nil
				}
			}
			return false, nil
		}
	}
	if planplugin.RenameIncludedColumnsForAlgo == nil {
		planplugin.RenameIncludedColumnsForAlgo = func(_ *planpb.TableDef, _, _, _ string) ([]string, error) {
			return nil, nil
		}
	}
}

type stubCompilerContext struct{ ctx context.Context }

func (c stubCompilerContext) GetContext() context.Context { return c.ctx }
func (c stubCompilerContext) ResolveVariable(string, bool, bool) (interface{}, error) {
	return nil, nil
}

var _ planplugin.CompilerContext = stubCompilerContext{}

func newStubCompilerContext() stubCompilerContext {
	return stubCompilerContext{ctx: context.Background()}
}

func vecColMap(pkName, vecName string) map[string]*planpb.ColDef {
	return map[string]*planpb.ColDef{
		pkName:  {Name: pkName, Typ: planpb.Type{Id: int32(types.T_int64)}},
		vecName: {Name: vecName, Typ: planpb.Type{Id: int32(types.T_array_float32)}},
	}
}

func indexOn(colName string) *tree.Index {
	un := tree.NewUnresolvedName(tree.NewCStr(colName, 0))
	return &tree.Index{KeyParts: []*tree.KeyPart{{ColName: un}}}
}

// --- schema.go: BuildSecondaryIndexDefs -----------------------------------

// The happy path builds the three IVFFLAT tables (metadata, centroids, entries).
func TestBuildSecondaryIndexDefs_OK(t *testing.T) {
	idxDefs, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), nil, "id")
	require.NoError(t, err)
	require.NotEmpty(t, idxDefs)
	require.NotEmpty(t, tblDefs)
	for _, td := range tblDefs {
		require.NotEmpty(t, td.Cols, "every generated table declares columns")
		require.NotNil(t, td.Pkey, "and a primary key")
	}
}

// IVFFLAT indexes exactly one column.
func TestBuildSecondaryIndexDefs_MultiColumn(t *testing.T) {
	idx := indexOn("vec")
	idx.KeyParts = append(idx.KeyParts, &tree.KeyPart{ColName: tree.NewUnresolvedName(tree.NewCStr("vec2", 0))})
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), idx, vecColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

func TestBuildSecondaryIndexDefs_ColumnNotExist(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("nope"), vecColMap("id", "vec"), nil, "id")
	require.Error(t, err)
}

// Only vector columns are indexable.
func TestBuildSecondaryIndexDefs_NotAVector(t *testing.T) {
	colMap := vecColMap("id", "vec")
	colMap["vec"].Typ.Id = int32(types.T_int64)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
	require.Error(t, err)
}

// A second IVFFLAT index on the same column is rejected.
func TestBuildSecondaryIndexDefs_DuplicateColumn(t *testing.T) {
	existed := []*planpb.IndexDef{{
		IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
		Parts:     []string{"vec"},
	}}
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexOn("vec"), vecColMap("id", "vec"), existed, "id")
	require.Error(t, err)
}

// vecf64 is accepted alongside vecf32.
func TestBuildSecondaryIndexDefs_F64Base(t *testing.T) {
	colMap := vecColMap("id", "vec")
	colMap["vec"].Typ.Id = int32(types.T_array_float64)
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), indexOn("vec"), colMap, nil, "id")
	require.NoError(t, err)
}

// --- schema.go: BuildFullTextIndexDefs ------------------------------------

// IVFFLAT is a vector algorithm; the fulltext hook is not applicable to it.
func TestBuildFullTextIndexDefs_NotSupported(t *testing.T) {
	_, _, err := Hooks{}.BuildFullTextIndexDefs(
		newStubCompilerContext(), &tree.FullTextIndex{}, vecColMap("id", "body"), nil, "id")
	require.Error(t, err)
}

// --- alter.go --------------------------------------------------------------

// An index that does not cover the column is unaffected by dropping it; one that
// INCLUDEs it is.
func TestAlterHooks_IncludedColumnAffected(t *testing.T) {
	bare := &planpb.IndexDef{Parts: []string{"vec"}}
	for _, c := range []struct {
		name string
		fn   func() (bool, error)
		on   func(string) (bool, error)
	}{
		{"drop",
			func() (bool, error) { return Hooks{}.HandleAlterDropColumn(nil, bare, "other") },
			func(c string) (bool, error) { return Hooks{}.HandleAlterDropColumn(nil, bare, c) }},
		{"rename-rebuild",
			func() (bool, error) { return Hooks{}.RenameColumnRequiresIndexRebuild(nil, bare, "other") },
			func(c string) (bool, error) { return Hooks{}.RenameColumnRequiresIndexRebuild(nil, bare, c) }},
		{"update-rewrite",
			func() (bool, error) { return Hooks{}.UpdateColumnRequiresIndexRewrite(nil, bare, "other") },
			func(c string) (bool, error) { return Hooks{}.UpdateColumnRequiresIndexRewrite(nil, bare, c) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			affected, err := c.fn()
			require.NoError(t, err)
			require.False(t, affected, "a column the index neither keys nor includes")

			// The same hook reports true for a column it does cover.
			covered, err := c.on("vec")
			require.NoError(t, err)
			require.True(t, covered)
		})
	}
}

// Renaming walks the table's IVFFLAT indexes; with no table def there is nothing
// to rewrite and it must not error.
func TestHandleAlterRenameColumn_NoIndexes(t *testing.T) {
	sqls, err := Hooks{}.HandleAlterRenameColumn(&planpb.TableDef{}, "old", "new")
	require.NoError(t, err)
	require.Empty(t, sqls)
}

// --- plan.go ---------------------------------------------------------------

// ValidateViewDefinition accepts any query: IVFFLAT places no view restriction.
func TestValidateViewDefinition(t *testing.T) {
	require.NoError(t, Hooks{}.ValidateViewDefinition(newStubCompilerContext(), &planpb.Query{}))
}

// --- schema.go: INCLUDE columns -------------------------------------------

// indexWithInclude builds a single-column index on vecCol that also carries an
// INCLUDE list.
func indexWithInclude(vecCol string, include ...string) *tree.Index {
	idx := indexOn(vecCol)
	cols := make([]*tree.UnresolvedName, 0, len(include))
	for _, c := range include {
		cols = append(cols, tree.NewUnresolvedName(tree.NewCStr(c, 0)))
	}
	idx.IndexOption = &tree.IndexOption{IncludeColumns: cols}
	return idx
}

// includeColMap is vecColMap plus a payload column INCLUDE can legally carry.
func includeColMap() map[string]*planpb.ColDef {
	m := vecColMap("id", "vec")
	m["payload"] = &planpb.ColDef{Name: "payload", Typ: planpb.Type{Id: int32(types.T_int64)}}
	m["body"] = &planpb.ColDef{Name: "body", Typ: planpb.Type{Id: int32(types.T_varchar)}}
	return m
}

func TestIvfflatIncludeColumnNames(t *testing.T) {
	require.Nil(t, ivfflatIncludeColumnNames(nil))
	require.Nil(t, ivfflatIncludeColumnNames(indexOn("vec")), "no IndexOption at all")
	require.Nil(t, ivfflatIncludeColumnNames(indexWithInclude("vec")), "IndexOption with an empty list")
	require.Equal(t, []string{"payload", "body"},
		ivfflatIncludeColumnNames(indexWithInclude("vec", "payload", "body")))
}

func TestValidateIvfflatIncludeColumns(t *testing.T) {
	ctx := newStubCompilerContext()
	colMap := includeColMap()

	for _, c := range []struct {
		name    string
		idx     *tree.Index
		wantErr bool
	}{
		{"no option", indexOn("vec"), false},
		{"empty list", indexWithInclude("vec"), false},
		{"ok", indexWithInclude("vec", "payload", "body"), false},
		{"not exist", indexWithInclude("vec", "nope"), true},
		{"the vector column itself", indexWithInclude("vec", "vec"), true},
		{"the primary key", indexWithInclude("vec", "id"), true},
		{"duplicate", indexWithInclude("vec", "payload", "payload"), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := validateIvfflatIncludeColumns(ctx, c.idx, colMap, "vec", "id")
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("unsupported type", func(t *testing.T) {
		m := includeColMap()
		m["payload"].Typ.Id = int32(types.T_array_float32)
		require.Error(t, validateIvfflatIncludeColumns(ctx, indexWithInclude("vec", "payload"), m, "vec", "id"))
	})

	t.Run("too many", func(t *testing.T) {
		m := includeColMap()
		names := make([]string, 0, maxIvfflatIncludeColumns+1)
		for i := 0; i <= maxIvfflatIncludeColumns; i++ {
			n := fmt.Sprintf("c%d", i)
			m[n] = &planpb.ColDef{Name: n, Typ: planpb.Type{Id: int32(types.T_int64)}}
			names = append(names, n)
		}
		require.Error(t, validateIvfflatIncludeColumns(ctx, indexWithInclude("vec", names...), m, "vec", "id"))
	})
}

// The INCLUDE list reaches the entries table as prefixed payload columns.
func TestBuildSecondaryIndexDefs_WithInclude(t *testing.T) {
	_, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexWithInclude("vec", "payload"), includeColMap(), nil, "id")
	require.NoError(t, err)

	var entries *planpb.TableDef
	for _, td := range tblDefs {
		if td.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries {
			entries = td
		}
	}
	require.NotNil(t, entries)

	var found *planpb.ColDef
	for _, col := range entries.Cols {
		if col.Name == catalog.SystemSI_IVFFLAT_IncludeColPrefix+"payload" {
			found = col
		}
	}
	require.NotNil(t, found, "the INCLUDE column is materialized in the entries table")
	require.Equal(t, int32(types.T_int64), found.Typ.Id, "and keeps the source column's type")
}

// --- schema.go: QUANTIZATION ----------------------------------------------

// QUANTIZATION retypes the entry column without touching the base column; an
// upcast (a wider quantization than the base) is refused.
func TestBuildSecondaryIndexDefs_Quantization(t *testing.T) {
	entryTypeOf := func(t *testing.T, tblDefs []*planpb.TableDef) planpb.Type {
		t.Helper()
		for _, td := range tblDefs {
			if td.TableType != catalog.SystemSI_IVFFLAT_TblType_Entries {
				continue
			}
			for _, col := range td.Cols {
				if col.Name == catalog.SystemSI_IVFFLAT_TblCol_Entries_entry {
					return col.Typ
				}
			}
		}
		t.Fatal("no entries table entry column")
		return planpb.Type{}
	}

	t.Run("downcast to int8", func(t *testing.T) {
		idx := indexOn("vec")
		idx.IndexOption = &tree.IndexOption{Quantization: "int8"}
		_, tblDefs, err := Hooks{}.BuildSecondaryIndexDefs(
			newStubCompilerContext(), idx, vecColMap("id", "vec"), nil, "id")
		require.NoError(t, err)
		require.Equal(t, int32(types.T_array_int8), entryTypeOf(t, tblDefs).Id)
	})

	t.Run("upcast is refused", func(t *testing.T) {
		colMap := vecColMap("id", "vec")
		colMap["vec"].Typ.Id = int32(types.T_array_int8)
		idx := indexOn("vec")
		idx.IndexOption = &tree.IndexOption{Quantization: "float32"}
		_, _, err := Hooks{}.BuildSecondaryIndexDefs(newStubCompilerContext(), idx, colMap, nil, "id")
		require.Error(t, err)
	})

	t.Run("int8 is L2-only", func(t *testing.T) {
		idx := indexOn("vec")
		idx.IndexOption = &tree.IndexOption{Quantization: "int8", AlgoParamVectorOpType: "vector_cosine_ops"}
		_, _, err := Hooks{}.BuildSecondaryIndexDefs(
			newStubCompilerContext(), idx, vecColMap("id", "vec"), nil, "id")
		require.Error(t, err)
	})
}

// A rejected INCLUDE list fails the whole index build.
func TestBuildSecondaryIndexDefs_BadInclude(t *testing.T) {
	_, _, err := Hooks{}.BuildSecondaryIndexDefs(
		newStubCompilerContext(), indexWithInclude("vec", "nope"), includeColMap(), nil, "id")
	require.Error(t, err)
}
