// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFunctionalIndexColumnNameIsStableAndCaseInsensitive(t *testing.T) {
	got := functionalIndexColumnName("Idx_JSON")
	require.Equal(t, catalog.FunctionalIndexColumnPrefix+"8f9256a3501a5ab88af691888f120a86", got)
	require.Equal(t, got, functionalIndexColumnName("  idx_json "))
	require.NotEqual(t, got, functionalIndexColumnName("idx_json_2"))
}

func TestSetEmptyIndexNameForFunctionalKeyPart(t *testing.T) {
	index := &tree.Index{KeyParts: []*tree.KeyPart{{Expr: tree.NewNumVal(int64(1), "1", false, tree.P_int64)}}}
	names := map[string]bool{indexNameKey("functional_index"): true}
	setEmptyIndexName(names, index)
	require.Equal(t, "functional_index_2", index.Name)
}

func TestRequireFunctionalIndexProtocol(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := runtime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, original)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion48)
		}
	}()

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion45)
	require.ErrorContains(t, requireFunctionalIndexProtocol(context.Background(), proc), "version 48")
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion48)
	require.NoError(t, requireFunctionalIndexProtocol(context.Background(), proc))
}

func TestNormalAlterIndexKeyPartIsNotFunctional(t *testing.T) {
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, "ALTER TABLE t1 DROP INDEX idx, ADD INDEX idx(b)", 1)
	require.NoError(t, err)
	alter, ok := stmts[0].(*tree.AlterTable)
	require.True(t, ok)
	for _, option := range alter.Options {
		if add, ok := option.(*tree.AlterOptionAdd); ok {
			index, ok := add.Def.(*tree.Index)
			require.True(t, ok)
			require.False(t, hasFunctionalIndexKeyPart(index.KeyParts))
		}
	}
	algorithm, err := ResolveAlterTableAlgorithm(context.Background(), alter.Options, &TableDef{})
	require.NoError(t, err)
	require.Equal(t, plan.AlterTable_INPLACE, algorithm)
}

func TestCheckFunctionalIndexResultTypeRejectsUnindexableTypes(t *testing.T) {
	ctx := context.Background()
	for _, id := range []types.T{types.T_json, types.T_text, types.T_blob, types.T_geometry, types.T_array_float32} {
		err := checkFunctionalIndexResultType(ctx, Type{Id: int32(id)})
		require.Error(t, err, id)
	}
	require.NoError(t, checkFunctionalIndexResultType(ctx, Type{Id: int32(types.T_varchar), Width: 64, Charset: uint32(types.CharsetType(types.T_varchar))}))
}

func TestFunctionalExpressionMatchesNormalizesRelationAndAssignmentCast(t *testing.T) {
	typ := Type{Id: int32(types.T_int64), Width: 64}
	generated := &plan.Expr{
		Typ:  typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 12, ColPos: 3}},
	}
	query := &plan.Expr{
		Typ:  typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 3}},
	}
	require.True(t, functionalExpressionMatches(generated, query))

	casted := &plan.Expr{
		Typ:  typ,
		Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "cast_assign"}, Args: []*plan.Expr{query}}},
	}
	require.True(t, functionalExpressionMatches(generated, casted))

	wrongColumn := DeepCopyExpr(query)
	wrongColumn.GetCol().ColPos++
	require.False(t, functionalExpressionMatches(generated, wrongColumn))
}

func TestFunctionalIndexDefRequiresCompleteHiddenGeneratedColumn(t *testing.T) {
	table := &TableDef{
		Cols: []*ColDef{{
			Name:   "__mo_fi_deadbeef",
			Hidden: true,
			GeneratedCol: &plan.GeneratedCol{
				Expr:         &plan.Expr{Typ: Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				OriginString: "lower(name)",
			},
		}},
		Indexes: []*plan.IndexDef{{
			IndexName: "idx_name",
			IndexAlgo: "",
			Parts:     []string{"__mo_fi_deadbeef", catalog.CreateAlias("id")},
		}},
	}
	require.True(t, isFunctionalIndexDef(table, table.Indexes[0]))
	require.Equal(t, "lower(name)", func() string {
		origin, ok := functionalIndexOrigin(table, table.Indexes[0])
		if !ok {
			return ""
		}
		return origin
	}())

	table.Cols[0].GeneratedCol = nil
	require.False(t, isFunctionalIndexDef(table, table.Indexes[0]))
	table.Cols[0].GeneratedCol = &plan.GeneratedCol{
		Expr:         &plan.Expr{Typ: Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		OriginString: "lower(name)",
		IsStored:     true,
	}
	require.False(t, isFunctionalIndexDef(table, table.Indexes[0]))
	require.Error(t, validateFunctionalIndexMetadata(context.Background(), table))

	table.Cols[0].GeneratedCol = &plan.GeneratedCol{
		Expr:         &plan.Expr{Typ: Type{Id: int32(types.T_int64)}},
		OriginString: "lower(name)",
	}
	table.Indexes[0].Parts = append(table.Indexes[0].Parts, "b")
	require.False(t, isFunctionalIndexDef(table, table.Indexes[0]))
	require.Error(t, validateFunctionalIndexMetadata(context.Background(), table))
	table.Indexes[0].Parts = table.Indexes[0].Parts[:2]
	table.Indexes = nil
	require.Error(t, validateFunctionalIndexMetadata(context.Background(), table))
}

func TestTrimFunctionalOuterParenthesesOnlyWhenTheyEncloseWholeExpr(t *testing.T) {
	require.Equal(t, "a + 1", trimFunctionalOuterParentheses("(a + 1)"))
	require.Equal(t, "(a + 1) * 2", trimFunctionalOuterParentheses("(a + 1) * 2"))
	require.Equal(t, "json_extract(doc, '$.sku')", trimFunctionalOuterParentheses("(json_extract(doc, '$.sku'))"))
	require.Equal(t, "(a + 1", trimFunctionalOuterParentheses("(a + 1"))
}

func TestLowerFunctionalIndexAddsPrivateVirtualGeneratedColumn(t *testing.T) {
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, "select a + 1", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	cctx := NewMockCompilerContext(false)
	cctx.SetContext(context.Background())
	table := &TableDef{
		Name: "t",
		Cols: []*ColDef{{
			Name: "a",
			Typ:  Type{Id: int32(types.T_int32), Width: 32},
		}},
	}
	index := &tree.Index{
		Name: "idx_a_plus_one",
		KeyParts: []*tree.KeyPart{{
			Expr: selectClause.Exprs[0].Expr,
		}},
	}
	lowered, err := lowerFunctionalIndex(cctx, index, table)
	require.NoError(t, err)
	require.Len(t, table.Cols, 2)
	hidden := table.Cols[1]
	require.True(t, hidden.Hidden)
	require.Equal(t, functionalIndexColumnName(index.Name), hidden.Name)
	require.NotNil(t, hidden.GeneratedCol)
	require.False(t, hidden.GeneratedCol.IsStored)
	require.Equal(t, "a + 1", hidden.GeneratedCol.OriginString)
	require.Equal(t, hidden.Name, lowered.KeyParts[0].ColName.ColName())
	require.Nil(t, index.KeyParts[0].ColName)
}

func TestBuildCreateTableLowersInlineFunctionalIndex(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	created, err := runOneStmt(optimizer, t, "create table functional_inline (id int primary key, a int, key idx_a ((a + 1)))")
	require.NoError(t, err)
	table := created.GetDdl().GetCreateTable().GetTableDef()
	require.Len(t, table.Indexes, 1)
	require.Len(t, table.Cols, 3)
	hidden := table.Cols[2]
	require.True(t, hidden.Hidden)
	require.NotNil(t, hidden.GeneratedCol)
	require.Equal(t, "a + 1", hidden.GeneratedCol.OriginString)
	require.Equal(t, hidden.Name, table.Indexes[0].Parts[0])

	jsonTable, err := runOneStmt(optimizer, t, "create table functional_json_text (id int primary key, doc json, key idx_sku ((json_unquote(json_extract(doc, '$.sku')))))")
	require.NoError(t, err)
	jsonDef := jsonTable.GetDdl().GetCreateTable().GetTableDef()
	require.Len(t, jsonDef.GetIndexes(), 1)
	jsonHidden := FindColumn(jsonDef.GetCols(), jsonDef.GetIndexes()[0].GetParts()[0])
	require.NotNil(t, jsonHidden)
	require.Equal(t, int32(types.T_varchar), jsonHidden.GetTyp().Id)

	noPK, err := runOneStmt(optimizer, t, "create table functional_no_pk (a int, key idx_a ((a + 1)))")
	require.NoError(t, err)
	noPKDef := noPK.GetDdl().GetCreateTable().GetTableDef()
	require.True(t, catalog.IsFakePkName(noPKDef.GetCols()[len(noPKDef.GetCols())-1].GetName()))
	require.True(t, noPKDef.GetCols()[len(noPKDef.GetCols())-2].GetHidden())
}

func TestBuildStandaloneAndAlterFunctionalIndexUseCopyDDL(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "create index", sql: "create index idx_expr on constraint_test.t1 ((a + 1))"},
		{name: "alter table", sql: "alter table constraint_test.t1 add index idx_expr2 ((a + 1))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			got, err := buildSingleStmt(optimizer, t, tc.sql)
			require.NoError(t, err)
			alter := got.GetDdl().GetAlterTable()
			require.NotNil(t, alter)
			require.Equal(t, plan.AlterTable_COPY, alter.GetAlgorithmType())
			require.NotNil(t, alter.GetCopyTableDef())
			var functional *plan.IndexDef
			for _, indexDef := range alter.GetCopyTableDef().GetIndexes() {
				if indexDef != nil && indexDef.GetIndexName() != "PRIMARY" && isFunctionalIndexDef(alter.GetCopyTableDef(), indexDef) {
					functional = indexDef
					break
				}
			}
			require.NotNil(t, functional)
			name := catalog.ResolveAlias(functional.GetParts()[0])
			hidden := FindColumn(alter.GetCopyTableDef().GetCols(), name)
			require.NotNil(t, hidden)
			require.True(t, hidden.GetHidden())
			require.NotNil(t, hidden.GetGeneratedCol())
		})
	}
}

func TestBuildCreateTableRejectsUnsupportedFunctionalIndexShapes(t *testing.T) {
	tests := []string{
		"create table functional_unique (id int primary key, a int, unique key idx_a ((a + 1)))",
		"create table functional_composite (id int primary key, a int, b int, key idx_ab ((a + 1), b))",
		"create table functional_json (id int primary key, doc json, key idx_doc ((json_extract(doc, '$.sku'))))",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, sql)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), "functional")
		})
	}
}

func TestShowCreateTableRendersFunctionalExpressionAndHidesImplementationColumn(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(optimizer, "create table functional_show (id int primary key, a int, key idx_a ((a + 1)))")
	require.NoError(t, err)
	got, _, err := ConstructCreateTableSQL(&optimizer.ctxt, tableDef, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, got, "KEY `idx_a` ((a + 1))")
	require.NotContains(t, got, catalog.FunctionalIndexColumnPrefix)

	tableDef.Cols[len(tableDef.Cols)-1].GeneratedCol.OriginString = ""
	_, _, err = ConstructCreateTableSQL(&optimizer.ctxt, tableDef, nil, false, nil)
	require.Error(t, err)
}
