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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestApplyIndicesForSortUsingIvfflat_PostModeOffsetCompensationUsesCompensatedLimit(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	scanTag := scanNode.BindingTags[0]
	scanNode.FilterList = []*planpb.Expr{
		{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "="},
					Args: []*planpb.Expr{
						{Typ: scanNode.TableDef.Cols[4].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: scanTag, ColPos: 4, Name: "note"}}},
						makePlan2StringConstExprWithType("n2"),
					},
				},
			},
		},
	}

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "post", 0, 2, 4)
	vecCtx.sortNode.Offset = makePlan2Uint64ConstExprWithType(1)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Equal(t, uint64(15), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(15), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
}

func TestApplyIndicesForSortUsingIvfflat_DistRangeOnlyFilterDoesNotCompensateOffset(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "post", 0, 2, 4)
	vecCtx.sortNode.Offset = makePlan2Uint64ConstExprWithType(1)
	scanNode.FilterList = []*planpb.Expr{
		{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "<="},
					Args: []*planpb.Expr{
						{
							Typ:  planpb.Type{Id: int32(types.T_float64)},
							Expr: &planpb.Expr_F{F: vecCtx.distFnExpr},
						},
						MakePlan2Float64ConstExprWithType(0.5),
					},
				},
			},
		},
	}

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Equal(t, uint64(2), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(2), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.NotNil(t, tableFuncNode.IndexReaderParam.GetDistRange())
	require.NotNil(t, tableFuncNode.IndexReaderParam.GetDistRange().GetUpperBound())
}

func TestRenameColumnUpdatesAlterContextAndClusterMetadata(t *testing.T) {
	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	copyTable := DeepCopyTableDef(origin, true)
	alterCtx := initAlterTableContext(origin, copyTable, origin.DbName)
	alterPlan := &planpb.AlterTable{
		Database:     origin.DbName,
		TableDef:     origin,
		CopyTableDef: copyTable,
	}

	err := RenameColumn(
		mock.CurrentContext(),
		alterPlan,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("headline"),
		},
		alterCtx,
	)
	require.NoError(t, err)
	require.Equal(t, "headline", copyTable.Cols[2].Name)
	require.Equal(t, "headline", copyTable.ClusterBy.Name)
	require.Equal(t, "title", alterCtx.alterColMap["headline"].sexprStr)
	require.Len(t, alterCtx.UpdateSqls, 2)
	require.Equal(t, []string{"headline", "note"}, copyTable.Indexes[0].IncludedColumns)
	require.NotContains(t, copyTable.Indexes[0].IndexAlgoParams, "include_columns")
	require.Equal(t, "headline", alterCtx.changColDefMap[3].Name)
}

func TestRenameColumnRejectsMissingColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	copyTable := DeepCopyTableDef(origin, true)
	alterCtx := initAlterTableContext(origin, copyTable, origin.DbName)
	alterPlan := &planpb.AlterTable{
		Database:     origin.DbName,
		TableDef:     origin,
		CopyTableDef: copyTable,
	}

	err := RenameColumn(
		mock.CurrentContext(),
		alterPlan,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("missing"),
			NewColumnName: tree.NewUnresolvedColName("headline"),
		},
		alterCtx,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
}

func TestChangeColumnRenamesClusterByAndTracksIvfIncludeMetadata(t *testing.T) {
	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	copyTable := DeepCopyTableDef(origin, true)
	alterCtx := initAlterTableContext(origin, copyTable, origin.DbName)
	alterPlan := &planpb.AlterTable{
		Database:     origin.DbName,
		TableDef:     origin,
		CopyTableDef: copyTable,
	}
	spec := mustParseAlterTableChangeColumnClause(
		t,
		mock.CurrentContext(),
		"alter table t1 change column title headline varchar(128)",
	)

	pkAffected, err := ChangeColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
	require.NoError(t, err)
	require.False(t, pkAffected)
	require.Equal(t, "headline", copyTable.ClusterBy.Name)
	require.Equal(t, "title", alterCtx.alterColMap["headline"].sexprStr)
	require.Equal(t, "headline", alterCtx.changColDefMap[3].Name)
	require.Len(t, alterCtx.UpdateSqls, 3)
	require.Contains(t, strings.Join(alterCtx.UpdateSqls, "\n"), "included_columns")
	require.Equal(t, []string{"headline", "note"}, copyTable.Indexes[0].IncludedColumns)
	require.NotContains(t, copyTable.Indexes[0].IndexAlgoParams, "include_columns")
}

func TestAppendAffectedAlterColumnNamesKeepsOldNameForChangeColumn(t *testing.T) {
	affectedCols := appendAffectedAlterColumnNames(nil, "title", "headline")
	require.Equal(t, []string{"title", "headline"}, affectedCols)

	indexes := []*planpb.IndexDef{
		{
			IndexName: "idx_title",
			IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
			Parts:     []string{"title"},
		},
		makeIvfIncludeAlterIndexDef("idx_ivf", []string{"title", "note"}),
	}

	names, err := collectAffectedIndexNamesForAlter(indexes, affectedCols)
	require.NoError(t, err)
	require.Equal(t, []string{"idx_ivf", "idx_title"}, names)

	require.Equal(t, []string{"title"}, appendAffectedAlterColumnNames(nil, "title", "title"))
}

func TestUpdateRenameColumnInTableDefRenamesPrimaryKeyAlias(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := makeAlterCoverageTableDef()

	sqls, err := updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[0],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("id"),
			NewColumnName: tree.NewUnresolvedColName("row_id"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, "row_id", tableDef.Pkey.PkeyColName)
	require.Equal(t, []string{"row_id"}, tableDef.Pkey.Names)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], catalog.CreateAlias("row_id"))
}

func TestUpdateRenameColumnInTableDefRejectsDuplicateTargetName(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := makeAlterCoverageTableDef()

	_, err := updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[2],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("note"),
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Duplicate column name")
}

func TestAlterColumnSetDefaultUpdatesCopiedColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	copyTable := DeepCopyTableDef(origin, true)
	alterCtx := initAlterTableContext(origin, copyTable, origin.DbName)
	alterPlan := &planpb.AlterTable{
		Database:     origin.DbName,
		TableDef:     origin,
		CopyTableDef: copyTable,
	}
	spec := mustParseAlterColumnClause(
		t,
		mock.CurrentContext(),
		"alter table t1 alter column note set default 'memo'",
	)

	pkAffected, err := AlterColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
	require.NoError(t, err)
	require.False(t, pkAffected)
	require.Contains(t, copyTable.Cols[3].Default.OriginString, "memo")
}

func TestOrderByColumnRejectsUnknownColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	copyTable := DeepCopyTableDef(origin, true)
	alterCtx := initAlterTableContext(origin, copyTable, origin.DbName)
	alterPlan := &planpb.AlterTable{
		Database:     origin.DbName,
		TableDef:     origin,
		CopyTableDef: copyTable,
	}
	spec := mustParseOrderByClause(
		t,
		mock.CurrentContext(),
		"alter table t1 order by missing",
	)

	err := OrderByColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
}

func TestSkipUniqueIdxDedupMatchesSameUniqueDefinition(t *testing.T) {
	oldTable := &TableDef{
		Indexes: []*planpb.IndexDef{
			{IndexName: "uk_title", Unique: true, Parts: []string{"title"}},
			{IndexName: "idx_note", Unique: false, Parts: []string{"note"}},
		},
	}
	newTable := &TableDef{
		Indexes: []*planpb.IndexDef{
			{IndexName: "uk_title", Unique: true, Parts: []string{"title"}},
			{IndexName: "uk_note", Unique: true, Parts: []string{"note"}},
		},
	}

	skip := skipUniqueIdxDedup(oldTable, newTable)
	require.Equal(t, map[string]bool{"uk_title": true}, skip)
}

func makeAlterCoverageTableDef() *TableDef {
	return &TableDef{
		DbName: "db1",
		Name:   "t1",
		TblId:  42,
		Cols: []*ColDef{
			{
				ColId:      1,
				Name:       "id",
				OriginName: "id",
				Typ:        planpb.Type{Id: int32(types.T_int64)},
				Default:    &planpb.Default{NullAbility: false},
			},
			{
				ColId:      2,
				Name:       "embedding",
				OriginName: "embedding",
				Typ:        planpb.Type{Id: int32(types.T_array_float32)},
				Default:    &planpb.Default{NullAbility: true},
			},
			{
				ColId:      3,
				Name:       "title",
				OriginName: "title",
				Typ:        planpb.Type{Id: int32(types.T_varchar), Width: 64},
				Default:    &planpb.Default{NullAbility: true},
				ClusterBy:  true,
			},
			{
				ColId:      4,
				Name:       "note",
				OriginName: "note",
				Typ:        planpb.Type{Id: int32(types.T_varchar), Width: 64},
				Default:    &planpb.Default{NullAbility: true},
			},
		},
		Name2ColIndex: map[string]int32{
			"id":        0,
			"embedding": 1,
			"title":     2,
			"note":      3,
		},
		Pkey: &planpb.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
		ClusterBy: &planpb.ClusterByDef{Name: "title"},
		Indexes: []*planpb.IndexDef{
			{
				IndexName:       "idx_ivf",
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
				IncludedColumns: []string{"title", "note"},
				Parts:           []string{"embedding"},
			},
		},
	}
}

func mustParseAlterTableChangeColumnClause(
	t *testing.T,
	ctx CompilerContext,
	sql string,
) *tree.AlterTableChangeColumnClause {
	t.Helper()

	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)

	stmt, ok := stmts[0].(*tree.AlterTable)
	require.True(t, ok)

	spec, ok := stmt.Options[0].(*tree.AlterTableChangeColumnClause)
	require.True(t, ok)
	return spec
}

func mustParseAlterColumnClause(
	t *testing.T,
	ctx CompilerContext,
	sql string,
) *tree.AlterTableAlterColumnClause {
	t.Helper()

	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)

	stmt, ok := stmts[0].(*tree.AlterTable)
	require.True(t, ok)

	spec, ok := stmt.Options[0].(*tree.AlterTableAlterColumnClause)
	require.True(t, ok)
	return spec
}

func mustParseOrderByClause(
	t *testing.T,
	ctx CompilerContext,
	sql string,
) *tree.AlterTableOrderByColumnClause {
	t.Helper()

	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)

	stmt, ok := stmts[0].(*tree.AlterTable)
	require.True(t, ok)

	spec, ok := stmt.Options[0].(*tree.AlterTableOrderByColumnClause)
	require.True(t, ok)
	return spec
}
