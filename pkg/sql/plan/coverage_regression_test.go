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
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	setIvfIncludeModeTestPagination(vecCtx, 2, 1)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Nil(t, tableFuncNode.Limit)
	require.Equal(t, uint64(3), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	// residual filter present → over-fetch display: FilteredPostModeLimit(3) = 15.
	require.Equal(t, uint64(15), tableFuncNode.IndexReaderParam.GetOverFetchLimit())
}

func TestApplyIndicesForSortUsingIvfflat_DistRangeOnlyFilterCompensatesOffset(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "post", 0, 2, 4)
	setIvfIncludeModeTestPagination(vecCtx, 2, 1)
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
	require.Nil(t, tableFuncNode.Limit)
	require.Equal(t, uint64(3), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
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
	require.Contains(t, strings.Join(alterCtx.UpdateSqls, "\n"), "set algo_params")
	require.Equal(t, []string{"headline", "note"}, copyTable.Indexes[0].IncludedColumns)
	require.NotContains(t, copyTable.Indexes[0].IndexAlgoParams, "include_columns")
}

func TestChangeColumnRewritesCheckOriginSQL(t *testing.T) {
	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	origin.Checks = []*planpb.CheckDef{
		{
			Name:      "chk_title",
			OriginSql: "`title` <> 'title' AND `note` <> 'title'",
		},
	}
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

	_, err := ChangeColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
	require.NoError(t, err)
	require.Equal(t,
		"`headline` != 'title' and `note` != 'title'",
		copyTable.Checks[0].OriginSql,
	)
	require.Equal(t,
		"`title` <> 'title' AND `note` <> 'title'",
		origin.Checks[0].OriginSql,
	)
}

func TestChangeColumnRenamesPrefixLengthMetadata(t *testing.T) {
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "title:4",
	})
	require.NoError(t, err)

	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	origin.Indexes = append(origin.Indexes, &planpb.IndexDef{
		IndexName:       "uq_title",
		IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
		IndexAlgoParams: prefixParams,
		Parts:           []string{"title"},
		Unique:          true,
	})
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
		"alter table t1 change column title headline varchar(64)",
	)

	_, err = ChangeColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
	require.NoError(t, err)
	idxDef := copyTable.Indexes[len(copyTable.Indexes)-1]
	require.Equal(t, []string{"headline"}, idxDef.Parts)
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"headline": 4}, prefixLengths)
}

func TestChangeColumnEncodesDelimiterBearingPrefixLengthMetadata(t *testing.T) {
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "title:4",
	})
	require.NoError(t, err)

	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	origin.Indexes = append(origin.Indexes, &planpb.IndexDef{
		IndexName:       "uq_title",
		IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
		IndexAlgoParams: prefixParams,
		Parts:           []string{"title"},
		Unique:          true,
	})
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
		"alter table t1 change column title `head:line` varchar(64)",
	)

	_, err = ChangeColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
	require.NoError(t, err)
	idxDef := copyTable.Indexes[len(copyTable.Indexes)-1]
	require.Equal(t, []string{"head:line"}, idxDef.Parts)
	require.Equal(t, map[string]int{"head:line": 4}, catalog.IndexPrefixLengthsFromParams(idxDef.IndexAlgoParams))

	params, err := catalog.IndexParamsStringToMap(idxDef.IndexAlgoParams)
	require.NoError(t, err)
	require.NotContains(t, params, catalog.IndexAlgoParamPrefixLengths)
	require.JSONEq(t, `{"head:line":4}`, params[catalog.IndexAlgoParamPrefixLengthsV2])
}

func TestInternalAliasPrefixIsRejectedForUserColumns(t *testing.T) {
	aliasName := catalog.CreateAlias("payload")
	require.False(t, checkTableColumnNameValid(aliasName))
	require.True(t, checkTableColumnNameValid("payload"))

	mock := NewMockOptimizer(false)
	require.Error(t, checkColumnNameValid(mock.CurrentContext().GetContext(), aliasName))
	require.NoError(t, checkColumnNameValid(mock.CurrentContext().GetContext(), "payload"))
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

func TestUpdateRenameColumnInTableDefEscapesMoIndexesColumnNameUpdate(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := &planpb.TableDef{
		TblId: 7,
		Cols: []*ColDef{
			{Name: "title", OriginName: "ti'tle"},
		},
		Pkey: &PrimaryKeyDef{},
		Indexes: []*planpb.IndexDef{
			{
				IndexName: "idx_title",
				IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
				Parts:     []string{"title"},
			},
		},
	}

	sqls, err := updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[0],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("head'line"),
		},
	)
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], "set column_name = 'head''line'")
	require.Contains(t, sqls[0], "column_name = 'ti''tle'")
}

func TestUpdateRenameColumnInTableDefRenamesPrefixLengthMetadata(t *testing.T) {
	singleParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "title:4",
	})
	require.NoError(t, err)
	compositeParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		"comment":                           "keep",
		catalog.IndexAlgoParamPrefixLengths: "note:2,title:4",
	})
	require.NoError(t, err)

	mock := NewMockOptimizer(false)
	tableDef := &planpb.TableDef{
		TblId: 42,
		Cols: []*ColDef{
			{Name: "id", OriginName: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "title", OriginName: "title", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
			{Name: "note", OriginName: "note", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
		},
		Pkey: &PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Indexes: []*planpb.IndexDef{
			{
				IndexName:       "uq_title",
				IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
				IndexAlgoParams: singleParams,
				Parts:           []string{"title"},
				Unique:          true,
			},
			{
				IndexName:       "idx_title_note",
				IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
				IndexAlgoParams: compositeParams,
				Parts:           []string{"title", "note", "id"},
			},
			{
				IndexName: "idx_note",
				IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
				Parts:     []string{"note", "id"},
			},
		},
	}

	sqls, err := updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[1],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("headline"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, "headline", tableDef.Cols[1].Name)
	require.Equal(t, []string{"headline"}, tableDef.Indexes[0].Parts)
	require.Equal(t, []string{"headline", "note", "id"}, tableDef.Indexes[1].Parts)

	singlePrefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(tableDef.Indexes[0].IndexAlgoParams)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"headline": 4}, singlePrefixLengths)
	compositePrefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(tableDef.Indexes[1].IndexAlgoParams)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"headline": 4, "note": 2}, compositePrefixLengths)
	compositeMetadata, err := catalog.IndexParamsStringToMap(tableDef.Indexes[1].IndexAlgoParams)
	require.NoError(t, err)
	require.Equal(t, "keep", compositeMetadata["comment"])
	require.Equal(t, "headline:4,note:2", compositeMetadata[catalog.IndexAlgoParamPrefixLengths])

	allSQL := strings.Join(sqls, "\n")
	require.Len(t, sqls, 3)
	require.Contains(t, allSQL, "set column_name = 'headline'")
	require.Contains(t, allSQL, "set algo_params = '{\"prefix_lengths\":\"headline:4\"}' where table_id = 42 and name = 'uq_title'")
	require.Contains(t, allSQL, "set algo_params = '{\"comment\":\"keep\",\"prefix_lengths\":\"headline:4,note:2\"}' where table_id = 42 and name = 'idx_title_note'")
	require.NotContains(t, allSQL, "name = 'idx_note'")
}

func TestUpdateRenameColumnInTableDefEncodesDelimiterBearingPrefixName(t *testing.T) {
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "title:4",
	})
	require.NoError(t, err)

	mock := NewMockOptimizer(false)
	tableDef := &planpb.TableDef{
		TblId: 7,
		Cols: []*ColDef{
			{Name: "id", OriginName: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "title", OriginName: "title", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 32}},
		},
		Pkey: &PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Indexes: []*planpb.IndexDef{{
			IndexName:       "idx_title",
			IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
			IndexAlgoParams: prefixParams,
			Parts:           []string{"title"},
		}},
	}

	_, err = updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[1],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("head:line"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"head:line"}, tableDef.Indexes[0].Parts)
	require.Equal(t, map[string]int{"head:line": 4}, catalog.IndexPrefixLengthsFromParams(tableDef.Indexes[0].IndexAlgoParams))

	params, err := catalog.IndexParamsStringToMap(tableDef.Indexes[0].IndexAlgoParams)
	require.NoError(t, err)
	require.NotContains(t, params, catalog.IndexAlgoParamPrefixLengths)
	require.JSONEq(t, `{"head:line":4}`, params[catalog.IndexAlgoParamPrefixLengthsV2])
}

func TestRenameIndexPrefixLengthMetadataBoundaryCases(t *testing.T) {
	t.Run("case-insensitive legacy key keeps nested session vars", func(t *testing.T) {
		params, err := catalog.IndexParamsMapToJsonStringWithSessionVars(
			map[string]string{catalog.IndexAlgoParamPrefixLengths: "Title:4"},
			json.RawMessage(`{"cfg":{}}`),
		)
		require.NoError(t, err)
		indexDef := &planpb.IndexDef{
			IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
			IndexAlgoParams: params,
		}

		affected, err := renameIndexPrefixLengthMetadata(indexDef, "title", "headline")
		require.NoError(t, err)
		require.True(t, affected)
		require.Equal(t, map[string]int{"headline": 4}, catalog.IndexPrefixLengthsFromParams(indexDef.IndexAlgoParams))
		sessionVars, err := catalog.IndexParamsSessionVars(indexDef.IndexAlgoParams)
		require.NoError(t, err)
		require.JSONEq(t, `{"cfg":{}}`, string(sessionVars))
	})

	t.Run("unprefixed renamed part does not rewrite params", func(t *testing.T) {
		params, err := catalog.IndexParamsMapToJsonString(map[string]string{
			catalog.IndexAlgoParamPrefixLengths: "note:2",
		})
		require.NoError(t, err)
		indexDef := &planpb.IndexDef{
			IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
			IndexAlgoParams: params,
		}

		affected, err := renameIndexPrefixLengthMetadata(indexDef, "title", "headline")
		require.NoError(t, err)
		require.False(t, affected)
		require.Equal(t, params, indexDef.IndexAlgoParams)
	})

	t.Run("invalid persisted metadata aborts rename before mutation", func(t *testing.T) {
		tableDef := &planpb.TableDef{
			Cols: []*ColDef{{Name: "title", OriginName: "title"}},
			Pkey: &PrimaryKeyDef{},
			Indexes: []*planpb.IndexDef{{
				IndexName:       "idx_title",
				IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
				IndexAlgoParams: `{"prefix_lengths":"title:0"}`,
				Parts:           []string{"title"},
			}},
		}
		mock := NewMockOptimizer(false)

		_, err := updateRenameColumnInTableDef(
			mock.CurrentContext(),
			tableDef.Cols[0],
			tableDef,
			&tree.AlterTableRenameColumnClause{
				OldColumnName: tree.NewUnresolvedColName("title"),
				NewColumnName: tree.NewUnresolvedColName("headline"),
			},
		)
		require.Error(t, err)
		require.Equal(t, []string{"title"}, tableDef.Indexes[0].Parts)
	})
}

func TestRenamePrefixIndexV2ProtocolGate(t *testing.T) {
	prefixParams, err := catalog.IndexParamsMapToJsonString(map[string]string{
		catalog.IndexAlgoParamPrefixLengths: "title:4",
	})
	require.NoError(t, err)
	tableDef := &planpb.TableDef{
		TblId: 1,
		Cols: []*ColDef{
			{Name: "id", OriginName: "id"},
			{Name: "title", OriginName: "title"},
		},
		Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Indexes: []*planpb.IndexDef{
			{
				IndexName: "idx_title_full",
				IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
				Parts:     []string{"title", catalog.CreateAlias("id")},
			},
			{
				IndexName:       "idx_title",
				IndexAlgo:       catalog.MoIndexDefaultAlgo.ToString(),
				IndexAlgoParams: prefixParams,
				Parts:           []string{"title", catalog.CreateAlias("id")},
			},
		},
	}
	mock := NewMockOptimizer(false)
	proc := mock.CurrentContext().GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
	_, err = updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[1],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("head:line"),
		},
	)
	require.ErrorContains(t, err, "protocol version 13")
	require.Equal(t, []string{"title", catalog.CreateAlias("id")}, tableDef.Indexes[0].Parts)
	require.Equal(t, []string{"title", catalog.CreateAlias("id")}, tableDef.Indexes[1].Parts)
	require.Equal(t, prefixParams, tableDef.Indexes[1].IndexAlgoParams)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion13)
	_, err = updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[1],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("head:line"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, "head:line", tableDef.Indexes[0].Parts[0])
	require.Equal(t, "head:line", tableDef.Indexes[1].Parts[0])
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

func TestUpdateRenameColumnInTableDefRewritesCheckOriginSQL(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := makeAlterCoverageTableDef()
	tableDef.Checks = []*planpb.CheckDef{
		{
			Name:      "chk_title",
			OriginSql: "`title` <> 'title' AND `note` <> 'title'",
		},
	}

	_, err := updateRenameColumnInTableDef(
		mock.CurrentContext(),
		tableDef.Cols[2],
		tableDef,
		&tree.AlterTableRenameColumnClause{
			OldColumnName: tree.NewUnresolvedColName("title"),
			NewColumnName: tree.NewUnresolvedColName("headline"),
		},
	)
	require.NoError(t, err)
	require.Equal(t,
		"`headline` != 'title' and `note` != 'title'",
		tableDef.Checks[0].OriginSql,
	)
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

func TestAlterColumnSetDefaultRejectsUnsupportedColumns(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*ColDef)
		checkErr  func(*testing.T, error)
	}{
		{
			name: "auto increment",
			configure: func(col *ColDef) {
				col.Typ.AutoIncr = true
			},
			checkErr: func(t *testing.T, err error) {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault))
			},
		},
		{
			name: "stored generated",
			configure: func(col *ColDef) {
				col.GeneratedCol = &planpb.GeneratedCol{IsStored: true}
			},
			checkErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "generated column 'note' cannot have a default value")
			},
		},
		{
			name: "virtual generated",
			configure: func(col *ColDef) {
				col.GeneratedCol = &planpb.GeneratedCol{IsStored: false}
			},
			checkErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "generated column 'note' cannot have a default value")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			origin := makeAlterCoverageTableDef()
			tc.configure(origin.Cols[3])
			copyTable := DeepCopyTableDef(origin, true)
			before := DeepCopyColDef(copyTable.Cols[3])
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

			_, err := AlterColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
			require.Error(t, err)
			tc.checkErr(t, err)
			require.Equal(t, before, copyTable.Cols[3])
		})
	}
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
		Cols: []*ColDef{
			{Name: "title", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64}},
			{Name: "note", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64}},
		},
		Indexes: []*planpb.IndexDef{
			{IndexName: "uk_title", Unique: true, Parts: []string{"title"}},
			{IndexName: "idx_note", Unique: false, Parts: []string{"note"}},
		},
	}
	newTable := &TableDef{
		Cols: DeepCopyColDefList(oldTable.Cols),
		Indexes: []*planpb.IndexDef{
			{IndexName: "uk_title", Unique: true, Parts: []string{"title"}},
			{IndexName: "uk_note", Unique: true, Parts: []string{"note"}},
		},
	}

	identitySources := map[string]selectExpr{
		"title": {sexprType: exprColumnName, sexprStr: "title"},
		"note":  {sexprType: exprColumnName, sexprStr: "note"},
	}
	skip := skipUniqueIdxDedup(oldTable, newTable, identitySources)
	require.Equal(t, map[string]bool{"uk_title": true}, skip)

	// Reusing a unique-index name and column name after DROP/ADD does not
	// preserve the source value: the target column is populated by its default.
	replacedSources := map[string]selectExpr{
		"note": {sexprType: exprColumnName, sexprStr: "note"},
	}
	require.Empty(t, skipUniqueIdxDedup(oldTable, newTable, replacedSources))

	newTable.Cols[0].Typ.Width = 8
	require.Empty(t, skipUniqueIdxDedup(oldTable, newTable, identitySources))

	newTable.Cols[0] = DeepCopyColDef(oldTable.Cols[0])
	newTable.Cols[0].GeneratedCol = &planpb.GeneratedCol{IsStored: true}
	require.Empty(t, skipUniqueIdxDedup(oldTable, newTable, identitySources))
}

func TestSkipPkDedupRequiresValuePreservingKeyColumns(t *testing.T) {
	oldTable := &TableDef{
		Cols: []*ColDef{
			{Name: "v", Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 6, Scale: 2}},
		},
		Pkey: &planpb.PrimaryKeyDef{PkeyColName: "v", Names: []string{"v"}},
	}
	newTable := DeepCopyTableDef(oldTable, true)
	identitySources := map[string]selectExpr{
		"v": {sexprType: exprColumnName, sexprStr: "v"},
	}
	require.True(t, skipPkDedup(oldTable, newTable, identitySources))
	require.False(t, skipPkDedup(oldTable, newTable, nil))

	newTable.Cols[0].Typ.Scale = 1
	require.False(t, skipPkDedup(oldTable, newTable, identitySources))

	newTable = DeepCopyTableDef(oldTable, true)
	newTable.Cols[0].GeneratedCol = &planpb.GeneratedCol{IsStored: true}
	require.False(t, skipPkDedup(oldTable, newTable, identitySources))
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
