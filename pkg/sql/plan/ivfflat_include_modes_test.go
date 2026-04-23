// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newIvfIncludeModeTestBuilder(t *testing.T) (*QueryBuilder, *BindContext, *plan.Node, int32, *MultiTableIndex) {
	baseMockCtx := NewMockCompilerContext(false)
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			switch varName {
			case "enable_vector_prefilter_by_default":
				return int8(0), nil
			case "ivf_threads_search":
				return int64(4), nil
			case "probe_limit":
				return int64(10), nil
			}
			return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
		},
	}

	tableDef := &plan.TableDef{
		Name: "t_include_modes",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "embedding", Typ: plan.Type{Id: int32(types.T_array_float32)}},
			{Name: "title", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "category", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "note", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id":        0,
			"embedding": 1,
			"title":     2,
			"category":  3,
			"note":      4,
		},
	}

	idxAlgoParams := `{"op_type":"` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	includedColumns := []string{"title", "category"}
	multiTableIndex := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexName:          "idx_include_modes",
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
				IndexTableName:     "meta",
				IndexAlgoParams:    idxAlgoParams,
				Parts:              []string{"embedding"},
				IncludedColumns:    includedColumns,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				IndexName:          "idx_include_modes",
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
				IndexTableName:     "centroids",
				Parts:              []string{"embedding"},
				IndexAlgoParams:    idxAlgoParams,
				IncludedColumns:    includedColumns,
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {
				IndexName:          "idx_include_modes",
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				IndexTableName:     "entries",
				IndexAlgoParams:    idxAlgoParams,
				Parts:              []string{"embedding"},
				IncludedColumns:    includedColumns,
			},
		},
	}

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)
	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db", ObjName: "t_include_modes"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	scanNodeID := builder.appendNode(scanNode, ctx)
	for int(scanNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	for i := 0; i < 20; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	return builder, ctx, scanNode, scanNodeID, multiTableIndex
}

func newIvfIncludeModeVectorSortContext(scanNode *plan.Node, scanNodeID int32, mode string, projectColPoses ...int32) *vectorSortContext {
	scanTag := scanNode.BindingTags[0]
	projectList := make([]*plan.Expr, 0, len(projectColPoses))
	for _, colPos := range projectColPoses {
		colDef := scanNode.TableDef.Cols[colPos]
		projectList = append(projectList, &plan.Expr{
			Typ: colDef.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: scanTag, ColPos: colPos, Name: colDef.Name},
			},
		})
	}

	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 1, Name: "embedding"}}},
			{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}

	return &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT},
		projNode: &plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{scanNodeID},
			ProjectList: projectList,
		},
		distFnExpr: distFnExpr,
		limit:      makePlan2Uint64ConstExprWithType(2),
		rankOption: &plan.RankOption{Mode: mode},
	}
}

func findIvfTableFunctionNode(builder *QueryBuilder, nodeID int32) *plan.Node {
	if int(nodeID) >= len(builder.qry.Nodes) || builder.qry.Nodes[nodeID] == nil {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_FUNCTION_SCAN {
		return node
	}
	for _, childID := range node.Children {
		if found := findIvfTableFunctionNode(builder, childID); found != nil {
			return found
		}
	}
	return nil
}

func TestApplyIndicesForSortUsingIvfflat_PostModeDoesNotAutoUseIncludeOptimization(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	scanTag := scanNode.BindingTags[0]
	scanNode.FilterList = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
						MakePlan2Int32ConstExprWithType(20),
					},
				},
			},
		},
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "="},
					Args: []*plan.Expr{
						{Typ: scanNode.TableDef.Cols[4].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 4, Name: "note"}}},
						makePlan2StringConstExprWithType("n2"),
					},
				},
			},
		},
	}

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "post", 0, 2, 4)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	joinNode := builder.qry.Nodes[sortNode.Children[0]]
	require.Equal(t, plan.Node_JOIN, joinNode.NodeType)

	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Equal(t, plan.Node_FUNCTION_SCAN, tableFuncNode.NodeType)
	require.Len(t, tableFuncNode.TableDef.Cols, 2)
	require.Equal(t, uint64(12), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(12), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Len(t, tableFuncNode.TblFuncExprList, 2)

	require.Len(t, scanNode.FilterList, 2)
	require.Equal(t, "category", scanNode.FilterList[0].GetF().Args[0].GetCol().Name)
	require.Equal(t, "note", scanNode.FilterList[1].GetF().Args[0].GetCol().Name)
}

func TestApplyIndicesForSortUsingIvfflat_IncludeModePartialPushdownKeepsResidualFilter(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	scanTag := scanNode.BindingTags[0]
	scanNode.FilterList = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
						MakePlan2Int32ConstExprWithType(20),
					},
				},
			},
		},
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "="},
					Args: []*plan.Expr{
						{Typ: scanNode.TableDef.Cols[4].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 4, Name: "note"}}},
						makePlan2StringConstExprWithType("n2"),
					},
				},
			},
		},
	}

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "include", 0, 2, 4)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	joinNode := builder.qry.Nodes[sortNode.Children[0]]
	require.Equal(t, plan.Node_JOIN, joinNode.NodeType)

	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Equal(t, plan.Node_FUNCTION_SCAN, tableFuncNode.NodeType)
	require.Len(t, tableFuncNode.TableDef.Cols, 2)
	require.Equal(t, maxSafeIvfSearchRoundLimit, tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, maxSafeIvfSearchRoundLimit, tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Len(t, tableFuncNode.TblFuncExprList, 5)
	assert.Contains(t, tableFuncNode.TblFuncExprList[2].GetLit().GetSval(), catalog.SystemSI_IVFFLAT_IncludeColPrefix+"category")
	assert.Equal(t, maxSafeIvfSearchRoundLimit, tableFuncNode.TblFuncExprList[3].GetLit().GetU64Val())

	require.Len(t, scanNode.FilterList, 1)
	require.Equal(t, "note", scanNode.FilterList[0].GetF().Args[0].GetCol().Name)
}

func TestApplyIndicesForSortUsingIvfflat_PreModeDoesNotAutoUseIncludePushdown(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	scanTag := scanNode.BindingTags[0]
	scanNode.FilterList = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
						MakePlan2Int32ConstExprWithType(20),
					},
				},
			},
		},
	}

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "pre", 0, 2, 3)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	require.Equal(t, plan.Node_JOIN, builder.qry.Nodes[sortNode.Children[0]].NodeType)

	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Len(t, tableFuncNode.TableDef.Cols, 2)
	require.Equal(t, uint64(2), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(2), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Len(t, tableFuncNode.TblFuncExprList, 2)
}

func TestApplyIndicesForSortUsingIvfflat_PreModeWithoutFiltersKeepsCandidateLimit(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "pre", 0, 2, 3)
	vecCtx.sortNode.Offset = makePlan2Uint64ConstExprWithType(1)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	require.Equal(t, plan.Node_JOIN, builder.qry.Nodes[sortNode.Children[0]].NodeType)

	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Len(t, tableFuncNode.TableDef.Cols, 2)
	require.Equal(t, uint64(2), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(2), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Len(t, tableFuncNode.TblFuncExprList, 2)
}

func TestApplyIndicesForSortUsingIvfflat_PreModeWithFiltersAddsOffsetToCandidateLimit(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	scanTag := scanNode.BindingTags[0]
	scanNode.FilterList = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "="},
					Args: []*plan.Expr{
						{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
						MakePlan2Int32ConstExprWithType(20),
					},
				},
			},
		},
	}

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "pre", 0, 2, 3)
	vecCtx.sortNode.Offset = makePlan2Uint64ConstExprWithType(1)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	require.Equal(t, plan.Node_JOIN, builder.qry.Nodes[sortNode.Children[0]].NodeType)

	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Len(t, tableFuncNode.TableDef.Cols, 2)
	require.Equal(t, uint64(3), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(3), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Len(t, tableFuncNode.TblFuncExprList, 2)
}

func TestApplyIndicesForSortUsingIvfflat_IncludeModeWithoutMetadataFallsBackToPost(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	idxAlgoParams := `{"op_type":"` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexAlgoParams = idxAlgoParams
	multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexAlgoParams = idxAlgoParams
	multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexAlgoParams = idxAlgoParams
	multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IncludedColumns = nil
	multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IncludedColumns = nil
	multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IncludedColumns = nil

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "include", 0, 2, 3)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	require.Equal(t, plan.Node_JOIN, builder.qry.Nodes[sortNode.Children[0]].NodeType)

	tableFuncNode := findIvfTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Len(t, tableFuncNode.TableDef.Cols, 2)
	require.Equal(t, uint64(2), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(2), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Len(t, tableFuncNode.TblFuncExprList, 5)
	assert.Empty(t, tableFuncNode.TblFuncExprList[2].GetLit().GetSval())
}

func TestApplyIndicesForSortUsingIvfflat_IncludeModeIndexOnlyPushdownOverfetchesFirstRound(t *testing.T) {
	builder, _, scanNode, scanNodeID, multiTableIndex := newIvfIncludeModeTestBuilder(t)

	scanTag := scanNode.BindingTags[0]
	scanNode.FilterList = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
						MakePlan2Int32ConstExprWithType(20),
					},
				},
			},
		},
	}

	vecCtx := newIvfIncludeModeVectorSortContext(scanNode, scanNodeID, "include", 0, 2, 3)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)

	tableFuncNode := builder.qry.Nodes[sortNode.Children[0]]
	require.Equal(t, plan.Node_FUNCTION_SCAN, tableFuncNode.NodeType)
	require.Equal(t, uint64(2), tableFuncNode.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(2), tableFuncNode.IndexReaderParam.GetLimit().GetLit().GetU64Val())
	require.Len(t, tableFuncNode.TblFuncExprList, 5)
	assert.Contains(t, tableFuncNode.TblFuncExprList[2].GetLit().GetSval(), catalog.SystemSI_IVFFLAT_IncludeColPrefix+"category")
	assert.Equal(t, uint64(12), tableFuncNode.TblFuncExprList[3].GetLit().GetU64Val())
}

func TestSerializeFiltersToSQL_DoesNotPushMixedOR(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]

	orExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "or"},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_bool)},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "="},
								Args: []*plan.Expr{
									{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
									MakePlan2Int32ConstExprWithType(20),
								},
							},
						},
					},
					{
						Typ: plan.Type{Id: int32(types.T_bool)},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "="},
								Args: []*plan.Expr{
									{Typ: scanNode.TableDef.Cols[4].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 4, Name: "note"}}},
									makePlan2StringConstExprWithType("n2"),
								},
							},
						},
					},
				},
			},
		},
	}

	sql, pushdown, remaining, err := serializeFiltersToSQL([]*plan.Expr{orExpr}, scanNode, []string{"title", "category"}, 1)
	require.NoError(t, err)
	assert.Empty(t, sql)
	assert.Empty(t, pushdown)
	require.Len(t, remaining, 1)
}

func TestSerializeFiltersToSQL_FormatsFunctionsCastAndArithmetic(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]

	lowerTitleFilter := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "="},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_varchar)},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "lower"},
								Args: []*plan.Expr{
									{Typ: scanNode.TableDef.Cols[2].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 2, Name: "title"}}},
								},
							},
						},
					},
					makePlan2StringConstExprWithType("beta"),
				},
			},
		},
	}

	castCategoryFilter := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: ">="},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "cast"},
								Args: []*plan.Expr{
									{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
									{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
								},
							},
						},
					},
					makePlan2Int64ConstExprWithType(20),
				},
			},
		},
	}

	arithmeticFilter := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: ">="},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(types.T_int32)},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "+"},
								Args: []*plan.Expr{
									{Typ: scanNode.TableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "category"}}},
									MakePlan2Int32ConstExprWithType(1),
								},
							},
						},
					},
					MakePlan2Int32ConstExprWithType(21),
				},
			},
		},
	}

	sql, pushdown, remaining, err := serializeFiltersToSQL(
		[]*plan.Expr{lowerTitleFilter, castCategoryFilter, arithmeticFilter},
		scanNode,
		[]string{"title", "category"},
		1,
	)
	require.NoError(t, err)
	require.Len(t, pushdown, 3)
	assert.Empty(t, remaining)
	assert.Contains(t, sql, `lower(`)
	assert.Contains(t, sql, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"title")
	assert.Contains(t, sql, `"beta"`)
	assert.Contains(t, sql, `cast(`)
	assert.Contains(t, sql, `as bigint`)
	assert.Contains(t, sql, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"category")
	assert.Contains(t, sql, `+ 1`)
	assert.Contains(t, sql, `>= 21`)
}

func TestIvfLiteralToAST_QuotesStringSafely(t *testing.T) {
	cases := []struct {
		name string
		lit  *plan.Literal
		want string
	}{
		{
			name: "single quote and backslash",
			lit:  &plan.Literal{Value: &plan.Literal_Sval{Sval: `O'Reilly\docs`}},
			want: "\"O'Reilly\\\\\\\\docs\"",
		},
		{
			name: "empty string",
			lit:  &plan.Literal{Value: &plan.Literal_Sval{Sval: ""}},
			want: `""`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := ivfLiteralToAST(tc.lit, plan.Type{Id: int32(types.T_varchar)})
			require.NoError(t, err)
			sql := tree.StringWithOpts(ast, dialect.MYSQL, tree.WithQuoteString(true))
			require.Equal(t, tc.want, sql)
		})
	}
}

func TestCanDoIndexOnlyScan_CompositePrimaryKeyIsConservative(t *testing.T) {
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Names:       []string{"id1", "id2"},
		},
	}

	requiredCols := map[string]struct{}{
		"id1": {},
	}
	assert.False(t, canDoIndexOnlyScan(requiredCols, tableDef, []string{"title"}))
}
