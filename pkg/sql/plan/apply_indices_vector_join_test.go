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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	vectorplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin"
	"github.com/stretchr/testify/require"
)

func newVectorJoinMockCtx() *customMockCompilerContext {
	baseMockCtx := NewMockCompilerContext(false)
	return &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			switch varName {
			case "hnsw_threads_search", "ivf_threads_search":
				return int64(4), nil
			case "probe_limit":
				return int64(10), nil
			case "enable_vector_prefilter_by_default", "enable_vector_auto_mode_by_default":
				return int8(0), nil
			default:
				return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
			}
		},
	}
}

func newVectorJoinTableDef(withVectorIndex bool, vectorNotNull bool) *plan.TableDef {
	tableDef := &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32), NotNullable: vectorNotNull}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
	if withVectorIndex {
		idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
		tableDef.Indexes = []*plan.IndexDef{
			{
				IndexName:          "idx_hnsw_v",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
				IndexTableName:     "hnsw_meta",
				Parts:              []string{"v"},
				IndexAlgoParams:    idxAlgoParams,
			},
			{
				IndexName:          "idx_hnsw_v",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage,
				IndexTableName:     "hnsw_storage",
				Parts:              []string{"v"},
				IndexAlgoParams:    idxAlgoParams,
			},
		}
	}
	return tableDef
}

func newVectorJoinHnswIndex() *MultiTableIndex {
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	return &MultiTableIndex{
		IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexTableName:  "hnsw_meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				IndexTableName:  "hnsw_storage",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}
}

func newVectorJoinIvfIndex() *MultiTableIndex {
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "lists": 10}`
	return &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexTableName:  "ivf_meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				IndexTableName:  "ivf_centroids",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {
				IndexTableName: "ivf_entries",
			},
		},
	}
}

func newVectorJoinEqFilter(tag int32, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: tag,
						ColPos: colPos,
						Name:   "id",
					}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{
						Value: &plan.Literal_Sval{Sval: "ref"},
					}},
				},
			},
		}},
	}
}

func newVectorJoinIsNotNullFilter(tag int32, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "isnotnull"},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: tag,
						ColPos: colPos,
						Name:   "v",
					}},
				},
			},
		}},
	}
}

func newVectorJoinColExpr(tag int32, colPos int32, name string, typ plan.Type) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: tag,
			ColPos: colPos,
			Name:   name,
		}},
	}
}

func newVectorJoinStringLitExpr() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "ref"},
		}},
	}
}

func newVectorJoinConstEqFilter(tag int32, colPos int32, name string, typ plan.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				newVectorJoinColExpr(tag, colPos, name, typ),
				newVectorJoinStringLitExpr(),
			},
		}},
	}
}

func newVectorJoinReverseConstEqFilter(tag int32, colPos int32, name string, typ plan.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				newVectorJoinStringLitExpr(),
				newVectorJoinColExpr(tag, colPos, name, typ),
			},
		}},
	}
}

type vectorJoinPlanCase struct {
	builder        *QueryBuilder
	ctx            *BindContext
	projNode       *plan.Node
	projNodeID     int32
	mainScanNodeID int32
	providerNodeID int32
}

type vectorJoinPlanOptions struct {
	joinType                    plan.Node_JoinType
	providerSingle              bool
	providerVectorNotNull       bool
	providerVectorNotNullFilter bool
	providerLimitOne            bool
	projectProvider             bool
}

func newVectorJoinPlanCase(t *testing.T, opts vectorJoinPlanOptions) vectorJoinPlanCase {
	t.Helper()

	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	ctx := NewBindContext(builder, nil)
	mainTableDef := newVectorJoinTableDef(true, false)
	providerTableDef := newVectorJoinTableDef(true, opts.providerVectorNotNull)
	mainVecTyp := mainTableDef.Cols[1].Typ
	providerVecTyp := providerTableDef.Cols[1].Typ

	mainScanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    mainTableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	mainScanNodeID := builder.appendNode(mainScanNode, ctx)

	providerScanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    providerTableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	var providerFilters []*plan.Expr
	if opts.providerSingle {
		providerFilters = append(providerFilters, newVectorJoinEqFilter(providerScanNode.BindingTags[0], 0))
	}
	if opts.providerVectorNotNullFilter {
		providerFilters = append(providerFilters, newVectorJoinIsNotNullFilter(providerScanNode.BindingTags[0], 1))
	}
	providerScanNode.FilterList = providerFilters
	if opts.providerLimitOne {
		providerScanNode.Limit = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_U64Val{U64Val: 1},
		}}}
	}
	providerScanNodeID := builder.appendNode(providerScanNode, ctx)

	joinNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: opts.joinType,
		Children: []int32{mainScanNodeID, providerScanNodeID},
	}
	joinNodeID := builder.appendNode(joinNode, ctx)

	distFnExpr := &plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{
				Typ: mainVecTyp,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: mainScanNode.BindingTags[0],
					ColPos: 1,
					Name:   "v",
				}},
			},
			{
				Typ: providerVecTyp,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: providerScanNode.BindingTags[0],
					ColPos: 1,
					Name:   "v",
				}},
			},
		},
	}

	sortChildID := joinNodeID
	sortExpr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_F{F: distFnExpr}}
	if opts.projectProvider {
		projectTag := builder.genNewBindTag()
		projectNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{joinNodeID},
			ProjectList: []*plan.Expr{
				{
					Typ: mainTableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: mainScanNode.BindingTags[0],
						ColPos: 0,
						Name:   "id",
					}},
				},
				{
					Typ: providerTableDef.Cols[1].Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: providerScanNode.BindingTags[0],
						ColPos: 1,
						Name:   "v",
					}},
				},
				{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_F{F: distFnExpr}},
			},
			BindingTags: []int32{projectTag},
		}
		sortChildID = builder.appendNode(projectNode, ctx)
		sortExpr = &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: 2}},
		}
	}

	sortNode := &plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{sortChildID},
		OrderBy: []*plan.OrderBySpec{{
			Expr: sortExpr,
			Flag: plan.OrderBySpec_ASC,
		}},
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_U64Val{U64Val: 10},
		}}},
	}
	sortNodeID := builder.appendNode(sortNode, ctx)

	projNode := &plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{sortNodeID},
	}
	projNodeID := builder.appendNode(projNode, ctx)

	for i := 0; i < 40; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	return vectorJoinPlanCase{
		builder:        builder,
		ctx:            ctx,
		projNode:       projNode,
		projNodeID:     projNodeID,
		mainScanNodeID: mainScanNodeID,
		providerNodeID: providerScanNodeID,
	}
}

func TestBuildVectorSortContextThroughJoin_RejectsSingleJoinProvider(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_SINGLE,
		providerSingle:        true,
		providerVectorNotNull: true,
	})

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.Nil(t, vecCtx)
}

func TestBuildVectorSortContextThroughJoin_InnerJoinSingleRowProvider(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_INNER,
		providerSingle:        true,
		providerVectorNotNull: true,
	})

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)
	require.Equal(t, tc.builder.qry.Nodes[tc.mainScanNodeID], vecCtx.scanNode)
	require.Equal(t, tc.providerNodeID, vecCtx.providerNodeID)
	require.NotNil(t, vecCtx.vecArgExpr)
}

func TestBuildVectorSortContextThroughJoin_InnerJoinProviderWithIsNotNullFilter(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:                    plan.Node_INNER,
		providerSingle:              true,
		providerVectorNotNullFilter: true,
	})

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)
	require.Equal(t, tc.providerNodeID, vecCtx.providerNodeID)
}

func TestBuildVectorSortContextThroughJoin_RejectsMultiRowProvider(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_INNER,
		providerVectorNotNull: true,
	})

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.Nil(t, vecCtx)
}

func TestBuildVectorSortContextThroughJoin_RejectsProviderProjection(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_INNER,
		providerSingle:        true,
		providerVectorNotNull: true,
		projectProvider:       true,
	})

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.Nil(t, vecCtx)
}

func TestBuildVectorSortContextThroughJoin_RejectsNullableProviderVector(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:       plan.Node_INNER,
		providerSingle: true,
	})

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.Nil(t, vecCtx)
}

func TestBuildVectorSortContextThroughJoin_RejectsLimitOnlyProvider(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_INNER,
		providerVectorNotNull: true,
		providerLimitOne:      true,
	})

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.Nil(t, vecCtx)
}

func TestApplyIndicesForSortUsingHnsw_JoinThroughKeepsProviderChild(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_INNER,
		providerSingle:        true,
		providerVectorNotNull: true,
	})
	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)

	p, ok := vectorplugin.Get(catalog.MoIndexHnswAlgo.ToString())
	require.True(t, ok, "hnsw plugin must be registered")
	mti := newVectorJoinHnswIndex()
	newNodeID, applied, err := p.Plan().ApplyForSort(
		tc.builder, vecCtx.export(), exportMultiTableIndex(mti), tc.projNodeID)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, tc.projNodeID, newNodeID)

	funcScan := findFirstNodeByType(tc.builder, plan.Node_FUNCTION_SCAN)
	require.NotNil(t, funcScan)
	require.Equal(t, []int32{tc.providerNodeID}, funcScan.Children)
	require.Equal(t, tc.providerNodeID, funcScan.Children[0])
	require.Equal(t, int32(1), funcScan.TblFuncExprList[1].GetCol().ColPos)
}

func TestApplyIndicesForSortUsingIvfflat_JoinThroughKeepsProviderChild(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_INNER,
		providerSingle:        true,
		providerVectorNotNull: true,
	})
	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)

	newNodeID, err := tc.builder.applyIndicesForSortUsingIvfflat(tc.projNodeID, vecCtx, newVectorJoinIvfIndex(), nil, nil)
	require.NoError(t, err)
	require.Equal(t, tc.projNodeID, newNodeID)

	funcScan := findFirstNodeByType(tc.builder, plan.Node_FUNCTION_SCAN)
	require.NotNil(t, funcScan)
	require.Equal(t, []int32{tc.providerNodeID}, funcScan.Children)
	require.Equal(t, int32(1), funcScan.TblFuncExprList[1].GetCol().ColPos)
}

func TestApplyIndicesForProject_JoinThroughReachesVectorRule(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		joinType:              plan.Node_INNER,
		providerSingle:        true,
		providerVectorNotNull: true,
	})

	newNodeID, err := tc.builder.applyIndicesForProject(tc.projNodeID, tc.projNode, nil, nil)
	require.NoError(t, err)
	require.Equal(t, tc.projNodeID, newNodeID)

	funcScan := findFirstNodeByType(tc.builder, plan.Node_FUNCTION_SCAN)
	require.NotNil(t, funcScan)
	require.Equal(t, []int32{tc.providerNodeID}, funcScan.Children)
}

func TestGetArgsFromDistFnForJoinBranches(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	floatTyp := plan.Type{Id: int32(types.T_array_float32)}
	intTyp := plan.Type{Id: int32(types.T_int64)}
	scanTag := int32(7)
	providerTag := int32(8)

	scanArg := newVectorJoinColExpr(scanTag, 1, "v", floatTyp)
	providerArg := newVectorJoinColExpr(providerTag, 1, "v", floatTyp)
	distFn := &plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{providerArg, scanArg},
	}

	key, value, found := builder.getArgsFromDistFnForJoin(distFn, 1, scanTag)
	require.True(t, found)
	require.Equal(t, scanArg, key)
	require.Equal(t, providerArg, value)
	require.Equal(t, scanArg.Typ, providerArg.Typ)

	_, _, found = builder.getArgsFromDistFnForJoin(&plan.Function{
		Func: &plan.ObjectRef{ObjName: "not_a_distance"},
		Args: []*plan.Expr{scanArg, providerArg},
	}, 1, scanTag)
	require.False(t, found)

	_, _, found = builder.getArgsFromDistFnForJoin(&plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			newVectorJoinColExpr(scanTag, 1, "id", intTyp),
			providerArg,
		},
	}, 1, scanTag)
	require.False(t, found)

	_, _, found = builder.getArgsFromDistFnForJoin(&plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{providerArg, scanArg},
	}, 2, scanTag)
	require.False(t, found)
}

func TestExtractJoinThroughProviderVectorArgBranches(t *testing.T) {
	floatTyp := plan.Type{Id: int32(types.T_array_float32)}
	mainTag := int32(10)
	providerTag := int32(11)
	mainTags := map[int32]bool{mainTag: true}
	providerTags := map[int32]bool{providerTag: true}
	mainArg := newVectorJoinColExpr(mainTag, 1, "v", floatTyp)
	providerArg := newVectorJoinColExpr(providerTag, 1, "v", floatTyp)

	distFn := &plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{providerArg, mainArg},
	}
	require.Equal(t, providerArg, extractJoinThroughProviderVectorArg(distFn, mainTag, mainTags, providerTags))

	require.Nil(t, extractJoinThroughProviderVectorArg(nil, mainTag, mainTags, providerTags))
	require.Nil(t, extractJoinThroughProviderVectorArg(&plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{mainArg},
	}, mainTag, mainTags, providerTags))
	require.Nil(t, extractJoinThroughProviderVectorArg(&plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{newVectorJoinStringLitExpr(), mainArg},
	}, mainTag, mainTags, providerTags))
	require.Nil(t, extractJoinThroughProviderVectorArg(distFn, mainTag, mainTags, map[int32]bool{12: true}))
}

func TestVectorProviderNonNullProofBranches(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	ctx := NewBindContext(builder, nil)
	floatTyp := plan.Type{Id: int32(types.T_array_float32)}
	notNullFloatTyp := plan.Type{Id: int32(types.T_array_float32), NotNullable: true}

	scanTag := builder.genNewBindTag()
	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    newVectorJoinTableDef(false, true),
		BindingTags: []int32{scanTag},
	}
	scanNodeID := builder.appendNode(scanNode, ctx)

	require.True(t, builder.providerColIsNonNull(scanNode, scanTag, 1))
	require.False(t, builder.providerColIsNonNull(scanNode, scanTag+100, 1))
	require.False(t, builder.providerColIsNonNull(scanNode, scanTag, 99))
	require.False(t, builder.providerColIsNonNull(nil, scanTag, 1))
	require.False(t, builder.providerColIsNonNull(&plan.Node{NodeType: plan.Node_TABLE_SCAN}, scanTag, 1))

	require.True(t, builder.isNonNullVectorProviderArg(scanNode, newVectorJoinColExpr(scanTag, 1, "v", floatTyp)))
	require.True(t, builder.isNonNullVectorProviderArg(scanNode, &plan.Expr{
		Typ:  notNullFloatTyp,
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{}},
	}))
	require.False(t, builder.isNonNullVectorProviderArg(scanNode, nil))
	require.False(t, builder.isNonNullVectorProviderArg(scanNode, newVectorJoinStringLitExpr()))

	projectTag := builder.genNewBindTag()
	projectNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{scanNodeID},
		BindingTags: []int32{projectTag},
		ProjectList: []*plan.Expr{
			newVectorJoinColExpr(scanTag, 1, "v", floatTyp),
			{Typ: notNullFloatTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}},
			newVectorJoinStringLitExpr(),
		},
	}
	projectNodeID := builder.appendNode(projectNode, ctx)
	require.True(t, builder.providerColIsNonNull(projectNode, projectTag, 0))
	require.True(t, builder.providerColIsNonNull(projectNode, projectTag, 1))
	require.False(t, builder.providerColIsNonNull(projectNode, projectTag, 2))
	require.False(t, builder.providerColIsNonNull(projectNode, projectTag, 9))

	sortNode := &plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{projectNodeID},
		FilterList: []*plan.Expr{
			newVectorJoinIsNotNullFilter(projectTag, 0),
		},
	}
	require.True(t, builder.providerColIsNonNull(sortNode, projectTag, 0))

	wrapperNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNodeID},
	}
	require.True(t, builder.providerColIsNonNull(wrapperNode, scanTag, 1))
	require.False(t, builder.providerColIsNonNull(&plan.Node{NodeType: plan.Node_JOIN}, scanTag, 1))
}

func TestSingleRowVectorProviderProofBranches(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	ctx := NewBindContext(builder, nil)
	varcharTyp := plan.Type{Id: int32(types.T_varchar)}
	floatTyp := plan.Type{Id: int32(types.T_array_float32)}
	tag := builder.genNewBindTag()

	tableDef := newVectorJoinTableDef(false, false)
	tableDef.Pkey = nil
	tableDef.Indexes = []*plan.IndexDef{{Unique: true, Parts: []string{"id", "v"}}}
	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		BindingTags: []int32{tag},
		FilterList: []*plan.Expr{
			newVectorJoinConstEqFilter(tag, 0, "id", varcharTyp),
			newVectorJoinReverseConstEqFilter(tag, 1, "v", floatTyp),
		},
	}
	scanNodeID := builder.appendNode(scanNode, ctx)
	require.True(t, builder.isSingleRowVectorProvider(scanNode))

	missingFilterScan := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		BindingTags: []int32{tag},
		FilterList: []*plan.Expr{
			newVectorJoinConstEqFilter(tag, 0, "id", varcharTyp),
		},
	}
	require.False(t, builder.isSingleRowVectorProvider(missingFilterScan))

	missingColDef := newVectorJoinTableDef(false, false)
	missingColDef.Pkey = nil
	missingColDef.Indexes = []*plan.IndexDef{{Unique: true, Parts: []string{"missing_col"}}}
	require.False(t, tableScanHasSingleRowFilter(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    missingColDef,
		BindingTags: []int32{tag},
	}))

	projectNode := &plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{scanNodeID},
	}
	require.True(t, builder.isSingleRowVectorProvider(projectNode))
	require.False(t, builder.isSingleRowVectorProvider(&plan.Node{NodeType: plan.Node_PROJECT}))
	require.True(t, builder.isSingleRowVectorProvider(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{scanNodeID},
	}))
	require.False(t, builder.isSingleRowVectorProvider(nil))
	require.False(t, builder.isSingleRowVectorProvider(&plan.Node{NodeType: plan.Node_JOIN}))
}

func TestVectorExprAndTagHelpersBranches(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	ctx := NewBindContext(builder, nil)
	varcharTyp := plan.Type{Id: int32(types.T_varchar)}
	tags := map[int32]bool{1: true}
	colExpr := newVectorJoinColExpr(1, 0, "id", varcharTyp)

	require.False(t, exprRefsAnyTag(nil, tags))
	require.True(t, exprRefsAnyTag(colExpr, tags))
	require.False(t, exprRefsAnyTag(newVectorJoinColExpr(2, 0, "id", varcharTyp), tags))
	require.True(t, exprRefsAnyTag(&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "abs"},
		Args: []*plan.Expr{colExpr},
	}}}, tags))
	require.True(t, exprRefsAnyTag(&plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{
		List: []*plan.Expr{colExpr},
	}}}, tags))
	require.False(t, exprRefsAnyTag(&plan.Expr{Expr: &plan.Expr_Sub{}}, tags))
	require.False(t, exprRefsAnyTag(newVectorJoinStringLitExpr(), tags))
	require.False(t, exprListRefsAnyTag(nil, tags))

	childNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{3},
	}
	childNodeID := builder.appendNode(childNode, ctx)
	parentNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		BindingTags: []int32{4},
		Children:    []int32{childNodeID, childNodeID},
	}
	builder.appendNode(parentNode, ctx)

	collectedTags := builder.collectBindingTags(parentNode)
	require.True(t, collectedTags[3])
	require.True(t, collectedTags[4])
	builder.collectBindingTagsRecursive(nil, collectedTags, make(map[int32]struct{}))
	require.False(t, collectedTags[5])
}

func TestVectorJoinGuardHelperBranches(t *testing.T) {
	floatTyp := plan.Type{Id: int32(types.T_array_float32)}
	require.False(t, isVectorProviderJoin(&plan.Node{JoinType: plan.Node_SINGLE}))
	require.False(t, isVectorProviderJoin(&plan.Node{
		JoinType: plan.Node_INNER,
		OnList: []*plan.Expr{
			{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}},
		},
	}))
	require.False(t, isVectorProviderJoin(&plan.Node{
		JoinType: plan.Node_INNER,
		OnList: []*plan.Expr{
			newVectorJoinColExpr(1, 0, "id", plan.Type{Id: int32(types.T_varchar)}),
		},
	}))

	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	require.Nil(t, builder.directScanWithVectorIndex(nil))
	require.Nil(t, builder.directScanWithVectorIndex(&plan.Node{NodeType: plan.Node_TABLE_SCAN}))
	require.Nil(t, builder.directScanWithVectorIndex(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    newVectorJoinTableDef(false, false),
		BindingTags: []int32{1},
	}))

	require.Nil(t, vectorSearchProviderChildren(nil))
	require.Nil(t, vectorSearchProviderChildren(&vectorSortContext{providerNodeID: 1}))
	require.Nil(t, vectorSearchProviderChildren(&vectorSortContext{
		providerNodeID: -1,
		vecArgExpr:     newVectorJoinColExpr(1, 1, "v", floatTyp),
	}))
}

func TestGetDistRangeFromFiltersWithJoinVectorArg(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	floatTyp := plan.Type{Id: int32(types.T_array_float32)}
	filter := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "<"},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_float64)},
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "l2_distance"},
						Args: []*plan.Expr{
							newVectorJoinColExpr(1, 1, "v", floatTyp),
							newVectorJoinColExpr(2, 1, "v", floatTyp),
						},
					}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_float64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{
						Value: &plan.Literal_Dval{Dval: 10},
					}},
				},
			},
		}},
	}

	remainingFilters, distRange := builder.getDistRangeFromFilters(
		[]*plan.Expr{filter},
		1,
		"l2_distance",
		newVectorJoinColExpr(2, 1, "v", floatTyp),
	)
	require.Nil(t, distRange)
	require.Equal(t, []*plan.Expr{filter}, remainingFilters)
}

func findFirstNodeByType(builder *QueryBuilder, nodeType plan.Node_NodeType) *plan.Node {
	for _, node := range builder.qry.Nodes {
		if node.NodeType == nodeType {
			return node
		}
	}
	return nil
}
