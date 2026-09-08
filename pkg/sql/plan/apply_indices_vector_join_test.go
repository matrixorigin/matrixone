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
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
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
				IndexName:          "idx_hnsw_v",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
				IndexTableName:     "hnsw_meta",
				Parts:              []string{"v"},
				IndexAlgoParams:    idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				IndexName:          "idx_hnsw_v",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage,
				IndexTableName:     "hnsw_storage",
				Parts:              []string{"v"},
				IndexAlgoParams:    idxAlgoParams,
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
				IndexName:          "idx_ivf_v",
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
				IndexTableName:     "ivf_meta",
				Parts:              []string{"v"},
				IndexAlgoParams:    idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				IndexName:          "idx_ivf_v",
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
				IndexTableName:     "ivf_centroids",
				Parts:              []string{"v"},
				IndexAlgoParams:    idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {
				IndexName:          "idx_ivf_v",
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				IndexTableName:     "ivf_entries",
				Parts:              []string{"v"},
				IndexAlgoParams:    idxAlgoParams,
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

func newVectorJoinSemiOn(leftTag, rightTag int32) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "="}, Args: []*plan.Expr{
			newVectorJoinColExpr(leftTag, 0, "id", plan.Type{Id: int32(types.T_varchar)}),
			newVectorJoinColExpr(rightTag, 0, "id", plan.Type{Id: int32(types.T_varchar)}),
		},
	}}}
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
	if opts.joinType == plan.Node_SEMI {
		for _, indexDef := range newVectorJoinIvfIndex().IndexDefs {
			mainTableDef.Indexes = append(mainTableDef.Indexes, indexDef)
		}
	}
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
	if opts.joinType == plan.Node_SEMI {
		joinNode.OnList = []*plan.Expr{newVectorJoinSemiOn(mainScanNode.BindingTags[0], providerScanNode.BindingTags[0])}
	}
	joinNodeID := builder.appendNode(joinNode, ctx)

	distFnExpr := &plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			newVectorJoinColExpr(mainScanNode.BindingTags[0], 1, "v", mainVecTyp),
			newVectorJoinColExpr(providerScanNode.BindingTags[0], 1, "v", providerVecTyp),
		},
	}
	if opts.joinType == plan.Node_SEMI {
		distFnExpr.Args[1] = newVectorJoinStringLitExpr()
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

func TestBuildVectorSortContextThroughJoin_SemiMembership(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)
	require.Equal(t, tc.mainScanNodeID, vecCtx.scanNode.NodeId)
	require.Equal(t, tc.builder.qry.Nodes[tc.projNode.Children[0]].Children[0], vecCtx.membershipNodeID)
	require.Nil(t, vecCtx.vecArgExpr)
}

func TestApplyIndicesForSortUsingIvfflat_SemiMembership(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)
	originalMembershipNode := tc.builder.qry.Nodes[vecCtx.membershipNodeID]
	originalMembershipLeftID := originalMembershipNode.Children[0]
	membershipInput := tc.builder.qry.Nodes[originalMembershipNode.Children[1]]
	membershipInput.Limit = makePlan2Uint64ConstExprWithType(2)
	membershipInput.Offset = makePlan2Uint64ConstExprWithType(1)

	pluginCtx, pluginIndex := toPlanplugin(vecCtx, newVectorJoinIvfIndex())
	newNodeID, applied, err := tc.builder.ApplyIndicesForSortUsingIvfflat(pluginCtx, pluginIndex, tc.projNodeID, planplugin.ApplyForSortOpts{})
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, tc.projNodeID, newNodeID)

	reachable := reachableNodeIDsFrom(tc.builder.qry, tc.projNodeID)
	require.False(t, reachable[originalMembershipNode.NodeId], "the rewrite must use a copy so fallback keeps the original plan intact")
	require.Equal(t, originalMembershipLeftID, originalMembershipNode.Children[0])
	require.Equal(t, uint64(2), membershipInput.Limit.GetLit().GetU64Val(), "membership subquery LIMIT is semantic")
	require.Equal(t, uint64(1), membershipInput.Offset.GetLit().GetU64Val(), "membership subquery OFFSET is semantic")
	require.True(t, reachable[tc.mainScanNodeID], "the original scan must remain the row-fetch side")

	membershipProducer := findReachableSemiJoinWithRight(tc.builder.qry, tc.projNodeID, tc.providerNodeID)
	require.NotNil(t, membershipProducer, "a copied membership SEMI JOIN must feed the runtime-filter producer")
	require.NotEqual(t, originalMembershipNode.NodeId, membershipProducer.NodeId)

	vectorScan := findFirstNodeByType(tc.builder, plan.Node_VECTOR_INDEX_SCAN)
	require.NotNil(t, vectorScan)
	require.True(t, reachable[vectorScan.NodeId])
	require.Len(t, vectorScan.RuntimeFilterProbeList, 1)
	require.True(t, vectorScan.RuntimeFilterProbeList[0].UseMembershipFilter)
	require.True(t, vectorScan.RuntimeFilterProbeList[0].MustApply)
	buildCount := 0
	for nodeID := range reachable {
		for _, spec := range tc.builder.qry.Nodes[nodeID].RuntimeFilterBuildList {
			buildCount++
			require.True(t, spec.UseMembershipFilter)
			require.True(t, spec.MustApply)
		}
	}
	require.Equal(t, 1, buildCount)
}

func TestApplyIndicesForSortUsingIvfflat_SemiMembershipRemapsCoveringIndexColumns(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	mainScan := tc.builder.qry.Nodes[tc.mainScanNodeID]
	varcharType := plan.Type{Id: int32(types.T_varchar)}
	mainScan.TableDef.Cols = append(mainScan.TableDef.Cols,
		&plan.ColDef{Name: "category", Typ: varcharType},
		&plan.ColDef{Name: "document_id", Typ: varcharType})
	mainScan.TableDef.Name2ColIndex["category"] = 2
	mainScan.TableDef.Name2ColIndex["document_id"] = 3
	const indexTable = "__mo_index_category_document"
	mainScan.TableDef.Indexes = append(mainScan.TableDef.Indexes, &plan.IndexDef{
		IndexName:      "idx_category_document",
		IndexAlgo:      "btree",
		IndexTableName: indexTable,
		TableExist:     true,
		Parts:          []string{"category", "document_id", catalog.CreateAlias("id")},
	})
	mainScan.FilterList = []*plan.Expr{
		newVectorJoinConstEqFilter(mainScan.BindingTags[0], 2, "category", varcharType),
	}
	membershipNode := tc.builder.qry.Nodes[tc.builder.qry.Nodes[tc.projNode.Children[0]].Children[0]]
	providerScan := tc.builder.qry.Nodes[tc.providerNodeID]
	membershipNode.OnList = []*plan.Expr{{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				newVectorJoinColExpr(mainScan.BindingTags[0], 3, "document_id", varcharType),
				newVectorJoinColExpr(providerScan.BindingTags[0], 0, "id", varcharType),
			},
		}},
	}}
	mockCtx := tc.builder.compCtx.(*customMockCompilerContext)
	mockCtx.tables[indexTable] = &plan.TableDef{
		Name: indexTable,
		Cols: []*plan.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: varcharType},
			{Name: catalog.IndexTablePrimaryColName, Typ: varcharType},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}
	mockCtx.objects[indexTable] = &plan.ObjectRef{SchemaName: "db", ObjName: indexTable}

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)
	pluginCtx, pluginIndex := toPlanplugin(vecCtx, newVectorJoinIvfIndex())
	_, applied, err := tc.builder.ApplyIndicesForSortUsingIvfflat(
		pluginCtx, pluginIndex, tc.projNodeID, planplugin.ApplyForSortOpts{
			ColRefCnt: map[[2]int32]int{},
			IdxColMap: map[[2]int32]*plan.Expr{},
		})
	require.NoError(t, err)
	require.True(t, applied)

	producer := findReachableSemiJoinWithRight(tc.builder.qry, tc.projNodeID, tc.providerNodeID)
	require.NotNil(t, producer)
	indexedInput := tc.builder.qry.Nodes[producer.Children[0]]
	require.Equal(t, plan.Node_TABLE_SCAN, indexedInput.NodeType)
	require.Equal(t, "idx_category_document", indexedInput.IndexScanInfo.IndexName)
	availableTags := tc.builder.collectBindingTags(producer)
	for _, expr := range producer.OnList {
		require.True(t, exprRefsOnlyAvailableTags(expr, availableTags),
			"membership expression must reference the optimized input's actual output")
	}
	leftKey := producer.OnList[0].GetF().Args[0]
	require.NotNil(t, leftKey.GetF(), "document_id must be remapped to the composite index expression")
}

func TestApplyLogicalVectorIndexForSortContext_SemiMembershipUsesIvf(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)

	newNodeID, handled, err := tc.builder.applyLogicalVectorIndexForSortContext(
		tc.projNodeID, vecCtx, map[[2]int32]int{}, map[[2]int32]*plan.Expr{})
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, tc.projNodeID, newNodeID)
	vectorScan := findFirstNodeByType(tc.builder, plan.Node_VECTOR_INDEX_SCAN)
	require.NotNil(t, vectorScan)
	require.NotNil(t, findReachableSemiJoinWithRight(tc.builder.qry, tc.projNodeID, tc.providerNodeID))
}

func TestApplyVectorIndexForSortContext_SemiMembershipUsesIvfWithMixedIndexes(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)

	newNodeID, applied, err := tc.builder.applyVectorIndexForSortContext(
		tc.projNodeID, vecCtx, map[[2]int32]int{}, map[[2]int32]*plan.Expr{})
	require.NoError(t, err)
	require.True(t, applied, "central dispatch must select IVF-FLAT for a membership context")
	require.Equal(t, tc.projNodeID, newNodeID)
	vectorScan := findFirstNodeByType(tc.builder, plan.Node_VECTOR_INDEX_SCAN)
	require.NotNil(t, vectorScan)
	require.Equal(t, catalog.MoIndexIvfFlatAlgo.ToString(), vectorScan.VectorIndexScan.Index.IndexAlgo)
	require.NotNil(t, findReachableSemiJoinWithRight(tc.builder.qry, tc.projNodeID, tc.providerNodeID))
}

func TestBuildVectorSortContextThroughJoin_SemiMembershipRejectsNonIvfIndex(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	tc.builder.qry.Nodes[tc.mainScanNodeID].TableDef.Indexes = newVectorJoinTableDef(true, false).Indexes
	require.Nil(t, tc.builder.buildVectorSortContextThroughJoin(tc.projNode))
}

func exprRefsOnlyAvailableTags(expr *plan.Expr, available map[int32]bool) bool {
	if expr == nil {
		return true
	}
	if col := expr.GetCol(); col != nil {
		return available[col.RelPos]
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if !exprRefsOnlyAvailableTags(arg, available) {
				return false
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if !exprRefsOnlyAvailableTags(item, available) {
				return false
			}
		}
	}
	return true
}

func findReachableSemiJoinWithRight(query *plan.Query, rootID, rightID int32) *plan.Node {
	for nodeID := range reachableNodeIDsFrom(query, rootID) {
		node := query.Nodes[nodeID]
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_SEMI &&
			len(node.Children) == 2 && node.Children[1] == rightID {
			return node
		}
	}
	return nil
}

func reachableNodeIDsFrom(query *plan.Query, rootID int32) map[int32]bool {
	reachable := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || reachable[nodeID] {
			return
		}
		reachable[nodeID] = true
		for _, childID := range query.Nodes[nodeID].Children {
			visit(childID)
		}
	}
	visit(rootID)
	return reachable
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

	newNodeID, err := tc.builder.applyIndicesForSortUsingHnsw(tc.projNodeID, vecCtx, newVectorJoinHnswIndex(), nil)
	require.NoError(t, err)
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

	vectorScan := findFirstNodeByType(tc.builder, plan.Node_VECTOR_INDEX_SCAN)
	require.NotNil(t, vectorScan)
	require.Empty(t, vectorScan.Children)
	require.Equal(t, int32(1), vectorScan.VectorIndexScan.QueryVector.GetCol().ColPos)
	applyNode := findFirstNodeByType(tc.builder, plan.Node_APPLY)
	require.NotNil(t, applyNode)
	require.Equal(t, []int32{tc.providerNodeID, vectorScan.NodeId}, applyNode.Children)
}

func TestApplyIndicesForSortUsingIvfflatPreservesScanSnapshot(t *testing.T) {
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
		providerSingle:        true,
		providerVectorNotNull: true,
	})
	snapshot := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 123},
		Tenant: &plan.SnapshotTenant{TenantID: 7},
	}
	tc.builder.qry.Nodes[tc.mainScanNodeID].ScanSnapshot = snapshot

	vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
	require.NotNil(t, vecCtx)
	_, err := tc.builder.applyIndicesForSortUsingIvfflat(
		tc.projNodeID, vecCtx, newVectorJoinIvfIndex(), nil, nil)
	require.NoError(t, err)

	vectorScan := findFirstNodeByType(tc.builder, plan.Node_VECTOR_INDEX_SCAN)
	require.NotNil(t, vectorScan)
	require.Equal(t, snapshot, vectorScan.ScanSnapshot)
	require.Equal(t, snapshot, vectorScan.VectorIndexScan.ScanSnapshot)
	require.NotSame(t, snapshot, vectorScan.ScanSnapshot)
	require.NotSame(t, snapshot, vectorScan.VectorIndexScan.ScanSnapshot)

	clone := DeepCopyNode(vectorScan)
	require.Equal(t, snapshot, clone.ScanSnapshot)
	require.Equal(t, snapshot, clone.VectorIndexScan.ScanSnapshot)
	require.NotSame(t, vectorScan.ScanSnapshot, clone.ScanSnapshot)
	require.NotSame(t, vectorScan.VectorIndexScan.ScanSnapshot, clone.VectorIndexScan.ScanSnapshot)
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

	// Narrow vector element types (bf16/f16/int8/uint8) must push down the JOIN too,
	// matching the direct getArgsFromDistFn (which uses IsArrayRelate). The old
	// hardcoded f32/f64 check made these fall through to brute-force execution.
	for _, narrow := range []types.T{
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8,
	} {
		nTyp := plan.Type{Id: int32(narrow)}
		nScan := newVectorJoinColExpr(scanTag, 1, "v", nTyp)
		nProvider := newVectorJoinColExpr(providerTag, 1, "v", nTyp)
		key, value, found := builder.getArgsFromDistFnForJoin(&plan.Function{
			Func: &plan.ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{nProvider, nScan},
		}, 1, scanTag)
		require.True(t, found, "narrow vector %v join must push down", narrow)
		require.Equal(t, nScan, key)
		require.Equal(t, nProvider, value)
		require.Equal(t, nScan.Typ, nProvider.Typ)
	}
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
	tableDef.Indexes = []*plan.IndexDef{nil, {Unique: true, Parts: []string{"id", "v"}}}
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
	vectorDef := newVectorJoinTableDef(true, false)
	vectorDef.Indexes = append([]*plan.IndexDef{nil}, vectorDef.Indexes...)
	vectorScan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: vectorDef, BindingTags: []int32{1}}
	require.Same(t, vectorScan, builder.directScanWithVectorIndex(vectorScan))

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
