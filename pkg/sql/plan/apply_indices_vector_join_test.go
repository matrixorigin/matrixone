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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newVectorJoinTestMockCtx() *customMockCompilerContext {
	baseMockCtx := NewMockCompilerContext(false)
	return &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			switch varName {
			case "hnsw_threads_search":
				return int64(4), nil
			case "ivf_threads_search":
				return int64(4), nil
			case "probe_limit":
				return int64(10), nil
			case "enable_vector_prefilter_by_default":
				return int8(0), nil
			case "enable_vector_auto_mode_by_default":
				return int8(0), nil
			}
			return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
		},
	}
}

func newMainTableDefWithVectorIndex() *plan.TableDef {
	return &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          "idx_hnsw_v",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
				IndexTableName:     "hnsw_meta",
				Parts:              []string{"v"},
				IndexAlgoParams:    `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
			{
				IndexName:          "idx_hnsw_v",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage,
				IndexTableName:     "hnsw_storage",
				Parts:              []string{"v"},
				IndexAlgoParams:    `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
		},
	}
}

func newSubqueryTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
}

func TestBuildVectorSortContextThroughJoin(t *testing.T) {
	mockCtx := newVectorJoinTestMockCtx()
	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	mainTableDef := newMainTableDefWithVectorIndex()
	subqueryTableDef := newSubqueryTableDef()

	t.Run("SINGLE_JOIN_positive", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
		ctx := NewBindContext(builder, nil)

		mainScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    mainTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		mainScanNodeID := builder.appendNode(mainScanNode, ctx)

		subqueryScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    subqueryTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
			FilterList: []*plan.Expr{
				{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}},
			},
		}
		subqueryScanNodeID := builder.appendNode(subqueryScanNode, ctx)

		joinNode := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_SINGLE,
			Children: []int32{mainScanNodeID, subqueryScanNodeID},
		}
		joinNodeID := builder.appendNode(joinNode, ctx)

		distFnExpr := &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: mainScanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: subqueryScanNode.BindingTags[0], ColPos: 1}}},
			},
		}

		sortNode := &plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{joinNodeID},
			OrderBy: []*plan.OrderBySpec{
				{
					Expr: &plan.Expr{
						Typ:  plan.Type{Id: int32(types.T_float64)},
						Expr: &plan.Expr_F{F: distFnExpr},
					},
					Flag: plan.OrderBySpec_ASC,
				},
			},
			Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		}
		sortNodeID := builder.appendNode(sortNode, ctx)

		projNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{sortNodeID},
		}
		builder.appendNode(projNode, ctx)
		for i := 0; i < 30; i++ {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}

		vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
		require.NotNil(t, vecCtx)
		assert.Equal(t, mainScanNode, vecCtx.scanNode)
		assert.Equal(t, joinNode, vecCtx.joinNode)
		assert.Equal(t, subqueryScanNodeID, vecCtx.subqueryScanID)
		assert.NotNil(t, vecCtx.vecArgExpr)
		assert.Equal(t, subqueryScanNode.BindingTags[0], vecCtx.vecArgExpr.GetCol().RelPos)
	})

	t.Run("right_side_has_vector_index", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
		ctx := NewBindContext(builder, nil)

		// Left side is the subquery (no vector index), right side has the index
		leftScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    subqueryTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		leftScanNodeID := builder.appendNode(leftScanNode, ctx)

		rightScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    mainTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		rightScanNodeID := builder.appendNode(rightScanNode, ctx)

		joinNode := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_SINGLE,
			Children: []int32{leftScanNodeID, rightScanNodeID},
		}
		joinNodeID := builder.appendNode(joinNode, ctx)

		distFnExpr := &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rightScanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: leftScanNode.BindingTags[0], ColPos: 1}}},
			},
		}

		sortNode := &plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{joinNodeID},
			OrderBy: []*plan.OrderBySpec{
				{
					Expr: &plan.Expr{
						Typ:  plan.Type{Id: int32(types.T_float64)},
						Expr: &plan.Expr_F{F: distFnExpr},
					},
					Flag: plan.OrderBySpec_ASC,
				},
			},
			Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		}
		sortNodeID := builder.appendNode(sortNode, ctx)

		projNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{sortNodeID},
		}
		builder.appendNode(projNode, ctx)
		for i := 0; i < 30; i++ {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}

		vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
		require.NotNil(t, vecCtx)
		assert.Equal(t, rightScanNode, vecCtx.scanNode)
		assert.Equal(t, leftScanNodeID, vecCtx.subqueryScanID)
	})

	t.Run("INNER_JOIN_rejected", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
		ctx := NewBindContext(builder, nil)

		mainScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    mainTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		mainScanNodeID := builder.appendNode(mainScanNode, ctx)

		subqueryScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    subqueryTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		subqueryScanNodeID := builder.appendNode(subqueryScanNode, ctx)

		joinNode := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_INNER,
			Children: []int32{mainScanNodeID, subqueryScanNodeID},
			OnList:   []*plan.Expr{{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}},
		}
		joinNodeID := builder.appendNode(joinNode, ctx)

		distFnExpr := &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: mainScanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: subqueryScanNode.BindingTags[0], ColPos: 1}}},
			},
		}

		sortNode := &plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{joinNodeID},
			OrderBy: []*plan.OrderBySpec{
				{
					Expr: &plan.Expr{
						Typ:  plan.Type{Id: int32(types.T_float64)},
						Expr: &plan.Expr_F{F: distFnExpr},
					},
					Flag: plan.OrderBySpec_ASC,
				},
			},
			Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		}
		sortNodeID := builder.appendNode(sortNode, ctx)

		projNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{sortNodeID},
		}
		builder.appendNode(projNode, ctx)
		for i := 0; i < 30; i++ {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}

		vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
		assert.Nil(t, vecCtx, "INNER JOIN must not be rewritten — would lose OnList semantics")
	})

	t.Run("both_sides_have_vector_index_skipped", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
		ctx := NewBindContext(builder, nil)

		mainScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    mainTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		mainScanNodeID := builder.appendNode(mainScanNode, ctx)

		rightScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    mainTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		rightScanNodeID := builder.appendNode(rightScanNode, ctx)

		joinNode := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_SINGLE,
			Children: []int32{mainScanNodeID, rightScanNodeID},
		}
		joinNodeID := builder.appendNode(joinNode, ctx)

		distFnExpr := &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: mainScanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rightScanNode.BindingTags[0], ColPos: 1}}},
			},
		}

		sortNode := &plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{joinNodeID},
			OrderBy: []*plan.OrderBySpec{
				{
					Expr: &plan.Expr{
						Typ:  plan.Type{Id: int32(types.T_float64)},
						Expr: &plan.Expr_F{F: distFnExpr},
					},
					Flag: plan.OrderBySpec_ASC,
				},
			},
			Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		}
		sortNodeID := builder.appendNode(sortNode, ctx)

		projNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{sortNodeID},
		}
		builder.appendNode(projNode, ctx)
		for i := 0; i < 30; i++ {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}

		vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
		assert.Nil(t, vecCtx, "should skip when both sides have vector indexes (self-join)")
	})

	t.Run("no_limit_skipped", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
		ctx := NewBindContext(builder, nil)

		mainScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    mainTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		mainScanNodeID := builder.appendNode(mainScanNode, ctx)

		subqueryScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    subqueryTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		subqueryScanNodeID := builder.appendNode(subqueryScanNode, ctx)

		joinNode := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_SINGLE,
			Children: []int32{mainScanNodeID, subqueryScanNodeID},
		}
		joinNodeID := builder.appendNode(joinNode, ctx)

		distFnExpr := &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: mainScanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: subqueryScanNode.BindingTags[0], ColPos: 1}}},
			},
		}

		sortNode := &plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{joinNodeID},
			OrderBy: []*plan.OrderBySpec{
				{
					Expr: &plan.Expr{
						Typ:  plan.Type{Id: int32(types.T_float64)},
						Expr: &plan.Expr_F{F: distFnExpr},
					},
					Flag: plan.OrderBySpec_ASC,
				},
			},
		}
		sortNodeID := builder.appendNode(sortNode, ctx)

		projNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{sortNodeID},
		}
		builder.appendNode(projNode, ctx)
		for i := 0; i < 30; i++ {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}

		vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
		assert.Nil(t, vecCtx, "should skip when there's no LIMIT")
	})

	t.Run("Project_Sort_Project_Join_pattern", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
		ctx := NewBindContext(builder, nil)

		mainScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    mainTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		mainScanNodeID := builder.appendNode(mainScanNode, ctx)

		subqueryScanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    subqueryTableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		subqueryScanNodeID := builder.appendNode(subqueryScanNode, ctx)

		joinNode := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_SINGLE,
			Children: []int32{mainScanNodeID, subqueryScanNodeID},
		}
		joinNodeID := builder.appendNode(joinNode, ctx)

		distFnExpr := &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: mainScanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: subqueryScanNode.BindingTags[0], ColPos: 1}}},
			},
		}

		// Extra PROJECT between Sort and Join (as happens with aliases like "AS distance")
		innerProjTag := builder.genNewBindTag()
		innerProjNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{joinNodeID},
			ProjectList: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_F{F: distFnExpr}},
			},
			BindingTags: []int32{innerProjTag},
		}
		innerProjNodeID := builder.appendNode(innerProjNode, ctx)

		// Sort references the inner projection's output
		sortNode := &plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{innerProjNodeID},
			OrderBy: []*plan.OrderBySpec{
				{
					Expr: &plan.Expr{
						Typ:  plan.Type{Id: int32(types.T_float64)},
						Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: innerProjTag, ColPos: 0}},
					},
					Flag: plan.OrderBySpec_ASC,
				},
			},
			Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		}
		sortNodeID := builder.appendNode(sortNode, ctx)

		projNode := &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{sortNodeID},
		}
		builder.appendNode(projNode, ctx)
		for i := 0; i < 30; i++ {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}

		vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
		require.NotNil(t, vecCtx, "should handle Project -> Sort -> Project -> Join pattern")
		assert.Equal(t, mainScanNode, vecCtx.scanNode)
		assert.Equal(t, innerProjNode, vecCtx.childNode)
	})
}

// TestApplyIndicesForSortUsingHnsw_JoinThrough tests the full HNSW rewrite path.
func TestApplyIndicesForSortUsingHnsw_JoinThrough(t *testing.T) {
	mockCtx := newVectorJoinTestMockCtx()
	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	mainTableDef := newMainTableDefWithVectorIndex()
	subqueryTableDef := newSubqueryTableDef()

	multiTableIndex := &MultiTableIndex{
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

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)

	mainScanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    mainTableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	mainScanNodeID := builder.appendNode(mainScanNode, ctx)

	subqueryScanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    subqueryTableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	subqueryScanNodeID := builder.appendNode(subqueryScanNode, ctx)

	joinNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_SINGLE,
		Children: []int32{mainScanNodeID, subqueryScanNodeID},
	}
	joinNodeID := builder.appendNode(joinNode, ctx)

	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: mainScanNode.BindingTags[0], ColPos: 1}}},
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: subqueryScanNode.BindingTags[0], ColPos: 1}}},
		},
	}

	sortNode := &plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinNodeID},
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ:  plan.Type{Id: int32(types.T_float64)},
					Expr: &plan.Expr_F{F: distFnExpr},
				},
				Flag: plan.OrderBySpec_ASC,
			},
		},
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
	}
	sortNodeID := builder.appendNode(sortNode, ctx)

	projNode := &plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{sortNodeID},
	}
	projNodeID := builder.appendNode(projNode, ctx)

	for int(projNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	for i := 0; i < 30; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
	require.NotNil(t, vecCtx)

	newNodeID, err := builder.applyIndicesForSortUsingHnsw(projNodeID, vecCtx, multiTableIndex)
	require.NoError(t, err)
	assert.Equal(t, projNodeID, newNodeID)

	// Verify plan: projNode -> Sort -> Join(scanNode, FUNCTION_SCAN(child: subquery))
	newSortNodeID := projNode.Children[0]
	newSortNode := builder.qry.Nodes[newSortNodeID]
	require.Equal(t, plan.Node_SORT, newSortNode.NodeType)

	newJoinNodeID := newSortNode.Children[0]
	newJoinNode := builder.qry.Nodes[newJoinNodeID]
	require.Equal(t, plan.Node_JOIN, newJoinNode.NodeType)
	assert.Equal(t, mainScanNodeID, newJoinNode.Children[0])

	funcScanNode := builder.qry.Nodes[newJoinNode.Children[1]]
	assert.Equal(t, plan.Node_FUNCTION_SCAN, funcScanNode.NodeType)
	require.Len(t, funcScanNode.Children, 1)
	assert.Equal(t, subqueryScanNodeID, funcScanNode.Children[0])
}

// TestApplyIndicesForSortUsingIvfflat_JoinThrough tests the full IVF rewrite path.
func TestApplyIndicesForSortUsingIvfflat_JoinThrough(t *testing.T) {
	mockCtx := newVectorJoinTestMockCtx()
	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`

	mainTableDef := &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          "idx_ivf_v",
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
				IndexTableName:     "meta",
				Parts:              []string{"v"},
				IndexAlgoParams:    `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
		},
	}

	subqueryTableDef := newSubqueryTableDef()

	multiTableIndex := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				IndexTableName:  "centroids",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {
				IndexTableName: "entries",
			},
		},
	}

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)

	mainScanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    mainTableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	mainScanNodeID := builder.appendNode(mainScanNode, ctx)

	subqueryScanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    subqueryTableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	subqueryScanNodeID := builder.appendNode(subqueryScanNode, ctx)

	joinNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_SINGLE,
		Children: []int32{mainScanNodeID, subqueryScanNodeID},
	}
	joinNodeID := builder.appendNode(joinNode, ctx)

	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: mainScanNode.BindingTags[0], ColPos: 1}}},
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: subqueryScanNode.BindingTags[0], ColPos: 1}}},
		},
	}

	sortNode := &plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinNodeID},
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ:  plan.Type{Id: int32(types.T_float64)},
					Expr: &plan.Expr_F{F: distFnExpr},
				},
				Flag: plan.OrderBySpec_ASC,
			},
		},
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
	}
	sortNodeID := builder.appendNode(sortNode, ctx)

	projNode := &plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{sortNodeID},
	}
	projNodeID := builder.appendNode(projNode, ctx)

	for int(projNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	for i := 0; i < 40; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	vecCtx := builder.buildVectorSortContextThroughJoin(projNode)
	require.NotNil(t, vecCtx)

	newNodeID, err := builder.applyIndicesForSortUsingIvfflat(projNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, projNodeID, newNodeID)

	// Verify FUNCTION_SCAN has subquery as child
	newSortNodeID := projNode.Children[0]
	newSortNode := builder.qry.Nodes[newSortNodeID]
	require.Equal(t, plan.Node_SORT, newSortNode.NodeType)

	newJoinNodeID := newSortNode.Children[0]
	newJoinNode := builder.qry.Nodes[newJoinNodeID]
	require.Equal(t, plan.Node_JOIN, newJoinNode.NodeType)
	assert.Equal(t, mainScanNodeID, newJoinNode.Children[0])

	// Find the FUNCTION_SCAN — it may be inside a nested join for pushdown
	rightChild := builder.qry.Nodes[newJoinNode.Children[1]]
	var funcScanNode *plan.Node
	if rightChild.NodeType == plan.Node_FUNCTION_SCAN {
		funcScanNode = rightChild
	} else if rightChild.NodeType == plan.Node_JOIN {
		// pushdown creates nested joins; find the function scan
		for _, childID := range rightChild.Children {
			child := builder.qry.Nodes[childID]
			if child.NodeType == plan.Node_FUNCTION_SCAN {
				funcScanNode = child
				break
			}
		}
	}
	require.NotNil(t, funcScanNode, "FUNCTION_SCAN should exist in the rewritten plan")
	require.Len(t, funcScanNode.Children, 1)
	assert.Equal(t, subqueryScanNodeID, funcScanNode.Children[0])
}
