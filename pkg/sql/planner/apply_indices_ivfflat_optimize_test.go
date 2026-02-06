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

package planner

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyIndicesForSortUsingIvfflat_PushdownOptimization(t *testing.T) {
	// Setup Compiler Context with mocked variables
	baseMockCtx := NewMockCompilerContext(false)
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			switch varName {
			case "enable_vector_prefilter_by_default":
				return int8(1), nil
			case "ivf_threads_search":
				return int64(4), nil
			case "probe_limit":
				return int64(10), nil
			}
			return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
		},
	}

	// Setup table definition
	tableDef := &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}

	// Setup MultiTableIndex
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
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

	float32Typ := plan.Type{Id: int32(types.T_array_float32)}

	initTestEnv := func() (*QueryBuilder, *BindContext, int32) {
		builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
		ctx := NewBindContext(builder, nil)

		scanNode := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    tableDef,
			ObjRef:      &plan.ObjectRef{SchemaName: "db"},
			BindingTags: []int32{builder.genNewBindTag()},
		}
		scanNodeID := builder.appendNode(scanNode, ctx)

		// IMPORTANT HACK: Ensure builder has context for the nodes we create
		// This prevents "index out of range" in addBinding
		for int(scanNodeID) >= len(builder.ctxByNode) {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}
		// Also preemptively reserve IDs for nodes that will be created during optimization
		// (JOIN, SORT, FUNCTION_SCAN, etc. will call appendNode)
		for i := 0; i < 30; i++ {
			builder.ctxByNode = append(builder.ctxByNode, ctx)
		}

		return builder, ctx, scanNodeID
	}

	// 1. Case: No filters. Should NOT use pushdown (simple join)
	t.Run("NoFilters_DisablePushdown", func(t *testing.T) {
		builder, _, scanNodeID := initTestEnv()
		scanNode := builder.qry.Nodes[scanNodeID]

		distFnExpr := &plan.Function{
			Func: &plan.ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
			},
		}

		vecCtx := &vectorSortContext{
			scanNode:   scanNode,
			sortNode:   &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
			projNode:   &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scanNodeID}},
			distFnExpr: distFnExpr,
			limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
			rankOption: &plan.RankOption{Mode: "pre"},
		}

		_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
		require.NoError(t, err)

		// Verification:
		// PROJECT -> SORT -> JOIN(SCAN, FUNCTION_SCAN)
		sortNodeID := vecCtx.projNode.Children[0]
		sortNode := builder.qry.Nodes[sortNodeID]
		require.Equal(t, plan.Node_SORT, sortNode.NodeType)

		joinNodeID := sortNode.Children[0]
		joinNode := builder.qry.Nodes[joinNodeID]
		require.Equal(t, plan.Node_JOIN, joinNode.NodeType)

		// In "No Filter" case, it should NOT be the nested join structure
		// Right child should be FUNCTION_SCAN, not another JOIN
		rightChildID := joinNode.Children[1]
		rightChild := builder.qry.Nodes[rightChildID]
		assert.Equal(t, plan.Node_FUNCTION_SCAN, rightChild.NodeType)
	})

	// 2. Case: With filters. Should ENABLE pushdown (nested join)
	t.Run("WithFilters_EnablePushdown", func(t *testing.T) {
		builder, _, scanNodeID := initTestEnv()
		scanNode := builder.qry.Nodes[scanNodeID]
		// Add a non-distance filter
		scanNode.FilterList = []*plan.Expr{
			{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}},
		}

		distFnExpr := &plan.Function{
			Func: &plan.ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
			},
		}

		vecCtx := &vectorSortContext{
			scanNode:   scanNode,
			sortNode:   &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
			projNode:   &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scanNodeID}},
			distFnExpr: distFnExpr,
			limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
			rankOption: &plan.RankOption{Mode: "pre"},
		}

		_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
		require.NoError(t, err)

		// Verification:
		// PROJECT -> SORT -> JOIN(SCAN, JOIN(FUNCTION_SCAN, PROJECT(SCAN)))
		sortNodeID := vecCtx.projNode.Children[0]
		sortNode := builder.qry.Nodes[sortNodeID]
		outerJoinNodeID := sortNode.Children[0]
		outerJoinNode := builder.qry.Nodes[outerJoinNodeID]
		require.Equal(t, plan.Node_JOIN, outerJoinNode.NodeType)

		innerJoinNodeID := outerJoinNode.Children[1]
		innerJoinNode := builder.qry.Nodes[innerJoinNodeID]
		assert.Equal(t, plan.Node_JOIN, innerJoinNode.NodeType, "With filter, right child should be nested JOIN")
	})
}
