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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

// rewriteIvfflatProjectingTitle applies the IVFFLAT sort rewrite to
// SELECT title FROM t1 ORDER BY l2_distance(v, '[1,1,1]') LIMIT 10 and returns the
// builder and the base table scan node.
func rewriteIvfflatProjectingTitle(t *testing.T, includedColumns []string) (*QueryBuilder, *plan.Node) {
	return rewriteIvfflat(t, includedColumns, false)
}

// rewriteIvfflat applies the IVFFLAT sort rewrite to SELECT title FROM t1 [WHERE
// title = 'x'] ORDER BY l2_distance(v, '[1,1,1]') LIMIT 10, with the filter pushed down
// to the search when filtered.
func rewriteIvfflat(t *testing.T, includedColumns []string, filtered bool) (*QueryBuilder, *plan.Node) {
	baseMockCtx := NewMockCompilerContext(false, newPlanTestProcess(t))
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			switch varName {
			case "enable_vector_prefilter_by_default":
				if filtered {
					return int8(1), nil
				}
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
		Name: "t1",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
			{Name: "title", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1, "title": 2},
	}

	idxAlgoParams := `{"op_type":"` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	indexDef := func(tblType, tblName string) *plan.IndexDef {
		return &plan.IndexDef{
			IndexName:          "idx",
			IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexAlgoTableType: tblType,
			IndexTableName:     tblName,
			IndexAlgoParams:    idxAlgoParams,
			Parts:              []string{"v"},
			IncludedColumns:    includedColumns,
		}
	}
	multiTableIndex := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.SystemSI_IVFFLAT_TblType_Metadata:  indexDef(catalog.SystemSI_IVFFLAT_TblType_Metadata, "meta"),
			catalog.SystemSI_IVFFLAT_TblType_Centroids: indexDef(catalog.SystemSI_IVFFLAT_TblType_Centroids, "centroids"),
			catalog.SystemSI_IVFFLAT_TblType_Entries:   indexDef(catalog.SystemSI_IVFFLAT_TblType_Entries, "entries"),
		},
	}

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)

	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	if filtered {
		titleEq, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			{Typ: tableDef.Cols[2].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 2, Name: "title"}}},
			makePlan2StringConstExprWithType("x"),
		})
		require.NoError(t, err)
		scanNode.FilterList = []*plan.Expr{titleEq}
	}
	scanNodeID := builder.appendNode(scanNode, ctx)
	for int(scanNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	for i := 0; i < 10; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	vecCtx := &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
		projNode: &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*plan.Expr{{
				Typ:  tableDef.Cols[2].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 2, Name: "title"}},
			}},
		},
		distFnExpr: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
				{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
			},
		},
		limit:       &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		resultLimit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
	}

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)
	return builder, scanNode
}

func hasJoinOnChild(nodes []*plan.Node, childID int32) bool {
	for _, n := range nodes {
		if n.NodeType != plan.Node_JOIN {
			continue
		}
		for _, c := range n.Children {
			if c == childID {
				return true
			}
		}
	}
	return false
}

func TestIvfflatJoinBackScanIsNarrow(t *testing.T) {
	builder, scanNode := rewriteIvfflatProjectingTitle(t, nil)
	require.True(t, hasJoinOnChild(builder.qry.Nodes, scanNode.NodeId),
		"a projected column outside the index must be read back from the base table")
	require.True(t, scanNode.NarrowScan)
}

func TestIvfflatPreFilterJoinBackScanIsNarrow(t *testing.T) {
	builder, scanNode := rewriteIvfflat(t, nil, true)
	require.True(t, hasJoinOnChild(builder.qry.Nodes, scanNode.NodeId),
		"the pre-filtered candidates are joined back to the base table")
	require.True(t, scanNode.NarrowScan)
	for _, n := range builder.qry.Nodes {
		if n.NodeType == plan.Node_TABLE_SCAN && n != scanNode && n.TableDef.GetName() == "t1" {
			require.False(t, n.NarrowScan, "the filter scan reads the whole base table")
		}
	}
}

func TestIvfflatIndexOnlyBaseScanIsNotNarrow(t *testing.T) {
	builder, scanNode := rewriteIvfflatProjectingTitle(t, []string{"title"})
	require.False(t, hasJoinOnChild(builder.qry.Nodes, scanNode.NodeId),
		"an INCLUDE column is served by the index without reading the base table")
	require.False(t, scanNode.NarrowScan)
}

func TestDeepCopyNodeKeepsNarrowScan(t *testing.T) {
	for _, v := range []bool{false, true} {
		node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, NarrowScan: v}
		require.Equal(t, v, DeepCopyNode(node).NarrowScan)
	}
}
