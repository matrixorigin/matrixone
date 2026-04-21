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
	"github.com/stretchr/testify/require"
)

func TestBuildIvfSearchColDefsIncludesCoveringColumnsWithoutMutatingBase(t *testing.T) {
	original := &plan.TableDef{
		Name2ColIndex: map[string]int32{
			"title":  0,
			"rank":   1,
			"unused": 2,
		},
		Cols: []*plan.ColDef{
			{Name: "title", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "rank", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "unused", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	colDefs := buildIvfSearchColDefs([]string{"title", "rank"}, original)
	require.Len(t, kIVFSearchColDefs, 2)
	require.Len(t, colDefs, 4)
	require.Equal(t, "pkid", colDefs[0].Name)
	require.Equal(t, "score", colDefs[1].Name)
	require.Equal(t, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"title", colDefs[2].Name)
	require.Equal(t, int32(types.T_varchar), colDefs[2].Typ.Id)
	require.Equal(t, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"rank", colDefs[3].Name)
	require.Equal(t, int32(types.T_int32), colDefs[3].Typ.Id)
}

func TestApplyIndicesForSortUsingIvfflatBuildsDynamicColsForOptimizerPath(t *testing.T) {
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
		Name: "t1",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
			{Name: "title", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1, "title": 2},
	}

	idxAlgoParams := `{"op_type":"` + metric.DistFuncOpTypes["l2_distance"] + `","include_columns":"title"}`
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
				IndexTableName:  "entries",
				IndexAlgoParams: idxAlgoParams,
			},
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
	scanNodeID := builder.appendNode(scanNode, ctx)
	for int(scanNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	for i := 0; i < 10; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
			{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}

	vecCtx := &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
		projNode: &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*plan.Expr{
				{
					Typ: tableDef.Cols[2].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 2, Name: "title"},
					},
				},
			},
		},
		distFnExpr: distFnExpr,
		limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
	}

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, nil, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	tableFuncNode := builder.qry.Nodes[sortNode.Children[0]]

	require.Equal(t, plan.Node_FUNCTION_SCAN, tableFuncNode.NodeType)
	require.Len(t, tableFuncNode.TableDef.Cols, 3)
	require.Equal(t, "pkid", tableFuncNode.TableDef.Cols[0].Name)
	require.Equal(t, "score", tableFuncNode.TableDef.Cols[1].Name)
	require.Equal(t, catalog.SystemSI_IVFFLAT_IncludeColPrefix+"title", tableFuncNode.TableDef.Cols[2].Name)
}
