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
	"context"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// customMockCompilerContext extends MockCompilerContext with custom ResolveVariable
type customMockCompilerContext struct {
	*MockCompilerContext
	resolveVarFunc func(string, bool, bool) (interface{}, error)
}

func (c *customMockCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if c.resolveVarFunc != nil {
		return c.resolveVarFunc(varName, isSystemVar, isGlobalVar)
	}
	return c.MockCompilerContext.ResolveVariable(varName, isSystemVar, isGlobalVar)
}

func makeConsistentHnswMultiTableIndexForTest(indexName, idxAlgoParams string, parts []string) *MultiTableIndex {
	clonedParts := append([]string(nil), parts...)
	return &MultiTableIndex{
		IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexName:          indexName,
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
				Parts:              append([]string(nil), clonedParts...),
				IndexAlgoParams:    idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				IndexName:          indexName,
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage,
				Parts:              append([]string(nil), clonedParts...),
				IndexAlgoParams:    idxAlgoParams,
			},
		},
	}
}

// Early rejection is read-only and needs one planner process for all cases.
func TestPrepareHnswIndexContextRejectsUnsupportedInputs(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	distance := &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}
	cases := []struct {
		name   string
		vecCtx *vectorSortContext
		index  *MultiTableIndex
	}{
		{name: "NilVecCtx", vecCtx: nil, index: &MultiTableIndex{}},
		{name: "NilMultiTableIndex", vecCtx: &vectorSortContext{}, index: nil},
		{name: "NilDistFnExpr", vecCtx: &vectorSortContext{
			distFnExpr: nil,
		}, index: &MultiTableIndex{}},
		{name: "ForceModeEnabled", vecCtx: &vectorSortContext{
			distFnExpr: distance,
			rankOption: &plan.RankOption{
				Mode: "force",
			},
		}, index: &MultiTableIndex{}},
		{name: "ImplicitDescendingOrderDisablesRewrite", vecCtx: &vectorSortContext{
			distFnExpr:    distance,
			sortDirection: plan.OrderBySpec_DESC,
		}, index: &MultiTableIndex{}},
		{name: "ExplicitDescendingOrderFallsBackToOriginalSearch", vecCtx: &vectorSortContext{
			distFnExpr:    distance,
			sortDirection: plan.OrderBySpec_DESC,
			rankOption:    &plan.RankOption{Mode: "post"},
		}, index: &MultiTableIndex{}},
		{name: "NilMetaDef", vecCtx: &vectorSortContext{
			distFnExpr: distance,
		}, index: &MultiTableIndex{
			IndexDefs: map[string]*plan.IndexDef{
				catalog.Hnsw_TblType_Metadata: nil,
				catalog.Hnsw_TblType_Storage:  {},
			},
		}},
		{name: "NilIdxDef", vecCtx: &vectorSortContext{
			distFnExpr: distance,
		}, index: &MultiTableIndex{
			IndexDefs: map[string]*plan.IndexDef{
				catalog.Hnsw_TblType_Metadata: {},
				catalog.Hnsw_TblType_Storage:  nil,
			},
		}},
		{name: "InvalidIndexAlgoParams", vecCtx: &vectorSortContext{
			distFnExpr: distance,
		}, index: &MultiTableIndex{
			IndexDefs: map[string]*plan.IndexDef{
				catalog.Hnsw_TblType_Metadata: {
					IndexAlgoParams: "invalid json",
				},
				catalog.Hnsw_TblType_Storage: {},
			},
		}},
		{name: "MissingOpType", vecCtx: &vectorSortContext{
			distFnExpr: distance,
		}, index: &MultiTableIndex{
			IndexDefs: map[string]*plan.IndexDef{
				catalog.Hnsw_TblType_Metadata: {
					IndexAlgoParams: `{"other_field": "value"}`,
				},
				catalog.Hnsw_TblType_Storage: {},
			},
		}},
		{name: "OpTypeNotString", vecCtx: &vectorSortContext{
			distFnExpr: distance,
		}, index: &MultiTableIndex{
			IndexDefs: map[string]*plan.IndexDef{
				catalog.Hnsw_TblType_Metadata: {
					IndexAlgoParams: `{"op_type": 123}`,
				},
				catalog.Hnsw_TblType_Storage: {},
			},
		}},
		{name: "OpTypeMismatch", vecCtx: &vectorSortContext{
			distFnExpr: distance,
		}, index: &MultiTableIndex{
			IndexDefs: map[string]*plan.IndexDef{
				catalog.Hnsw_TblType_Metadata: {
					IndexAlgoParams: `{"op_type": "cosine_similarity"}`,
				},
				catalog.Hnsw_TblType_Storage: {},
			},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := builder.prepareHnswIndexContext(tc.vecCtx, tc.index)
			require.NoError(t, err)
			require.Nil(t, result)
		})
	}
}

// TestPrepareHnswIndexContext_ArgsNotFound tests the case where getArgsFromDistFn returns found=false
func TestPrepareHnswIndexContext_ArgsNotFound(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)

	// Create a scan node with proper table def
	scanNode := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "test_table",
			Name2ColIndex: map[string]int32{
				"vec_col": 0,
				"id":      1,
			},
			Cols: []*plan.ColDef{
				{
					Name: "vec_col",
					Typ: plan.Type{
						Id: int32(types.T_array_float32),
					},
				},
				{
					Name: "id",
					Typ: plan.Type{
						Id: int32(types.T_int64),
					},
				},
			},
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
		},
	}

	// Create distFnExpr that will fail getArgsFromDistFn
	// (e.g., both args are literals instead of col + literal)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{},
					},
				},
			},
		},
		scanNode: scanNode,
	}

	multiTableIndex := makeConsistentHnswMultiTableIndexForTest(
		"idx_hnsw_args_not_found",
		`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`,
		[]string{"vec_col"},
	)

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_ResolveVariableError tests the case where ResolveVariable returns an error
func TestPrepareHnswIndexContext_ResolveVariableError(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true, newPlanTestProcess(t))
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "hnsw_threads_search" {
				return nil, moerr.NewInternalError(context.Background(), "test error")
			}
			return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
		},
	}

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)

	// Create a properly configured vecCtx
	scanNode := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "test_table",
			Name2ColIndex: map[string]int32{
				"vec_col": 0,
				"id":      1,
			},
			Cols: []*plan.ColDef{
				{
					Name: "vec_col",
					Typ: plan.Type{
						Id: int32(types.T_array_float32),
					},
				},
				{
					Name: "id",
					Typ: plan.Type{
						Id: int32(types.T_int64),
					},
				},
			},
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
		},
	}

	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: 0,
						},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{},
					},
				},
			},
		},
		scanNode: scanNode,
	}

	multiTableIndex := makeConsistentHnswMultiTableIndexForTest(
		"idx_hnsw_resolve_variable",
		`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`,
		[]string{"vec_col"},
	)

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "test error")
}

// TestPrepareHnswIndexContext_Success tests the successful case where all conditions are met
func TestPrepareHnswIndexContext_Success(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true, newPlanTestProcess(t))
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "hnsw_threads_search" {
				return int64(4), nil
			}
			return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
		},
	}

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)

	// Create a properly configured vecCtx
	scanNode := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "test_table",
			Name2ColIndex: map[string]int32{
				"vec_col": 0,
				"id":      1,
			},
			Cols: []*plan.ColDef{
				{
					Name: "vec_col",
					Typ: plan.Type{
						Id: int32(types.T_array_float32),
					},
				},
				{
					Name: "id",
					Typ: plan.Type{
						Id:    int32(types.T_int64),
						Width: 64,
					},
				},
			},
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
		},
	}

	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: 0,
						},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{},
					},
				},
			},
		},
		scanNode: scanNode,
	}

	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "m": 16, "ef_construction": 200}`
	multiTableIndex := makeConsistentHnswMultiTableIndexForTest(
		"idx_hnsw_success",
		idxAlgoParams,
		[]string{"vec_col"},
	)

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify the returned context has correct values
	assert.Equal(t, vecCtx, result.vecCtx)
	assert.Equal(t, multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Metadata], result.metaDef)
	assert.Equal(t, multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Storage], result.idxDef)
	assert.Equal(t, "l2_distance", result.origFuncName)
	assert.Equal(t, int32(0), result.partPos)
	assert.Equal(t, int32(1), result.pkPos)
	assert.Equal(t, idxAlgoParams, result.params)
	assert.Equal(t, int64(4), result.nThread)
	assert.NotNil(t, result.vecLitArg)
}

func TestApplyIndicesForSortUsingHnswKeepsFiltersOnScan(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true, newPlanTestProcess(t))
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "hnsw_threads_search" {
				return int64(4), nil
			}
			return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
		},
	}

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)
	scanTag := builder.genNewBindTag()
	tableDef := &plan.TableDef{
		Name: "t_hnsw_payload",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "embedding", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 3}},
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
			"category":  2,
			"note":      3,
		},
	}
	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db", ObjName: "t_hnsw_payload"},
		BindingTags: []int32{scanTag},
	}
	scanNodeID := builder.appendNode(scanNode, ctx)

	scanNode.FilterList = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{Typ: tableDef.Cols[2].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 2, Name: "category"}}},
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
						{Typ: tableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "note"}}},
						makePlan2StringConstExprWithType("cold"),
					},
				},
			},
		},
	}

	vecTyp := plan.Type{Id: int32(types.T_array_float32), Width: 3}
	vecCtx := &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT},
		projNode: &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*plan.Expr{
				{Typ: tableDef.Cols[0].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 0, Name: "id"}}},
				{Typ: tableDef.Cols[2].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 2, Name: "category"}}},
			},
		},
		distFnExpr: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 1, Name: "embedding"}}},
				{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[0,1,0]"}}}},
			},
		},
		limit:       makePlan2Uint64ConstExprWithType(2),
		resultLimit: makePlan2Uint64ConstExprWithType(2),
	}

	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	multiTableIndex := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexName:          "idx_hnsw_payload",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
				IndexTableName:     "hnsw_meta",
				Parts:              []string{"embedding"},
				IncludedColumns:    []string{"category"},
				IndexAlgoParams:    idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				IndexName:          "idx_hnsw_payload",
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage,
				IndexTableName:     "hnsw_index",
				Parts:              []string{"embedding"},
				IncludedColumns:    []string{"category"},
				IndexAlgoParams:    idxAlgoParams,
			},
		},
	}

	_, err := builder.applyIndicesForSortUsingHnsw(scanNodeID, vecCtx, multiTableIndex, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	tableFuncNode := findHnswTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	spec := tableFuncNode.IndexSearchScan
	require.Equal(t, "[0,1,0]", spec.QueryPayload.GetLit().GetVecVal())
	require.Equal(t, "l2_distance", spec.DistanceFunction)
	require.True(t, spec.PostFilterOverFetch, "the residual filter drops candidates after the search")
	require.False(t, tableFuncNode.Stats.ForceOneCN, "an hnsw search must not keep the rest of the query on one CN")
	require.False(t, IndexSearchScanPartitioned(spec), "an hnsw search reads its whole index once")
	require.Equal(t, []*plan.IndexHiddenTableRef{
		{Role: catalog.Hnsw_TblType_Metadata, Object: &plan.ObjectRef{SchemaName: "db", ObjName: "hnsw_meta"}},
		{Role: catalog.Hnsw_TblType_Storage, Object: &plan.ObjectRef{SchemaName: "db", ObjName: "hnsw_index"}},
	}, spec.HiddenTables)
	opts, err := hnswplan.DecodeScanOptions(spec.AlgoOptions)
	require.NoError(t, err)
	require.Equal(t, int64(4), opts.ThreadsSearch)
	require.Len(t, scanNode.FilterList, 2)
}

// applyHnswAndGetSearchNode runs the hnsw rewrite for a filtered top-k with the
// given limit and returns the index search node.
func applyHnswAndGetSearchNode(t *testing.T, limit *plan.Expr) *plan.Node {
	t.Helper()
	baseMockCtx := NewMockCompilerContext(true, newPlanTestProcess(t))
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "hnsw_threads_search" {
				return int64(4), nil
			}
			return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
		},
	}

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)
	scanTag := builder.genNewBindTag()
	tableDef := &plan.TableDef{
		Name: "t_hnsw_of",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "embedding", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 3}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "embedding": 1},
	}
	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db", ObjName: "t_hnsw_of"},
		BindingTags: []int32{scanTag},
	}
	scanNodeID := builder.appendNode(scanNode, ctx)
	// residual filter: id >= 4 (post-filter that drops index candidates).
	scanNode.FilterList = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: ">="},
					Args: []*plan.Expr{
						{Typ: tableDef.Cols[0].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 0, Name: "id"}}},
						MakePlan2Int64ConstExprWithType(4),
					},
				},
			},
		},
	}

	vecTyp := plan.Type{Id: int32(types.T_array_float32), Width: 3}
	vecCtx := &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT},
		projNode: &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*plan.Expr{
				{Typ: tableDef.Cols[0].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 0, Name: "id"}}},
			},
		},
		distFnExpr: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 1, Name: "embedding"}}},
				{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[0,1,0]"}}}},
			},
		},
		limit:       limit,
		resultLimit: DeepCopyExpr(limit),
	}

	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	multiTableIndex := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexName: "idx_hnsw_of", IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata, IndexTableName: "hnsw_meta",
				Parts: []string{"embedding"}, IndexAlgoParams: idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				IndexName: "idx_hnsw_of", IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage, IndexTableName: "hnsw_index",
				Parts: []string{"embedding"}, IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, err := builder.applyIndicesForSortUsingHnsw(scanNodeID, vecCtx, multiTableIndex, nil)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	tableFuncNode := findHnswTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	return tableFuncNode
}

// The search node carries the semantic k and the post-filter flag; execution
// sizes the hnsw candidate budget from them (overfetch.PostFilterLimit, #26869).
// A prepared k stays an execution-time expression.
func TestApplyIndicesForSortUsingHnswFlagsPreparedLimitOverFetch(t *testing.T) {
	paramLimit := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_uint64)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	tf := applyHnswAndGetSearchNode(t, paramLimit)
	require.True(t, tf.IndexSearchScan.PostFilterOverFetch)
	require.NotNil(t, tf.IndexSearchScan.CandidateLimit.GetP(), "a prepared k is bound at execution")
	require.Nil(t, tf.Limit, "the candidate budget is the reader's, not a node limit")
}

// A literal LIMIT with a filter keeps the literal k; the budget is sized at
// execution as for a prepared one.
func TestApplyIndicesForSortUsingHnswLiteralLimitOverFetch(t *testing.T) {
	tf := applyHnswAndGetSearchNode(t, makePlan2Uint64ConstExprWithType(2))
	require.True(t, tf.IndexSearchScan.PostFilterOverFetch)
	require.Equal(t, uint64(2), tf.IndexSearchScan.CandidateLimit.GetLit().GetU64Val(), "k, not the over-fetched budget")
	require.Nil(t, tf.Limit)
}

func findHnswTableFunctionNode(builder *QueryBuilder, nodeID int32) *plan.Node {
	if int(nodeID) >= len(builder.qry.Nodes) || builder.qry.Nodes[nodeID] == nil {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_INDEX_SEARCH_SCAN &&
		node.IndexSearchScan.GetIndex().GetIndexAlgo() == catalog.MoIndexHnswAlgo.ToString() {
		return node
	}
	for _, childID := range node.Children {
		if found := findHnswTableFunctionNode(builder, childID); found != nil {
			return found
		}
	}
	return nil
}

// TestPrepareHnswIndexContext_DifferentDistanceFunctions tests success with different distance functions
func TestPrepareHnswIndexContext_DifferentDistanceFunctions(t *testing.T) {
	testCases := []struct {
		name         string
		funcName     string
		shouldHaveOp bool
	}{
		{
			name:         "cosine_similarity",
			funcName:     "cosine_similarity",
			shouldHaveOp: true,
		},
		{
			name:         "inner_product",
			funcName:     "inner_product",
			shouldHaveOp: true,
		},
		{
			// A normalized (non-degenerate) cosine query now uses the index; a zero/subnormal
			// query vector is rejected at runtime in Search (TestHnswSearchCosineRejected), not here.
			name:         "cosine_distance",
			funcName:     "cosine_distance",
			shouldHaveOp: true,
		},
		{
			name:         "l1_distance",
			funcName:     "l1_distance",
			shouldHaveOp: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Check if this function has an op type mapping
			opType, exists := metric.DistFuncOpTypes[tc.funcName]
			if !exists {
				t.Skipf("Function %s not in DistFuncOpTypes", tc.funcName)
				return
			}

			baseMockCtx := NewMockCompilerContext(true, newPlanTestProcess(t))
			mockCtx := &customMockCompilerContext{
				MockCompilerContext: baseMockCtx,
				resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
					if varName == "hnsw_threads_search" {
						return int64(4), nil
					}
					return baseMockCtx.ResolveVariable(varName, isSystem, isGlobal)
				},
			}

			builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)

			scanNode := &plan.Node{
				TableDef: &plan.TableDef{
					Name: "test_table",
					Name2ColIndex: map[string]int32{
						"vec_col": 0,
						"id":      1,
					},
					Cols: []*plan.ColDef{
						{Name: "vec_col", Typ: plan.Type{Id: int32(types.T_array_float32)}},
						{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
					},
					Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
				},
			}

			vecCtx := &vectorSortContext{
				distFnExpr: &plan.Function{
					Func: &ObjectRef{ObjName: tc.funcName},
					Args: []*plan.Expr{
						{
							Typ:  plan.Type{Id: int32(types.T_array_float32)},
							Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
						},
						{
							Typ:  plan.Type{Id: int32(types.T_array_float32)},
							Expr: &plan.Expr_Lit{Lit: &plan.Literal{}},
						},
					},
				},
				scanNode: scanNode,
			}

			idxAlgoParams := `{"op_type": "` + opType + `"}`
			multiTableIndex := makeConsistentHnswMultiTableIndexForTest(
				"idx_hnsw_distance_fn",
				idxAlgoParams,
				[]string{"vec_col"},
			)

			result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
			require.NoError(t, err)
			if !tc.shouldHaveOp {
				require.Nil(t, result)
				return
			}
			require.NotNil(t, result)
			assert.Equal(t, tc.funcName, result.origFuncName)
		})
	}
}
