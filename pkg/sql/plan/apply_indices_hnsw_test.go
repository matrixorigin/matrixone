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

// TestPrepareHnswIndexContext_NilVecCtx tests the case where vecCtx is nil
func TestPrepareHnswIndexContext_NilVecCtx(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	multiTableIndex := &MultiTableIndex{}

	result, err := builder.prepareHnswIndexContext(nil, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_NilMultiTableIndex tests the case where multiTableIndex is nil
func TestPrepareHnswIndexContext_NilMultiTableIndex(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{}

	result, err := builder.prepareHnswIndexContext(vecCtx, nil)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_NilDistFnExpr tests the case where distFnExpr is nil
func TestPrepareHnswIndexContext_NilDistFnExpr(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: nil,
	}
	multiTableIndex := &MultiTableIndex{}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_ForceModeEnabled tests the case where rankOption.Mode is "force"
func TestPrepareHnswIndexContext_ForceModeEnabled(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
		rankOption: &plan.RankOption{
			Mode: "force",
		},
	}
	multiTableIndex := &MultiTableIndex{}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestPrepareHnswIndexContext_ImplicitDescendingOrderDisablesRewrite(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
		sortDirection: plan.OrderBySpec_DESC,
	}
	multiTableIndex := &MultiTableIndex{}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestPrepareHnswIndexContext_ExplicitDescendingOrderFallsBackToOriginalSearch(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
		sortDirection: plan.OrderBySpec_DESC,
		rankOption:    &plan.RankOption{Mode: "post"},
	}
	multiTableIndex := &MultiTableIndex{}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_NilMetaDef tests the case where metaDef is nil
func TestPrepareHnswIndexContext_NilMetaDef(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
	}
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: nil,
			catalog.Hnsw_TblType_Storage:  {},
		},
	}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_NilIdxDef tests the case where idxDef is nil
func TestPrepareHnswIndexContext_NilIdxDef(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
	}
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {},
			catalog.Hnsw_TblType_Storage:  nil,
		},
	}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_InvalidIndexAlgoParams tests the case where IndexAlgoParams is invalid JSON
func TestPrepareHnswIndexContext_InvalidIndexAlgoParams(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
	}
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: "invalid json",
			},
			catalog.Hnsw_TblType_Storage: {},
		},
	}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_MissingOpType tests the case where op_type field is missing
func TestPrepareHnswIndexContext_MissingOpType(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
	}
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: `{"other_field": "value"}`,
			},
			catalog.Hnsw_TblType_Storage: {},
		},
	}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_OpTypeNotString tests the case where op_type is not a string
func TestPrepareHnswIndexContext_OpTypeNotString(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
	}
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": 123}`,
			},
			catalog.Hnsw_TblType_Storage: {},
		},
	}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_OpTypeMismatch tests the case where op_type doesn't match the distance function
func TestPrepareHnswIndexContext_OpTypeMismatch(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{
				ObjName: "l2_distance",
			},
		},
	}
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "cosine_similarity"}`,
			},
			catalog.Hnsw_TblType_Storage: {},
		},
	}

	result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareHnswIndexContext_ArgsNotFound tests the case where getArgsFromDistFn returns found=false
func TestPrepareHnswIndexContext_ArgsNotFound(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

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
	baseMockCtx := NewMockCompilerContext(true)
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
	baseMockCtx := NewMockCompilerContext(true)
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

func TestApplyIndicesForSortUsingHnswPassesCoveredFilterPayload(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true)
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
		limit: makePlan2Uint64ConstExprWithType(2),
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

	_, err := builder.applyIndicesForSortUsingHnsw(scanNodeID, vecCtx, multiTableIndex)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	tableFuncNode := findHnswTableFunctionNode(builder, sortNode.Children[0])
	require.NotNil(t, tableFuncNode)
	require.Len(t, tableFuncNode.TblFuncExprList, 3)

	payload := tableFuncNode.TblFuncExprList[2].GetLit().GetSval()
	require.NotEmpty(t, payload)
	assert.Contains(t, payload, `"column":"category"`)
	assert.NotContains(t, payload, `"column":"note"`)
	assert.NotContains(t, payload, "RelPos")
	assert.NotContains(t, payload, "ColPos")
	require.Len(t, scanNode.FilterList, 2)
}

func findHnswTableFunctionNode(builder *QueryBuilder, nodeID int32) *plan.Node {
	if int(nodeID) >= len(builder.qry.Nodes) || builder.qry.Nodes[nodeID] == nil {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_FUNCTION_SCAN &&
		node.TableDef != nil &&
		node.TableDef.TblFunc != nil &&
		node.TableDef.TblFunc.Name == kHNSWSearchFuncName {
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

			baseMockCtx := NewMockCompilerContext(true)
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
						ObjName: tc.funcName,
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

			idxAlgoParams := `{"op_type": "` + opType + `"}`
			multiTableIndex := makeConsistentHnswMultiTableIndexForTest(
				"idx_hnsw_distance_fn",
				idxAlgoParams,
				[]string{"vec_col"},
			)

			result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tc.funcName, result.origFuncName)
		})
	}
}
