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

	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
			catalog.Hnsw_TblType_Storage: {
				Parts: []string{"vec_col"},
			},
		},
	}

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

	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
			catalog.Hnsw_TblType_Storage: {
				Parts: []string{"vec_col"},
			},
		},
	}

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
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				Parts:           []string{"vec_col"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

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
			multiTableIndex := &MultiTableIndex{
				IndexDefs: map[string]*plan.IndexDef{
					catalog.Hnsw_TblType_Metadata: {
						IndexAlgoParams: idxAlgoParams,
					},
					catalog.Hnsw_TblType_Storage: {
						Parts:           []string{"vec_col"},
						IndexAlgoParams: idxAlgoParams,
					},
				},
			}

			result, err := builder.prepareHnswIndexContext(vecCtx, multiTableIndex)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tc.funcName, result.origFuncName)
		})
	}
}
