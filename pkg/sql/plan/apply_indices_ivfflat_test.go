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

// ============================================================================
// Tests for prepareIvfIndexContext
// ============================================================================

// TestPrepareIvfIndexContext_NilVecCtx tests the case where vecCtx is nil
func TestPrepareIvfIndexContext_NilVecCtx(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	multiTableIndex := &MultiTableIndex{}

	result, err := builder.prepareIvfIndexContext(nil, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_NilMultiTableIndex tests the case where multiTableIndex is nil
func TestPrepareIvfIndexContext_NilMultiTableIndex(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{}

	result, err := builder.prepareIvfIndexContext(vecCtx, nil)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_NilDistFnExpr tests the case where distFnExpr is nil
func TestPrepareIvfIndexContext_NilDistFnExpr(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	vecCtx := &vectorSortContext{
		distFnExpr: nil,
	}
	multiTableIndex := &MultiTableIndex{}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_ForceModeEnabled tests the case where rankOption.Mode is "force"
func TestPrepareIvfIndexContext_ForceModeEnabled(t *testing.T) {
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

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_NilMetaDef tests the case where metaDef is nil
func TestPrepareIvfIndexContext_NilMetaDef(t *testing.T) {
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata:  nil,
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {},
			catalog.SystemSI_IVFFLAT_TblType_Entries:   {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_NilIdxDef tests the case where idxDef is nil
func TestPrepareIvfIndexContext_NilIdxDef(t *testing.T) {
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata:  {},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: nil,
			catalog.SystemSI_IVFFLAT_TblType_Entries:   {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_NilEntriesDef tests the case where entriesDef is nil
func TestPrepareIvfIndexContext_NilEntriesDef(t *testing.T) {
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata:  {},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {},
			catalog.SystemSI_IVFFLAT_TblType_Entries:   nil,
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_InvalidIndexAlgoParams tests the case where IndexAlgoParams is invalid JSON
func TestPrepareIvfIndexContext_InvalidIndexAlgoParams(t *testing.T) {
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoParams: "invalid json",
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {},
			catalog.SystemSI_IVFFLAT_TblType_Entries:   {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_OpTypeMismatch tests the case where op_type doesn't match the distance function
func TestPrepareIvfIndexContext_OpTypeMismatch(t *testing.T) {
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "cosine_similarity"}`,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {},
			catalog.SystemSI_IVFFLAT_TblType_Entries:   {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_ArgsNotFound tests the case where getArgsFromDistFn returns found=false
func TestPrepareIvfIndexContext_ArgsNotFound(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

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
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				Parts: []string{"vec_col"},
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestPrepareIvfIndexContext_ResolveVariableError_IvfThreads tests the case where ResolveVariable returns error for ivf_threads_search
func TestPrepareIvfIndexContext_ResolveVariableError_IvfThreads(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true)
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "ivf_threads_search" {
				return nil, moerr.NewInternalError(context.Background(), "test error")
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				Parts: []string{"vec_col"},
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "test error")
}

// TestPrepareIvfIndexContext_ResolveVariableError_ProbeLimit tests the case where ResolveVariable returns error for probe_limit
func TestPrepareIvfIndexContext_ResolveVariableError_ProbeLimit(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true)
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "ivf_threads_search" {
				return int64(4), nil
			}
			if varName == "probe_limit" {
				return nil, moerr.NewInternalError(context.Background(), "probe_limit error")
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				Parts: []string{"vec_col"},
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "probe_limit error")
}

// TestPrepareIvfIndexContext_ProbeLimitNotInt64 tests the case where probe_limit is not int64
func TestPrepareIvfIndexContext_ProbeLimitNotInt64(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true)
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "ivf_threads_search" {
				return int64(4), nil
			}
			if varName == "probe_limit" {
				return "not an int64", nil // Wrong type
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
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoParams: `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				Parts: []string{"vec_col"},
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "probe_limit is not int64")
}

// TestPrepareIvfIndexContext_Success tests the successful case
func TestPrepareIvfIndexContext_Success(t *testing.T) {
	baseMockCtx := NewMockCompilerContext(true)
	mockCtx := &customMockCompilerContext{
		MockCompilerContext: baseMockCtx,
		resolveVarFunc: func(varName string, isSystem, isGlobal bool) (interface{}, error) {
			if varName == "ivf_threads_search" {
				return int64(4), nil
			}
			if varName == "probe_limit" {
				return int64(10), nil
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

	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "lists": 100}`
	multiTableIndex := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				Parts:           []string{"vec_col"},
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {},
		},
	}

	result, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, vecCtx, result.vecCtx)
	assert.Equal(t, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata], result.metaDef)
	assert.Equal(t, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids], result.idxDef)
	assert.Equal(t, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries], result.entriesDef)
	assert.Equal(t, "l2_distance", result.origFuncName)
	assert.Equal(t, int32(0), result.partPos)
	assert.Equal(t, int32(1), result.pkPos)
	assert.Equal(t, idxAlgoParams, result.params)
	assert.Equal(t, int64(4), result.nThread)
	assert.Equal(t, int64(10), result.nProbe)
	assert.NotNil(t, result.vecLitArg)
}

// ============================================================================
// Tests for buildPkExprFromNode
// ============================================================================

// TestBuildPkExprFromNode_NilBuilder tests when builder is nil
func TestBuildPkExprFromNode_NilBuilder(t *testing.T) {
	var builder *QueryBuilder
	result := builder.buildPkExprFromNode(0, plan.Type{}, "id")
	assert.Nil(t, result)
}

// TestBuildPkExprFromNode_NegativeNodeID tests when nodeID is negative
func TestBuildPkExprFromNode_NegativeNodeID(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	result := builder.buildPkExprFromNode(-1, plan.Type{}, "id")
	assert.Nil(t, result)
}

// TestBuildPkExprFromNode_TableScan_Success tests TABLE_SCAN node with valid setup
func TestBuildPkExprFromNode_TableScan_Success(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	scanNode := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{
			Name2ColIndex: map[string]int32{
				"id":   0,
				"name": 1,
			},
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "id",
			},
		},
		BindingTags: []int32{100},
	}

	builder.qry.Nodes = append(builder.qry.Nodes, scanNode)

	pkType := plan.Type{Id: int32(types.T_int64)}
	result := builder.buildPkExprFromNode(0, pkType, "id")

	require.NotNil(t, result)
	assert.Equal(t, pkType, result.Typ)
	col := result.GetCol()
	require.NotNil(t, col)
	assert.Equal(t, int32(100), col.RelPos)
	assert.Equal(t, int32(0), col.ColPos)
	assert.Equal(t, "id", col.Name)
}

// TestBuildPkExprFromNode_TableScan_NilTableDef tests TABLE_SCAN with nil TableDef
func TestBuildPkExprFromNode_TableScan_NilTableDef(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	scanNode := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: nil,
	}

	builder.qry.Nodes = append(builder.qry.Nodes, scanNode)
	result := builder.buildPkExprFromNode(0, plan.Type{}, "id")
	assert.Nil(t, result)
}

// TestBuildPkExprFromNode_TableScan_NoBindingTags tests TABLE_SCAN with no BindingTags
func TestBuildPkExprFromNode_TableScan_NoBindingTags(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	scanNode := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{
			Name2ColIndex: map[string]int32{"id": 0},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
		BindingTags: []int32{},
	}

	builder.qry.Nodes = append(builder.qry.Nodes, scanNode)
	result := builder.buildPkExprFromNode(0, plan.Type{}, "id")
	assert.Nil(t, result)
}

// TestBuildPkExprFromNode_Project_Success tests PROJECT node
func TestBuildPkExprFromNode_Project_Success(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.nameByColRef = make(map[[2]int32]string)
	builder.nameByColRef[[2]int32{100, 0}] = "id"

	projNode := &plan.Node{
		NodeType: plan.Node_PROJECT,
		ProjectList: []*plan.Expr{
			{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 100,
						ColPos: 0,
						Name:   "id",
					},
				},
			},
		},
		Children: []int32{},
	}

	builder.qry.Nodes = append(builder.qry.Nodes, projNode)

	result := builder.buildPkExprFromNode(0, plan.Type{Id: int32(types.T_int64)}, "id")
	require.NotNil(t, result)
	assert.Equal(t, int32(types.T_int64), result.Typ.Id)
}

// TestBuildPkExprFromNode_Project_Recursive tests PROJECT node that recurses to child
func TestBuildPkExprFromNode_Project_Recursive(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	scanNode := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{
			Name2ColIndex: map[string]int32{"id": 0},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
		BindingTags: []int32{100},
	}

	projNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: []*plan.Expr{},
		Children:    []int32{0},
	}

	builder.qry.Nodes = append(builder.qry.Nodes, scanNode, projNode)

	result := builder.buildPkExprFromNode(1, plan.Type{Id: int32(types.T_int64)}, "id")
	require.NotNil(t, result)
}

// TestBuildPkExprFromNode_Join_Recursive tests JOIN node
func TestBuildPkExprFromNode_Join_Recursive(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	scanNode := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{
			Name2ColIndex: map[string]int32{"id": 0},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
		BindingTags: []int32{100},
	}

	joinNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{0},
	}

	builder.qry.Nodes = append(builder.qry.Nodes, scanNode, joinNode)

	result := builder.buildPkExprFromNode(1, plan.Type{Id: int32(types.T_int64)}, "id")
	require.NotNil(t, result)
}

// ============================================================================
// Tests for getColName
// ============================================================================

// TestGetColName_NilCol tests when col is nil
func TestGetColName_NilCol(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	result := builder.getColName(nil)
	assert.Equal(t, "", result)
}

// TestGetColName_NilBuilder tests when builder is nil
func TestGetColName_NilBuilder(t *testing.T) {
	var builder *QueryBuilder
	col := &plan.ColRef{Name: "test_col"}
	result := builder.getColName(col)
	assert.Equal(t, "test_col", result)
}

// TestGetColName_NilNameByColRef tests when nameByColRef is nil
func TestGetColName_NilNameByColRef(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.nameByColRef = nil
	col := &plan.ColRef{Name: "test_col"}
	result := builder.getColName(col)
	assert.Equal(t, "test_col", result)
}

// TestGetColName_FoundInMap tests when name is found in map
func TestGetColName_FoundInMap(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.nameByColRef = make(map[[2]int32]string)
	builder.nameByColRef[[2]int32{100, 5}] = "mapped_name"

	col := &plan.ColRef{
		RelPos: 100,
		ColPos: 5,
		Name:   "original_name",
	}

	result := builder.getColName(col)
	assert.Equal(t, "mapped_name", result)
}

// TestGetColName_NotFoundInMap tests when name is not in map
func TestGetColName_NotFoundInMap(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.nameByColRef = make(map[[2]int32]string)

	col := &plan.ColRef{
		RelPos: 100,
		ColPos: 5,
		Name:   "original_name",
	}

	result := builder.getColName(col)
	assert.Equal(t, "original_name", result)
}

// ============================================================================
// Tests for rebindScanNode
// ============================================================================

// TestRebindScanNode_NilNode tests when scanNode is nil
func TestRebindScanNode_NilNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	// Should not panic
	builder.rebindScanNode(nil)
}

// TestRebindScanNode_NoBindingTags tests when BindingTags is empty
func TestRebindScanNode_NoBindingTags(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	scanNode := &plan.Node{
		BindingTags: []int32{},
	}
	// Should not panic
	builder.rebindScanNode(scanNode)
}

// TestRebindScanNode_Success tests successful rebinding
func TestRebindScanNode_Success(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	oldTag := int32(100)
	scanNode := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id"},
				{Name: "name"},
			},
		},
		BindingTags: []int32{oldTag},
		FilterList: []*plan.Expr{
			{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: oldTag,
						ColPos: 0,
					},
				},
			},
		},
		BlockFilterList: []*plan.Expr{
			{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: oldTag,
						ColPos: 1,
					},
				},
			},
		},
	}

	builder.rebindScanNode(scanNode)

	newTag := scanNode.BindingTags[0]
	assert.NotEqual(t, oldTag, newTag)

	// Check FilterList was updated
	filterCol := scanNode.FilterList[0].GetCol()
	assert.Equal(t, newTag, filterCol.RelPos)

	// Check BlockFilterList was updated
	blockFilterCol := scanNode.BlockFilterList[0].GetCol()
	assert.Equal(t, newTag, blockFilterCol.RelPos)
}

// ============================================================================
// Tests for replaceColRefTag
// ============================================================================

// TestReplaceColRefTag_NilExpr tests when expr is nil
func TestReplaceColRefTag_NilExpr(t *testing.T) {
	// Should not panic
	replaceColRefTag(nil, 100, 200)
}

// TestReplaceColRefTag_ColRef tests replacing tag in ColRef
func TestReplaceColRefTag_ColRef(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 100,
				ColPos: 5,
			},
		},
	}

	replaceColRefTag(expr, 100, 200)

	col := expr.GetCol()
	assert.Equal(t, int32(200), col.RelPos)
}

// TestReplaceColRefTag_ColRef_NoMatch tests when tag doesn't match
func TestReplaceColRefTag_ColRef_NoMatch(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 150,
				ColPos: 5,
			},
		},
	}

	replaceColRefTag(expr, 100, 200)

	col := expr.GetCol()
	assert.Equal(t, int32(150), col.RelPos)
}

// TestReplaceColRefTag_Function tests replacing tag in function args
func TestReplaceColRefTag_Function(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 100,
							},
						},
					},
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 100,
							},
						},
					},
				},
			},
		},
	}

	replaceColRefTag(expr, 100, 200)

	fn := expr.GetF()
	assert.Equal(t, int32(200), fn.Args[0].GetCol().RelPos)
	assert.Equal(t, int32(200), fn.Args[1].GetCol().RelPos)
}

// TestReplaceColRefTag_List tests replacing tag in list
func TestReplaceColRefTag_List(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 100,
							},
						},
					},
				},
			},
		},
	}

	replaceColRefTag(expr, 100, 200)

	list := expr.GetList()
	assert.Equal(t, int32(200), list.List[0].GetCol().RelPos)
}

// ============================================================================
// Tests for canApplyRegularIndex
// ============================================================================

// TestCanApplyRegularIndex_NilNode tests when node is nil
func TestCanApplyRegularIndex_NilNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	result := builder.canApplyRegularIndex(nil)
	assert.False(t, result)
}

// TestCanApplyRegularIndex_NilTableDef tests when TableDef is nil
func TestCanApplyRegularIndex_NilTableDef(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	node := &plan.Node{
		TableDef: nil,
	}
	result := builder.canApplyRegularIndex(node)
	assert.False(t, result)
}

// TestCanApplyRegularIndex_ZeroCols tests when there are no columns
func TestCanApplyRegularIndex_ZeroCols(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	node := &plan.Node{
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{},
		},
	}
	result := builder.canApplyRegularIndex(node)
	assert.False(t, result)
}

// TestCanApplyRegularIndex_NoFilters tests when there are no filters
func TestCanApplyRegularIndex_NoFilters(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	node := &plan.Node{
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id"},
				{Name: "name"},
			},
		},
		FilterList: []*plan.Expr{},
	}
	result := builder.canApplyRegularIndex(node)
	assert.False(t, result)
}

// TestCanApplyRegularIndex_ColRefOutOfRange tests when colPos is out of range
func TestCanApplyRegularIndex_ColRefOutOfRange(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	node := &plan.Node{
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id"},
				{Name: "name"},
			},
		},
		FilterList: []*plan.Expr{
			{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: 10, // Out of range
					},
				},
			},
		},
	}
	result := builder.canApplyRegularIndex(node)
	assert.False(t, result)
}

// TestCanApplyRegularIndex_Success tests successful case
func TestCanApplyRegularIndex_Success(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	node := &plan.Node{
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id"},
				{Name: "name"},
			},
		},
		FilterList: []*plan.Expr{
			{
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: 0,
					},
				},
			},
		},
	}
	result := builder.canApplyRegularIndex(node)
	assert.True(t, result)
}

// ============================================================================
// Tests for colRefsWithin
// ============================================================================

// TestColRefsWithin_NilExpr tests when expr is nil
func TestColRefsWithin_NilExpr(t *testing.T) {
	result := colRefsWithin(nil, 10)
	assert.True(t, result)
}

// TestColRefsWithin_ColRef_Within tests when ColPos is within range
func TestColRefsWithin_ColRef_Within(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 5,
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.True(t, result)
}

// TestColRefsWithin_ColRef_OutOfRange tests when ColPos is out of range
func TestColRefsWithin_ColRef_OutOfRange(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 15,
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.False(t, result)
}

// TestColRefsWithin_Function_AllWithin tests when all args are within range
func TestColRefsWithin_Function_AllWithin(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 3},
						},
					},
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 7},
						},
					},
				},
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.True(t, result)
}

// TestColRefsWithin_Function_OneOutOfRange tests when one arg is out of range
func TestColRefsWithin_Function_OneOutOfRange(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 3},
						},
					},
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 15},
						},
					},
				},
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.False(t, result)
}

// TestColRefsWithin_List_AllWithin tests when all list items are within range
func TestColRefsWithin_List_AllWithin(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 2},
						},
					},
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 8},
						},
					},
				},
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.True(t, result)
}

// TestColRefsWithin_List_OneOutOfRange tests when one list item is out of range
func TestColRefsWithin_List_OneOutOfRange(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 2},
						},
					},
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 20},
						},
					},
				},
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.False(t, result)
}

// TestColRefsWithin_Literal tests with literal (non-ColRef) expr
func TestColRefsWithin_Literal(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 42},
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.True(t, result)
}

// TestColRefsWithin_NestedFunction tests nested function expressions
func TestColRefsWithin_NestedFunction(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{ColPos: 5},
										},
									},
								},
							},
						},
					},
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{ColPos: 3},
						},
					},
				},
			},
		},
	}
	result := colRefsWithin(expr, 10)
	assert.True(t, result)
}
