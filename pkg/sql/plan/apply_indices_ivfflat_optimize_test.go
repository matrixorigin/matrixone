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
			Func: &ObjectRef{ObjName: "l2_distance"},
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
			Func: &ObjectRef{ObjName: "l2_distance"},
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

func TestApplyIndicesForSortUsingIvfflat_OuterScanRegularIndexPreservesProtection(t *testing.T) {
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

	const (
		tableName  = "t_idx"
		indexTable = "__mo_index_status"
		schemaName = "db"
	)

	tableDef := &plan.TableDef{
		Name: tableName,
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "status", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id":     0,
			"status": 1,
			"v":      2,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idx_status",
				IndexAlgo:      "btree",
				IndexTableName: indexTable,
				Unique:         true,
				TableExist:     true,
				Parts:          []string{"status"},
			},
		},
	}

	idxTableDef := &plan.TableDef{
		Name: indexTable,
		Cols: []*plan.ColDef{
			{Name: "__mo_index_idx_col", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "__mo_index_pk_col", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
		Name2ColIndex: map[string]int32{
			"__mo_index_idx_col": 0,
			"__mo_index_pk_col":  1,
		},
	}
	mockCtx.tables[indexTable] = idxTableDef
	mockCtx.objects[indexTable] = &plan.ObjectRef{SchemaName: schemaName, ObjName: indexTable}

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

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)

	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: schemaName, ObjName: tableName},
		BindingTags: []int32{builder.genNewBindTag()},
		FilterList: []*plan.Expr{
			{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Typ:  plan.Type{Id: int32(types.T_int32)},
								Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: "status"}},
							},
							{
								Typ:  plan.Type{Id: int32(types.T_int32)},
								Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1}}},
							},
						},
					},
				},
				Selectivity: 0.01,
			},
		},
		Stats: &plan.Stats{
			TableCnt:    1000,
			Selectivity: 0.01,
			Outcnt:      10,
			Cost:        1000,
		},
	}
	scanNode.FilterList[0].GetF().Args[0].GetCol().RelPos = scanNode.BindingTags[0]
	scanNodeID := builder.appendNode(scanNode, ctx)

	for int(scanNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	// Preallocate enough BindContexts for all plan nodes appended by the IVF rewrite
	// path in this test (function scan, nested joins, projects, sort, and index join).
	for i := 0; i < 40; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 2}}},
			{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}

	vecCtx := &vectorSortContext{
		scanNode:   scanNode,
		sortNode:   &plan.Node{NodeType: plan.Node_SORT},
		projNode:   &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scanNodeID}},
		distFnExpr: distFnExpr,
		limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}

	colRefCnt := map[[2]int32]int{
		{scanNode.BindingTags[0], 0}: 1,
		{scanNode.BindingTags[0], 1}: 1,
		{scanNode.BindingTags[0], 2}: 1,
	}
	idxColMap := make(map[[2]int32]*plan.Expr)

	builder.protectedScans[scanNode.NodeId] = 2

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, colRefCnt, idxColMap)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	outerJoinNode := builder.qry.Nodes[sortNode.Children[0]]
	outerLeft := builder.qry.Nodes[outerJoinNode.Children[0]]

	require.Equal(t, plan.Node_SORT, sortNode.NodeType)
	require.Equal(t, plan.Node_JOIN, outerJoinNode.NodeType)
	assert.Equal(t, plan.Node_JOIN, outerLeft.NodeType)
	assert.Equal(t, plan.Node_INDEX, outerLeft.JoinType)
	assert.Equal(t, 2, builder.protectedScans[scanNode.NodeId])
	assert.NotEmpty(t, scanNode.RuntimeFilterProbeList)
}

func TestApplyIndicesForSortUsingIvfflat_SecondScanIndexOnlyKeepsPk(t *testing.T) {
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

	const (
		tableName  = "t_file_idx"
		indexTable = "__mo_index_file_id"
		schemaName = "db"
	)

	tableDef := &plan.TableDef{
		Name: tableName,
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "file_id", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id":      0,
			"file_id": 1,
			"v":       2,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idx_file_id",
				IndexAlgo:      "btree",
				IndexTableName: indexTable,
				TableExist:     true,
				Parts:          []string{"file_id", "id"},
			},
		},
	}

	idxTableDef := &plan.TableDef{
		Name: indexTable,
		Cols: []*plan.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: catalog.IndexTablePrimaryColName, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}
	mockCtx.tables[indexTable] = idxTableDef
	mockCtx.objects[indexTable] = &plan.ObjectRef{SchemaName: schemaName, ObjName: indexTable}

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

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)

	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: schemaName, ObjName: tableName},
		BindingTags: []int32{builder.genNewBindTag()},
		FilterList: []*plan.Expr{
			{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Typ:  plan.Type{Id: int32(types.T_varchar)},
								Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: "file_id"}},
							},
							{
								Typ:  plan.Type{Id: int32(types.T_varchar)},
								Expr: makePlan2StringConstExprWithType("file01").Expr,
							},
						},
					},
				},
				Selectivity: 0.2,
			},
		},
		Stats: &plan.Stats{
			TableCnt:    1000,
			Selectivity: 0.2,
			Outcnt:      200,
			Cost:        1000,
		},
	}
	scanNode.FilterList[0].GetF().Args[0].GetCol().RelPos = scanNode.BindingTags[0]
	scanNodeID := builder.appendNode(scanNode, ctx)

	for int(scanNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	for i := 0; i < 40; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 2}}},
			{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}

	vecCtx := &vectorSortContext{
		scanNode:   scanNode,
		sortNode:   &plan.Node{NodeType: plan.Node_SORT},
		projNode:   &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scanNodeID}},
		distFnExpr: distFnExpr,
		limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}

	colRefCnt := map[[2]int32]int{
		{scanNode.BindingTags[0], 0}: 1,
		{scanNode.BindingTags[0], 1}: 1,
		{scanNode.BindingTags[0], 2}: 1,
	}
	idxColMap := make(map[[2]int32]*plan.Expr)

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, colRefCnt, idxColMap)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	outerJoinNode := builder.qry.Nodes[sortNode.Children[0]]
	innerJoinNode := builder.qry.Nodes[outerJoinNode.Children[1]]
	secondProjectNode := builder.qry.Nodes[innerJoinNode.Children[1]]

	require.Equal(t, plan.Node_PROJECT, secondProjectNode.NodeType)
	require.Len(t, secondProjectNode.ProjectList, 1)
	secondPkCol := secondProjectNode.ProjectList[0].GetCol()
	require.NotNil(t, secondPkCol)
	assert.Equal(t, int32(1), secondPkCol.ColPos)

	secondScanNode := builder.qry.Nodes[secondProjectNode.Children[0]]
	require.Equal(t, plan.Node_TABLE_SCAN, secondScanNode.NodeType)
	assert.True(t, secondScanNode.IndexScanInfo.IsIndexScan)
}

func TestApplyIndicesForSortUsingIvfflat_OuterScanIndexOnlyUsesOptimizedPk(t *testing.T) {
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

	const (
		tableName  = "t_outer_file_idx"
		indexTable = "__mo_index_outer_file_id"
		schemaName = "db"
	)

	tableDef := &plan.TableDef{
		Name: tableName,
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "file_id", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id":      0,
			"file_id": 1,
			"v":       2,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idx_file_id",
				IndexAlgo:      "btree",
				IndexTableName: indexTable,
				TableExist:     true,
				Parts:          []string{"file_id", "id"},
			},
		},
	}

	idxTableDef := &plan.TableDef{
		Name: indexTable,
		Cols: []*plan.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: catalog.IndexTablePrimaryColName, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}
	mockCtx.tables[indexTable] = idxTableDef
	mockCtx.objects[indexTable] = &plan.ObjectRef{SchemaName: schemaName, ObjName: indexTable}

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

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)

	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: schemaName, ObjName: tableName},
		BindingTags: []int32{builder.genNewBindTag()},
		FilterList: []*plan.Expr{
			{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "="},
						Args: []*plan.Expr{
							{
								Typ:  plan.Type{Id: int32(types.T_varchar)},
								Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: "file_id"}},
							},
							{
								Typ:  plan.Type{Id: int32(types.T_varchar)},
								Expr: makePlan2StringConstExprWithType("file01").Expr,
							},
						},
					},
				},
				Selectivity: 0.2,
			},
		},
		Stats: &plan.Stats{
			TableCnt:    1000,
			Selectivity: 0.2,
			Outcnt:      200,
			Cost:        1000,
		},
	}
	scanNode.FilterList[0].GetF().Args[0].GetCol().RelPos = scanNode.BindingTags[0]
	scanNodeID := builder.appendNode(scanNode, ctx)

	for int(scanNodeID) >= len(builder.ctxByNode) {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}
	for i := 0; i < 40; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	float32Typ := plan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: float32Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 2}}},
			{Typ: float32Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}

	vecCtx := &vectorSortContext{
		scanNode:   scanNode,
		sortNode:   &plan.Node{NodeType: plan.Node_SORT},
		projNode:   &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scanNodeID}},
		distFnExpr: distFnExpr,
		limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}

	colRefCnt := map[[2]int32]int{
		{scanNode.BindingTags[0], 0}: 1,
		{scanNode.BindingTags[0], 1}: 1,
	}
	idxColMap := make(map[[2]int32]*plan.Expr)

	builder.protectedScans[scanNode.NodeId] = 1

	_, err := builder.applyIndicesForSortUsingIvfflat(scanNodeID, vecCtx, multiTableIndex, colRefCnt, idxColMap)
	require.NoError(t, err)

	sortNode := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	outerJoinNode := builder.qry.Nodes[sortNode.Children[0]]
	outerLeft := builder.qry.Nodes[outerJoinNode.Children[0]]

	require.Equal(t, plan.Node_TABLE_SCAN, outerLeft.NodeType)
	assert.True(t, outerLeft.IndexScanInfo.IsIndexScan)

	joinLeftPk := outerJoinNode.OnList[0].GetF().Args[0].GetCol()
	require.NotNil(t, joinLeftPk)
	assert.Equal(t, outerLeft.BindingTags[0], joinLeftPk.RelPos)
	assert.Equal(t, int32(1), joinLeftPk.ColPos)

	require.NotEmpty(t, outerLeft.RuntimeFilterProbeList)
	outerProbe := outerLeft.RuntimeFilterProbeList[0].Expr.GetCol()
	require.NotNil(t, outerProbe)
	assert.Equal(t, outerLeft.BindingTags[0], outerProbe.RelPos)
	assert.Equal(t, int32(1), outerProbe.ColPos)

	assert.Empty(t, scanNode.RuntimeFilterProbeList)
	assert.Equal(t, 1, builder.protectedScans[scanNode.NodeId])
}
