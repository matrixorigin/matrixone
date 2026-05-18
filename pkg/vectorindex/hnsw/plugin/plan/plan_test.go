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

// Ported from pkg/sql/plan/apply_indices_ivfpq_test.go (now deleted).
// The tests target PrepareContext and Hooks.ApplyForSort — the lifted
// bodies of prepareIvfpqIndexContext and applyIndicesForSortUsingIvfpq.
//
// External test package (package plan_test) so we can import pkg/sql/plan
// for the real *QueryBuilder mock infrastructure (NewMockCompilerContext
// etc.). pkg/sql/plan blank-imports this plugin for production
// registration, but external test packages don't participate in the
// production import graph, so there's no cycle.
package plan_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// customMockCompilerContext extends MockCompilerContext with a per-test
// ResolveVariable override. Mirrors the unexported type in
// pkg/sql/plan/apply_indices_hnsw_test.go:31.
type customMockCompilerContext struct {
	*sqlplan.MockCompilerContext
	resolveVarFunc func(string, bool, bool) (interface{}, error)
}

func (c *customMockCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if c.resolveVarFunc != nil {
		return c.resolveVarFunc(varName, isSystemVar, isGlobalVar)
	}
	return c.MockCompilerContext.ResolveVariable(varName, isSystemVar, isGlobalVar)
}

// hnswScanNode mirrors the original test's fixture: vec_col at pos 0,
// id PK at pos 1.
func hnswScanNode() *pbplan.Node {
	return &pbplan.Node{
		TableDef: &pbplan.TableDef{
			Name: "test_table",
			Name2ColIndex: map[string]int32{
				"vec_col": 0,
				"id":      1,
			},
			Cols: []*pbplan.ColDef{
				{Name: "vec_col", Typ: pbplan.Type{Id: int32(types.T_array_float32)}},
				{Name: "id", Typ: pbplan.Type{Id: int32(types.T_int64), Width: 64}},
			},
			Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "id"},
		},
	}
}

func hnswVecCtx(scanNode *pbplan.Node) *planplugin.VectorSortContext {
	return &planplugin.VectorSortContext{
		DistFnExpr: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
			Args: []*pbplan.Expr{
				{
					Typ:  pbplan.Type{Id: int32(types.T_array_float32)},
					Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{ColPos: 0}},
				},
				{
					Typ:  pbplan.Type{Id: int32(types.T_array_float32)},
					Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{}},
				},
			},
		},
		ScanNode: scanNode,
	}
}

func hnswMTI(algoParams string) *planplugin.MultiTableIndexRef {
	return &planplugin.MultiTableIndexRef{
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexAlgoParams: algoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				Parts:           []string{"vec_col"},
				IndexAlgoParams: algoParams,
			},
		},
	}
}

func newBuilder(t *testing.T) *sqlplan.QueryBuilder {
	t.Helper()
	return sqlplan.NewQueryBuilder(pbplan.Query_SELECT, sqlplan.NewMockCompilerContext(true), false, true)
}

// ---- PrepareContext -------------------------------------------------------

func TestPrepareHnswIndexContext_NilVecCtx(t *testing.T) {
	b := newBuilder(t)
	r, err := hnswplan.PrepareContext(b, nil, &planplugin.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_NilMultiTableIndex(t *testing.T) {
	b := newBuilder(t)
	r, err := hnswplan.PrepareContext(b, &planplugin.VectorSortContext{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_NilDistFnExpr(t *testing.T) {
	b := newBuilder(t)
	r, err := hnswplan.PrepareContext(b, &planplugin.VectorSortContext{DistFnExpr: nil}, &planplugin.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_ForceMode(t *testing.T) {
	b := newBuilder(t)
	v := &planplugin.VectorSortContext{
		DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}},
		RankOption: &pbplan.RankOption{Mode: "force"},
	}
	r, err := hnswplan.PrepareContext(b, v, &planplugin.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_DescBlocksRewrite(t *testing.T) {
	b := newBuilder(t)
	v := &planplugin.VectorSortContext{
		DistFnExpr:    &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}},
		SortDirection: pbplan.OrderBySpec_DESC,
	}
	r, err := hnswplan.PrepareContext(b, v, &planplugin.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_NilMetaDef(t *testing.T) {
	b := newBuilder(t)
	v := &planplugin.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := &planplugin.MultiTableIndexRef{
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Hnsw_TblType_Metadata: nil,
			catalog.Hnsw_TblType_Storage:  {},
		},
	}
	r, err := hnswplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_NilIdxDef(t *testing.T) {
	b := newBuilder(t)
	v := &planplugin.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := &planplugin.MultiTableIndexRef{
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {},
			catalog.Hnsw_TblType_Storage:  nil,
		},
	}
	r, err := hnswplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_InvalidAlgoParamsJSON(t *testing.T) {
	b := newBuilder(t)
	v := &planplugin.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := hnswMTI("not valid json")
	r, err := hnswplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_OpTypeMismatch(t *testing.T) {
	b := newBuilder(t)
	v := &planplugin.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := hnswMTI(`{"op_type": "vector_cosine_ops"}`)
	r, err := hnswplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

// op_type present but not a string → StrictString fails and the function
// returns (nil, nil).
func TestPrepareHnswIndexContext_OpTypeNotString(t *testing.T) {
	b := newBuilder(t)
	v := &planplugin.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := hnswMTI(`{"op_type": 123}`)
	r, err := hnswplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_ArgsNotFound(t *testing.T) {
	b := newBuilder(t)
	scan := hnswScanNode()
	v := &planplugin.VectorSortContext{
		DistFnExpr: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
			Args: []*pbplan.Expr{
				{Typ: pbplan.Type{Id: int32(types.T_array_float32)}, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{}}},
				{Typ: pbplan.Type{Id: int32(types.T_array_float32)}, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{}}},
			},
		},
		ScanNode: scan,
	}
	mti := hnswMTI(`{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`)
	r, err := hnswplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareHnswIndexContext_ResolveThreadsError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "hnsw_threads_search" {
				return nil, moerr.NewInternalError(context.Background(), "threads error")
			}
			return int64(0), nil
		},
	}
	b := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	r, err := hnswplan.PrepareContext(b, hnswVecCtx(hnswScanNode()),
		hnswMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "threads error")
}

// (TestPrepareHnswIndexContext_ResolveBatchWindow/ProbeLimit omitted —
// HNSW resolves neither variable; only hnsw_threads_search.)

func TestPrepareHnswIndexContext_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "hnsw_threads_search" {
				return int64(8), nil
			}
			return int64(0), nil
		},
	}
	b := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	algo := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	r, err := hnswplan.PrepareContext(b, hnswVecCtx(hnswScanNode()), hnswMTI(algo))
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.Equal(t, "l2_distance", r.OrigFuncName())
	assert.Equal(t, int32(0), r.PartPos())
	assert.Equal(t, int32(1), r.PkPos())
	assert.Equal(t, algo, r.Params())
	assert.Equal(t, int64(8), r.NThread())
	assert.NotNil(t, r.VecLitArg())
}

// ---- Hooks.ApplyForSort ---------------------------------------------------

func TestApplyIndicesForSortUsingHnsw_NilGuards(t *testing.T) {
	b := newBuilder(t)

	got, applied, err := hnswplan.Hooks{}.ApplyForSort(b, nil, &planplugin.MultiTableIndexRef{}, 7, planplugin.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(7), got)

	got, applied, err = hnswplan.Hooks{}.ApplyForSort(b, &planplugin.VectorSortContext{}, &planplugin.MultiTableIndexRef{}, 7, planplugin.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(7), got)

	got, applied, err = hnswplan.Hooks{}.ApplyForSort(b, &planplugin.VectorSortContext{SortNode: &pbplan.Node{}}, &planplugin.MultiTableIndexRef{}, 7, planplugin.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(7), got)
}

func TestApplyIndicesForSortUsingHnsw_PrepareReturnsNil(t *testing.T) {
	b := newBuilder(t)
	scan := hnswScanNode()
	v := hnswVecCtx(scan)
	v.SortNode = &pbplan.Node{}
	v.RankOption = &pbplan.RankOption{Mode: "force"}

	got, applied, err := hnswplan.Hooks{}.ApplyForSort(b, v, &planplugin.MultiTableIndexRef{}, 0, planplugin.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(0), got)
}

func TestApplyIndicesForSortUsingHnsw_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "hnsw_threads_search":
				return int64(4), nil
			}
			return int64(0), nil
		},
	}
	builder := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	bindCtx := sqlplan.NewBindContext(builder, nil)

	tableDef := &pbplan.TableDef{
		Name: "t",
		Cols: []*pbplan.ColDef{
			{Name: "id", Typ: pbplan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "v", Typ: pbplan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &pbplan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
	scanTag := builder.GenNewBindTag()
	scanNode := &pbplan.Node{
		NodeType:    pbplan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &pbplan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{scanTag},
	}
	scanNodeID := builder.AppendNode(scanNode, bindCtx)

	vecTyp := pbplan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &pbplan.Function{
		Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
		Args: []*pbplan.Expr{
			{Typ: vecTyp, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 1}}},
			{Typ: vecTyp, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}
	vecCtx := &planplugin.VectorSortContext{
		ScanNode: scanNode,
		SortNode: &pbplan.Node{NodeType: pbplan.Node_SORT, Offset: &pbplan.Expr{}},
		ProjNode: &pbplan.Node{
			NodeType: pbplan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*pbplan.Expr{
				{Typ: vecTyp, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 1}}},
			},
		},
		DistFnExpr: distFnExpr,
		OrderExpr: &pbplan.Expr{
			Typ:  pbplan.Type{Id: int32(types.T_float64)},
			Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{ColPos: 0}},
		},
		Limit:      &pbplan.Expr{Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_U64Val{U64Val: 10}}}},
		RankOption: &pbplan.RankOption{Mode: "pre"},
	}
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	mti := &planplugin.MultiTableIndexRef{
		IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, applied, err := hnswplan.Hooks{}.ApplyForSort(builder, vecCtx, mti, scanNodeID, planplugin.ApplyForSortOpts{})
	require.NoError(t, err)
	require.True(t, applied)

	sortID := vecCtx.ProjNode.Children[0]
	q := builder.Query()
	sort := q.Nodes[sortID]
	require.Equal(t, pbplan.Node_SORT, sort.NodeType)
	joinID := sort.Children[0]
	join := q.Nodes[joinID]
	require.Equal(t, pbplan.Node_JOIN, join.NodeType)
	right := q.Nodes[join.Children[1]]
	assert.Equal(t, pbplan.Node_FUNCTION_SCAN, right.NodeType)
	assert.Equal(t, hnswplan.HNSWSearchFuncName, right.TableDef.TblFunc.Name)
}

func TestApplyIndicesForSortUsingHnsw_Success_WithFiltersOverFetch(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "hnsw_threads_search":
				return int64(4), nil
			}
			return int64(0), nil
		},
	}
	builder := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	bindCtx := sqlplan.NewBindContext(builder, nil)

	tableDef := &pbplan.TableDef{
		Name: "t",
		Cols: []*pbplan.ColDef{
			{Name: "id", Typ: pbplan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "v", Typ: pbplan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &pbplan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
	scanTag := builder.GenNewBindTag()
	scanNode := &pbplan.Node{
		NodeType:    pbplan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &pbplan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{scanTag},
		FilterList: []*pbplan.Expr{
			{Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_Bval{Bval: true}}}},
		},
	}
	scanNodeID := builder.AppendNode(scanNode, bindCtx)

	vecTyp := pbplan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &pbplan.Function{
		Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
		Args: []*pbplan.Expr{
			{Typ: vecTyp, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 1}}},
			{Typ: vecTyp, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}
	vecCtx := &planplugin.VectorSortContext{
		ScanNode: scanNode,
		SortNode: &pbplan.Node{NodeType: pbplan.Node_SORT, Offset: &pbplan.Expr{}},
		ProjNode: &pbplan.Node{
			NodeType: pbplan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*pbplan.Expr{
				{Typ: vecTyp, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 1}}},
			},
		},
		DistFnExpr: distFnExpr,
		OrderExpr: &pbplan.Expr{
			Typ:  pbplan.Type{Id: int32(types.T_float64)},
			Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{ColPos: 0}},
		},
		Limit:      &pbplan.Expr{Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{}}},
		RankOption: &pbplan.RankOption{Mode: "pre"},
	}
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	mti := &planplugin.MultiTableIndexRef{
		IndexAlgo: catalog.MoIndexHnswAlgo.ToString(),
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Hnsw_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Hnsw_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, applied, err := hnswplan.Hooks{}.ApplyForSort(builder, vecCtx, mti, scanNodeID, planplugin.ApplyForSortOpts{})
	require.NoError(t, err)
	require.True(t, applied)
}
