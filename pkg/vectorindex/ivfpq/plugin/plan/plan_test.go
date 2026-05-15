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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
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

// ivfpqScanNode mirrors the original test's fixture: vec_col at pos 0,
// id PK at pos 1.
func ivfpqScanNode() *pbplan.Node {
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

func ivfpqVecCtx(scanNode *pbplan.Node) *vectorplan.VectorSortContext {
	return &vectorplan.VectorSortContext{
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

func ivfpqMTI(algoParams string) *vectorplan.MultiTableIndexRef {
	return &vectorplan.MultiTableIndexRef{
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: {
				IndexAlgoParams: algoParams,
			},
			catalog.Ivfpq_TblType_Storage: {
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

func TestPrepareIvfpqIndexContext_NilVecCtx(t *testing.T) {
	b := newBuilder(t)
	r, err := ivfpqplan.PrepareContext(b, nil, &vectorplan.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilMultiTableIndex(t *testing.T) {
	b := newBuilder(t)
	r, err := ivfpqplan.PrepareContext(b, &vectorplan.VectorSortContext{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilDistFnExpr(t *testing.T) {
	b := newBuilder(t)
	r, err := ivfpqplan.PrepareContext(b, &vectorplan.VectorSortContext{DistFnExpr: nil}, &vectorplan.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_ForceMode(t *testing.T) {
	b := newBuilder(t)
	v := &vectorplan.VectorSortContext{
		DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}},
		RankOption: &pbplan.RankOption{Mode: "force"},
	}
	r, err := ivfpqplan.PrepareContext(b, v, &vectorplan.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_DescBlocksRewrite(t *testing.T) {
	b := newBuilder(t)
	v := &vectorplan.VectorSortContext{
		DistFnExpr:    &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}},
		SortDirection: pbplan.OrderBySpec_DESC,
	}
	r, err := ivfpqplan.PrepareContext(b, v, &vectorplan.MultiTableIndexRef{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilMetaDef(t *testing.T) {
	b := newBuilder(t)
	v := &vectorplan.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := &vectorplan.MultiTableIndexRef{
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: nil,
			catalog.Ivfpq_TblType_Storage:  {},
		},
	}
	r, err := ivfpqplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilIdxDef(t *testing.T) {
	b := newBuilder(t)
	v := &vectorplan.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := &vectorplan.MultiTableIndexRef{
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: {},
			catalog.Ivfpq_TblType_Storage:  nil,
		},
	}
	r, err := ivfpqplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_InvalidAlgoParamsJSON(t *testing.T) {
	b := newBuilder(t)
	v := &vectorplan.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := ivfpqMTI("not valid json")
	r, err := ivfpqplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_OpTypeMismatch(t *testing.T) {
	b := newBuilder(t)
	v := &vectorplan.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := ivfpqMTI(`{"op_type": "vector_cosine_ops"}`)
	r, err := ivfpqplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

// op_type present but not a string → StrictString fails and the function
// returns (nil, nil).
func TestPrepareIvfpqIndexContext_OpTypeNotString(t *testing.T) {
	b := newBuilder(t)
	v := &vectorplan.VectorSortContext{DistFnExpr: &pbplan.Function{Func: &pbplan.ObjectRef{ObjName: "l2_distance"}}}
	mti := ivfpqMTI(`{"op_type": 123}`)
	r, err := ivfpqplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_ArgsNotFound(t *testing.T) {
	b := newBuilder(t)
	scan := ivfpqScanNode()
	v := &vectorplan.VectorSortContext{
		DistFnExpr: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
			Args: []*pbplan.Expr{
				{Typ: pbplan.Type{Id: int32(types.T_array_float32)}, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{}}},
				{Typ: pbplan.Type{Id: int32(types.T_array_float32)}, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{}}},
			},
		},
		ScanNode: scan,
	}
	mti := ivfpqMTI(`{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`)
	r, err := ivfpqplan.PrepareContext(b, v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_ResolveThreadsError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "ivfpq_threads_search" {
				return nil, moerr.NewInternalError(context.Background(), "threads error")
			}
			return int64(0), nil
		},
	}
	b := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	r, err := ivfpqplan.PrepareContext(b, ivfpqVecCtx(ivfpqScanNode()),
		ivfpqMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "threads error")
}

func TestPrepareIvfpqIndexContext_ResolveBatchWindowError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "ivfpq_threads_search" {
				return int64(4), nil
			}
			if name == "ivfpq_batch_window" {
				return nil, moerr.NewInternalError(context.Background(), "batch_window error")
			}
			return int64(0), nil
		},
	}
	b := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	r, err := ivfpqplan.PrepareContext(b, ivfpqVecCtx(ivfpqScanNode()),
		ivfpqMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "batch_window error")
}

func TestPrepareIvfpqIndexContext_ResolveProbeLimitError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "ivfpq_threads_search" {
				return int64(4), nil
			}
			if name == "ivfpq_batch_window" {
				return int64(64), nil
			}
			if name == "probe_limit" {
				return nil, moerr.NewInternalError(context.Background(), "probe_limit error")
			}
			return int64(0), nil
		},
	}
	b := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	r, err := ivfpqplan.PrepareContext(b, ivfpqVecCtx(ivfpqScanNode()),
		ivfpqMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "probe_limit error")
}

func TestPrepareIvfpqIndexContext_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "ivfpq_threads_search":
				return int64(8), nil
			case "ivfpq_batch_window":
				return int64(64), nil
			case "probe_limit":
				return int64(15), nil
			}
			return int64(0), nil
		},
	}
	b := sqlplan.NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	algo := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "lists": "100", "m": "8"}`
	r, err := ivfpqplan.PrepareContext(b, ivfpqVecCtx(ivfpqScanNode()), ivfpqMTI(algo))
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.Equal(t, "l2_distance", r.OrigFuncName())
	assert.Equal(t, int32(0), r.PartPos())
	assert.Equal(t, int32(1), r.PkPos())
	assert.Equal(t, algo, r.Params())
	assert.Equal(t, int64(8), r.NThread())
	assert.Equal(t, int64(64), r.BatchWindow())
	assert.Equal(t, int64(15), r.NProbe())
	assert.NotNil(t, r.VecLitArg())
}

// ---- Hooks.ApplyForSort ---------------------------------------------------

func TestApplyIndicesForSortUsingIvfpq_NilGuards(t *testing.T) {
	b := newBuilder(t)

	got, applied, err := ivfpqplan.Hooks{}.ApplyForSort(b, nil, &vectorplan.MultiTableIndexRef{}, 7, vectorplan.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(7), got)

	got, applied, err = ivfpqplan.Hooks{}.ApplyForSort(b, &vectorplan.VectorSortContext{}, &vectorplan.MultiTableIndexRef{}, 7, vectorplan.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(7), got)

	got, applied, err = ivfpqplan.Hooks{}.ApplyForSort(b, &vectorplan.VectorSortContext{SortNode: &pbplan.Node{}}, &vectorplan.MultiTableIndexRef{}, 7, vectorplan.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(7), got)
}

func TestApplyIndicesForSortUsingIvfpq_PrepareReturnsNil(t *testing.T) {
	b := newBuilder(t)
	scan := ivfpqScanNode()
	v := ivfpqVecCtx(scan)
	v.SortNode = &pbplan.Node{}
	v.RankOption = &pbplan.RankOption{Mode: "force"}

	got, applied, err := ivfpqplan.Hooks{}.ApplyForSort(b, v, &vectorplan.MultiTableIndexRef{}, 0, vectorplan.ApplyForSortOpts{})
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int32(0), got)
}

func TestApplyIndicesForSortUsingIvfpq_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "ivfpq_threads_search":
				return int64(4), nil
			case "ivfpq_batch_window":
				return int64(64), nil
			case "probe_limit":
				return int64(10), nil
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
	vecCtx := &vectorplan.VectorSortContext{
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
	mti := &vectorplan.MultiTableIndexRef{
		IndexAlgo: catalog.MoIndexIvfpqAlgo.ToString(),
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Ivfpq_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, applied, err := ivfpqplan.Hooks{}.ApplyForSort(builder, vecCtx, mti, scanNodeID, vectorplan.ApplyForSortOpts{})
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
	assert.Equal(t, ivfpqplan.IVFPQSearchFuncName, right.TableDef.TblFunc.Name)
}

// TestApplyIndicesForSortUsingIvfpq_RichPushdown drives the optimizer through
// branches the basic success/over-fetch tests don't reach:
//   - INCLUDE columns + PK pushdown into the predsJSON arg
//   - a peelable distance filter that lands on tableFuncNode.FilterList
//   - constant-limit + residual filter → the over-fetch numeric branch
//   - vecCtx.ChildNode set so the projMap rewrite runs
func TestApplyIndicesForSortUsingIvfpq_RichPushdown(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "ivfpq_threads_search":
				return int64(4), nil
			case "ivfpq_batch_window":
				return int64(64), nil
			case "probe_limit":
				return int64(10), nil
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
			{Name: "price", Typ: pbplan.Type{Id: int32(types.T_float32)}},
		},
		Pkey:          &pbplan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1, "price": 2},
	}
	scanTag := builder.GenNewBindTag()
	vecTyp := pbplan.Type{Id: int32(types.T_array_float32)}

	priceFilter := &pbplan.Expr{
		Typ: pbplan.Type{Id: int32(types.T_bool)},
		Expr: &pbplan.Expr_F{F: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: "<"},
			Args: []*pbplan.Expr{
				{Typ: pbplan.Type{Id: int32(types.T_float32)}, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 2, Name: "price"}}},
				{Typ: pbplan.Type{Id: int32(types.T_float32)}, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_Fval{Fval: 10}}}},
			},
		}},
	}

	distFilter := &pbplan.Expr{
		Typ: pbplan.Type{Id: int32(types.T_bool)},
		Expr: &pbplan.Expr_F{F: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: "<"},
			Args: []*pbplan.Expr{
				{
					Typ: pbplan.Type{Id: int32(types.T_float64)},
					Expr: &pbplan.Expr_F{F: &pbplan.Function{
						Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
						Args: []*pbplan.Expr{
							{Typ: vecTyp, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 1}}},
							{Typ: vecTyp, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
						},
					}},
				},
				{Typ: pbplan.Type{Id: int32(types.T_float32)}, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_Fval{Fval: 0.5}}}},
			},
		}},
	}

	residual := &pbplan.Expr{Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_Bval{Bval: true}}}}

	scanNode := &pbplan.Node{
		NodeType:    pbplan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &pbplan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{scanTag},
		FilterList:  []*pbplan.Expr{priceFilter, distFilter, residual},
	}
	scanNodeID := builder.AppendNode(scanNode, bindCtx)

	distFnExpr := &pbplan.Function{
		Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
		Args: []*pbplan.Expr{
			{Typ: vecTyp, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 1}}},
			{Typ: vecTyp, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}

	childTag := builder.GenNewBindTag()
	childNode := &pbplan.Node{
		NodeType:    pbplan.Node_PROJECT,
		BindingTags: []int32{childTag},
		ProjectList: []*pbplan.Expr{
			{
				Typ: pbplan.Type{Id: int32(types.T_float64)},
				Expr: &pbplan.Expr_F{F: &pbplan.Function{
					Func: &pbplan.ObjectRef{ObjName: "l2_distance"},
					Args: []*pbplan.Expr{
						{Typ: vecTyp, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 1}}},
						{Typ: vecTyp, Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
					},
				}},
			},
			{Typ: pbplan.Type{Id: int32(types.T_int64)}, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: scanTag, ColPos: 0}}},
		},
	}

	projTag := builder.GenNewBindTag()
	projNode := &pbplan.Node{
		NodeType:    pbplan.Node_PROJECT,
		BindingTags: []int32{projTag},
		Children:    []int32{scanNodeID},
		ProjectList: []*pbplan.Expr{
			{Typ: pbplan.Type{Id: int32(types.T_float64)}, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: childTag, ColPos: 0}}},
		},
	}

	vecCtx := &vectorplan.VectorSortContext{
		ScanNode:   scanNode,
		SortNode:   &pbplan.Node{NodeType: pbplan.Node_SORT, Offset: &pbplan.Expr{}},
		ProjNode:   projNode,
		ChildNode:  childNode,
		DistFnExpr: distFnExpr,
		OrderExpr: &pbplan.Expr{
			Typ:  pbplan.Type{Id: int32(types.T_float64)},
			Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{ColPos: 0}},
		},
		Limit:      &pbplan.Expr{Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Value: &pbplan.Literal_U64Val{U64Val: 5}}}},
		RankOption: &pbplan.RankOption{Mode: "pre"},
	}

	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "included_columns":"price"}`
	mti := &vectorplan.MultiTableIndexRef{
		IndexAlgo: catalog.MoIndexIvfpqAlgo.ToString(),
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Ivfpq_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, applied, err := ivfpqplan.Hooks{}.ApplyForSort(builder, vecCtx, mti, scanNodeID, vectorplan.ApplyForSortOpts{})
	require.NoError(t, err)
	require.True(t, applied)

	sortID := vecCtx.ProjNode.Children[0]
	q := builder.Query()
	sort := q.Nodes[sortID]
	join := q.Nodes[sort.Children[0]]
	tf := q.Nodes[join.Children[1]]
	assert.Equal(t, pbplan.Node_FUNCTION_SCAN, tf.NodeType)
	assert.Equal(t, 3, len(tf.TblFuncExprList), "expected predsJSON arg appended")
	assert.NotEmpty(t, tf.FilterList)
}

func TestApplyIndicesForSortUsingIvfpq_Success_WithFiltersOverFetch(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: sqlplan.NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "ivfpq_threads_search":
				return int64(4), nil
			case "ivfpq_batch_window":
				return int64(64), nil
			case "probe_limit":
				return int64(10), nil
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
	vecCtx := &vectorplan.VectorSortContext{
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
	mti := &vectorplan.MultiTableIndexRef{
		IndexAlgo: catalog.MoIndexIvfpqAlgo.ToString(),
		IndexDefs: map[string]*pbplan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Ivfpq_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, applied, err := ivfpqplan.Hooks{}.ApplyForSort(builder, vecCtx, mti, scanNodeID, vectorplan.ApplyForSortOpts{})
	require.NoError(t, err)
	require.True(t, applied)
}
