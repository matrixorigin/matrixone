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

// cagraScanNode builds a minimal scan node fixture suitable for the cagra
// prepare-context tests. Same shape used by hnsw/ivfflat fixtures: vec_col at
// pos 0, id PK at pos 1.
func cagraScanNode() *plan.Node {
	return &plan.Node{
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
}

// cagraVecCtx wraps the supplied scanNode in a vectorSortContext with a
// l2_distance(col, vec_lit) shape — matches what buildVectorSortContext
// produces in the planner for the prepare* path.
func cagraVecCtx(scanNode *plan.Node) *vectorSortContext {
	return &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
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
}

// cagraMTI builds a MultiTableIndex with the given algo params on the
// metadata def; the storage def carries the part list used by getArgsFromDistFn.
func cagraMTI(algoParams string) *MultiTableIndex {
	return &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Cagra_TblType_Metadata: {
				IndexAlgoParams: algoParams,
			},
			catalog.Cagra_TblType_Storage: {
				Parts:           []string{"vec_col"},
				IndexAlgoParams: algoParams,
			},
		},
	}
}

func TestPrepareCagraIndexContext_NilVecCtx(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	r, err := b.prepareCagraIndexContext(nil, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_NilMultiTableIndex(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	r, err := b.prepareCagraIndexContext(&vectorSortContext{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_NilDistFnExpr(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	r, err := b.prepareCagraIndexContext(&vectorSortContext{distFnExpr: nil}, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_ForceMode(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{
		distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}},
		rankOption: &plan.RankOption{Mode: "force"},
	}
	r, err := b.prepareCagraIndexContext(v, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_DescBlocksRewrite(t *testing.T) {
	// validateVectorIndexSortRewrite returns false for DESC.
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{
		distFnExpr:    &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}},
		sortDirection: plan.OrderBySpec_DESC,
	}
	r, err := b.prepareCagraIndexContext(v, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_NilMetaDef(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Cagra_TblType_Metadata: nil,
			catalog.Cagra_TblType_Storage:  {},
		},
	}
	r, err := b.prepareCagraIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_NilIdxDef(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Cagra_TblType_Metadata: {},
			catalog.Cagra_TblType_Storage:  nil,
		},
	}
	r, err := b.prepareCagraIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_InvalidAlgoParamsJSON(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := cagraMTI("not valid json")
	r, err := b.prepareCagraIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_OpTypeMismatch(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := cagraMTI(`{"op_type": "vector_cosine_ops"}`)
	r, err := b.prepareCagraIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

// op_type present but not a string → StrictString fails and the function
// returns (nil, nil).
func TestPrepareCagraIndexContext_OpTypeNotString(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := cagraMTI(`{"op_type": 123}`)
	r, err := b.prepareCagraIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_ArgsNotFound(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	scan := cagraScanNode()
	// Both args are literals → getArgsFromDistFn returns found=false.
	v := &vectorSortContext{
		distFnExpr: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_array_float32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}},
				{Typ: plan.Type{Id: int32(types.T_array_float32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}},
			},
		},
		scanNode: scan,
	}
	mti := cagraMTI(`{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`)
	r, err := b.prepareCagraIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareCagraIndexContext_ResolveThreadsError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "cagra_threads_search" {
				return nil, moerr.NewInternalError(context.Background(), "threads error")
			}
			return int64(0), nil
		},
	}
	b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	r, err := b.prepareCagraIndexContext(cagraVecCtx(cagraScanNode()),
		cagraMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "threads error")
}

func TestPrepareCagraIndexContext_ResolveBatchWindowError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "cagra_threads_search" {
				return int64(4), nil
			}
			if name == "cagra_batch_window" {
				return nil, moerr.NewInternalError(context.Background(), "batch_window error")
			}
			return int64(0), nil
		},
	}
	b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	r, err := b.prepareCagraIndexContext(cagraVecCtx(cagraScanNode()),
		cagraMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "batch_window error")
}

func TestPrepareCagraIndexContext_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "cagra_threads_search":
				return int64(8), nil
			case "cagra_batch_window":
				return int64(64), nil
			}
			return int64(0), nil
		},
	}
	b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	algo := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "m": 32}`
	r, err := b.prepareCagraIndexContext(cagraVecCtx(cagraScanNode()), cagraMTI(algo))
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.Equal(t, "l2_distance", r.origFuncName)
	assert.Equal(t, int32(0), r.partPos)
	assert.Equal(t, int32(1), r.pkPos)
	assert.Equal(t, algo, r.params)
	assert.Equal(t, int64(8), r.nThread)
	assert.Equal(t, int64(64), r.batchWindow)
	assert.NotNil(t, r.vecLitArg)
}

// applyIndicesForSortUsingCagra short-circuits cleanly when vecCtx or its
// inner sortNode/scanNode are nil; cover those guard paths. The full success
// path is exercised through the higher-level tests in apply_indices_test.go.
func TestApplyIndicesForSortUsingCagra_NilGuards(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	got, err := b.applyIndicesForSortUsingCagra(7, nil, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(7), got)

	got, err = b.applyIndicesForSortUsingCagra(7, &vectorSortContext{}, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(7), got)

	got, err = b.applyIndicesForSortUsingCagra(7, &vectorSortContext{sortNode: &plan.Node{}}, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(7), got)
}

// When prepareCagraIndexContext returns nil (e.g. force mode), the wrapper
// returns nodeID unchanged with no error.
func TestApplyIndicesForSortUsingCagra_PrepareReturnsNil(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	// applyIndicesForSortUsingCagra indexes builder.ctxByNode[nodeID] before
	// calling prepare, so we must seed at least one slot.
	b.ctxByNode = append(b.ctxByNode, NewBindContext(b, nil))

	scan := cagraScanNode()
	v := cagraVecCtx(scan)
	v.sortNode = &plan.Node{}
	v.rankOption = &plan.RankOption{Mode: "force"}

	got, err := b.applyIndicesForSortUsingCagra(0, v, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), got)
}

// applyIndicesForSortUsingCagraSuccess sets up a full vectorSortContext and
// MultiTableIndex and checks the pipeline produces a SORT → JOIN(SCAN, FUNC)
// chain with the expected node types.
func TestApplyIndicesForSortUsingCagra_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "cagra_threads_search":
				return int64(4), nil
			case "cagra_batch_window":
				return int64(64), nil
			}
			return int64(0), nil
		},
	}
	builder := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	bindCtx := NewBindContext(builder, nil)

	tableDef := &plan.TableDef{
		Name: "t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	scanNodeID := builder.appendNode(scanNode, bindCtx)

	// Pre-extend ctxByNode for the JOIN/SORT/FUNCTION_SCAN nodes the optimizer
	// will append.
	for i := 0; i < 30; i++ {
		builder.ctxByNode = append(builder.ctxByNode, bindCtx)
	}

	vecTyp := plan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
			{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}
	vecCtx := &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
		projNode: &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*plan.Expr{
				{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
			},
		},
		distFnExpr: distFnExpr,
		orderExpr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		},
		limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	mti := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexCagraAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Cagra_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Cagra_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, err := builder.applyIndicesForSortUsingCagra(scanNodeID, vecCtx, mti)
	require.NoError(t, err)

	// PROJECT now points at SORT → JOIN(SCAN, FUNCTION_SCAN)
	sortID := vecCtx.projNode.Children[0]
	sort := builder.qry.Nodes[sortID]
	require.Equal(t, plan.Node_SORT, sort.NodeType)

	joinID := sort.Children[0]
	join := builder.qry.Nodes[joinID]
	require.Equal(t, plan.Node_JOIN, join.NodeType)
	right := builder.qry.Nodes[join.Children[1]]
	assert.Equal(t, plan.Node_FUNCTION_SCAN, right.NodeType)
	assert.Equal(t, kCAGRASearchFuncName, right.TableDef.TblFunc.Name)
}

// TestApplyIndicesForSortUsingCagra_RichPushdown drives the optimizer through
// the branches the basic success/over-fetch tests don't reach:
//   - INCLUDE columns + PK pushdown into the predsJSON arg
//   - a peelable distance filter that lands on tableFuncNode.FilterList
//   - constant-limit + residual filter → the over-fetch numeric branch
//   - vecCtx.childNode set so the projMap rewrite runs
func TestApplyIndicesForSortUsingCagra_RichPushdown(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "cagra_threads_search":
				return int64(4), nil
			case "cagra_batch_window":
				return int64(64), nil
			}
			return int64(0), nil
		},
	}
	builder := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	bindCtx := NewBindContext(builder, nil)

	// 3-column table: id (PK), v (vec), price (INCLUDE)
	tableDef := &plan.TableDef{
		Name: "t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
			{Name: "price", Typ: plan.Type{Id: int32(types.T_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1, "price": 2},
	}
	scanTag := builder.genNewBindTag()
	vecTyp := plan.Type{Id: int32(types.T_array_float32)}

	// Filter "price < 10" — peelable into predsJSON (price is in INCLUDE list).
	priceFilter := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "<"},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_float32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 2, Name: "price"}}},
				{Typ: plan.Type{Id: int32(types.T_float32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 10}}}},
			},
		}},
	}

	// Distance filter "l2_distance(v, [1,1,1]) < 0.5" — peelable onto the
	// table function FilterList by peelAndRewriteDistFnFilters.
	distFilter := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "<"},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_float64)},
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &ObjectRef{ObjName: "l2_distance"},
						Args: []*plan.Expr{
							{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 1}}},
							{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
						},
					}},
				},
				{Typ: plan.Type{Id: int32(types.T_float32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 0.5}}}},
			},
		}},
	}

	// Residual filter that survives both peels — keeps over-fetch branch alive.
	residual := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}

	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{scanTag},
		FilterList:  []*plan.Expr{priceFilter, distFilter, residual},
	}
	scanNodeID := builder.appendNode(scanNode, bindCtx)
	for i := 0; i < 30; i++ {
		builder.ctxByNode = append(builder.ctxByNode, bindCtx)
	}

	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 1}}},
			{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}

	childTag := builder.genNewBindTag()
	childNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		BindingTags: []int32{childTag},
		ProjectList: []*plan.Expr{
			// One slot for the order-by distance, one passthrough.
			{
				Typ: plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &ObjectRef{ObjName: "l2_distance"},
					Args: []*plan.Expr{
						{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 1}}},
						{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
					},
				}},
			},
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 0}}},
		},
	}

	projTag := builder.genNewBindTag()
	projNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		BindingTags: []int32{projTag},
		Children:    []int32{scanNodeID},
		// Reference into childNode's projection — so replaceColumnsForNode
		// has something to rewrite in the projMap loop.
		ProjectList: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: childTag, ColPos: 0}}},
		},
	}

	vecCtx := &vectorSortContext{
		scanNode:   scanNode,
		sortNode:   &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
		projNode:   projNode,
		childNode:  childNode,
		distFnExpr: distFnExpr,
		orderExpr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		},
		// Constant limit + non-empty FilterList → triggers the over-fetch
		// numeric branch (lines that scale the limit by an over-fetch factor).
		limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 5}}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}

	// IndexAlgoParams declares "price" as INCLUDE; ensure both metaDef and
	// idxDef carry it (parseIncludedColumnsFromParams reads idxDef params).
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "included_columns":"price"}`
	mti := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexCagraAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Cagra_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Cagra_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, err := builder.applyIndicesForSortUsingCagra(scanNodeID, vecCtx, mti)
	require.NoError(t, err)

	// Locate the function-scan node and confirm INCLUDE pushdown produced a
	// 3rd arg (the predsJSON literal).
	sortID := vecCtx.projNode.Children[0]
	sort := builder.qry.Nodes[sortID]
	join := builder.qry.Nodes[sort.Children[0]]
	tf := builder.qry.Nodes[join.Children[1]]
	assert.Equal(t, plan.Node_FUNCTION_SCAN, tf.NodeType)
	assert.Equal(t, 3, len(tf.TblFuncExprList), "expected predsJSON arg appended")
	// Distance filter should have been peeled onto the function scan.
	assert.NotEmpty(t, tf.FilterList, "distance filter should land on the table function")
}

// Same as the success test but with a non-constant LIMIT and a residual
// FilterList on the scan, exercising the over-fetch limit branch and the
// peelAndRewriteDistFnFilters branch.
func TestApplyIndicesForSortUsingCagra_Success_WithFiltersOverFetch(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case "cagra_threads_search":
				return int64(4), nil
			case "cagra_batch_window":
				return int64(64), nil
			}
			return int64(0), nil
		},
	}
	builder := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	bindCtx := NewBindContext(builder, nil)

	tableDef := &plan.TableDef{
		Name: "t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
		// Trigger the "over-fetch"/limit branch with a residual filter the
		// pushdown can't peel.
		FilterList: []*plan.Expr{
			{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}},
		},
	}
	scanNodeID := builder.appendNode(scanNode, bindCtx)
	for i := 0; i < 30; i++ {
		builder.ctxByNode = append(builder.ctxByNode, bindCtx)
	}

	vecTyp := plan.Type{Id: int32(types.T_array_float32)}
	distFnExpr := &plan.Function{
		Func: &ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
			{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		},
	}
	vecCtx := &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
		projNode: &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*plan.Expr{
				{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
			},
		},
		distFnExpr: distFnExpr,
		orderExpr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		},
		// Non-Lit limit forces the "DeepCopyExpr(limit)" branch in the
		// over-fetch code path.
		limit:      &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	mti := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexCagraAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Cagra_TblType_Metadata: {
				IndexTableName:  "meta",
				IndexAlgoParams: idxAlgoParams,
			},
			catalog.Cagra_TblType_Storage: {
				IndexTableName:  "idx",
				Parts:           []string{"v"},
				IndexAlgoParams: idxAlgoParams,
			},
		},
	}

	_, err := builder.applyIndicesForSortUsingCagra(scanNodeID, vecCtx, mti)
	require.NoError(t, err)
}
