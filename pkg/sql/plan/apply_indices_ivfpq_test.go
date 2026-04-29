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

// ivfpqScanNode mirrors cagraScanNode for the ivfpq tests — same column shape
// (vec_col at pos 0, id PK at pos 1).
func ivfpqScanNode() *plan.Node {
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

func ivfpqVecCtx(scanNode *plan.Node) *vectorSortContext {
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

func ivfpqMTI(algoParams string) *MultiTableIndex {
	return &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
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

func TestPrepareIvfpqIndexContext_NilVecCtx(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	r, err := b.prepareIvfpqIndexContext(nil, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilMultiTableIndex(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	r, err := b.prepareIvfpqIndexContext(&vectorSortContext{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilDistFnExpr(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	r, err := b.prepareIvfpqIndexContext(&vectorSortContext{distFnExpr: nil}, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_ForceMode(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{
		distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}},
		rankOption: &plan.RankOption{Mode: "force"},
	}
	r, err := b.prepareIvfpqIndexContext(v, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_DescBlocksRewrite(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{
		distFnExpr:    &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}},
		sortDirection: plan.OrderBySpec_DESC,
	}
	r, err := b.prepareIvfpqIndexContext(v, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilMetaDef(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: nil,
			catalog.Ivfpq_TblType_Storage:  {},
		},
	}
	r, err := b.prepareIvfpqIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_NilIdxDef(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := &MultiTableIndex{
		IndexDefs: map[string]*plan.IndexDef{
			catalog.Ivfpq_TblType_Metadata: {},
			catalog.Ivfpq_TblType_Storage:  nil,
		},
	}
	r, err := b.prepareIvfpqIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_InvalidAlgoParamsJSON(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := ivfpqMTI("not valid json")
	r, err := b.prepareIvfpqIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_OpTypeMismatch(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := ivfpqMTI(`{"op_type": "vector_cosine_ops"}`)
	r, err := b.prepareIvfpqIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

// op_type present but not a string → StrictString fails and the function
// returns (nil, nil).
func TestPrepareIvfpqIndexContext_OpTypeNotString(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	v := &vectorSortContext{distFnExpr: &plan.Function{Func: &ObjectRef{ObjName: "l2_distance"}}}
	mti := ivfpqMTI(`{"op_type": 123}`)
	r, err := b.prepareIvfpqIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_ArgsNotFound(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	scan := ivfpqScanNode()
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
	mti := ivfpqMTI(`{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`)
	r, err := b.prepareIvfpqIndexContext(v, mti)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPrepareIvfpqIndexContext_ResolveThreadsError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			if name == "ivfpq_threads_search" {
				return nil, moerr.NewInternalError(context.Background(), "threads error")
			}
			return int64(0), nil
		},
	}
	b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	r, err := b.prepareIvfpqIndexContext(ivfpqVecCtx(ivfpqScanNode()),
		ivfpqMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "threads error")
}

func TestPrepareIvfpqIndexContext_ResolveBatchWindowError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
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
	b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	r, err := b.prepareIvfpqIndexContext(ivfpqVecCtx(ivfpqScanNode()),
		ivfpqMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "batch_window error")
}

func TestPrepareIvfpqIndexContext_ResolveProbeLimitError(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
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
	b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	r, err := b.prepareIvfpqIndexContext(ivfpqVecCtx(ivfpqScanNode()),
		ivfpqMTI(`{"op_type": "`+metric.DistFuncOpTypes["l2_distance"]+`"}`))
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "probe_limit error")
}

func TestPrepareIvfpqIndexContext_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
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
	b := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	algo := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "lists": "100", "m": "8"}`
	r, err := b.prepareIvfpqIndexContext(ivfpqVecCtx(ivfpqScanNode()), ivfpqMTI(algo))
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.Equal(t, "l2_distance", r.origFuncName)
	assert.Equal(t, int32(0), r.partPos)
	assert.Equal(t, int32(1), r.pkPos)
	assert.Equal(t, algo, r.params)
	assert.Equal(t, int64(8), r.nThread)
	assert.Equal(t, int64(64), r.batchWindow)
	assert.Equal(t, int64(15), r.nProbe)
	assert.NotNil(t, r.vecLitArg)
}

func TestApplyIndicesForSortUsingIvfpq_NilGuards(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	got, err := b.applyIndicesForSortUsingIvfpq(7, nil, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(7), got)

	got, err = b.applyIndicesForSortUsingIvfpq(7, &vectorSortContext{}, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(7), got)

	got, err = b.applyIndicesForSortUsingIvfpq(7, &vectorSortContext{sortNode: &plan.Node{}}, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(7), got)
}

func TestApplyIndicesForSortUsingIvfpq_PrepareReturnsNil(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	// applyIndicesForSortUsingIvfpq indexes builder.ctxByNode[nodeID] before
	// calling prepare, so we must seed at least one slot.
	b.ctxByNode = append(b.ctxByNode, NewBindContext(b, nil))

	scan := ivfpqScanNode()
	v := ivfpqVecCtx(scan)
	v.sortNode = &plan.Node{}
	v.rankOption = &plan.RankOption{Mode: "force"}

	got, err := b.applyIndicesForSortUsingIvfpq(0, v, &MultiTableIndex{})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), got)
}

func TestApplyIndicesForSortUsingIvfpq_Success(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
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
		IndexAlgo: catalog.MoIndexIvfpqAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
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

	_, err := builder.applyIndicesForSortUsingIvfpq(scanNodeID, vecCtx, mti)
	require.NoError(t, err)

	sortID := vecCtx.projNode.Children[0]
	sort := builder.qry.Nodes[sortID]
	require.Equal(t, plan.Node_SORT, sort.NodeType)
	joinID := sort.Children[0]
	join := builder.qry.Nodes[joinID]
	require.Equal(t, plan.Node_JOIN, join.NodeType)
	right := builder.qry.Nodes[join.Children[1]]
	assert.Equal(t, plan.Node_FUNCTION_SCAN, right.NodeType)
	assert.Equal(t, kIVFPQSearchFuncName, right.TableDef.TblFunc.Name)
}

// TestApplyIndicesForSortUsingIvfpq_RichPushdown drives the optimizer through
// the branches the basic success/over-fetch tests don't reach:
//   - INCLUDE columns + PK pushdown into the predsJSON arg
//   - a peelable distance filter that lands on tableFuncNode.FilterList
//   - constant-limit + residual filter → the over-fetch numeric branch
//   - vecCtx.childNode set so the projMap rewrite runs
func TestApplyIndicesForSortUsingIvfpq_RichPushdown(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
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
	builder := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	bindCtx := NewBindContext(builder, nil)

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
		limit:      &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 5}}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}

	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `", "included_columns":"price"}`
	mti := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfpqAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
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

	_, err := builder.applyIndicesForSortUsingIvfpq(scanNodeID, vecCtx, mti)
	require.NoError(t, err)

	sortID := vecCtx.projNode.Children[0]
	sort := builder.qry.Nodes[sortID]
	join := builder.qry.Nodes[sort.Children[0]]
	tf := builder.qry.Nodes[join.Children[1]]
	assert.Equal(t, plan.Node_FUNCTION_SCAN, tf.NodeType)
	assert.Equal(t, 3, len(tf.TblFuncExprList), "expected predsJSON arg appended")
	assert.NotEmpty(t, tf.FilterList)
}

func TestApplyIndicesForSortUsingIvfpq_Success_WithFiltersOverFetch(t *testing.T) {
	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
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
		limit:      &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{}}},
		rankOption: &plan.RankOption{Mode: "pre"},
	}
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	mti := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfpqAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
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

	_, err := builder.applyIndicesForSortUsingIvfpq(scanNodeID, vecCtx, mti)
	require.NoError(t, err)
}
