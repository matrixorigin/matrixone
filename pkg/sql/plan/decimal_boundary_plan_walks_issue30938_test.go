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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// This file pins the plan-walk arms of the issue #30938 execute-time
// machinery against approved design revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780):
// the fold engine's provenance guard (design decision log 54), the
// prepared plan's widened-decimal scan (decision log 69-70), the
// set-operation common-type retry funnel (decision log 73 (iii)), the
// WINDOW branch of the execute-time projection refresh, the CTE shadow
// reservation's provenance walk (decision log 50), and the exact-common
// width/parent-domain helpers. Every expectation derives from the design
// and the parent's own registered rules, never from observed candidate
// behavior.

func guardWalkWideCol() *pb.Expr {
	return &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 0},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}},
	}
}

func guardWalkDecimalLit(oid types.T, width, scale int32) *pb.Expr {
	return &pb.Expr{
		Typ:  pb.Type{Id: int32(oid), Width: width, Scale: scale},
		Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Decimal128Val{Decimal128Val: &pb.Decimal128{A: 1}}}},
	}
}

func TestDecimalBoundaryConstantFoldGuardIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	narrow := types.New(types.T_decimal128, 38, 0)
	mul, err := function.GetFunctionByName(proc.Ctx, "*", []types.Type{narrow, narrow})
	require.NoError(t, err)
	wide := mul.GetReturnType()
	require.Equal(t, types.T_decimal256, wide.Oid)
	require.Greater(t, wide.Width, int32(65))
	literalOf := func(a int64) *pb.Expr {
		return &pb.Expr{
			Typ:  pb.Type{Id: int32(narrow.Oid), Width: narrow.Width, Scale: narrow.Scale},
			Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Decimal128Val{Decimal128Val: &pb.Decimal128{A: a}}}},
		}
	}
	star := &pb.Expr{
		Typ: pb.Type{Id: int32(wide.Oid), Width: wide.Width, Scale: wide.Scale},
		Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "*", Obj: mul.GetEncodedOverloadID()},
			Args: []*pb.Expr{literalOf(3), literalOf(4)},
		}},
	}
	folded, err := ConstantFold(nil, star, proc, false, false)
	require.NoError(t, err)
	require.Same(t, star, folded,
		"the fold guard keeps the widened product's executable shape (decision log 54)")
	require.NotNil(t, folded.GetF())
}

func TestPreparedPlanCarriesWidenedDecimalIssue30938(t *testing.T) {
	varcharTyp := pb.Type{Id: int32(types.T_varchar), Width: 36, Scale: 0}
	queryWith := func(exprs ...*pb.Expr) *pb.Query {
		return &pb.Query{Nodes: []*pb.Node{
			{NodeType: pb.Node_PROJECT, NodeId: 0, ProjectList: exprs},
		}}
	}
	t.Run("window function payload", func(t *testing.T) {
		expr := &pb.Expr{Typ: varcharTyp, Expr: &pb.Expr_W{W: &pb.WindowSpec{
			WindowFunc: guardWalkWideCol()}}}
		require.True(t, preparedPlanCarriesWidenedDecimal(queryWith(expr)))
	})
	t.Run("window partition payload", func(t *testing.T) {
		expr := &pb.Expr{Typ: varcharTyp, Expr: &pb.Expr_W{W: &pb.WindowSpec{
			PartitionBy: []*pb.Expr{guardWalkWideCol()}}}}
		require.True(t, preparedPlanCarriesWidenedDecimal(queryWith(expr)))
	})
	t.Run("window order payload", func(t *testing.T) {
		expr := &pb.Expr{Typ: varcharTyp, Expr: &pb.Expr_W{W: &pb.WindowSpec{
			OrderBy: []*pb.OrderBySpec{{Expr: guardWalkWideCol()}}}}}
		require.True(t, preparedPlanCarriesWidenedDecimal(queryWith(expr)))
	})
	t.Run("list payload", func(t *testing.T) {
		expr := &pb.Expr{Typ: varcharTyp, Expr: &pb.Expr_List{List: &pb.ExprList{
			List: []*pb.Expr{guardWalkWideCol()}}}}
		require.True(t, preparedPlanCarriesWidenedDecimal(queryWith(expr)))
	})
	t.Run("nil node is skipped", func(t *testing.T) {
		require.False(t, preparedPlanCarriesWidenedDecimal(&pb.Query{Nodes: []*pb.Node{nil}}))
	})
	t.Run("node order-by payload", func(t *testing.T) {
		query := &pb.Query{Nodes: []*pb.Node{
			{NodeType: pb.Node_SORT, NodeId: 0,
				OrderBy: []*pb.OrderBySpec{{Expr: guardWalkWideCol()}}, ProjectList: []*pb.Expr{}},
		}}
		require.True(t, preparedPlanCarriesWidenedDecimal(query))
	})
	t.Run("node limit payload", func(t *testing.T) {
		query := &pb.Query{Nodes: []*pb.Node{
			{NodeType: pb.Node_PROJECT, NodeId: 0, Limit: guardWalkWideCol(), ProjectList: []*pb.Expr{}},
		}}
		require.True(t, preparedPlanCarriesWidenedDecimal(query))
	})
}

func TestPreparedSetOperationCommonTypeRetryIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	currentOutputType := pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 0}
	// The widened publication DECIMAL256(76,0) beside DECIMAL64(20,10)
	// overflows the carrier under the surface merge (integral 76 + scale
	// 10 = 86 > 76); the parent publishes the product DECIMAL256(65,0),
	// whose merge converges integral 65 + scale 10 = DECIMAL256(75,10)
	// (design decision log 73 (iii)).
	product := func() *pb.Expr {
		return &pb.Expr{
			Typ: pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 0},
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "*"},
				Args: []*pb.Expr{
					{Typ: pb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 0},
						Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}}},
					{Typ: pb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 0},
						Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 1}}},
				},
			}},
		}
	}
	decimal6410 := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_decimal64), Width: 20, Scale: 10},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}},
	}
	intLit := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_int64), Width: 19, Scale: 0},
		Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: 5}}},
	}
	wideCol := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_decimal256), Width: 76, Scale: 20},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}},
	}
	buildQuery := func(children []int32, nodeCount int32) (*pb.Query, *pb.Node) {
		nodes := make([]*pb.Node, 0, nodeCount)
		for i := int32(0); i < nodeCount-1; i++ {
			nodes = append(nodes, &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: i})
		}
		union := &pb.Node{NodeType: pb.Node_UNION_ALL, NodeId: nodeCount - 1, Children: children}
		nodes = append(nodes, union)
		return &pb.Query{Nodes: nodes, Steps: []int32{union.NodeId}}, union
	}

	t.Run("retry converges the parent common type", func(t *testing.T) {
		query, union := buildQuery([]int32{0, 1, 2}, 4)
		resolved, ok, err := preparedSetOperationCommonType(
			proc.Ctx, nil, query, union, currentOutputType,
			[]*pb.Expr{product(), decimal6410, intLit}, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.T_decimal256, resolved.Oid)
		require.Equal(t, int32(75), resolved.Width)
		require.Equal(t, int32(10), resolved.Scale,
			"the parent merge over (65,0), (20,10) and the literal's (1,0) binds (75,10)")
	})
	t.Run("a still-overflowing retry re-arms the original error", func(t *testing.T) {
		query, union := buildQuery([]int32{0, 1}, 3)
		_, ok, err := preparedSetOperationCommonType(
			proc.Ctx, nil, query, union, currentOutputType,
			[]*pb.Expr{product(), wideCol}, nil)
		require.Error(t, err,
			"the parent's (65,0) beside the genuine (76,20) still overflows at 85 digits")
		require.False(t, ok)
	})
	t.Run("a short child list keeps the original error", func(t *testing.T) {
		query, union := buildQuery([]int32{0, 1}, 4)
		_, ok, err := preparedSetOperationCommonType(
			proc.Ctx, nil, query, union, currentOutputType,
			[]*pb.Expr{product(), decimal6410, intLit}, nil)
		require.Error(t, err)
		require.False(t, ok)
	})
	t.Run("an out-of-range child id keeps the original error", func(t *testing.T) {
		query, union := buildQuery([]int32{0, 1, 7}, 4)
		_, ok, err := preparedSetOperationCommonType(
			proc.Ctx, nil, query, union, currentOutputType,
			[]*pb.Expr{product(), decimal6410, intLit}, nil)
		require.Error(t, err)
		require.False(t, ok)
	})
}

func TestRefreshPreparedPlanWindowArmsIssue30938(t *testing.T) {
	decimalTyp := pb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	lit := func() *pb.Expr {
		return &pb.Expr{
			Typ:  decimalTyp,
			Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: 1}}},
		}
	}
	spec := &pb.Expr{
		Typ: decimalTyp,
		Expr: &pb.Expr_W{W: &pb.WindowSpec{
			WindowFunc:  lit(),
			PartitionBy: []*pb.Expr{nil, lit()},
			OrderBy:     []*pb.OrderBySpec{nil, {Expr: lit()}},
			Frame: &pb.FrameClause{
				Start: &pb.FrameBound{Val: lit()},
				End:   &pb.FrameBound{Val: lit()},
			},
		}},
	}
	windowNode := &pb.Node{
		NodeType:    pb.Node_WINDOW,
		NodeId:      0,
		Children:    []int32{99},
		WinSpecList: []*pb.Expr{nil, lit(), spec},
	}
	query := &pb.Query{Nodes: []*pb.Node{windowNode}, Steps: []int32{0}}
	plan0 := &pb.Plan{Plan: &pb.Plan_Query{Query: query}}
	_, err := refreshPreparedPlanProjectionTypes(
		context.Background(), plan0, nil, nil,
		NewResetParamRefRule(context.Background(), nil))
	require.NoError(t, err,
		"every spec-internal expression refreshes through the window node's own view")
}

func TestCteShadowProvenanceWalkGuardsIssue30938(t *testing.T) {
	newBuilder := func(nodes ...*pb.Node) *QueryBuilder {
		return &QueryBuilder{qry: &Query{Nodes: nodes}, tag2NodeID: map[int32]int32{}}
	}
	t.Run("exported slot outside the node inventory misses", func(t *testing.T) {
		builder := newBuilder(&pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0})
		require.False(t, builder.cteExportedSlotWideDecimalProvenance(-5, 0))
	})
	t.Run("slot descent outside the node inventory misses", func(t *testing.T) {
		builder := newBuilder(&pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0})
		require.False(t, builder.cteSlotWideDecimalProvenance(7, 0, true, make(map[[2]int32]bool)))
	})
	t.Run("slot descent over a nil node misses", func(t *testing.T) {
		builder := newBuilder(nil)
		require.False(t, builder.cteSlotWideDecimalProvenance(0, 0, true, make(map[[2]int32]bool)))
	})
	t.Run("mirror chain with an unknown binding tag misses", func(t *testing.T) {
		project := &pb.Node{NodeType: pb.Node_PROJECT, NodeId: 0, ProjectList: []*pb.Expr{
			{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 42, ColPos: 0}}},
		}}
		builder := newBuilder(project)
		require.False(t, builder.cteSlotWideDecimalProvenance(0, 0, true, make(map[[2]int32]bool)),
			"a column whose RelPos names no bound node resolves nothing")
	})
	t.Run("pass-through self loop misses", func(t *testing.T) {
		filter := &pb.Node{NodeType: pb.Node_FILTER, NodeId: 0, Children: []int32{0}}
		builder := newBuilder(filter)
		require.False(t, builder.cteSlotWideDecimalProvenance(0, 0, true, make(map[[2]int32]bool)))
	})
	t.Run("scalar operand with an unknown binding tag misses", func(t *testing.T) {
		builder := newBuilder(&pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0})
		expr := &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 42, ColPos: 0}}}
		require.False(t, builder.cteScalarOperandWideDecimalProvenance(expr, 0, make(map[[2]int32]bool)))
	})
	t.Run("scalar operand behind a row-changing branch scans the subtree", func(t *testing.T) {
		union := &pb.Node{NodeType: pb.Node_UNION, NodeId: 1}
		builder := newBuilder(&pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0}, union)
		builder.tag2NodeID[7] = 1
		expr := &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 7, ColPos: 0}}}
		require.False(t, builder.cteScalarOperandWideDecimalProvenance(expr, 0, make(map[[2]int32]bool)),
			"the conservative branch scan finds no widened product in an empty union")
	})
	t.Run("scalar operand call without a payload misses", func(t *testing.T) {
		builder := newBuilder(&pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0})
		expr := &pb.Expr{Expr: &pb.Expr_F{F: nil}}
		require.False(t, builder.cteScalarOperandWideDecimalProvenance(expr, 0, make(map[[2]int32]bool)))
	})
	t.Run("scalar operand skips target descriptors", func(t *testing.T) {
		builder := newBuilder(&pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0})
		expr := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "cast"},
			Args: []*pb.Expr{{Expr: &pb.Expr_T{T: &pb.TargetType{}}}},
		}}}
		require.False(t, builder.cteScalarOperandWideDecimalProvenance(expr, 0, make(map[[2]int32]bool)),
			"a binder-produced target descriptor is metadata, not a value leaf")
	})
	t.Run("subtree walk guards", func(t *testing.T) {
		builder := newBuilder(&pb.Node{NodeType: pb.Node_SINK_SCAN, NodeId: 1})
		require.False(t, builder.cteSubtreeWideDecimalProvenance(-1, make(map[int32]bool)))
		require.True(t, builder.cteSubtreeWideDecimalProvenance(0, make(map[int32]bool)),
			"a shared inner producer stays conservative")
	})
	t.Run("subtree walk over a nil node misses", func(t *testing.T) {
		builder := newBuilder(nil)
		require.False(t, builder.cteSubtreeWideDecimalProvenance(0, make(map[int32]bool)))
	})
	t.Run("subtree walk reaches a child's payload", func(t *testing.T) {
		child := &pb.Node{NodeType: pb.Node_SINK_SCAN, NodeId: 1}
		parent := &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: 0, Children: []int32{1}}
		builder := newBuilder(parent, child)
		require.True(t, builder.cteSubtreeWideDecimalProvenance(0, make(map[int32]bool)))
	})
	t.Run("node classification tolerates nil", func(t *testing.T) {
		require.False(t, cteShadowRowChangingNode(nil))
		require.False(t, cteShadowPassThroughNode(nil))
	})
}

func TestPreparedFixedDecimalGuardsIssue30938(t *testing.T) {
	floatLit := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_float64), Width: 0, Scale: 0},
		Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Dval{Dval: 1.5}}},
	}
	t.Run("inexact float operand without a text prefix declines", func(t *testing.T) {
		_, fixed, converged := preparedFixedDecimalCommonWidth(
			[]*pb.Expr{floatLit}, nil, nil, nil)
		require.False(t, fixed)
		require.True(t, converged,
			"the float arm converges to the family's fallback without a fixed width")
	})
	t.Run("out-of-domain operand declines", func(t *testing.T) {
		dateLit := &pb.Expr{
			Typ:  pb.Type{Id: int32(types.T_date), Width: 0, Scale: 0},
			Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: 1}}},
		}
		_, fixed, converged := preparedFixedDecimalCommonWidth(
			[]*pb.Expr{dateLit}, nil, nil, nil)
		require.False(t, fixed)
		require.True(t, converged)
	})
	t.Run("width ladder", func(t *testing.T) {
		typ, fixed, converged := preparedFixedDecimalCommonWidth(
			[]*pb.Expr{guardWalkDecimalLit(types.T_decimal64, 10, 2)}, nil, nil, nil)
		require.True(t, fixed)
		require.True(t, converged)
		require.Equal(t, types.T_decimal64, typ.Oid)
		require.Equal(t, int32(10), typ.Width)
		require.Equal(t, int32(2), typ.Scale)

		typ, fixed, converged = preparedFixedDecimalCommonWidth(
			[]*pb.Expr{guardWalkDecimalLit(types.T_decimal128, 20, 2)}, nil, nil, nil)
		require.True(t, fixed)
		require.True(t, converged)
		require.Equal(t, types.T_decimal128, typ.Oid)
		require.Equal(t, int32(20), typ.Width)
	})
	t.Run("parent domains decline without a process", func(t *testing.T) {
		_, ok := preparedFixedDecimalParentDomains(context.Background(), nil,
			[]*pb.Expr{guardWalkDecimalLit(types.T_decimal128, 38, 0)})
		require.False(t, ok)
	})
	t.Run("parent domains decline without a resolution scope", func(t *testing.T) {
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		_, ok := preparedFixedDecimalParentDomains(context.Background(), proc,
			[]*pb.Expr{guardWalkDecimalLit(types.T_decimal128, 38, 0)})
		require.False(t, ok)
	})
	t.Run("parent domains decline on a list operand", func(t *testing.T) {
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		ctx := context.WithValue(context.Background(), decimalBoundaryScopeContextKey{},
			&decimalBoundaryScope{})
		_, ok := preparedFixedDecimalParentDomains(ctx, proc, []*pb.Expr{nil})
		require.False(t, ok)
	})
	t.Run("parent domains decline on an unresolvable decimal operand", func(t *testing.T) {
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		ctx := context.WithValue(context.Background(), decimalBoundaryScopeContextKey{},
			&decimalBoundaryScope{})
		sum := &pb.Expr{
			Typ:  pb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 0},
			Expr: &pb.Expr_F{F: &pb.Function{Func: nil}},
		}
		_, ok := preparedFixedDecimalParentDomains(ctx, proc, []*pb.Expr{sum})
		require.False(t, ok)
	})
}
