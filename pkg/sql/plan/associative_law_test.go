// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestOuterJoinAssociativityIsReachableFromSQL(t *testing.T) {
	t.Run("preserved side", func(t *testing.T) {
		logicalPlan, err := runOneStmt(NewMockOptimizer(false), t, `
			select n.n_nationkey, o.o_orderkey
			from nation n
			left join orders o on n.n_nationkey = o.o_custkey
			join region r on n.n_regionkey = r.r_regionkey`)
		require.NoError(t, err)

		query := logicalPlan.GetQuery()
		require.True(t, reachableJoinHasChildTableSets(
			query,
			planpb.Node_LEFT,
			[]string{"nation", "region"},
			[]string{"orders"},
		), query.String())
	})

	t.Run("nullable side", func(t *testing.T) {
		logicalPlan, err := runOneStmt(NewMockOptimizer(false), t, `
			select n.n_nationkey, c.c_custkey, o.o_orderkey
			from nation n
			left join customer c on n.n_nationkey = c.c_nationkey
			join orders o on c.c_custkey = o.o_custkey`)
		require.NoError(t, err)

		query := logicalPlan.GetQuery()
		require.False(t, reachablePlanHasJoinType(query, planpb.Node_LEFT), query.String())
		require.True(t, reachableJoinHasChildTableSets(
			query,
			planpb.Node_INNER,
			[]string{"nation"},
			[]string{"customer", "orders"},
		), query.String())
	})

	t.Run("rollback hint preserves legacy outer shapes", func(t *testing.T) {
		rt := runtime.ServiceRuntime("")
		rt.SetGlobalVariables("optimizer_hints", "outerAntiPlanning=1")
		defer rt.SetGlobalVariables("optimizer_hints", "")

		preservedPlan, err := runOneStmt(NewMockOptimizer(false), t, `
			select n.n_nationkey, o.o_orderkey
			from nation n
			left join orders o on n.n_nationkey = o.o_custkey
			join region r on n.n_regionkey = r.r_regionkey`)
		require.NoError(t, err)
		require.True(t, reachableJoinHasChildTableSets(
			preservedPlan.GetQuery(),
			planpb.Node_INNER,
			[]string{"nation", "orders"},
			[]string{"region"},
		), preservedPlan.GetQuery().String())

		nullablePlan, err := runOneStmt(NewMockOptimizer(false), t, `
			select n.n_nationkey, c.c_custkey, o.o_orderkey
			from nation n
			left join customer c on n.n_nationkey = c.c_nationkey
			join orders o on c.c_custkey = o.o_custkey`)
		require.NoError(t, err)
		require.True(t, reachablePlanHasJoinType(nullablePlan.GetQuery(), planpb.Node_LEFT),
			nullablePlan.GetQuery().String())
		require.True(t, reachableJoinHasChildTableSets(
			nullablePlan.GetQuery(),
			planpb.Node_INNER,
			[]string{"nation", "customer"},
			[]string{"orders"},
		), nullablePlan.GetQuery().String())
	})
}

func TestOuterJoinPreservedSideAssociativity(t *testing.T) {
	t.Run("moves unique inner join below left join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, false)

		root, changed := builder.applyOuterJoinPreservedSideRule(4)

		require.True(t, changed)
		require.Equal(t, int32(3), root)
		require.Equal(t, []int32{4, 1}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{0, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("handles commuted upper inner join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, true)

		root, changed := builder.applyOuterJoinPreservedSideRule(4)

		require.True(t, changed)
		require.Equal(t, int32(3), root)
		require.Equal(t, []int32{4, 1}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{0, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps non unique inner input above left join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)

		root, changed := builder.applyOuterJoinPreservedSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
		require.Equal(t, []int32{0, 1}, builder.qry.Nodes[3].Children)
	})

	t.Run("keeps condition that references nullable side", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(1, 2)}

		root, changed := builder.applyOuterJoinPreservedSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps local limit boundary", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, false)
		builder.qry.Nodes[3].Limit = &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}}},
		}

		root, changed := builder.applyOuterJoinPreservedSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps deterministic condition that can fail", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, false)
		builder.qry.Nodes[3].OnList = append(builder.qry.Nodes[3].OnList,
			associativityUnsafeCastEqExpr(t, builder, 1, 2))

		root, changed := builder.applyOuterJoinPreservedSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
		require.Equal(t, []int32{0, 1}, builder.qry.Nodes[3].Children)
	})
}

func TestOuterJoinNullableSideAssociativity(t *testing.T) {
	t.Run("moves null rejecting inner join below left join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(2, 3)}

		root, changed := builder.applyOuterJoinNullableSideRule(4)

		require.True(t, changed)
		require.Equal(t, int32(3), root)
		require.Equal(t, planpb.Node_INNER, builder.qry.Nodes[3].JoinType)
		require.Equal(t, []int32{0, 4}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{1, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("handles commuted upper inner join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, true)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(2, 3)}

		root, changed := builder.applyOuterJoinNullableSideRule(4)

		require.True(t, changed)
		require.Equal(t, int32(3), root)
		require.Equal(t, planpb.Node_INNER, builder.qry.Nodes[3].JoinType)
		require.Equal(t, []int32{0, 4}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{1, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps condition that references preserved side", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)

		root, changed := builder.applyOuterJoinNullableSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps mixed nullable and preserved-side conditions", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{
			associativityEqExpr(2, 3),
			associativityEqExpr(1, 3),
		}

		root, changed := builder.applyOuterJoinNullableSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
	})

	t.Run("keeps upper join without null rejecting nullable column", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(3, 3)}

		root, changed := builder.applyOuterJoinNullableSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
	})

	t.Run("keeps local limit boundary", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(2, 3)}
		builder.qry.Nodes[3].Limit = &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}}},
		}

		root, changed := builder.applyOuterJoinNullableSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
	})

	t.Run("keeps deterministic condition that can fail", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{
			associativityEqExpr(2, 3),
			associativityUnsafeCastEqExpr(t, builder, 2, 3),
		}

		root, changed := builder.applyOuterJoinNullableSideRule(4)

		require.False(t, changed)
		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
	})
}

func TestOuterJoinAssociativityReportsNoChangeForInnerOnlyTree(t *testing.T) {
	builder := newOuterJoinAssociativityBuilder(true, false)
	builder.qry.Nodes[3].JoinType = planpb.Node_INNER

	root, changed := builder.applyOuterJoinPreservedSideRule(4)
	require.False(t, changed)
	require.Equal(t, int32(4), root)

	root, changed = builder.applyOuterJoinNullableSideRule(root)
	require.False(t, changed)
	require.Equal(t, int32(4), root)
}

func newOuterJoinAssociativityBuilder(uniqueInner, commuteUpper bool) *QueryBuilder {
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	scan := func(id, tag int32, name string) *planpb.Node {
		return &planpb.Node{
			NodeType:    planpb.Node_TABLE_SCAN,
			NodeId:      id,
			BindingTags: []int32{tag},
			TableDef: &planpb.TableDef{
				Name:          name,
				Cols:          []*planpb.ColDef{{Name: "id", Typ: intType}},
				Name2ColIndex: map[string]int32{"id": 0},
			},
			Stats: associativityStats(100, 1),
		}
	}

	nodes := []*planpb.Node{
		scan(0, 1, "a"),
		scan(1, 2, "b"),
		scan(2, 3, "c"),
		{
			NodeType: planpb.Node_JOIN,
			NodeId:   3,
			JoinType: planpb.Node_LEFT,
			Children: []int32{0, 1},
			OnList:   []*planpb.Expr{associativityEqExpr(1, 2)},
			Stats:    associativityStats(100, 1),
		},
		{
			NodeType: planpb.Node_JOIN,
			NodeId:   4,
			JoinType: planpb.Node_INNER,
			Children: []int32{3, 2},
			OnList:   []*planpb.Expr{associativityEqExpr(1, 3)},
			Stats:    associativityStats(10, 0.1),
		},
	}
	if uniqueInner {
		nodes[2].TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
	}
	if commuteUpper {
		nodes[4].Children[0], nodes[4].Children[1] = nodes[4].Children[1], nodes[4].Children[0]
	}

	return &QueryBuilder{
		qry:     &planpb.Query{Nodes: nodes},
		compCtx: NewMockCompilerContext(true),
	}
}

func associativityEqExpr(leftTag, rightTag int32) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Ndv: 100,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
			Args: []*planpb.Expr{GetColExpr(typ, leftTag, 0), GetColExpr(typ, rightTag, 0)},
		}},
	}
}

func associativityUnsafeCastEqExpr(
	t *testing.T,
	builder *QueryBuilder,
	leftTag, rightTag int32,
) *planpb.Expr {
	t.Helper()
	sourceType := planpb.Type{Id: int32(types.T_varchar)}
	targetType := types.T_int64.ToType()
	castExpr, err := makePlan2CastExpr(
		builder.GetContext(),
		GetColExpr(sourceType, leftTag, 0),
		makePlan2Type(&targetType),
	)
	require.NoError(t, err)
	equality, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
		castExpr,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, rightTag, 0),
	})
	require.NoError(t, err)
	return equality
}

func associativityStats(outcnt, selectivity float64) *planpb.Stats {
	return &planpb.Stats{
		Outcnt:      outcnt,
		TableCnt:    100,
		Cost:        outcnt,
		Selectivity: selectivity,
		BlockNum:    1,
		HashmapStats: &planpb.HashMapStats{
			HashmapSize: 1,
		},
	}
}

func reachableJoinHasChildTableSets(
	query *planpb.Query,
	joinType planpb.Node_JoinType,
	leftWant, rightWant []string,
) bool {
	for nodeID := range reachablePlanNodeIDs(query) {
		node := query.Nodes[nodeID]
		if node == nil || node.NodeType != planpb.Node_JOIN || node.JoinType != joinType ||
			len(node.Children) != 2 {
			continue
		}
		left := tableNamesBelow(query, node.Children[0], make(map[int32]bool))
		right := tableNamesBelow(query, node.Children[1], make(map[int32]bool))
		if (sameStrings(left, leftWant) && sameStrings(right, rightWant)) ||
			(sameStrings(left, rightWant) && sameStrings(right, leftWant)) {
			return true
		}
	}
	return false
}

func reachablePlanNodeIDs(query *planpb.Query) map[int32]bool {
	visited := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || visited[nodeID] {
			return
		}
		visited[nodeID] = true
		node := query.Nodes[nodeID]
		if node == nil {
			return
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, rootID := range query.Steps {
		visit(rootID)
	}
	return visited
}

func tableNamesBelow(query *planpb.Query, nodeID int32, visited map[int32]bool) []string {
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) || visited[nodeID] {
		return nil
	}
	visited[nodeID] = true
	node := query.Nodes[nodeID]
	if node == nil {
		return nil
	}
	if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil {
		return []string{node.TableDef.Name}
	}
	result := make([]string, 0)
	for _, childID := range node.Children {
		result = append(result, tableNamesBelow(query, childID, visited)...)
	}
	sort.Strings(result)
	return result
}

func sameStrings(got, want []string) bool {
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
