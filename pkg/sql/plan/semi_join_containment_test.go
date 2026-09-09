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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type semiContainmentTestConfig struct {
	omitSubsetPredicate bool
	disconnectedKey     bool
	unsafeSubset        bool
	nonInnerSuperset    bool
}

func TestRemoveImpliedSemiJoin(t *testing.T) {
	tests := []struct {
		name   string
		config semiContainmentTestConfig
		mutate func(*QueryBuilder, int32, int32)
		want   bool
	}{
		{name: "conjunctive superset with equivalent key", want: true},
		{name: "missing subset predicate", config: semiContainmentTestConfig{omitSubsetPredicate: true}},
		{name: "disconnected membership key", config: semiContainmentTestConfig{disconnectedKey: true}},
		{name: "fallible subset predicate", config: semiContainmentTestConfig{unsafeSubset: true}},
		{name: "outer join is not conjunctive", config: semiContainmentTestConfig{nonInnerSuperset: true}},
		{
			name: "physical right orientation preserves logical children", want: true,
			mutate: func(builder *QueryBuilder, outerID, innerID int32) {
				builder.qry.Nodes[outerID].IsRightJoin = true
				builder.qry.Nodes[innerID].IsRightJoin = true
			},
		},
		{
			name: "different scan snapshot cannot witness membership",
			mutate: func(builder *QueryBuilder, _, _ int32) {
				builder.qry.Nodes[0].ScanSnapshot = &planpb.Snapshot{}
			},
		},
		{
			name: "limit can remove the required witness",
			mutate: func(builder *QueryBuilder, outerID, _ int32) {
				build := builder.qry.Nodes[outerID].Children[1]
				builder.qry.Nodes[build].Limit = makePlan2Int64ConstExprWithType(1)
			},
		},
		{
			name: "commuted inequality is the same predicate", want: true,
			mutate: func(builder *QueryBuilder, outerID, _ int32) {
				build := builder.qry.Nodes[outerID].Children[1]
				args := builder.qry.Nodes[build].FilterList[0].GetF().Args
				args[0], args[1] = args[1], args[0]
			},
		},
		{
			name: "every component of a composite key must be implied",
			mutate: func(builder *QueryBuilder, outerID, _ int32) {
				node := builder.qry.Nodes[outerID]
				node.OnList = append(node.OnList, semiContainmentTestComparison(t, "=", 30, 1, 10, 1))
			},
		},
		{
			name: "composite key uses one consistent witness", want: true,
			mutate: func(builder *QueryBuilder, outerID, innerID int32) {
				outer, inner := builder.qry.Nodes[outerID], builder.qry.Nodes[innerID]
				outer.OnList = append(outer.OnList, semiContainmentTestComparison(t, "=", 30, 1, 10, 1))
				inner.OnList = append(inner.OnList, semiContainmentTestComparison(t, "=", 30, 1, 20, 1))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder, outerID, innerID := makeSemiContainmentTestPlan(t, test.config)
			if test.mutate != nil {
				test.mutate(builder, outerID, innerID)
			}
			got := builder.removeImpliedSemiJoins(outerID)
			if test.want {
				require.Equal(t, innerID, got)
			} else {
				require.Equal(t, outerID, got)
			}
		})
	}
}

func TestImpliedSemiJoinPlanRewrite(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `
		with multi_part_suppliers as (
			select l1.l_suppkey
			from lineitem l1, lineitem l2
			where l1.l_orderkey = l2.l_orderkey
			  and l1.l_partkey <> l2.l_partkey
		)
		select count(*)
		from supplier s
		where s.s_suppkey in (
			select l_suppkey from multi_part_suppliers
		)
		and s.s_suppkey in (
			select ps.ps_suppkey
			from partsupp ps, multi_part_suppliers m
			where ps.ps_suppkey = m.l_suppkey
		)`)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.Equal(t, 2, countReachableTableScans(query, "lineitem"))
	require.Equal(t, 1, countReachableSemiJoins(query))

	control, err := runOneStmt(NewMockOptimizer(false), t, `
		with multi_part_suppliers as (
			select l1.l_suppkey
			from lineitem l1, lineitem l2
			where l1.l_orderkey = l2.l_orderkey
			  and l1.l_partkey <> l2.l_partkey
		)
		select count(*)
		from supplier s
		where s.s_suppkey in (
			select l_suppkey from multi_part_suppliers
		)
		and s.s_suppkey in (
			select ps.ps_suppkey from partsupp ps
		)`)
	require.NoError(t, err)
	require.Equal(t, 2, countReachableSemiJoins(control.GetQuery()))
}

func countReachableTableScans(query *planpb.Query, table string) int {
	count := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil &&
			node.TableDef.Name == table {
			count++
		}
	}
	return count
}

func countReachableSemiJoins(query *planpb.Query) int {
	count := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_SEMI {
			count++
		}
	}
	return count
}

func makeSemiContainmentTestPlan(
	t *testing.T,
	config semiContainmentTestConfig,
) (*QueryBuilder, int32, int32) {
	t.Helper()
	query := &planpb.Query{}
	appendNode := func(node *planpb.Node) int32 {
		node.NodeId = int32(len(query.Nodes))
		query.Nodes = append(query.Nodes, node)
		return node.NodeId
	}
	scan := func(object int64, tag int32) int32 {
		return appendNode(&planpb.Node{
			NodeType:    planpb.Node_TABLE_SCAN,
			BindingTags: []int32{tag},
			ObjRef:      &planpb.ObjectRef{Obj: object, ObjName: "t"},
			TableDef:    &planpb.TableDef{Name: "t"},
		})
	}

	// A is a self-join constrained by both key equality and a second-column
	// inequality. B contains the same witnesses plus one more joined relation.
	a1, a2 := scan(1, 10), scan(1, 11)
	aJoin := appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_INNER,
		Children: []int32{a1, a2},
		OnList:   []*planpb.Expr{semiContainmentTestComparison(t, "=", 10, 0, 11, 0)},
	})
	aPredicate := semiContainmentTestComparison(t, "<>", 10, 1, 11, 1)
	if config.unsafeSubset {
		aPredicate = &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: -1, ObjName: "fallible"},
				Args: []*planpb.Expr{semiContainmentTestColumn(10, 1)},
			}},
		}
	}
	aRoot := appendNode(&planpb.Node{
		NodeType:   planpb.Node_FILTER,
		Children:   []int32{aJoin},
		FilterList: []*planpb.Expr{aPredicate},
	})

	b1, extra, b2 := scan(1, 20), scan(2, 22), scan(1, 21)
	bExtra := appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_INNER,
		Children: []int32{b1, extra},
		OnList:   []*planpb.Expr{semiContainmentTestComparison(t, "=", 20, 0, 22, 0)},
	})
	bJoinType := planpb.Node_INNER
	if config.nonInnerSuperset {
		bJoinType = planpb.Node_LEFT
	}
	bJoin := appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: bJoinType,
		Children: []int32{bExtra, b2},
		OnList:   []*planpb.Expr{semiContainmentTestComparison(t, "=", 20, 0, 21, 0)},
	})
	bPredicates := []*planpb.Expr{semiContainmentTestComparison(t, "<>", 20, 1, 21, 1)}
	if config.omitSubsetPredicate {
		bPredicates = nil
	}
	bRoot := appendNode(&planpb.Node{
		NodeType:   planpb.Node_FILTER,
		Children:   []int32{bJoin},
		FilterList: bPredicates,
	})

	x := scan(3, 30)
	innerBuildPos := int32(0)
	if config.disconnectedKey {
		innerBuildPos = 1
	}
	innerID := appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_SEMI,
		Children: []int32{x, bRoot},
		OnList:   []*planpb.Expr{semiContainmentTestComparison(t, "=", 30, 0, 22, innerBuildPos)},
	})
	outerID := appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_SEMI,
		Children: []int32{innerID, aRoot},
		OnList:   []*planpb.Expr{semiContainmentTestComparison(t, "=", 30, 0, 10, 0)},
	})
	return &QueryBuilder{qry: query}, outerID, innerID
}

func semiContainmentTestComparison(
	t *testing.T,
	name string,
	leftTag, leftPos, rightTag, rightPos int32,
) *planpb.Expr {
	t.Helper()
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
		semiContainmentTestColumn(leftTag, leftPos),
		semiContainmentTestColumn(rightTag, rightPos),
	})
	require.NoError(t, err)
	return expr
}

func semiContainmentTestColumn(tag, pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64), NotNullable: true},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: tag, ColPos: pos}},
	}
}
