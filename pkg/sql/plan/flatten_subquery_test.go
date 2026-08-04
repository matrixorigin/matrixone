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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func TestCanPullupDeepCorrelatedPredicates(t *testing.T) {
	for _, tc := range []struct {
		typ  plan.SubqueryRef_Type
		want bool
	}{
		{typ: plan.SubqueryRef_EXISTS, want: true},
		{typ: plan.SubqueryRef_NOT_EXISTS, want: true},
		{typ: plan.SubqueryRef_IN, want: true},
		{typ: plan.SubqueryRef_NOT_IN, want: true},
		{typ: plan.SubqueryRef_ANY, want: true},
		{typ: plan.SubqueryRef_ALL, want: true},
		{typ: plan.SubqueryRef_SCALAR, want: false},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			require.Equal(t, tc.want, canPullupDeepCorrelatedPredicates(tc.typ))
		})
	}
}

func TestHasInnerColumnInDeepCorrelatedFilters(t *testing.T) {
	const (
		subID    int32 = 0
		innerTag int32 = 1
		outerTag int32 = 2
	)

	builder := &QueryBuilder{
		qry: &plan.Query{
			Nodes: []*plan.Node{
				{
					NodeId:      subID,
					NodeType:    plan.Node_TABLE_SCAN,
					BindingTags: []int32{innerTag},
				},
			},
		},
	}

	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, nil))
	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{}))
	require.True(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{
		newFlattenSubqueryTestColExpr(innerTag),
	}))
	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{
		newFlattenSubqueryTestColExpr(outerTag),
	}))
}

func TestScalarAggregatePlanSupportsDeepCorrelation(t *testing.T) {
	const (
		groupTag     int32 = 10
		aggregateTag int32 = 11
	)

	tests := []struct {
		name      string
		wrapper   plan.Node_NodeType
		configure func(*plan.Node)
		directAgg bool
		wrongTag  bool
		want      bool
	}{
		{name: "direct aggregate", directAgg: true, want: true},
		{name: "projection", wrapper: plan.Node_PROJECT, want: true},
		{
			name:    "limit",
			wrapper: plan.Node_PROJECT,
			configure: func(node *plan.Node) {
				node.Limit = makePlan2Uint64ConstExprWithType(1)
			},
		},
		{
			name:    "offset",
			wrapper: plan.Node_PROJECT,
			configure: func(node *plan.Node) {
				node.Offset = makePlan2Uint64ConstExprWithType(1)
			},
		},
		{
			name:    "rank",
			wrapper: plan.Node_PROJECT,
			configure: func(node *plan.Node) {
				node.RankOption = &plan.RankOption{Mode: "rank"}
			},
		},
		{name: "sort", wrapper: plan.Node_SORT},
		{name: "distinct", wrapper: plan.Node_DISTINCT},
		{name: "filter", wrapper: plan.Node_FILTER},
		{name: "wrong aggregate", directAgg: true, wrongTag: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tag := aggregateTag
			if test.wrongTag {
				tag++
			}
			nodes := []*plan.Node{{
				NodeType:    plan.Node_AGG,
				BindingTags: []int32{groupTag, tag},
			}}
			rootID := int32(0)
			if !test.directAgg {
				wrapper := &plan.Node{
					NodeType: test.wrapper,
					Children: []int32{0},
				}
				if test.configure != nil {
					test.configure(wrapper)
				}
				nodes = append(nodes, wrapper)
				rootID = 1
			}

			builder := &QueryBuilder{qry: &plan.Query{Nodes: nodes}}
			require.Equal(t, test.want,
				builder.scalarAggregatePlanSupportsDeepCorrelation(rootID, aggregateTag))
		})
	}
}

func TestNestedCorrelatedScalarAggregatePullsUpGroupingKey(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		SELECT n1.N_NATIONKEY,
		       (SELECT MAX(n2.N_REGIONKEY)
		          FROM NATION n2
		         WHERE n2.N_REGIONKEY = (
		               SELECT MAX(n3.N_REGIONKEY)
		                 FROM NATION n3
		                WHERE n3.N_NATIONKEY = n1.N_NATIONKEY))
		  FROM NATION n1`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.NotEmpty(t, query.Steps)

	visited := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		require.GreaterOrEqual(t, nodeID, int32(0))
		require.Less(t, int(nodeID), len(query.Nodes))
		if visited[nodeID] {
			return
		}
		visited[nodeID] = true

		node := query.Nodes[nodeID]
		require.NotNil(t, node)
		for _, exprs := range [][]*plan.Expr{
			node.ProjectList,
			node.OnList,
			node.FilterList,
			node.GroupBy,
			node.AggList,
		} {
			for _, expr := range exprs {
				require.False(t, hasCorrCol(expr), "reachable %s node contains a correlated expression", node.NodeType)
			}
		}
		for _, orderBy := range node.OrderBy {
			require.False(t, hasCorrCol(orderBy.Expr), "reachable SORT contains a correlated expression")
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}

	for _, rootID := range query.Steps {
		visit(rootID)
	}
}

func TestTransparentCorrelatedDerivedTableChain(t *testing.T) {
	for _, tt := range []struct {
		name     string
		sql      string
		wantKeys int
	}{
		{
			name:     "one projection",
			wantKeys: 1,
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY) d1)
			         FROM NATION n1`,
		},
		{
			name:     "aliases reorder and local filter",
			wantKeys: 1,
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT d1.region_alias, d1.nation_alias
			                         FROM (SELECT n2.N_NATIONKEY AS nation_alias,
			                                      n2.N_REGIONKEY AS region_alias
			                                 FROM NATION n2
			                                WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                                  AND n2.N_NATIONKEY >= 0) d1) d2)
			         FROM NATION n1`,
		},
		{
			name:     "multiple correlation keys",
			wantKeys: 2,
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                          AND n1.N_NATIONKEY = n2.N_NATIONKEY) d1)
			         FROM NATION n1`,
		},
		{
			name:     "reverse correlation operands",
			wantKeys: 1,
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n2.N_REGIONKEY = n1.N_REGIONKEY) d1)
			         FROM NATION n1`,
		},
		{
			name:     "prisma json aggregate shape",
			wantKeys: 1,
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COALESCE(JSON_ARRAYAGG(__prisma_data__), CONVERT('[]', JSON))
			                 FROM (SELECT d3.__prisma_data__
			                         FROM (SELECT JSON_OBJECT('id', d2.N_NATIONKEY,
			                                                  'name', d2.N_NAME,
			                                                  'regionId', d2.N_REGIONKEY) AS __prisma_data__
			                                 FROM (SELECT n2.*
			                                         FROM NATION n2
			                                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY) d2) d3) d4)
			         FROM NATION n1`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			var scalarJoin *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_LEFT {
					scalarJoin = node
					break
				}
			}
			require.NotNil(t, scalarJoin)
			require.Len(t, scalarJoin.OnList, tt.wantKeys)
			assertReachablePlanHasNoCorrelatedExpr(t, query)
		})
	}
}

func TestTransparentCorrelatedDerivedTableRejectsUnsafeShapes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "same FROM scope",
			wantErr: "missing FROM-clause entry for table 'n1'",
			sql: `SELECT n1.N_NATIONKEY
			         FROM NATION n1
			         JOIN (SELECT n2.N_NATIONKEY
			                 FROM NATION n2
			                WHERE n1.N_REGIONKEY = n2.N_REGIONKEY) d ON TRUE`,
		},
		{
			name: "join inside derived table",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2 JOIN REGION r
			                           ON n2.N_REGIONKEY = r.R_REGIONKEY
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY) d)
			         FROM NATION n1`,
		},
		{
			name: "correlation in derived projection",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n1.N_REGIONKEY
			                         FROM NATION n2) d)
			         FROM NATION n1`,
		},
		{
			name: "group by",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_REGIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                        GROUP BY n2.N_REGIONKEY) d)
			         FROM NATION n1`,
		},
		{
			name: "having",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_REGIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                        GROUP BY n2.N_REGIONKEY
			                       HAVING COUNT(*) > 0) d)
			         FROM NATION n1`,
		},
		{
			name: "distinct",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT DISTINCT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY) d)
			         FROM NATION n1`,
		},
		{
			name: "window",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT ROW_NUMBER() OVER (ORDER BY n2.N_NATIONKEY) AS rn
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY) d)
			         FROM NATION n1`,
		},
		{
			name: "set operation",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                       UNION ALL
			                       SELECT r.R_REGIONKEY
			                         FROM REGION r
			                        WHERE n1.N_REGIONKEY = r.R_REGIONKEY) d)
			         FROM NATION n1`,
		},
		{
			name: "order by",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                        ORDER BY n2.N_NATIONKEY) d)
			         FROM NATION n1`,
		},
		{
			name: "limit and offset",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                        LIMIT 1 OFFSET 1) d)
			         FROM NATION n1`,
		},
		{
			name: "or correlation",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY = n2.N_REGIONKEY
			                           OR n2.N_NATIONKEY = 0) d)
			         FROM NATION n1`,
		},
		{
			name: "non equality correlation",
			sql: `SELECT n1.N_NATIONKEY,
			              (SELECT COUNT(*)
			                 FROM (SELECT n2.N_NATIONKEY
			                         FROM NATION n2
			                        WHERE n1.N_REGIONKEY < n2.N_REGIONKEY) d)
			         FROM NATION n1`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			if tt.wantErr == "" {
				tt.wantErr = "correlated subquery in FROM clause is not yet implemented"
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestTransparentCorrelatedDerivedTableNormalizationIsAtomic(t *testing.T) {
	const outerTag int32 = 41

	newBuilder := func(nodes []*plan.Node) *QueryBuilder {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		builder.qry.Nodes = nodes
		return builder
	}
	newNodes := func(corr *plan.CorrColRef, tail *plan.Node) []*plan.Node {
		return []*plan.Node{
			{NodeType: plan.Node_TABLE_SCAN},
			{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{2},
				FilterList: []*plan.Expr{newTransparentDerivedEquality(corr, 42)},
			},
			tail,
			{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{1},
				ProjectList: []*plan.Expr{newFlattenSubqueryTestColExpr(42)},
			},
		}
	}

	t.Run("unsupported tail leaves depth and context unchanged", func(t *testing.T) {
		corr := &plan.CorrColRef{RelPos: outerTag, Depth: 2}
		nodes := newNodes(corr, &plan.Node{NodeType: plan.Node_SORT, Children: []int32{0}})
		ctx := NewBindContext(nil, nil)
		err := newBuilder(nodes).normalizeTransparentCorrelatedDerivedTable(3, ctx)
		require.ErrorContains(t, err, "correlated subquery in FROM clause is not yet implemented")
		require.Equal(t, int32(2), corr.Depth)
		require.False(t, ctx.isCorrelated)
	})

	t.Run("scan block filter remains unsupported and atomic", func(t *testing.T) {
		corr := &plan.CorrColRef{RelPos: outerTag, Depth: 2}
		nodes := newNodes(corr, &plan.Node{NodeType: plan.Node_TABLE_SCAN})
		nodes[1].Children[0] = 0
		nodes[0].BlockFilterList = []*plan.Expr{newTransparentDerivedEquality(corr, 42)}
		ctx := NewBindContext(nil, nil)
		err := newBuilder(nodes).normalizeTransparentCorrelatedDerivedTable(3, ctx)
		require.ErrorContains(t, err, "correlated subquery in FROM clause is not yet implemented")
		require.Equal(t, int32(2), corr.Depth)
		require.False(t, ctx.isCorrelated)
	})

	t.Run("same scope remains non lateral", func(t *testing.T) {
		corr := &plan.CorrColRef{RelPos: outerTag, Depth: 1}
		nodes := newNodes(corr, &plan.Node{NodeType: plan.Node_TABLE_SCAN})
		nodes[1].Children[0] = 0
		ctx := NewBindContext(nil, nil)
		ctx.bindingByTag[outerTag] = nil
		err := newBuilder(nodes).normalizeTransparentCorrelatedDerivedTable(3, ctx)
		require.ErrorContains(t, err, "correlated subquery in FROM clause is not yet implemented")
		require.Equal(t, int32(1), corr.Depth)
		require.False(t, ctx.isCorrelated)
	})

	t.Run("ancestor correlation propagates through binderless scope", func(t *testing.T) {
		corr := &plan.CorrColRef{RelPos: outerTag, Depth: 1}
		nodes := newNodes(corr, &plan.Node{NodeType: plan.Node_TABLE_SCAN})
		nodes[1].Children[0] = 0
		ctx := NewBindContext(nil, nil)
		require.NoError(t, newBuilder(nodes).normalizeTransparentCorrelatedDerivedTable(3, ctx))
		require.Equal(t, int32(1), corr.Depth)
		require.True(t, ctx.isCorrelated)
	})

	t.Run("repeated ancestor reference decrements depth exactly once", func(t *testing.T) {
		corr := &plan.CorrColRef{RelPos: outerTag, Depth: 2}
		nodes := newNodes(corr, &plan.Node{NodeType: plan.Node_TABLE_SCAN})
		nodes[1].Children[0] = 0
		nodes[1].FilterList = append(nodes[1].FilterList, newTransparentDerivedEquality(corr, 42))
		ctx := NewBindContext(nil, nil)
		require.NoError(t, newBuilder(nodes).normalizeTransparentCorrelatedDerivedTable(3, ctx))
		require.Equal(t, int32(1), corr.Depth)
		require.True(t, ctx.isCorrelated)
	})
}

func TestNestedCorrelatedScalarStillRejectsUnsafeShapes(t *testing.T) {
	for _, sql := range []string{
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = (
		                SELECT n3.N_REGIONKEY
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY))
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = (
		                SELECT COUNT(*)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY))
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = (
		                SELECT APPROX_COUNT(n3.N_REGIONKEY)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY))
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = (
		                SELECT APPROX_COUNT_DISTINCT(n3.N_REGIONKEY)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY))
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT COUNT(COALESCE((
		                SELECT MAX(n3.N_REGIONKEY)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY), 0))
		           FROM NATION n2)
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = COALESCE((
		                SELECT MAX(n3.N_REGIONKEY)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY), 0))
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = (
		                SELECT COALESCE(MAX(n3.N_REGIONKEY), 0)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY))
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = (
		                SELECT MAX(n3.N_REGIONKEY)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY
		                 LIMIT 1))
		   FROM NATION n1`,
		`SELECT n1.N_NATIONKEY,
		        (SELECT MAX(n2.N_REGIONKEY)
		           FROM NATION n2
		          WHERE n2.N_REGIONKEY = (
		                SELECT MAX(n3.N_REGIONKEY)
		                  FROM NATION n3
		                 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY
		                 LIMIT 1 OFFSET 1))
		   FROM NATION n1`,
	} {
		_, err := runOneStmt(NewMockOptimizer(true), t, sql)
		require.ErrorContains(t, err, "correlated columns in SCALAR subquery deeper than 1 level")
	}
}

func TestInSubqueryJoinShapePreservesThreeValuedSemantics(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       plan.Node_JoinType
		forbidMark bool
	}{
		{
			name:       "where in becomes semi join",
			sql:        "select n_name from tpch.nation where n_regionkey in (select r_regionkey from tpch.region)",
			want:       plan.Node_SEMI,
			forbidMark: true,
		},
		{
			name:       "non-null where not in becomes anti join",
			sql:        "select n_name from tpch.nation where n_regionkey not in (select r_regionkey from tpch.region)",
			want:       plan.Node_ANTI,
			forbidMark: true,
		},
		{
			name: "projected in keeps mark result",
			sql:  "select n_regionkey in (select r_regionkey from tpch.region) from tpch.nation",
			want: plan.Node_MARK,
		},
		{
			name: "nullable where not in keeps null-aware mark result",
			sql:  "select n_name from tpch.nation where n_comment not in (select r_comment from tpch.region)",
			want: plan.Node_MARK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			require.Truef(t, hasJoinType(query, tt.want), "expected %s join in plan", tt.want)
			if tt.forbidMark {
				require.False(t, hasJoinType(query, plan.Node_MARK))
			}
		})
	}
}

func TestDirectCorrelatedScalarProjectionUsesMatchMarker(t *testing.T) {
	for _, tt := range []struct {
		name         string
		subquery     string
		joinType     plan.Node_JoinType
		wantTruePred bool
	}{
		{
			name:     "direct",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey",
			joinType: plan.Node_SINGLE,
		},
		{
			name:     "order by",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey order by r.r_regionkey",
			joinType: plan.Node_SINGLE,
		},
		{
			name:     "literal limit two",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey limit 2",
			joinType: plan.Node_SINGLE,
		},
		{
			name:     "order by limit two",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey order by r.r_regionkey limit 2",
			joinType: plan.Node_SINGLE,
		},
		{
			name:     "distinct",
			subquery: "select distinct n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey",
			joinType: plan.Node_MARK,
		},
		{
			name:         "predicate-free distinct",
			subquery:     "select distinct n.n_regionkey from tpch.region r",
			joinType:     plan.Node_MARK,
			wantTruePred: true,
		},
		{
			name:     "order by limit one",
			subquery: "select n.n_regionkey from tpch.region r where r.r_regionkey > n.n_regionkey order by r.r_regionkey limit 1",
			joinType: plan.Node_MARK,
		},
		{
			name:         "predicate-free limit one",
			subquery:     "select n.n_regionkey from tpch.region r limit 1",
			joinType:     plan.Node_MARK,
			wantTruePred: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
				"select n.*, ("+tt.subquery+") as x from tpch.nation n")
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)

			var scalarJoin *plan.Node
			hasCase := false
			for _, node := range query.Nodes {
				for _, expr := range node.ProjectList {
					require.False(t, hasCorrCol(expr), "executable PROJECT contains a correlated expression")
					if f := expr.GetF(); f != nil && f.Func.GetObjName() == "case" {
						hasCase = true
						require.Len(t, f.Args, 3)
					}
				}
				for _, expr := range node.OnList {
					require.False(t, hasCorrCol(expr), "executable JOIN contains a correlated expression")
				}
				for _, expr := range node.FilterList {
					require.False(t, hasCorrCol(expr), "executable FILTER contains a correlated expression")
				}
				for _, orderBy := range node.OrderBy {
					require.False(t, hasCorrCol(orderBy.Expr), "executable SORT contains a correlated expression")
				}
				if node.NodeType == plan.Node_JOIN && node.JoinType == tt.joinType {
					scalarJoin = node
				}
			}

			require.NotNil(t, scalarJoin)
			require.Len(t, scalarJoin.Children, 2)
			if tt.wantTruePred {
				require.NotEmpty(t, scalarJoin.OnList)
				pred := scalarJoin.OnList[0].GetLit()
				require.NotNil(t, pred)
				require.True(t, pred.GetBval())
			}
			rightProject := query.Nodes[scalarJoin.Children[1]]
			require.Equal(t, plan.Node_PROJECT, rightProject.NodeType)
			require.NotEmpty(t, rightProject.ProjectList)
			require.Nil(t, rightProject.Limit)
			if tt.joinType == plan.Node_SINGLE {
				marker := rightProject.ProjectList[0].GetLit()
				require.NotNil(t, marker)
				require.True(t, marker.GetBval())
			}
			require.True(t, hasCase)
		})
	}
}

func TestCorrelatedScalarAggregateProjectionRunsAfterJoin(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"select n.n_nationkey, (select coalesce(sum(r.r_regionkey), 0) from tpch.region r where r.r_regionkey = n.n_regionkey) as total from tpch.nation n")
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	var rightAggregate *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_LEFT || len(node.Children) != 2 {
			continue
		}
		candidate := query.Nodes[node.Children[1]]
		if candidate.NodeType != plan.Node_AGG || len(candidate.AggList) == 0 {
			continue
		}
		rightAggregate = candidate
		break
	}

	require.NotNil(t, rightAggregate)
	require.Equal(t, "sum", rightAggregate.AggList[0].GetF().Func.GetObjName())
	assertReachablePlanHasNoCorrelatedExpr(t, query)
}

func assertReachablePlanHasNoCorrelatedExpr(t *testing.T, query *plan.Query) {
	t.Helper()
	visited := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		if visited[nodeID] {
			return
		}
		visited[nodeID] = true
		node := query.Nodes[nodeID]
		exprs := make([]*plan.Expr, 0, len(node.ProjectList)+len(node.FilterList)+len(node.OnList)+len(node.GroupBy)+len(node.AggList)+len(node.WinSpecList)+2)
		exprs = append(exprs, node.ProjectList...)
		exprs = append(exprs, node.FilterList...)
		exprs = append(exprs, node.OnList...)
		exprs = append(exprs, node.GroupBy...)
		exprs = append(exprs, node.AggList...)
		exprs = append(exprs, node.WinSpecList...)
		for _, order := range node.OrderBy {
			if order != nil {
				exprs = append(exprs, order.Expr)
			}
		}
		if node.Limit != nil {
			exprs = append(exprs, node.Limit)
		}
		if node.Offset != nil {
			exprs = append(exprs, node.Offset)
		}
		for _, expr := range exprs {
			require.False(t, hasCorrCol(expr), "reachable %s node %d contains a correlated expression", node.NodeType.String(), nodeID)
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	for _, root := range query.Steps {
		visit(root)
	}
}

func TestCorrelatedScalarAggregatePostJoinProjectionEligibility(t *testing.T) {
	for _, tt := range []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "ifnull aggregate",
			sql:  "select n.n_nationkey, (select ifnull(avg(r.r_regionkey), 7) from tpch.region r where r.r_regionkey = n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "mixed sum and count",
			sql:  "select n.n_nationkey, (select sum(r.r_regionkey) + count(*) from tpch.region r where r.r_regionkey = n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "null-safe equality",
			sql:  "select n.n_nationkey, (select coalesce(max(r.r_regionkey), 0) from tpch.region r where r.r_regionkey <=> n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "cte aggregate input",
			sql:  "with r as (select r_regionkey from tpch.region) select n.n_nationkey, (select coalesce(min(r.r_regionkey), 0) from r where r.r_regionkey = n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "json aggregate",
			sql:  "select n.n_nationkey, (select coalesce(json_arrayagg(r.r_regionkey), convert('[]', json)) from tpch.region r where r.r_regionkey = n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "explicit group by",
			sql:  "select n.n_nationkey, (select coalesce(sum(r.r_regionkey), 0) from tpch.region r where r.r_regionkey = n.n_regionkey group by r.r_regionkey) from tpch.nation n",
		},
		{
			name: "having can remove aggregate row",
			sql:  "select n.n_nationkey, (select coalesce(sum(r.r_regionkey), 0) from tpch.region r where r.r_regionkey = n.n_regionkey having sum(r.r_regionkey) > 100) from tpch.nation n",
		},
		{
			name: "neutral aggregate",
			sql:  "select n.n_nationkey, (select coalesce(bit_or(r.r_regionkey), 0) from tpch.region r where r.r_regionkey = n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "raw neutral aggregate",
			sql:  "select n.n_nationkey, (select bit_and(r.r_regionkey) from tpch.region r where r.r_regionkey = n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "raw approximate count aggregate",
			sql:  "select n.n_nationkey, (select approx_count_distinct(r.r_regionkey) from tpch.region r where r.r_regionkey = n.n_regionkey) from tpch.nation n",
			want: true,
		},
		{
			name: "limited aggregate",
			sql:  "select n.n_nationkey, (select coalesce(sum(r.r_regionkey), 0) from tpch.region r where r.r_regionkey = n.n_regionkey limit 1) from tpch.nation n",
		},
		{
			name: "sorted aggregate",
			sql:  "select n.n_nationkey, (select coalesce(sum(r.r_regionkey), 0) from tpch.region r where r.r_regionkey = n.n_regionkey order by sum(r.r_regionkey)) from tpch.nation n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)
			require.Equal(t, tt.want, hasCorrelatedAggregatePostJoinProjection(logicPlan.GetQuery()))
		})
	}
}

func TestPrepareCorrelatedScalarAggregatePostJoinProjection(t *testing.T) {
	const (
		groupTag     int32 = 10
		aggregateTag int32 = 11
		projectTag   int32 = 12
		outerTag     int32 = 13
	)

	aggregateType := func(id types.T) plan.Type {
		return plan.Type{Id: int32(id), NotNullable: true}
	}
	aggregates := []*plan.Expr{
		newFlattenSubqueryTestAggregate("sum", aggregateType(types.T_decimal128)),
		newFlattenSubqueryTestAggregate("avg", aggregateType(types.T_float64)),
		newFlattenSubqueryTestAggregate("min", aggregateType(types.T_int32)),
		newFlattenSubqueryTestAggregate("max", aggregateType(types.T_int64)),
		newFlattenSubqueryTestAggregate("json_arrayagg", aggregateType(types.T_json)),
		newFlattenSubqueryTestAggregate("count", aggregateType(types.T_int64)),
		newFlattenSubqueryTestAggregate("starcount", aggregateType(types.T_int64)),
	}
	projectionArgs := make([]*plan.Expr, 0, len(aggregates)+1)
	for i, aggregate := range aggregates {
		projectionArgs = append(projectionArgs, GetColExpr(aggregate.Typ, aggregateTag, int32(i)))
	}
	projectionArgs = append(projectionArgs, &plan.Expr{
		Typ: aggregateType(types.T_int64),
		Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{
			RelPos: outerTag,
			ColPos: 3,
			Depth:  1,
		}},
	})
	projection := &plan.Expr{
		Typ: aggregateType(types.T_int64),
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "case"},
			Args: projectionArgs,
		}},
	}
	correlationKey := GetColExpr(plan.Type{Id: int32(types.T_int32)}, groupTag, 0)

	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.qry.Nodes = []*plan.Node{
		{
			NodeType:    plan.Node_AGG,
			AggList:     aggregates,
			BindingTags: []int32{groupTag, aggregateTag},
		},
		{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{0},
			BindingTags: []int32{projectTag},
			ProjectList: []*plan.Expr{projection, correlationKey},
		},
	}
	ctx := &BindContext{
		hasSingleRow: true,
		aggregateTag: aggregateTag,
		aggregates:   aggregates,
	}

	postJoinProjection, ok, err := builder.prepareCorrelatedScalarAggregatePostJoinProjection(1, ctx, []*plan.Expr{constTrue})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, builder.qry.Nodes[1].ProjectList, len(aggregates)+1)
	require.Equal(t, groupTag, builder.qry.Nodes[1].ProjectList[1].GetCol().RelPos)
	require.Equal(t, int32(0), builder.qry.Nodes[1].ProjectList[1].GetCol().ColPos)
	rawPositions := []int{0, 2, 3, 4, 5, 6, 7}
	for i, pos := range rawPositions {
		raw := builder.qry.Nodes[1].ProjectList[pos]
		require.Equal(t, aggregateTag, raw.GetCol().RelPos)
		require.Equal(t, int32(i), raw.GetCol().ColPos)
	}

	postJoinArgs := postJoinProjection.GetF().Args
	require.Len(t, postJoinArgs, len(aggregates)+1)
	for i := 0; i < 5; i++ {
		require.Equal(t, projectTag, postJoinArgs[i].GetCol().RelPos)
		projectPos := int32(i + 1)
		if i == 0 {
			projectPos = 0
		}
		require.Equal(t, projectPos, postJoinArgs[i].GetCol().ColPos)
		require.False(t, postJoinArgs[i].Typ.NotNullable)
	}
	for i := 5; i < 7; i++ {
		countFallback := postJoinArgs[i].GetF()
		require.Equal(t, "case", countFallback.Func.GetObjName())
		require.Equal(t, projectTag, countFallback.Args[2].GetCol().RelPos)
		require.Equal(t, int32(i+1), countFallback.Args[2].GetCol().ColPos)
	}
	require.Nil(t, postJoinArgs[7].GetCorr())
	require.Equal(t, outerTag, postJoinArgs[7].GetCol().RelPos)
	require.Equal(t, int32(3), postJoinArgs[7].GetCol().ColPos)
}

func TestMakeAggregateEmptyResultExpr(t *testing.T) {
	for _, tt := range []struct {
		name string
		kind aggexec.EmptyResultKind
		typ  plan.Type
		want any
	}{
		{name: "uint64 zero", kind: aggexec.EmptyResultZero, typ: plan.Type{Id: int32(types.T_uint64)}, want: uint64(0)},
		{name: "uint64 all bits set", kind: aggexec.EmptyResultAllBitsSet, typ: plan.Type{Id: int32(types.T_uint64)}, want: uint64(math.MaxUint64)},
		{name: "binary zero", kind: aggexec.EmptyResultZero, typ: plan.Type{Id: int32(types.T_binary), Width: 3}, want: string([]byte{0, 0, 0})},
		{name: "varbinary all bits set", kind: aggexec.EmptyResultAllBitsSet, typ: plan.Type{Id: int32(types.T_varbinary), Width: 3}, want: string([]byte{0xff, 0xff, 0xff})},
		{name: "int64 zero", kind: aggexec.EmptyResultZero, typ: plan.Type{Id: int32(types.T_int64)}, want: int64(0)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := makeAggregateEmptyResultExpr(tt.kind, tt.typ)
			require.NoError(t, err)
			require.Equal(t, tt.typ.Id, expr.Typ.Id)
			require.Equal(t, tt.typ.Width, expr.Typ.Width)
			require.True(t, expr.Typ.NotNullable)
			switch want := tt.want.(type) {
			case uint64:
				require.Equal(t, want, expr.GetLit().GetU64Val())
			case int64:
				require.Equal(t, want, expr.GetLit().GetI64Val())
			case string:
				require.Equal(t, want, expr.GetLit().GetSval())
			}
		})
	}

	_, err := makeAggregateEmptyResultExpr(aggexec.EmptyResultAllBitsSet, plan.Type{Id: int32(types.T_int64)})
	require.Error(t, err)
}

func TestPrepareCorrelatedScalarAggregatePostJoinProjectionRejectsUnsupportedShapes(t *testing.T) {
	const (
		aggregateTag int32 = 21
		projectTag   int32 = 22
	)

	for _, tt := range []struct {
		name      string
		aggregate string
		wantErr   bool
		mutate    func(*BindContext, []*plan.Node)
	}{
		{name: "unsupported aggregate", aggregate: "hll_add_agg", wantErr: true},
		{
			name:      "explicit group",
			aggregate: "sum",
			mutate: func(ctx *BindContext, _ []*plan.Node) {
				ctx.groups = []*plan.Expr{makePlan2Int64ConstExprWithType(1)}
			},
		},
		{
			name:      "having filter",
			aggregate: "sum",
			mutate: func(_ *BindContext, nodes []*plan.Node) {
				nodes[1].Children[0] = 2
			},
		},
		{
			name:      "limit",
			aggregate: "sum",
			mutate: func(_ *BindContext, nodes []*plan.Node) {
				nodes[1].Limit = makePlan2Uint64ConstExprWithType(1)
			},
		},
		{
			name:      "deep correlation",
			aggregate: "sum",
			mutate: func(_ *BindContext, nodes []*plan.Node) {
				nodes[1].ProjectList[0] = &plan.Expr{
					Typ:  plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: 30, Depth: 2}},
				}
			},
		},
		{
			name:      "non aggregate inner column",
			aggregate: "sum",
			mutate: func(_ *BindContext, nodes []*plan.Node) {
				nodes[1].ProjectList[0] = GetColExpr(plan.Type{Id: int32(types.T_int64)}, 99, 0)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			aggregate := newFlattenSubqueryTestAggregate(tt.aggregate, plan.Type{Id: int32(types.T_int64)})
			nodes := []*plan.Node{
				{NodeType: plan.Node_AGG, AggList: []*plan.Expr{aggregate}, BindingTags: []int32{20, aggregateTag}},
				{
					NodeType:    plan.Node_PROJECT,
					Children:    []int32{0},
					BindingTags: []int32{projectTag},
					ProjectList: []*plan.Expr{GetColExpr(aggregate.Typ, aggregateTag, 0)},
				},
				{NodeType: plan.Node_FILTER, Children: []int32{0}},
			}
			ctx := &BindContext{hasSingleRow: true, aggregateTag: aggregateTag, aggregates: []*plan.Expr{aggregate}}
			if tt.mutate != nil {
				tt.mutate(ctx, nodes)
			}
			builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
			builder.qry.Nodes = nodes

			postJoinProjection, ok, err := builder.prepareCorrelatedScalarAggregatePostJoinProjection(1, ctx, []*plan.Expr{constTrue})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.False(t, ok)
			require.Nil(t, postJoinProjection)
			require.Len(t, builder.qry.Nodes[1].ProjectList, 1)
		})
	}
}

func TestPrepareCorrelatedScalarAggregatePostJoinProjectionRejectsUnsupportedDirectAggregate(t *testing.T) {
	aggregate := newFlattenSubqueryTestAggregate("hll_add_agg", plan.Type{Id: int32(types.T_varbinary)})
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.qry.Nodes = []*plan.Node{{
		NodeType:    plan.Node_AGG,
		AggList:     []*plan.Expr{aggregate},
		BindingTags: []int32{20, 21},
	}}
	ctx := &BindContext{
		hasSingleRow: true,
		aggregateTag: 21,
		aggregates:   []*plan.Expr{aggregate},
		results:      []*plan.Expr{GetColExpr(aggregate.Typ, 21, 0)},
	}

	postJoinProjection, ok, err := builder.prepareCorrelatedScalarAggregatePostJoinProjection(0, ctx, []*plan.Expr{constTrue})
	require.Error(t, err)
	require.False(t, ok)
	require.Nil(t, postJoinProjection)
}

func hasCorrelatedAggregatePostJoinProjection(query *plan.Query) bool {
	if query == nil {
		return false
	}
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_LEFT || len(node.Children) != 2 {
			continue
		}
		right := query.Nodes[node.Children[1]]
		if right.NodeType == plan.Node_AGG {
			return true
		}
		if right.NodeType != plan.Node_PROJECT || len(right.Children) != 1 || len(right.ProjectList) == 0 {
			continue
		}
		agg := query.Nodes[right.Children[0]]
		if agg.NodeType != plan.Node_AGG || len(agg.BindingTags) < 2 {
			continue
		}
		first := right.ProjectList[0].GetCol()
		if first != nil && first.RelPos == agg.BindingTags[1] && first.ColPos == 0 {
			return true
		}
	}
	return false
}

func newFlattenSubqueryTestAggregate(name string, typ plan.Type) *plan.Expr {
	ids := map[string]int64{
		"sum":           aggexec.AggIdOfSum,
		"avg":           aggexec.AggIdOfAvg,
		"min":           aggexec.AggIdOfMin,
		"max":           aggexec.AggIdOfMax,
		"json_arrayagg": aggexec.AggIdOfJsonArrayAgg,
		"count":         aggexec.AggIdOfCountColumn,
		"starcount":     aggexec.AggIdOfCountStar,
		"bit_or":        aggexec.AggIdOfBitOr,
		"hll_add_agg":   aggexec.AggIdOfHllAdd,
	}
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name, Obj: ids[name]},
		}},
	}
}

func TestDirectCorrelatedScalarProjectionCasePreservesType(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)

	for _, tt := range []struct {
		name string
		typ  plan.Type
		want bool
	}{
		{name: "integer", typ: plan.Type{Id: int32(types.T_int32), Width: 32, Scale: -1}, want: true},
		{name: "enum coerces to ordinal", typ: plan.Type{Id: int32(types.T_enum), Enumvalues: "small,large"}},
		{name: "rowid unsupported", typ: plan.Type{Id: int32(types.T_Rowid)}},
		{name: "vector unsupported", typ: plan.Type{Id: int32(types.T_array_float32), Width: 3}},
		{name: "bit width changes", typ: plan.Type{Id: int32(types.T_bit), Width: 8}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, builder.casePreservesType(&plan.Expr{Typ: tt.typ}))
		})
	}
}

func TestNormalizeDirectCorrelatedScalarProjectionFallsBack(t *testing.T) {
	const (
		projectTag int32 = 10
		outerTag   int32 = 20
	)

	newCorr := func(relPos, colPos, depth int32) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Corr{
				Corr: &plan.CorrColRef{RelPos: relPos, ColPos: colPos, Depth: depth},
			},
		}
	}
	validProject := func() *plan.Node {
		return &plan.Node{
			NodeType:    plan.Node_PROJECT,
			BindingTags: []int32{projectTag},
			ProjectList: []*plan.Expr{newCorr(outerTag, 0, 1)},
		}
	}

	for _, tt := range []struct {
		name     string
		results  []*plan.Expr
		projects []*plan.Expr
		nodes    []*plan.Node
		subID    int32
	}{
		{
			name:     "missing result",
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes:    []*plan.Node{validProject()},
		},
		{
			name:     "top-level offset",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				validProject(),
				{NodeType: plan.Node_SORT, Children: []int32{0}, Offset: makePlan2Uint64ConstExprWithType(1)},
			},
			subID: 1,
		},
		{
			name:     "nested offset",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				validProject(),
				{NodeType: plan.Node_SORT, Children: []int32{0}, Offset: makePlan2Uint64ConstExprWithType(1)},
				{NodeType: plan.Node_SORT, Children: []int32{1}},
			},
			subID: 2,
		},
		{
			name:     "dynamic limit below rim",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, BindingTags: []int32{projectTag}, ProjectList: []*plan.Expr{newCorr(outerTag, 0, 1)}, Limit: &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}},
				{NodeType: plan.Node_SORT, Children: []int32{0}},
			},
			subID: 1,
		},
		{
			name:     "limit zero",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, BindingTags: []int32{projectTag}, ProjectList: []*plan.Expr{newCorr(outerTag, 0, 1)}, Limit: makePlan2Uint64ConstExprWithType(0)},
			},
		},
		{
			name:     "rank option",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				validProject(),
				{NodeType: plan.Node_SORT, Children: []int32{0}, RankOption: &plan.RankOption{Mode: "force"}},
			},
			subID: 1,
		},
		{
			name:     "empty project list",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, BindingTags: []int32{projectTag}},
			},
		},
		{
			name:     "mismatched projected column",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_PROJECT, BindingTags: []int32{projectTag}, ProjectList: []*plan.Expr{newCorr(outerTag, 1, 1)}},
			},
		},
		{
			name:     "non-unary wrapper",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				{NodeType: plan.Node_SORT, Children: []int32{0, 1}},
			},
		},
		{
			name:     "unsupported wrapper",
			results:  []*plan.Expr{newFlattenSubqueryTestColExpr(projectTag)},
			projects: []*plan.Expr{newCorr(outerTag, 0, 1)},
			nodes: []*plan.Node{
				validProject(),
				{NodeType: plan.Node_FILTER, Children: []int32{0}},
			},
			subID: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
			builder.qry.Nodes = tt.nodes
			ctx := &BindContext{
				projectTag: projectTag,
				results:    tt.results,
				projects:   tt.projects,
			}

			nodeID, match, outerResult, existential :=
				builder.normalizeDirectCorrelatedScalarProjection(tt.subID, ctx)
			require.Equal(t, tt.subID, nodeID)
			require.Nil(t, match)
			require.Nil(t, outerResult)
			require.False(t, existential)
		})
	}
}

func TestGenerateRowComparisonBuildsBalancedTree(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	subqueryCtx := NewBindContext(builder, nil)
	subqueryCtx.projectTag = 2
	subqueryCtx.results = make([]*plan.Expr, TableColumnCountLimit)

	childItems := make([]*plan.Expr, TableColumnCountLimit)
	for i := range childItems {
		childItems[i] = newRowComparisonTestColumn(1, int32(i))
		subqueryCtx.results[i] = newRowComparisonTestColumn(subqueryCtx.projectTag, int32(i))
	}
	child := &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: childItems},
		},
	}

	for _, tt := range []struct {
		name      string
		op        string
		logicalOp string
	}{
		{name: "tuple in equality", op: "=", logicalOp: "and"},
		{name: "tuple not in inequality", op: "<>", logicalOp: "or"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := builder.generateRowComparison(tt.op, child, subqueryCtx, false)
			require.NoError(t, err)
			require.Equal(t, tt.logicalOp, expr.GetF().Func.GetObjName())

			depth, leaves := planExprDepthAndLeaves(expr)
			require.Equal(t, TableColumnCountLimit*2, leaves)
			require.LessOrEqual(t, depth, 14)
		})
	}
}

func TestGenerateRowComparisonRejectsEmptyTuple(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	subqueryCtx := NewBindContext(builder, nil)

	_, err := builder.generateRowComparison("=", &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{},
		},
	}, subqueryCtx, false)
	require.ErrorContains(t, err, "row comparison requires at least one column")
}

func newRowComparisonTestColumn(relPos, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: colPos,
			},
		},
	}
}

func hasJoinType(query *plan.Query, joinType plan.Node_JoinType) bool {
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == joinType {
			return true
		}
	}
	return false
}

func newFlattenSubqueryTestColExpr(tag int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tag,
			},
		},
	}
}

func newTransparentDerivedEquality(corr *plan.CorrColRef, localTag int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "="},
				Args: []*plan.Expr{
					{Expr: &plan.Expr_Corr{Corr: corr}},
					newFlattenSubqueryTestColExpr(localTag),
				},
			},
		},
	}
}
