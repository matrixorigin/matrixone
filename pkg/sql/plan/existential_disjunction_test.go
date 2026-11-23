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
	"fmt"
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestExistentialDisjunctionPlanning(t *testing.T) {
	var budgetArms []string
	for i := 1; i <= maxExistentialArms; i++ {
		budgetArms = append(budgetArms, fmt.Sprintf("r.r_regionkey=n.n_regionkey-%d", i))
	}
	for _, tc := range []struct{ name, predicate string }{
		{"outer keys", "r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=n.n_regionkey+1"},
		{"inner keys", "r.r_regionkey+1=n.n_regionkey or r.r_regionkey-1=n.n_regionkey"},
		{"commuted", "n.n_regionkey-1=r.r_regionkey or n.n_regionkey+1=r.r_regionkey"},
		{"three branches", "r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=n.n_regionkey+1 or r.r_regionkey=n.n_nationkey"},
		{"local filter", "r.r_regionkey>0 and (r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=n.n_regionkey+1)"},
		{"nullable strings", "r.r_comment=n.n_comment or r.r_comment=n.n_name"},
		{"arm budget", strings.Join(budgetArms, " or ")},
	} {
		for _, consumer := range []string{"filter", "negative filter", "projection", "negative projection"} {
			t.Run(tc.name+"/"+consumer, func(t *testing.T) {
				expr := "exists (select 1 from tpch.region r where " + tc.predicate + ")"
				if strings.HasPrefix(consumer, "negative") {
					expr = "not " + expr
				}
				sql := "select " + expr + " from tpch.nation n"
				if strings.HasSuffix(consumer, "filter") {
					sql = "select n.n_name from tpch.nation n where " + expr
				}
				p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, sql)
				require.NoError(t, err)
				var joins int
				for _, node := range reachableFlattenSubqueryNodes(p.GetQuery()) {
					if node.NodeType != planpb.Node_JOIN {
						continue
					}
					joins++
					require.Len(t, node.OnList, 1)
					fn := node.OnList[0].GetF()
					require.NotNil(t, fn)
					require.True(t, IsEqualFunc(fn.Func.Obj), "every existence branch must expose its own equality, not OR/IS TRUE")
				}
				require.Positive(t, joins)
				assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
			})
		}
	}
}

func TestExistentialDisjunctionKeepsFallback(t *testing.T) {
	var tooMany []string
	for i := 1; i <= maxExistentialArms+1; i++ {
		tooMany = append(tooMany, fmt.Sprintf("r.r_regionkey=n.n_regionkey-%d", i))
	}
	for _, tc := range []struct{ name, predicate string }{
		{"arm budget", strings.Join(tooMany, " or ")},
		{"inequality", "r.r_regionkey=n.n_regionkey-1 or r.r_regionkey>n.n_regionkey+1"},
		{"one sided arm", "r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=r.r_regionkey+1"},
		{"mixed operand", "r.r_regionkey+n.n_regionkey=n.n_regionkey or r.r_regionkey=n.n_regionkey+1"},
		{"overflow", "r.r_regionkey=n.n_regionkey-9223372036854775807 or r.r_regionkey=n.n_regionkey+1"},
		{"volatile key", "r.r_regionkey=n.n_regionkey+rand() or r.r_regionkey=n.n_regionkey+1"},
		{"volatile filter", "rand()>0 and (r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=n.n_regionkey+1)"},
		{"fallible filter", "cast(r.r_name as bigint)>0 and (r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=n.n_regionkey+1)"},
		{"correlated residual", "r.r_name=n.n_name and (r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=n.n_regionkey+1)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t,
				"select exists(select 1 from tpch.region r where "+tc.predicate+") from tpch.nation n")
			require.NoError(t, err)
			var joins int
			for _, node := range reachableFlattenSubqueryNodes(p.GetQuery()) {
				if node.NodeType == planpb.Node_JOIN {
					joins++
				}
			}
			require.Equal(t, 1, joins, "ineligible predicates must not become independent branches")
			assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
		})
	}
}

func TestExistentialDisjunctionRelationBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, prefix, relation string }{
		{"cte", "with shared as (select r_regionkey from tpch.region) ", "shared r"},
		{"derived block", "", "(select r_regionkey from tpch.region) r"},
		{"pagination", "", "(select r_regionkey from tpch.region limit 1 offset 1) r"},
		{"aggregate", "", "(select r_regionkey from tpch.region group by r_regionkey) r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t,
				tc.prefix+"select exists(select 1 from "+tc.relation+
					" where r.r_regionkey=n.n_regionkey-1 or r.r_regionkey=n.n_regionkey+1) from tpch.nation n")
			require.NoError(t, err)
			var joins int
			for _, node := range reachableFlattenSubqueryNodes(p.GetQuery()) {
				if node.NodeType == planpb.Node_JOIN {
					joins++
				}
			}
			require.Equal(t, 1, joins, "do not copy a different query block or materializing operator")
			assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
		})
	}
}

func existentialDisjunctionFixture(t *testing.T) (*QueryBuilder, *planpb.Node, []*planpb.Expr) {
	t.Helper()
	builder, mark := buildMarkPlanBeforeOptimization(t,
		`select exists(select 1 from tpch.region r where r.r_regionkey=n.n_regionkey-1) from tpch.nation n`)
	require.Len(t, mark.OnList, 1)
	require.True(t, IsEqualFunc(mark.OnList[0].GetF().Func.Obj))
	predicate, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*planpb.Expr{
		DeepCopyExpr(mark.OnList[0]), DeepCopyExpr(mark.OnList[0]),
	})
	require.NoError(t, err)
	return builder, mark, []*planpb.Expr{predicate}
}

func TestExistentialDisjunctionInputBarriers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*QueryBuilder, *planpb.Node)
	}{
		{"limit", func(_ *QueryBuilder, n *planpb.Node) { n.Limit = makePlan2Int64ConstExprWithType(1) }},
		{"offset", func(_ *QueryBuilder, n *planpb.Node) { n.Offset = makePlan2Int64ConstExprWithType(1) }},
		{"order", func(_ *QueryBuilder, n *planpb.Node) { n.OrderBy = []*planpb.OrderBySpec{{Expr: n.ProjectList[0]}} }},
		{"filter barrier", func(_ *QueryBuilder, n *planpb.Node) { n.FilterIsBarrier = true }},
		{"end barrier", func(_ *QueryBuilder, n *planpb.Node) { n.IsEnd = true }},
		{"view", func(_ *QueryBuilder, n *planpb.Node) { n.ViewPath = []*planpb.ViewStep{{}} }},
		{"lock", func(_ *QueryBuilder, n *planpb.Node) { n.LockTargets = []*planpb.LockTarget{{}} }},
		{"aggregate", func(_ *QueryBuilder, n *planpb.Node) { n.NodeType = planpb.Node_AGG }},
		{"window", func(_ *QueryBuilder, n *planpb.Node) { n.NodeType = planpb.Node_WINDOW }},
		{"shared input", func(_ *QueryBuilder, n *planpb.Node) { n.NodeType = planpb.Node_SINK_SCAN }},
		{"invalid scan", func(b *QueryBuilder, n *planpb.Node) { b.qry.Nodes[n.Children[0]].TableDef = nil }},
		{"invalid project", func(_ *QueryBuilder, n *planpb.Node) { n.BindingTags = nil }},
		{"invalid filter", func(_ *QueryBuilder, n *planpb.Node) { n.NodeType = planpb.Node_FILTER }},
		{"other block", func(b *QueryBuilder, n *planpb.Node) {
			b.ctxByNode[n.NodeId] = &BindContext{queryBlockOwner: &BindContext{}}
		}},
		{"missing owner", func(b *QueryBuilder, n *planpb.Node) { b.ctxByNode[n.NodeId] = nil }},
		{"prepared projection", func(_ *QueryBuilder, n *planpb.Node) {
			n.ProjectList[0] = &planpb.Expr{Typ: n.ProjectList[0].Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{}}}
		}},
		{"literal source", func(_ *QueryBuilder, n *planpb.Node) {
			n.ProjectList[0].GetLit().Src = makePlan2Int64ConstExprWithType(1)
		}},
		{"foreign column", func(_ *QueryBuilder, n *planpb.Node) {
			n.ProjectList[0] = &planpb.Expr{Typ: n.ProjectList[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 99999}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, mark, _ := existentialDisjunctionFixture(t)
			input := mark.Children[1]
			tags := make(map[int32]bool)
			for _, tag := range b.enumerateTags(input) {
				tags[tag] = true
			}
			tc.mutate(b, b.qry.Nodes[input])
			before, nextTag := b.qry.String(), b.nextBindTag
			_, ok := b.existentialDisjunctionInput(input, tags)
			require.False(t, ok)
			require.Equal(t, before, b.qry.String())
			require.Equal(t, nextTag, b.nextBindTag)
		})
	}
}

func TestExistentialDisjunctionOwnsCopiedBindings(t *testing.T) {
	b, mark, predicates := existentialDisjunctionFixture(t)
	before := make([]string, len(b.qry.Nodes))
	for i, node := range b.qry.Nodes {
		before[i] = node.String()
	}
	originalPredicate := predicates[0].String()
	root, marker, ok, err := b.tryExistentialDisjunction(mark.Children[0], mark.Children[1], predicates, false, b.ctxByNode[mark.NodeId])
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, marker.Typ.NotNullable)
	require.Len(t, b.qry.Nodes, len(before)+6, "two project/scan copies and two MARK joins")
	for i, original := range before {
		require.Equal(t, original, b.qry.Nodes[i].String(), "source node %d was mutated", i)
	}
	require.Equal(t, originalPredicate, predicates[0].String())
	nodes := reachableFlattenSubqueryNodes(&planpb.Query{Nodes: b.qry.Nodes, Steps: []int32{root}})
	tags := make(map[int32]bool)
	for _, node := range nodes {
		for _, tag := range node.BindingTags {
			require.False(t, tags[tag], "binding tag %d is shared by copied inputs", tag)
			tags[tag] = true
		}
	}
	for _, node := range nodes {
		for _, list := range [][]*planpb.Expr{node.OnList, node.ProjectList, node.FilterList} {
			for _, expr := range list {
				existentialWalk(expr, func(e *planpb.Expr) {
					if col := e.GetCol(); col != nil {
						require.True(t, tags[col.RelPos], "reference to unreachable old tag %d", col.RelPos)
					}
				})
			}
		}
	}
}

func TestExistentialDisjunctionDisabledAndCanceled(t *testing.T) {
	for _, mode := range []string{"hint", "for update", "dml", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			b, mark, predicates := existentialDisjunctionFixture(t)
			switch mode {
			case "hint":
				handleOptimizerHints("subqueryPredicatePlanning=1", b)
			case "for update":
				b.isForUpdate = true
			case "dml":
				b.qry.StmtType = planpb.Query_DELETE
			case "cancel":
				ctx, cancel := context.WithCancel(b.GetContext())
				cancel()
				b.compCtx.(*MockCompilerContext).SetContext(ctx)
			}
			before, nextTag := b.qry.String(), b.nextBindTag
			_, _, handled, err := b.tryExistentialDisjunction(mark.Children[0], mark.Children[1], predicates, false, b.ctxByNode[mark.NodeId])
			if mode == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
				require.True(t, handled)
			} else {
				require.NoError(t, err)
				require.False(t, handled)
			}
			require.Equal(t, before, b.qry.String())
			require.Equal(t, nextTag, b.nextBindTag)
		})
	}
}

func TestExistentialDisjunctionPreservesIndexHints(t *testing.T) {
	for _, tc := range []struct {
		name, hint string
		indexScans int
	}{
		{"no hint", "", 2},
		{"ignore", "ignore index(idx_a)", 0},
		{"use empty", "use index()", 0},
		{"use", "use index(idx_a)", 2},
		{"force", "force index(idx_a)", 2},
		// JOIN-scoped hints must not suppress local scan-filter access.
		{"ignore for join leaves scan access", "ignore index for join(idx_a)", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			// Keep a single candidate: ignored indexes must not be replaced by
			// another index, and the no-hint control must exercise index access.
			mock.ctxt.tables["index_hint_t"].Indexes = mock.ctxt.tables["index_hint_t"].Indexes[:1]
			p, err := runOneStmt(mock, t,
				"select exists(select 1 from index_hint_t i "+tc.hint+
					" where i.a=1 and (i.id=n.n_regionkey-1 or i.id=n.n_regionkey+1)) from tpch.nation n")
			require.NoError(t, err)
			var indexScans, marks int
			for _, node := range reachableFlattenSubqueryNodes(p.GetQuery()) {
				if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_MARK {
					marks++
				}
				if node.NodeType == planpb.Node_TABLE_SCAN && node.IndexScanInfo.IsIndexScan {
					indexScans++
					require.Equal(t, "idx_a", node.IndexScanInfo.IndexName)
				}
			}
			require.Equal(t, 2, marks, "the regression must exercise both copied OR arms")
			require.Equal(t, tc.indexScans, indexScans, "every reachable arm must honor the inner table hint")
		})
	}
}

func TestExistentialDisjunctionCopiesIndexHintOwner(t *testing.T) {
	b, mark, _ := existentialDisjunctionFixture(t)
	tags := make(map[int32]bool)
	for _, tag := range b.enumerateTags(mark.Children[1]) {
		tags[tag] = true
	}
	chain, ok := b.existentialDisjunctionInput(mark.Children[1], tags)
	require.True(t, ok)
	source := chain[len(chain)-1]
	hints := &indexHintSet{scan: indexHintScopeSet{useSpecified: true}, join: indexHintScopeSet{forceSpecified: true}}
	b.indexHintsByScan = map[int32]*indexHintSet{source: hints}
	b.indexHintOwnerByNode[source] = source
	for generation := 0; generation < 2; generation++ {
		input, _ := b.copyExistentialDisjunctionInput(chain)
		tags = make(map[int32]bool)
		for _, tag := range b.enumerateTags(input) {
			tags[tag] = true
		}
		chain, ok = b.existentialDisjunctionInput(input, tags)
		require.True(t, ok)
		copyID := chain[len(chain)-1]
		require.NotEqual(t, source, copyID)
		require.Same(t, hints, b.indexHintsByScan[copyID])
		owner, inherited := b.indexHintOwnerByNode[copyID]
		require.True(t, inherited)
		require.Equal(t, source, owner, "copying a copy must retain the original table-reference owner")
		require.Same(t, b.qry.Nodes[source], b.indexHintOwnerScan(copyID))
		require.True(t, b.scanForcesJoinIndex(copyID))
	}
	require.Same(t, hints, b.indexHintsByScan[source])
	require.Equal(t, source, b.indexHintOwnerByNode[source])
}

func TestExistentialDisjunctionOldPathDoesNotAllocate(t *testing.T) {
	b := &QueryBuilder{qry: &planpb.Query{StmtType: planpb.Query_SELECT}}
	predicates := []*planpb.Expr{{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EQUAL, ObjName: "="}}}}}
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		_, _, handled, err := b.tryExistentialDisjunction(0, 0, predicates, false, nil)
		if handled || err != nil {
			panic("ordinary equality entered OR expansion")
		}
	}))
}
