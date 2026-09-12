// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDeepExistentialPublicPlans(t *testing.T) {
	for _, tc := range []struct {
		name, condition   string
		semi, anti, scans int
	}{
		{"not_parentheses", `not (exists(select 1 from nation i where exists(select 1 from nation j where j.n_nationkey=i.n_nationkey and j.n_regionkey=o.n_regionkey)))`, 1, 1, 3},
		{"outer_equality_bridge", `exists(select 1 from nation i where exists(select 1 from nation j where o.n_nationkey=i.n_nationkey and o.n_regionkey=j.n_regionkey and o.n_nationkey=o.n_regionkey))`, 2, 0, 3},
		{"transitive_anchor", `exists(select 1 from nation i where exists(select 1 from nation j where o.n_nationkey=i.n_nationkey and i.n_nationkey=j.n_nationkey and o.n_regionkey=j.n_regionkey and i.n_regionkey=j.n_regionkey))`, 2, 0, 3},
		{"and", `exists(select 1 from nation i where exists(select 1 from nation j where j.n_nationkey=i.n_nationkey and j.n_regionkey=o.n_regionkey))`, 2, 0, 3},
		{"anti", `not exists(select 1 from nation i where exists(select 1 from nation j where j.n_nationkey=i.n_nationkey and j.n_regionkey=o.n_regionkey))`, 1, 1, 3},
		{"in", `o.n_regionkey in(select i.n_regionkey from nation i where i.n_nationkey in(select j.n_nationkey from nation j where j.n_nationkey=i.n_nationkey and j.n_regionkey=o.n_regionkey))`, 2, 0, 3},
		{"direct", `exists(select 1 from nation i where exists(select 1 from nation j where j.n_regionkey=o.n_regionkey))`, 1, 0, 3},
		{"or", `exists(select 1 from nation i where exists(select 1 from nation j where j.n_nationkey=i.n_nationkey or j.n_regionkey=o.n_regionkey))`, 1, 0, 5},
		{"outer_gate_anti", `not exists(select 1 from nation i where exists(select 1 from nation j where j.n_nationkey=i.n_nationkey and j.n_regionkey=o.n_regionkey and o.n_nationkey=1))`, 1, 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false), t, "select o.n_nationkey from nation o where "+tc.condition)
			require.NoError(t, err)
			q := p.GetQuery()
			require.NotNil(t, q)
			semi, anti, scans := 0, 0, 0
			var walk func(int32)
			walk = func(id int32) {
				n := q.Nodes[id]
				switch n.NodeType {
				case plan.Node_TABLE_SCAN:
					scans++
				case plan.Node_JOIN:
					if n.JoinType == plan.Node_ANTI {
						for _, e := range n.OnList {
							require.True(t, e.GetF() != nil && IsEqualFunc(e.GetF().Func.Obj), "ANTI must have no residual: %s", e)
							var sides []int32
							for _, arg := range e.GetF().Args {
								rels := map[int32]bool{}
								existentialWalk(arg, func(x *plan.Expr) {
									if c := x.GetCol(); c != nil {
										rels[c.RelPos] = true
									}
								})
								require.Len(t, rels, 1, "hash gate was inlined: %s", e)
								for rel := range rels {
									sides = append(sides, rel)
								}
							}
							require.ElementsMatch(t, []int32{0, 1}, sides)
						}
					}
					switch n.JoinType {
					case plan.Node_SEMI:
						semi++
					case plan.Node_ANTI:
						anti++
					}
					if n.JoinType == plan.Node_SEMI || n.JoinType == plan.Node_ANTI || n.JoinType == plan.Node_MARK {
						hash := false
						for _, e := range n.OnList {
							hash = hash || (e.GetF() != nil && IsEqualFunc(e.GetF().Func.Obj))
						}
						require.True(t, hash, "witness join must retain an equality key: %s", n)
					}
				}
				for _, list := range [][]*plan.Expr{n.FilterList, n.OnList, n.ProjectList, n.AggList, n.GroupBy} {
					for _, e := range list {
						existentialWalk(e, func(e *plan.Expr) { require.Nil(t, e.GetSub()); require.Nil(t, e.GetCorr()) })
					}
				}
				for _, c := range n.Children {
					walk(c)
				}
			}
			for _, root := range q.Steps {
				walk(root)
			}
			require.Equal(t, tc.semi, semi)
			require.Equal(t, tc.anti, anti)
			require.Equal(t, tc.scans, scans)
		})
	}
}

func TestDeepExistentialAdmission(t *testing.T) {
	deep := `exists(select 1 from nation j where j.n_nationkey=i.n_nationkey and j.n_regionkey=o.n_regionkey)`
	for _, tc := range []struct{ name, sql string }{
		{"projected", "select exists(select 1 from nation i where " + deep + ") from nation o"},
		{"outer_or", "select 1 from nation o where o.n_nationkey=1 or exists(select 1 from nation i where " + deep + ")"},
		{"middle_or", "select 1 from nation o where exists(select 1 from nation i where i.n_nationkey=1 or " + deep + ")"},
		{"derived", "select 1 from nation o where exists(select 1 from (select * from nation) i where " + deep + ")"},
		{"order_by", "select 1 from nation o where exists(select 1 from nation i where " + deep + " order by i.n_nationkey)"},
		{"for_update", "select 1 from nation o where exists(select 1 from nation i where " + deep + ") for update"},
		{"nullable_outer_composite", `select 1 from nation a left join nation o on a.n_nationkey=o.n_nationkey where exists(select 1 from nation i where exists(select 1 from nation j where (j.n_regionkey=o.n_regionkey and j.n_nationkey=o.n_nationkey and j.n_nationkey=i.n_nationkey) or (j.n_regionkey=o.n_nationkey and j.n_nationkey=o.n_regionkey and j.n_nationkey=i.n_nationkey)))`},
		{"limit", "select 1 from nation o where exists(select 1 from nation i where " + deep + " limit 1)"},
		{"aggregate", "select 1 from nation o where exists(select count(*) from nation i where " + deep + ")"},
		{"inner_not", "select 1 from nation o where exists(select 1 from nation i where not " + deep + ")"},
		{"non_equality", "select 1 from nation o where exists(select 1 from nation i where exists(select 1 from nation j where j.n_nationkey=i.n_nationkey and j.n_regionkey>o.n_regionkey))"},
		{"two_pending", "select 1 from nation o where exists(select 1 from nation i where " + deep + " and " + deep + ")"},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := runOneStmt(NewMockOptimizer(false), t, tc.sql); require.Error(t, err) })
	}
	for _, n := range []int{8, 9} {
		t.Run(fmt.Sprint("arms_", n), func(t *testing.T) {
			arms := make([]string, n)
			for i := range arms {
				arms[i] = fmt.Sprintf("(j.n_regionkey=o.n_regionkey and j.n_nationkey=i.n_nationkey and j.n_nationkey=%d)", i)
			}
			sql := "select 1 from nation o where exists(select 1 from nation i where exists(select 1 from nation j where " + strings.Join(arms, " or ") + "))"
			_, err := runOneStmt(NewMockOptimizer(false), t, sql)
			if n <= maxExistentialArms {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestDeepExistentialOldPathAdmissionAllocations(t *testing.T) {
	for _, depth := range []uint32{1, 2} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			ctx := &BindContext{existentialBlock: 1}
			subCtx := &BindContext{existentialBlock: 2, subqueryNestingDepth: depth}
			b := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{10}, FilterList: []*plan.Expr{constTrue}}}}, ctxByNode: []*BindContext{subCtx}}
			sub := &plan.SubqueryRef{NodeId: 0, Typ: plan.SubqueryRef_EXISTS}
			allocations := testing.AllocsPerRun(1000, func() {
				_, _, handled, err := b.tryDeepExistential(0, sub, ctx, existentialFilterTrue)
				if handled || err != nil {
					panic("old success entered new path")
				}
			})
			require.Zero(t, allocations)
			require.Nil(t, b.pendingExistentials)
			require.False(t, b.hadPendingExistentials)
		})
	}
}

func TestDeepExistentialFilterPreservesMemo(t *testing.T) {
	// Memo lookup must precede any subquery-node access, even for a root WHERE
	// conjunct. Its node ID need not remain reachable after prior flattening.
	ctx := &BindContext{flattenedVolatileExprs: map[int32]*plan.Expr{-1: constTrue}}
	e := &plan.Expr{AuxId: -1, Typ: constTrue.Typ, Expr: &plan.Expr_Sub{Sub: &plan.SubqueryRef{NodeId: 999, Typ: plan.SubqueryRef_EXISTS}}}
	b := &QueryBuilder{}
	id, result, err := b.flattenFilterSubqueries(7, e, ctx)
	require.NoError(t, err)
	require.Equal(t, int32(7), id)
	require.Equal(t, int32(-1), result.AuxId)
	require.True(t, result.GetLit().GetBval())
	// A shallow NOT(EXISTS) has no pending owner. Keep the legacy child
	// wrapper, including a memo hit whose original subquery is unreachable.
	not := &plan.Expr{Typ: constTrue.Typ, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "not"}, Args: []*plan.Expr{e},
	}}}
	id, result, err = b.flattenFilterSubqueries(7, not, ctx)
	require.NoError(t, err)
	require.Equal(t, int32(7), id)
	require.Equal(t, "not", result.GetF().Func.ObjName)
	require.Equal(t, int32(-1), result.GetF().Args[0].AuxId)
	require.True(t, result.GetF().Args[0].GetLit().GetBval())
}

func TestDeepExistentialGateProjectionSurvivesCopy(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(false), false, false)
	ctx := NewBindContext(b, nil)
	scan := b.appendNode(&plan.Node{NodeType: plan.Node_VALUE_SCAN, RowsetData: &plan.RowsetData{RowCount: 1}}, ctx)
	project := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scan}, BindingTags: []int32{b.genNewBindTag()}, ProjectList: []*plan.Expr{constTrue}}, ctx)
	b.existentialGateProjects = map[int32]struct{}{project: {}}
	copy := b.copyNode(ctx, project)
	_, protected := b.existentialGateProjects[copy]
	require.True(t, protected)
	require.False(t, b.canRemoveProject(plan.Node_JOIN, b.qry.Nodes[copy]))
}
