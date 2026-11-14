// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGroupingVolatileAliasSlot(t *testing.T) {
	for _, sample := range []bool{false, true} {
		for _, distinct := range []string{"distinct ", ""} {
			if !sample && distinct == "" {
				continue // Ordinary non-DISTINCT projection binding is outside this transport.
			}
			for _, order := range []string{"concat(a,'')", "concat(b,'')", "concat(reverse(b),'')"} {
				query := "select " + distinct + "md5(cast(uuid() as char)) a,md5(cast(uuid() as char)) b"
				if sample {
					query += ",sample(n_nationkey,100 percent) chosen"
				}
				query += " from nation group by n_regionkey with rollup order by " + order
				t.Run(query, func(t *testing.T) {
					stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, query, 1)
					require.NoError(t, err)
					t.Cleanup(stmt.Free)
					builder := NewQueryBuilder(plan.Query_SELECT, NewMockOptimizer(false).CurrentContext(), false, true)
					_, err = builder.bindSelect(stmt.(*tree.Select), NewBindContext(builder, nil), true)
					require.NoError(t, err)
					seen := make(map[*groupingStage]bool)
					for _, ctx := range builder.ctxByNode {
						if ctx == nil || ctx.groupingStage == nil || seen[ctx.groupingStage] {
							continue
						}
						stage := ctx.groupingStage
						seen[stage] = true
						var cols []*plan.ColRef
						walkGroupingExpr(stage.projects[stage.resultLen], func(e *plan.Expr) {
							if col := e.GetCol(); col != nil {
								cols = append(cols, col)
							}
							require.NotEqual(t, "uuid", e.GetF().GetFunc().GetObjName(), "sorting must consume the selected output, not execute UUID again")
						})
						require.Len(t, cols, 1)
						want := int32(1)
						if order == "concat(a,'')" {
							want = 0
						}
						require.Equal(t, want, cols[0].ColPos, "identical expressions a/b still own different output slots")
					}
					require.NotEmpty(t, seen)
				})
			}
		}
	}
}

func TestGroupingOrderReferenceAndStructuralFallback(t *testing.T) {
	ctx := context.Background()
	volatile, err := BindFuncExprImplByPlanExpr(ctx, "random_bytes", []*plan.Expr{MakePlan2Int64ConstExprWithType(8)})
	require.NoError(t, err)
	wrapped, err := BindFuncExprImplByPlanExpr(ctx, "hex", []*plan.Expr{volatile})
	require.NoError(t, err)
	for _, project := range []*plan.Expr{volatile, wrapped} {
		selected := []*plan.Expr{DeepCopyExpr(project), DeepCopyExpr(project)}
		for _, pos := range []int32{0, 1, 2, -1} {
			ref := &plan.Expr{Typ: project.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 11, ColPos: pos}}}
			got, ok := groupingOrderExpr(ref, selected, 11, 12)
			require.Equal(t, pos == 0 || pos == 1, ok)
			if ok {
				require.Equal(t, int32(12), got.GetCol().RelPos)
				require.Equal(t, pos, got.GetCol().ColPos)
			}
		}
		independent, ok := groupingOrderExpr(DeepCopyExpr(project), selected, 11, 12)
		require.True(t, ok)
		require.NotNil(t, independent.GetF(), "an independent volatile call cannot be identified with either selected result")
		require.True(t, exprStructuralEqual(independent, project))
	}
	constant := MakePlan2Int64ConstExprWithType(8)
	got, ok := groupingOrderExpr(DeepCopyExpr(constant), []*plan.Expr{constant}, 11, 12)
	require.True(t, ok)
	require.Equal(t, int32(0), got.GetCol().ColPos, "deterministic structural matching remains available")
}

func TestGroupingArgumentAliasOrigin(t *testing.T) {
	for _, query := range []string{
		"select n_regionkey k,sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup order by grouping(k) desc,chosen",
		"select n_regionkey n_nationkey,sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup order by grouping(n_nationkey) desc,chosen",
		"select n_regionkey `KeyAlias`,sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup order by chosen-100*grouping(`KeyAlias`)",
		"select n_regionkey k,sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup order by case when grouping(k)=1 then chosen-100 else chosen end",
		"select distinct n_regionkey k,grouping(n_regionkey) flag,sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup order by grouping(k) desc,chosen",
		"select n_regionkey k,n_name label,sample(n_nationkey,100 percent) chosen from nation group by cube(n_regionkey,n_name) order by grouping(k,label) desc,chosen",
		"select n_regionkey k,sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup order by grouping(nation.n_regionkey) desc,chosen",
		"select distinct n_regionkey k,grouping(n_regionkey) flag,count(*) total from nation group by n_regionkey with rollup order by grouping(k) desc,k",
	} {
		t.Run(query, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, query, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			builder := NewQueryBuilder(plan.Query_SELECT, NewMockOptimizer(false).CurrentContext(), false, true)
			_, err = builder.bindSelect(stmt.(*tree.Select), NewBindContext(builder, nil), true)
			require.NoError(t, err)
			seen := make(map[*groupingStage]bool)
			found := 0
			for _, ctx := range builder.ctxByNode {
				if ctx == nil || ctx.groupingStage == nil || seen[ctx.groupingStage] {
					continue
				}
				stage := ctx.groupingStage
				seen[stage] = true
				projects := stage.sampleProjects
				if projects == nil {
					projects = stage.projects
				}
				for _, expr := range projects {
					walkGroupingExpr(expr, func(e *plan.Expr) {
						if fn := e.GetF(); fn != nil && fn.Func.ObjName == "grouping" {
							found++
							for _, arg := range fn.Args {
								col := arg.GetCol()
								require.NotNil(t, col)
								require.Equal(t, ctx.groupTag, col.RelPos, "GROUPING consumes the alias's grouping origin, not its future selected output")
								require.GreaterOrEqual(t, col.ColPos, int32(0))
								require.Less(t, int(col.ColPos), len(ctx.groups))
							}
						}
					})
				}
			}
			require.NotEmpty(t, seen)
			require.Positive(t, found)
			for _, prefix := range []string{"", "create table grouping_alias_order as "} {
				_, err := buildSingleStmt(NewMockOptimizer(false), t, prefix+query)
				require.NoError(t, err)
			}
		})
	}
}

func TestGroupingSampleMixedOrderScope(t *testing.T) {
	for _, order := range []string{
		"chosen-100*grouping(n_regionkey)",
		"abs(chosen)-100*grouping(n_regionkey)",
		"case when grouping(n_regionkey)=1 then chosen-100 else chosen end",
		"(chosen,grouping(n_regionkey))",
		"case when chosen in(select r_regionkey from region) then chosen else 0 end-100*grouping(n_regionkey)",
		"coalesce((select max(r_regionkey) from region),0)+chosen-100*grouping(n_regionkey)",
	} {
		query := "select sample(n_nationkey,100 percent) chosen from nation group by n_regionkey with rollup order by " + order + " limit 3"
		t.Run(order, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, query, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			builder := NewQueryBuilder(plan.Query_SELECT, NewMockOptimizer(false).CurrentContext(), false, true)
			_, err = builder.bindSelect(stmt.(*tree.Select), NewBindContext(builder, nil), true)
			require.NoError(t, err)
			seen := make(map[*groupingStage]bool)
			for _, ctx := range builder.ctxByNode {
				if ctx == nil || ctx.groupingStage == nil || seen[ctx.groupingStage] {
					continue
				}
				stage := ctx.groupingStage
				seen[stage] = true
				payloads := 0
				for _, node := range builder.qry.Nodes {
					if node.NodeType != plan.Node_PROJECT || len(node.Children) != 1 ||
						builder.qry.Nodes[node.Children[0]].NodeType != plan.Node_SAMPLE ||
						builder.ctxByNode[node.NodeId] != ctx {
						continue
					}
					payloads++
					for _, expr := range node.ProjectList {
						walkGroupingExpr(expr, func(e *plan.Expr) {
							if col := e.GetCol(); col != nil {
								require.NotEqual(t, ctx.projectTag, col.RelPos, "branch payload cannot consume its own future output")
							}
						})
					}
				}
				require.Positive(t, payloads)
				refs := make(map[int32]bool)
				walkGroupingExpr(stage.projects[stage.resultLen], func(e *plan.Expr) {
					if col := e.GetCol(); col != nil {
						require.Equal(t, stage.projectionInputTag, col.RelPos)
						refs[col.ColPos] = true
					}
				})
				require.True(t, refs[0], "sorting consumes the selected chosen output")
				require.Greater(t, len(refs), 1, "branch-owned GROUPING is a distinct transported dependency")
			}
			require.NotEmpty(t, seen)
			for _, prefix := range []string{"", "create table mixed_sample_order as "} {
				_, err := buildSingleStmt(NewMockOptimizer(false), t, prefix+query)
				require.NoError(t, err)
			}
		})
	}
}
