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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// Reuse the existing tiny planner fixture; the projected score must remain
// served even when a later ordering makes the implicit relevance sort unused.
func fullTextOrderFixture(t *testing.T, kind string) (*QueryBuilder, int32, int32) {
	t.Helper()
	b, scanID, projectID := buildWrappedMatchGuardPlan(t, true)
	scan, project := b.qry.Nodes[scanID], b.qry.Nodes[projectID]
	scan.TableDef.Indexes = scan.TableDef.Indexes[:1]
	match := project.ProjectList[0]
	scan.FilterList = []*planpb.Expr{DeepCopyExpr(match)}
	project.ProjectList = []*planpb.Expr{ftjColExpr(scan.TableDef, scan.BindingTags[0], 0), match}
	if kind != "fulltext" {
		convertFullTextJoinTestToFulltext2(b, scan)
		if kind == "covered fulltext2" {
			scan.TableDef.Indexes[0].IncludedColumns = []string{"base_id"}
		}
	}
	return b, scanID, projectID
}

func TestFullTextImplicitOrderOverwrittenByAncestorSort(t *testing.T) {
	for _, kind := range []string{"fulltext", "fulltext2", "covered fulltext2"} {
		t.Run(kind, func(t *testing.T) {
			b, _, projectID := fullTextOrderFixture(t, kind)
			project := b.qry.Nodes[projectID]
			order := &planpb.OrderBySpec{Expr: GetColExpr(project.ProjectList[0].Typ, project.BindingTags[0], 0)}
			outer := &planpb.Node{NodeType: planpb.Node_SORT, Children: []int32{projectID},
				OrderBy: []*planpb.OrderBySpec{order}, Limit: makePlan2Uint64ConstExprWithType(2),
				Offset: makePlan2Uint64ConstExprWithType(1)}
			rootID := b.appendNode(outer, b.ctxByNode[projectID])
			b.prepareSpecialIndexGuards(rootID)
			gotID, err := b.applyIndices(rootID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			require.Equal(t, rootID, gotID)
			require.Equal(t, 1, reachableFullTextOrderSorts(b, gotID), "only the explicit order may buffer/sort hits")
			require.Same(t, order, outer.OrderBy[0])
			require.Equal(t, uint64(2), outer.Limit.GetLit().GetU64Val())
			require.Equal(t, uint64(1), outer.Offset.GetLit().GetU64Val())
			scans := collectFullTextFunctionScans(b, gotID)
			require.Len(t, scans, 1)
			require.Nil(t, fulltextScanLimit(scans[0]), "all hits must reach the explicit sort before pagination")
			require.NotNil(t, project.ProjectList[1].GetCol(), "projected relevance must still be computed")
			if kind == "covered fulltext2" {
				require.Equal(t, planpb.Node_INDEX_SEARCH_SCAN, b.qry.Nodes[project.Children[0]].NodeType)
			}
		})
	}
}

// These controls distinguish overwritten ordering from ordering observed before
// the ancestor sort. A local LIMIT/OFFSET or an order-sensitive operator is a
// semantic barrier even if the final result is explicitly ordered.
func TestFullTextImplicitOrderConsumerBoundaries(t *testing.T) {
	for _, kind := range []string{"fulltext", "fulltext2", "covered fulltext2"} {
		for _, tc := range []struct {
			name                                                              string
			bridge                                                            planpb.Node_NodeType
			projectLimit, projectOffset, scanLimit, bridgeLimit, bridgeOffset bool
			explicit, multiple, scoreOrder                                    bool
			wantSorts                                                         int
		}{
			{name: "default relevance", wantSorts: 1},
			{name: "multiple outer keys", explicit: true, multiple: true, wantSorts: 1},
			{name: "explicit relevance", explicit: true, scoreOrder: true, wantSorts: 1},
			{name: "transparent filter", bridge: planpb.Node_FILTER, explicit: true, wantSorts: 1},
			{name: "transparent project", bridge: planpb.Node_PROJECT, explicit: true, wantSorts: 1},
			{name: "local limit", projectLimit: true, explicit: true, wantSorts: 2},
			{name: "local offset", projectOffset: true, explicit: true, wantSorts: 2},
			{name: "scan limit", scanLimit: true, explicit: true, wantSorts: 2},
			{name: "filter limit", bridge: planpb.Node_FILTER, bridgeLimit: true, explicit: true, wantSorts: 2},
			{name: "filter offset", bridge: planpb.Node_FILTER, bridgeOffset: true, explicit: true, wantSorts: 2},
			{name: "window", bridge: planpb.Node_WINDOW, explicit: true, wantSorts: 2},
			{name: "aggregate", bridge: planpb.Node_AGG, explicit: true, wantSorts: 2},
			{name: "partition", bridge: planpb.Node_PARTITION, explicit: true, wantSorts: 2},
			{name: "fill", bridge: planpb.Node_FILL, explicit: true, wantSorts: 2},
			{name: "time window", bridge: planpb.Node_TIME_WINDOW, explicit: true, wantSorts: 2},
			{name: "join", bridge: planpb.Node_JOIN, explicit: true, wantSorts: 2},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				b, scanID, projectID := fullTextOrderFixture(t, kind)
				project := b.qry.Nodes[projectID]
				if tc.projectLimit {
					project.Limit = makePlan2Uint64ConstExprWithType(2)
				}
				if tc.projectOffset {
					project.Offset = makePlan2Uint64ConstExprWithType(1)
				}
				if tc.scanLimit {
					b.qry.Nodes[scanID].Limit = makePlan2Uint64ConstExprWithType(2)
				}
				rootID := projectID
				if tc.bridge != 0 {
					bridge := &planpb.Node{NodeType: tc.bridge, Children: []int32{rootID}}
					if tc.bridge == planpb.Node_JOIN {
						bridge.JoinType = planpb.Node_LEFT
						otherID := b.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, b.ctxByNode[projectID])
						bridge.Children = append(bridge.Children, otherID)
					}
					if tc.bridgeLimit {
						bridge.Limit = makePlan2Uint64ConstExprWithType(2)
					}
					if tc.bridgeOffset {
						bridge.Offset = makePlan2Uint64ConstExprWithType(1)
					}
					rootID = b.appendNode(bridge, b.ctxByNode[projectID])
				}
				if tc.explicit {
					colPos := int32(0)
					if tc.scoreOrder {
						colPos = 1
					}
					orders := []*planpb.OrderBySpec{{Expr: GetColExpr(project.ProjectList[colPos].Typ, project.BindingTags[0], colPos)}}
					if tc.multiple {
						orders = append(orders, &planpb.OrderBySpec{Expr: GetColExpr(project.ProjectList[1].Typ, project.BindingTags[0], 1), Flag: planpb.OrderBySpec_DESC})
					}
					rootID = b.appendNode(&planpb.Node{NodeType: planpb.Node_SORT, Children: []int32{rootID}, OrderBy: orders}, b.ctxByNode[projectID])
				}
				b.prepareSpecialIndexGuards(rootID)
				gotID, err := b.applyIndices(rootID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
				require.NoError(t, err)
				require.Equal(t, tc.wantSorts, reachableFullTextOrderSorts(b, gotID))
				require.Len(t, collectFullTextFunctionScans(b, gotID), 1)
				require.NotNil(t, project.ProjectList[1].GetCol())
			})
		}
	}
}

type fullTextOrderPythonUdfContext struct {
	*fullTextJoinMockCompilerContext
}

func (c *fullTextOrderPythonUdfContext) ResolveUdf(name string, args []*planpb.Expr) (*function.Udf, error) {
	if name != "py_tick" {
		return c.fullTextJoinMockCompilerContext.ResolveUdf(name, args)
	}
	return &function.Udf{
		Language: string(tree.PYTHON),
		RetType:  "bigint",
		Args:     []*function.Arg{{Name: "x", Type: "bigint"}},
		ArgsType: []types.Type{types.T_int64.ToType()},
	}, nil
}

// Parse/bind/optimize the public SQL shape as a separate reachability oracle.
// No service or large corpus is needed to prove which sort owns the candidates;
// execution/result equivalence has a separate SQL regression oracle.
func TestFullTextImplicitOrderSQLReachability(t *testing.T) {
	for _, kind := range []string{"fulltext", "fulltext2", "covered fulltext2"} {
		for _, tc := range []struct {
			name, sql string
			wantSorts int
		}{
			{"default relevance", "select id from ft_order where match(title) against('needle')", 1},
			{"vector order", "select id from ft_order where match(title) against('needle') order by l2_distance(v,'[0,0]') limit 2 offset 1", 1},
			{"multiple keys", "select id from ft_order where match(title) against('needle') order by l2_distance(v,'[0,0]'),id limit 2", 1},
			{"nonvector order and score", "select id,match(title) against('needle') as score from ft_order where match(title) against('needle') order by base_id,id limit 2 offset 1", 1},
			{"score threshold", "select id from ft_order where match(title) against('needle') > 0.04 order by base_id,id limit 2", 1},
			{"stateful score projection", "select id,match(title) against('needle') as score,nextval('order_seq') as seq from ft_order where match(title) against('needle') order by id", 2},
			{"python udf alias", "select id,py_tick(id) as seq from ft_order where match(title) against('needle') order by seq", 2},
			{"nested python udf alias", "select id,abs(py_tick(id)) as seq from ft_order where match(title) against('needle') order by seq", 2},
			{"nested relevance page", "select id from (select id,base_id from ft_order where match(title) against('needle') limit 2) q order by base_id,id", 2},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				b, scanID, _ := fullTextOrderFixture(t, kind)
				table := b.qry.Nodes[scanID].TableDef
				table.Name = "ft_order"
				table.TblId = 10001
				table.Cols[0].Typ = planpb.Type{Id: int32(types.T_int64), NotNullable: true}
				table.Cols[1].Typ = planpb.Type{Id: int32(types.T_int32)}
				table.Cols = append(table.Cols, &planpb.ColDef{Name: "v", Typ: planpb.Type{Id: int32(types.T_array_float32), Width: 2}})
				table.Name2ColIndex["v"] = 4
				for _, idx := range table.Indexes {
					idx.Parts = []string{"title"}
				}
				ctx := b.compCtx.(*fullTextJoinMockCompilerContext)
				ctx.tables[table.Name] = table
				ctx.objects[table.Name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
				stmts, err := mysql.Parse(ctx.GetContext(), tc.sql, 1)
				require.NoError(t, err)
				require.Len(t, stmts, 1)
				defer stmts[0].Free()
				p, err := BuildPlan(&fullTextOrderPythonUdfContext{ctx}, stmts[0], false)
				require.NoError(t, err)
				query := p.GetQuery()
				require.NotNil(t, query)
				require.Len(t, query.Steps, 1)
				planned := &QueryBuilder{qry: query}
				require.Equal(t, tc.wantSorts, reachableFullTextOrderSorts(planned, query.Steps[0]))
				scans := collectFullTextFunctionScans(planned, query.Steps[0])
				require.Len(t, scans, 1)
				require.Zero(t, countReachableFullTextMatches(query))
				if kind == "covered fulltext2" && (tc.name == "nonvector order and score" ||
					tc.name == "python udf alias" || tc.name == "nested python udf alias") {
					require.Zero(t, countReachableNodeType(query, planpb.Node_JOIN), "the covered owner must actually be reached")
				}
				if tc.wantSorts == 1 && tc.name != "default relevance" {
					require.Nil(t, fulltextScanLimit(scans[0]))
				}
			})
		}
	}
}

func TestFullTextImplicitOrderStatefulConsumers(t *testing.T) {
	for _, kind := range []string{"fulltext", "fulltext2", "covered fulltext2"} {
		for _, location := range []string{"anchor project", "bridge project", "bridge filter", "sort key"} {
			for _, stateful := range []bool{false, true} {
				name := "pure scalar"
				if stateful {
					name = "nested nextval"
				}
				t.Run(kind+"/"+location+"/"+name, func(t *testing.T) {
					b, _, projectID := fullTextOrderFixture(t, kind)
					project := b.qry.Nodes[projectID]
					value := makePlan2StringConstExprWithType("x")
					if stateful {
						var err error
						value, err = BindFuncExprImplByPlanExpr(context.Background(), "nextval", []*planpb.Expr{makePlan2StringConstExprWithType("order_seq")})
						require.NoError(t, err)
					}
					expr, err := BindFuncExprImplByPlanExpr(context.Background(), "concat", []*planpb.Expr{value, makePlan2StringConstExprWithType("x")})
					require.NoError(t, err)
					childID := projectID
					switch location {
					case "anchor project":
						project.ProjectList = append(project.ProjectList, expr)
					case "bridge project":
						childID = b.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{childID},
							ProjectList: []*planpb.Expr{GetColExpr(project.ProjectList[0].Typ, project.BindingTags[0], 0), expr}, BindingTags: []int32{b.genNewBindTag()}}, b.ctxByNode[projectID])
					case "bridge filter":
						pred, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{expr, makePlan2StringConstExprWithType("xx")})
						require.NoError(t, err)
						childID = b.appendNode(&planpb.Node{NodeType: planpb.Node_FILTER, Children: []int32{childID}, FilterList: []*planpb.Expr{pred}}, b.ctxByNode[projectID])
					}
					key := GetColExpr(project.ProjectList[0].Typ, project.BindingTags[0], 0)
					if location == "sort key" {
						key = expr
					}
					rootID := b.appendNode(&planpb.Node{NodeType: planpb.Node_SORT, Children: []int32{childID},
						OrderBy: []*planpb.OrderBySpec{{Expr: key}}}, b.ctxByNode[projectID])
					b.prepareSpecialIndexGuards(rootID)
					gotID, err := b.applyIndices(rootID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
					require.NoError(t, err)
					wantSorts := 1
					if stateful {
						wantSorts = 2
					}
					require.Equal(t, wantSorts, reachableFullTextOrderSorts(b, gotID), "stateful evaluation must still observe relevance order")
					require.Len(t, collectFullTextFunctionScans(b, gotID), 1)
					require.NotNil(t, project.ProjectList[1].GetCol(), "safe MATCH scores must remain served")
				})
			}
		}
	}
}

func TestFullTextOrderProofBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name            string
		node            *planpb.Node
		inherited, want bool
	}{
		{"unordered sort", &planpb.Node{NodeType: planpb.Node_SORT}, true, false},
		{"explicit sort", &planpb.Node{NodeType: planpb.Node_SORT, OrderBy: []*planpb.OrderBySpec{{Expr: makePlan2Uint64ConstExprWithType(1)}}}, false, true},
		{"malformed sort key", &planpb.Node{NodeType: planpb.Node_SORT, OrderBy: []*planpb.OrderBySpec{{}}}, true, false},
		{"rank sort", &planpb.Node{NodeType: planpb.Node_SORT, OrderBy: []*planpb.OrderBySpec{{}}, RankOption: &planpb.RankOption{}}, true, false},
		{"project without ancestor", &planpb.Node{NodeType: planpb.Node_PROJECT}, false, false},
		{"project rank", &planpb.Node{NodeType: planpb.Node_PROJECT, RankOption: &planpb.RankOption{}}, true, false},
		{"filter rank", &planpb.Node{NodeType: planpb.Node_FILTER, RankOption: &planpb.RankOption{}}, true, false},
		{"unknown operator", &planpb.Node{NodeType: planpb.Node_SINK}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, fullTextOrderOverwritten(tc.node, tc.inherited)) })
	}
}

func reachableFullTextOrderSorts(b *QueryBuilder, id int32) int {
	node := b.qry.Nodes[id]
	count := 0
	if node.NodeType == planpb.Node_SORT {
		count++
	}
	for _, child := range node.Children {
		count += reachableFullTextOrderSorts(b, child)
	}
	return count
}
