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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func equiVectorPlanFixture(t *testing.T, sql string, unique, defaultAuto bool) *plan.Query {
	t.Helper()
	ctx := newVectorJoinMockCtx()
	resolve := ctx.resolveVarFunc
	ctx.resolveVarFunc = func(name string, system, global bool) (interface{}, error) {
		if name == "enable_vector_auto_mode_by_default" && defaultAuto {
			return int8(1), nil
		}
		return resolve(name, system, global)
	}
	a := newVectorJoinTableDef(false, true)
	a.Name, a.TblId = "equi_chunks", 801
	a.Cols[0].Typ = plan.Type{Id: int32(types.T_varchar), Width: 64, NotNullable: true}
	a.Cols[1].Typ = plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true}
	a.Cols = append(a.Cols,
		&plan.ColDef{Name: "document_id", Typ: a.Cols[0].Typ},
		&plan.ColDef{Name: "category", Typ: a.Cols[0].Typ})
	a.Name2ColIndex["document_id"] = 2
	a.Name2ColIndex["category"] = 3
	for _, def := range newVectorJoinIvfIndex().IndexDefs {
		def.TableExist = true
		def.IndexAlgoParams = `{"op_type":"vector_l2_ops","lists":"1"}`
		a.Indexes = append(a.Indexes, def)
		ctx.tables[def.IndexTableName] = &plan.TableDef{Name: def.IndexTableName, TblId: uint64(810 + len(a.Indexes))}
		ctx.objects[def.IndexTableName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: def.IndexTableName}
	}
	b := &plan.TableDef{Name: "equi_documents", TblId: 802,
		Cols: []*plan.ColDef{
			{Name: "document_id", Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}},
			{Name: "label", Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}},
			{Name: "row_id", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
		}, Name2ColIndex: map[string]int32{"document_id": 0, "label": 1, "row_id": 2},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "row_id", Names: []string{"row_id"}}}
	if unique {
		b.Pkey = &plan.PrimaryKeyDef{PkeyColName: "document_id", Names: []string{"document_id"}}
		b.Cols[0].Typ.NotNullable = true
	}
	for _, table := range []*plan.TableDef{a, b} {
		ctx.tables[table.Name] = table
		ctx.objects[table.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	}
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	p, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)
	if prepare := p.GetDcl().GetPrepare(); prepare != nil {
		return prepare.Plan.GetQuery()
	}
	return p.GetQuery()
}

func equiVectorSearchNodes(q *plan.Query) []*plan.Node {
	var searches []*plan.Node
	seen := make(map[int32]bool)
	var walk func(int32)
	walk = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		n := q.Nodes[id]
		if n.NodeType == plan.Node_FUNCTION_SCAN && n.TableDef != nil && n.TableDef.TblFunc != nil && n.TableDef.TblFunc.Name == "ivf_search" {
			searches = append(searches, n)
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	for _, root := range q.Steps {
		walk(root)
	}
	return searches
}

func TestEquiVectorTopKPublicPlan(t *testing.T) {
	for _, unique := range []bool{false, true} {
		for _, tc := range []struct{ name, sql string }{
			{"plain", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				where l2_distance(a.v,'[0,0]')<=0.5 order by l2_distance(a.v,'[0,0]') limit 2`},
			{"pre", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=pre'`},
			{"reversed", `select a.id,b.label from equi_documents b join equi_chunks a on b.document_id=a.document_id
				order by l2_distance(a.v,'[0,0]') limit 2`},
			{"alias", `select a.id,b.label,l2_distance(a.v,'[0,0]') as distance from equi_chunks a
				join equi_documents b on a.document_id=b.document_id order by distance limit 2`},
			{"right_filter", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2`},
			{"two_keys", `select a.id from equi_chunks a join equi_documents b
				on a.document_id=b.document_id and a.id=b.label order by l2_distance(a.v,'[0,0]') limit 2`},
			{"offset", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				order by l2_distance(a.v,'[0,0]') limit 2 offset 1`},
		} {
			t.Run(fmt.Sprintf("unique=%t/%s", unique, tc.name), func(t *testing.T) {
				q := equiVectorPlanFixture(t, tc.sql, unique, false)
				searches := equiVectorSearchNodes(q)
				require.Len(t, searches, 1)
				require.True(t, searches[0].Stats.ForceOneCN)
				require.Len(t, searches[0].RuntimeFilterProbeList, 1)
				require.True(t, searches[0].RuntimeFilterProbeList[0].MustApply)
				require.NotEqual(t, ExecTypeAP_MULTICN, GetExecType(q, false, false))
			})
		}
	}
}

func TestEquiVectorTopKPreservesMixedFilters(t *testing.T) {
	const lower = "l2_distance(a.v,'[0,0]')>0.15"
	const upper = "l2_distance(a.v,'[0,0]')<=0.25"
	const category = "a.category='x'"
	for _, shape := range []struct{ name, sql string }{
		{"inner", "select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id where %s"},
		{"semi", "select a.id from equi_chunks a where a.document_id in (select document_id from equi_documents) and %s"},
	} {
		for _, tc := range []struct {
			name       string
			filters    string
			predicates []string
		}{
			{"lower_first", lower + " and " + category, []string{">0.15", "category=x"}},
			{"lower_last", category + " and " + lower, []string{">0.15", "category=x"}},
			{"upper_first", upper + " and " + category, []string{"<=0.25", "category=x"}},
			{"upper_last", category + " and " + upper, []string{"<=0.25", "category=x"}},
			{"lower_upper_category", lower + " and " + upper + " and " + category, []string{">0.15", "<=0.25", "category=x"}},
			{"upper_lower_category", upper + " and " + lower + " and " + category, []string{">0.15", "<=0.25", "category=x"}},
			{"lower_category_upper", lower + " and " + category + " and " + upper, []string{">0.15", "<=0.25", "category=x"}},
			{"upper_category_lower", upper + " and " + category + " and " + lower, []string{">0.15", "<=0.25", "category=x"}},
			{"category_lower_upper", category + " and " + lower + " and " + upper, []string{">0.15", "<=0.25", "category=x"}},
			{"category_upper_lower", category + " and " + upper + " and " + lower, []string{">0.15", "<=0.25", "category=x"}},
			{"distance_only", lower, []string{">0.15"}},
			{"category_only", category, []string{"category=x"}},
		} {
			t.Run(shape.name+"/"+tc.name, func(t *testing.T) {
				sql := fmt.Sprintf(shape.sql, tc.filters) + " order by l2_distance(a.v,'[0,0]') limit 1"
				q := equiVectorPlanFixture(t, sql, false, false)
				require.Len(t, equiVectorSearchNodes(q), 1)
				scans := 0
				var walk func(int32)
				walk = func(id int32) {
					node := q.Nodes[id]
					if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Name == "equi_chunks" {
						scans++
						var predicates []string
						var inspect func(*plan.Expr)
						inspect = func(expr *plan.Expr) {
							f := expr.GetF()
							if f == nil {
								return
							}
							if len(f.Args) == 2 {
								if distance := f.Args[0].GetF(); distance != nil && distance.Func.ObjName == "l2_distance" {
									bound, ok := plan.GetLiteralFloat64(f.Args[1])
									require.True(t, ok)
									predicates = append(predicates, fmt.Sprintf("%s%g", f.Func.ObjName, bound))
									return
								}
								if f.Func.ObjName == "=" && f.Args[0].GetCol() != nil && f.Args[1].GetLit().GetSval() == "x" {
									predicates = append(predicates, "category=x")
									return
								}
							}
							for _, arg := range f.Args {
								inspect(arg)
							}
						}
						for _, filter := range node.FilterList {
							inspect(filter)
						}
						require.ElementsMatch(t, tc.predicates, predicates, "资格域和回表扫描都必须保留完整谓词，不能仅依靠 DistRange")
					}
					for _, child := range node.Children {
						walk(child)
					}
				}
				for _, root := range q.Steps {
					walk(root)
				}
				require.Equal(t, 2, scans)
			})
		}
	}
}

func TestEquiVectorTopKConservativeBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"force", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=force'`},
		{"post", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=post'`},
		{"auto", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=auto'`},
		{"include", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=include'`},
		{"descending", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') desc limit 2`},
		{"two_sort_keys", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]'),b.label limit 2`},
		{"no_limit", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]')`},
		{"left_join", `select a.id from equi_chunks a left join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2`},
		{"non_equi", `select a.id from equi_chunks a join equi_documents b on a.document_id<b.document_id order by l2_distance(a.v,'[0,0]') limit 2`},
		{"mixed_on", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id and a.id<>b.label order by l2_distance(a.v,'[0,0]') limit 2`},
		{"volatile", `select a.id,rand() from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2`},
		{"null", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,NULL) limit 2`},
		{"found_rows", `select sql_calc_found_rows a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Empty(t, equiVectorSearchNodes(equiVectorPlanFixture(t, tc.sql, false, false)))
		})
	}
}

func newEquiVectorPlanCase(t *testing.T) vectorJoinPlanCase {
	t.Helper()
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	sort := tc.builder.qry.Nodes[tc.projNode.Children[0]]
	tc.builder.qry.Nodes[sort.Children[0]].JoinType = plan.Node_INNER
	scan := tc.builder.qry.Nodes[tc.mainScanNodeID]
	scan.TableDef.Cols[1].Typ.NotNullable = true
	sort.OrderBy[0].Expr.GetF().Args[0].Typ.NotNullable = true
	sort.OrderBy[0].Expr.GetF().Args[1] = makePlan2StringConstExprWithType("[0,0]")
	tc.projNode.ProjectList = []*plan.Expr{newVectorJoinColExpr(scan.BindingTags[0], 0, "id", scan.TableDef.Cols[0].Typ)}
	tc.builder.qry.Steps = []int32{tc.projNodeID}
	return tc
}

func TestEquiVectorTopKPreservesJoinAndPagination(t *testing.T) {
	tc := newEquiVectorPlanCase(t)
	vc := tc.builder.buildVectorEquiJoinContext(tc.projNode)
	require.NotNil(t, vc)
	join, original := vc.join, DeepCopyNode(vc.join)
	sort := tc.builder.qry.Nodes[tc.projNode.Children[0]]
	sort.Limit, sort.Offset = makePlan2Uint64ConstExprWithType(2), makePlan2Uint64ConstExprWithType(3)
	id, applied, err := tc.builder.applyVectorIndexForEquiJoin(tc.projNodeID)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, tc.projNodeID, id)
	require.Equal(t, plan.Node_INNER, join.JoinType)
	require.Equal(t, original.OnList, join.OnList)
	require.Equal(t, original.Children[1-vc.scanSide], join.Children[1-vc.scanSide])
	require.Equal(t, uint64(2), sort.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(3), sort.Offset.GetLit().GetU64Val())
	candidate := tc.builder.qry.Nodes[join.Children[vc.scanSide]]
	require.Equal(t, vc.scan.BindingTags, candidate.BindingTags)
	require.Len(t, candidate.ProjectList, len(vc.scan.TableDef.Cols))
	searches := equiVectorSearchNodes(tc.builder.qry)
	require.Len(t, searches, 1)
	require.Equal(t, uint64(5), searches[0].IndexReaderParam.Limit.GetLit().GetU64Val())
	require.True(t, searches[0].RuntimeFilterProbeList[0].MustApply)
	seen := make(map[int32]bool)
	var walk func(int32)
	walk = func(id int32) {
		require.False(t, seen[id], "资格生产者不能和最终 JOIN 共用可变子树")
		seen[id] = true
		for _, child := range tc.builder.qry.Nodes[id].Children {
			walk(child)
		}
	}
	walk(id)
}

func TestEquiVectorTopKRejectionPreservesOriginal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(vectorJoinPlanCase)
	}{
		{"no_pk", func(v vectorJoinPlanCase) { v.builder.qry.Nodes[v.mainScanNodeID].TableDef.Pkey = nil }},
		{"nullable", func(v vectorJoinPlanCase) {
			v.builder.qry.Nodes[v.mainScanNodeID].TableDef.Cols[1].Typ.NotNullable = false
			v.builder.qry.Nodes[v.projNode.Children[0]].OrderBy[0].Expr.GetF().Args[0].Typ.NotNullable = false
		}},
		{"param", func(v vectorJoinPlanCase) {
			v.builder.qry.Nodes[v.projNode.Children[0]].OrderBy[0].Expr.GetF().Args[1].Expr = &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}
		}},
		{"provider_limit", func(v vectorJoinPlanCase) {
			v.builder.qry.Nodes[v.providerNodeID].Limit = makePlan2Uint64ConstExprWithType(1)
		}},
		{"external", func(v vectorJoinPlanCase) { v.builder.qry.Nodes[v.providerNodeID].TableDef.TableType = "e" }},
		{"runtime_filter", func(v vectorJoinPlanCase) {
			v.builder.qry.Nodes[v.providerNodeID].RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: 42}}
		}},
		{"overflow", func(v vectorJoinPlanCase) {
			s := v.builder.qry.Nodes[v.projNode.Children[0]]
			s.Limit, s.Offset = makePlan2Uint64ConstExprWithType(^uint64(0)), makePlan2Uint64ConstExprWithType(1)
		}},
		{"hnsw_only", func(v vectorJoinPlanCase) {
			v.builder.qry.Nodes[v.mainScanNodeID].TableDef.Indexes = newVectorJoinTableDef(true, true).Indexes
		}},
		{"other_auto", func(v vectorJoinPlanCase) {
			v.builder.qry.Nodes = append(v.builder.qry.Nodes, &plan.Node{NodeId: int32(len(v.builder.qry.Nodes)), RankOption: &plan.RankOption{Mode: "auto"}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newEquiVectorPlanCase(t)
			tc.mutate(v)
			original := make([]string, len(v.builder.qry.Nodes))
			for i, n := range v.builder.qry.Nodes {
				original[i] = n.String()
			}
			bindings := make(map[int32]int32)
			for k, value := range v.builder.tag2NodeID {
				bindings[k] = value
			}
			require.Empty(t, v.builder.detectEquiVectorJoinGuard(v.projNode))
			id, applied, err := v.builder.applyVectorIndexForEquiJoin(v.projNodeID)
			require.NoError(t, err)
			require.False(t, applied)
			require.Equal(t, v.projNodeID, id)
			for i, before := range original {
				require.Equal(t, before, v.builder.qry.Nodes[i].String())
			}
			for k, before := range bindings {
				require.Equal(t, before, v.builder.tag2NodeID[k])
			}
		})
	}
}

func TestEquiVectorTopKDefaultAutoAndMetadata(t *testing.T) {
	sql := `select a.id,b.label,l2_distance(a.v,'[0,0]') as distance from equi_chunks a
		join equi_documents b on a.document_id=b.document_id order by distance limit 2`
	q := equiVectorPlanFixture(t, sql, false, true)
	require.Len(t, equiVectorSearchNodes(q), 1)
	for _, node := range q.Nodes {
		require.NotEqual(t, "auto", node.GetRankOption().GetMode())
	}
	columns := func(q *plan.Query) []*plan.ColDef {
		return GetResultColumnsFromPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: q}})
	}
	require.Equal(t, columns(equiVectorPlanFixture(t, sql+" by rank with option 'mode=force'", false, true)), columns(q))
	prepared := equiVectorPlanFixture(t, `prepare p from 'select a.id,b.label from equi_chunks a join equi_documents b
		on a.document_id=b.document_id order by l2_distance(a.v,"[0,0]") limit ?'`, false, false)
	require.Len(t, equiVectorSearchNodes(prepared), 1)
	for _, limit := range []uint64{0, 1, 4, 0} {
		filled, err := FillValuesOfParamsInPlan(context.Background(), &plan.Plan{Plan: &plan.Plan_Query{Query: prepared}}, []any{limit})
		require.NoError(t, err)
		cols := GetResultColumnsFromPlan(filled)
		require.Len(t, cols, 2)
		require.Equal(t, int32(types.T_varchar), cols[0].Typ.Id)
		require.Equal(t, int32(types.T_varchar), cols[1].Typ.Id)
	}
}
