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

func equiVectorPlanFixture(t *testing.T, sql string, unique bool) *plan.Query {
	t.Helper()
	ctx := newVectorJoinMockCtx(t)
	a := newVectorJoinTableDef(false, true)
	a.Name, a.TblId = "equi_chunks", 801
	a.Cols[0].Typ = plan.Type{Id: int32(types.T_varchar), Width: 64, NotNullable: true}
	a.Cols[1].Typ = plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true}
	a.Cols = append(a.Cols, &plan.ColDef{Name: "document_id", Typ: a.Cols[0].Typ})
	a.Name2ColIndex["document_id"] = 2
	for _, def := range newVectorJoinIvfIndex().IndexDefs {
		def.TableExist = true
		def.IndexAlgoParams = `{"op_type":"vector_l2_ops","lists":"1"}`
		a.Indexes = append(a.Indexes, def)
		ctx.tables[def.IndexTableName] = &plan.TableDef{Name: def.IndexTableName, TblId: uint64(810 + len(a.Indexes))}
		ctx.objects[def.IndexTableName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: def.IndexTableName}
	}
	b := &plan.TableDef{
		Name: "equi_documents", TblId: 802,
		Cols: []*plan.ColDef{
			{Name: "document_id", Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}},
			{Name: "label", Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}},
		},
		Name2ColIndex: map[string]int32{"document_id": 0, "label": 1},
	}
	if unique {
		b.Pkey = &plan.PrimaryKeyDef{PkeyColName: "document_id", Names: []string{"document_id"}}
		b.Cols[0].Typ.NotNullable = true
	}
	for _, table := range []*plan.TableDef{a, b} {
		ctx.tables[table.Name] = table
		ctx.objects[table.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	}
	statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()
	p, err := BuildPlan(ctx, statements[0], false)
	require.NoError(t, err)
	if prepare := p.GetDcl().GetPrepare(); prepare != nil {
		return prepare.Plan.GetQuery()
	}
	return p.GetQuery()
}

func TestEquiVectorTopKPublicPlan(t *testing.T) {
	for _, unique := range []bool{false, true} {
		for _, tc := range []struct{ name, sql string }{
			{"plain", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				where l2_distance(a.v,'[0,0]')<=0.5 order by l2_distance(a.v,'[0,0]') limit 2`},
			{"pre", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=pre'`},
			{"indexed_right", `select a.id,b.label from equi_documents b join equi_chunks a on b.document_id=a.document_id
				order by l2_distance(a.v,'[0,0]') limit 2`},
			{"distance_alias", `select a.id,b.label,l2_distance(a.v,'[0,0]') as distance
				from equi_chunks a join equi_documents b on a.document_id=b.document_id order by distance limit 2`},
			{"right_filter", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2`},
			{"two_equi_keys", `select a.id from equi_chunks a join equi_documents b
				on a.document_id=b.document_id and a.id=b.label order by l2_distance(a.v,'[0,0]') limit 2`},
			{"offset", `select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id
				order by l2_distance(a.v,'[0,0]') limit 2 offset 1`},
		} {
			t.Run(fmt.Sprintf("unique=%t/%s", unique, tc.name), func(t *testing.T) {
				q := equiVectorPlanFixture(t, tc.sql, unique)
				require.True(t, scalarVectorPlanHasIndex(q), "普通等值 JOIN 的公开 SQL 应能使用向量索引")
				_, _, _, distributed := RequiredIVFPlacement(q)
				require.False(t, distributed, "新增复合形态不能误用原有标量 PRE 的分布式资格证明")
				cols := GetResultColumnsFromPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: q}})
				require.Equal(t, "id", cols[0].OriginName)
				require.Equal(t, int32(types.T_varchar), cols[0].Typ.Id)
			})
		}
	}
}

func TestEquiVectorTopKConservativeBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"force", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=force'`},
		{"post", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=post'`},
		{"auto", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=auto'`},
		{"descending", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]') desc limit 2`},
		{"two_order_keys", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]'),b.label limit 2`},
		{"no_limit", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]')`},
		{"left_join", `select a.id from equi_chunks a left join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]') limit 2`},
		{"non_equi", `select a.id from equi_chunks a join equi_documents b on a.document_id<b.document_id
			order by l2_distance(a.v,'[0,0]') limit 2`},
		{"mixed_predicate", `select a.id from equi_chunks a join equi_documents b
			on a.document_id=b.document_id and a.id<>b.label order by l2_distance(a.v,'[0,0]') limit 2`},
		{"volatile", `select a.id,rand() from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]') limit 2`},
		{"null_query", `select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,NULL) limit 2`},
		{"found_rows", `select sql_calc_found_rows a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id
			order by l2_distance(a.v,'[0,0]') limit 2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := equiVectorPlanFixture(t, tc.sql, false)
			require.False(t, scalarVectorPlanHasIndex(q), "资格证明不完整时必须保留原关系路径")
		})
	}
}

func newEquiVectorPlanCase(t *testing.T) vectorJoinPlanCase {
	t.Helper()
	tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SEMI})
	join := tc.builder.qry.Nodes[tc.builder.qry.Nodes[tc.projNode.Children[0]].Children[0]]
	join.JoinType = plan.Node_INNER
	scan := tc.builder.qry.Nodes[tc.mainScanNodeID]
	scan.TableDef.Cols[1].Typ.NotNullable = true
	sort := tc.builder.qry.Nodes[tc.projNode.Children[0]]
	sort.OrderBy[0].Expr.GetF().Args[0].Typ.NotNullable = true
	sort.OrderBy[0].Expr.GetF().Args[1] = makePlan2StringConstExprWithType("[0,0]")
	tc.projNode.ProjectList = []*plan.Expr{
		newVectorJoinColExpr(scan.BindingTags[0], 0, "id", scan.TableDef.Cols[0].Typ),
	}
	tc.builder.tag2NodeID[scan.BindingTags[0]] = scan.NodeId
	other := tc.builder.qry.Nodes[tc.providerNodeID]
	tc.builder.tag2NodeID[other.BindingTags[0]] = other.NodeId
	return tc
}

func TestEquiVectorTopKPreservesJoinAndPagination(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprintf("early=%t", early), func(t *testing.T) {
			tc := newEquiVectorPlanCase(t)
			vc := tc.builder.buildVectorEquiJoinContext(tc.projNode)
			require.NotNil(t, vc)
			join := vc.join
			on := DeepCopyExprList(join.OnList)
			oldA := vc.scan.BindingTags[0]
			otherID := join.Children[1-vc.scanSide]
			sort := tc.builder.qry.Nodes[tc.projNode.Children[0]]
			sort.Limit, sort.Offset = makePlan2Uint64ConstExprWithType(2), makePlan2Uint64ConstExprWithType(3)
			sort.RankOption = &plan.RankOption{Mode: "pre"}
			id, applied, err := tc.builder.applyVectorIndexForEquiJoin(tc.projNodeID, early)
			require.NoError(t, err)
			require.True(t, applied)
			require.Equal(t, tc.projNodeID, id)
			require.Equal(t, plan.Node_INNER, join.JoinType)
			require.Equal(t, on, join.OnList)
			require.Equal(t, otherID, join.Children[1-vc.scanSide])
			require.Equal(t, otherID, tc.builder.tag2NodeID[vc.other.BindingTags[0]])
			require.Equal(t, uint64(2), sort.Limit.GetLit().GetU64Val())
			require.Equal(t, uint64(3), sort.Offset.GetLit().GetU64Val())
			candidate := tc.builder.qry.Nodes[join.Children[vc.scanSide]]
			require.Equal(t, []int32{oldA}, candidate.BindingTags)
			require.Len(t, candidate.ProjectList, len(vc.scan.TableDef.Cols))
			innerSort := tc.builder.qry.Nodes[candidate.Children[0]]
			require.Equal(t, plan.Node_SORT, innerSort.NodeType)
			require.Equal(t, uint64(5), innerSort.Limit.GetLit().GetU64Val())
			require.Nil(t, innerSort.Offset)
			vector := findFirstNodeByType(tc.builder, plan.Node_VECTOR_INDEX_SCAN)
			require.NotNil(t, vector)
			require.Equal(t, uint64(5), vector.VectorIndexScan.CandidateLimit.GetLit().GetU64Val())
			require.True(t, vector.Stats.ForceOneCN)
			require.Len(t, vector.RuntimeFilterProbeList, 1)
			require.True(t, vector.RuntimeFilterProbeList[0].MustApply)
			// 完整可达图必须是树，不能复用 B 节点或重新执行带旧消息的节点。
			seen := make(map[int32]bool)
			var walk func(int32)
			walk = func(id int32) {
				require.False(t, seen[id], "membership 与保留 JOIN 不能共享可变子树")
				seen[id] = true
				n := tc.builder.qry.Nodes[id]
				for _, child := range n.Children {
					walk(child)
				}
			}
			walk(id)
		})
	}
}

func TestEquiVectorTopKEligibilityRejectsWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(vectorJoinPlanCase)
	}{
		{"no_pk", func(tc vectorJoinPlanCase) { tc.builder.qry.Nodes[tc.mainScanNodeID].TableDef.Pkey = nil }},
		{"nullable_vector", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.mainScanNodeID].TableDef.Cols[1].Typ.NotNullable = false
			tc.builder.qry.Nodes[tc.projNode.Children[0]].OrderBy[0].Expr.GetF().Args[0].Typ.NotNullable = false
		}},
		{"unbound_vector", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.projNode.Children[0]].OrderBy[0].Expr.GetF().Args[1].Expr = &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}
		}},
		{"null_vector", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.projNode.Children[0]].OrderBy[0].Expr.GetF().Args[1].Expr = &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}
		}},
		{"hnsw", func(tc vectorJoinPlanCase) {
			for _, def := range tc.builder.qry.Nodes[tc.mainScanNodeID].TableDef.Indexes {
				def.IndexAlgo = "hnsw"
			}
		}},
		{"other_limit", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.providerNodeID].Limit = makePlan2Uint64ConstExprWithType(1)
		}},
		{"other_table_kind", func(tc vectorJoinPlanCase) { tc.builder.qry.Nodes[tc.providerNodeID].TableDef.TableType = "e" }},
		{"partition", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.mainScanNodeID].TableDef.Partition = &plan.Partition{}
		}},
		{"runtime_filter", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.providerNodeID].RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: 42}}
		}},
		{"overflow", func(tc vectorJoinPlanCase) {
			sort := tc.builder.qry.Nodes[tc.projNode.Children[0]]
			sort.Limit, sort.Offset = makePlan2Uint64ConstExprWithType(^uint64(0)), makePlan2Uint64ConstExprWithType(1)
		}},
		{"unbound_offset", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.projNode.Children[0]].Offset = &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
		}},
		{"join_filter", func(tc vectorJoinPlanCase) {
			tc.builder.buildVectorEquiJoinContext(tc.projNode).join.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{"join_limit", func(tc vectorJoinPlanCase) {
			tc.builder.buildVectorEquiJoinContext(tc.projNode).join.Limit = makePlan2Uint64ConstExprWithType(1)
		}},
		{"bad_equality_type", func(tc vectorJoinPlanCase) {
			tc.builder.buildVectorEquiJoinContext(tc.projNode).join.OnList[0].GetF().Args[1].Typ.Id = int32(types.T_int32)
		}},
		{"non_distance", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.projNode.Children[0]].OrderBy[0].Expr = makePlan2Int64ConstExprWithType(1)
		}},
		{"non_column_distance", func(tc vectorJoinPlanCase) {
			tc.builder.qry.Nodes[tc.projNode.Children[0]].OrderBy[0].Expr.GetF().Args[0] = makePlan2StringConstExprWithType("[1,1]")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newEquiVectorPlanCase(t)
			tc.mutate(v)
			root := DeepCopyNode(v.projNode)
			sort := DeepCopyNode(v.builder.qry.Nodes[v.projNode.Children[0]])
			join := DeepCopyNode(v.builder.qry.Nodes[sort.Children[0]])
			bindings := make(map[int32]int32)
			for tag, node := range v.builder.tag2NodeID {
				bindings[tag] = node
			}
			require.Empty(t, v.builder.detectEquiVectorJoinGuard(v.projNode))
			id, applied, err := v.builder.applyVectorIndexForEquiJoin(v.projNodeID, true)
			require.NoError(t, err)
			require.False(t, applied)
			require.Equal(t, v.projNodeID, id)
			require.Equal(t, root.String(), v.projNode.String())
			require.Equal(t, sort.String(), v.builder.qry.Nodes[sort.NodeId].String())
			require.Equal(t, join.String(), v.builder.qry.Nodes[join.NodeId].String())
			for tag, node := range bindings {
				require.Equal(t, node, v.builder.tag2NodeID[tag], "插件拒绝不能覆盖原 binding 注册")
			}
		})
	}
}

func TestEquiVectorTopKMetadataMatchesForce(t *testing.T) {
	for _, sql := range []string{
		`select a.id,b.label from equi_chunks a join equi_documents b
			on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 2`,
		`select a.id,b.label,l2_distance(a.v,'[0,0]') as distance from equi_chunks a join equi_documents b
			on a.document_id=b.document_id order by distance limit 2`,
	} {
		indexed := equiVectorPlanFixture(t, sql, false)
		force := equiVectorPlanFixture(t, sql+" by rank with option 'mode=force'", false)
		got := GetResultColumnsFromPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: indexed}})
		want := GetResultColumnsFromPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: force}})
		require.Equal(t, want, got)
		require.True(t, got[0].Primary)
		require.False(t, got[1].Typ.NotNullable)
		if len(got) == 3 {
			require.Empty(t, got[2].OriginName)
			require.False(t, got[2].Primary)
		}
	}
}

func TestEquiVectorTopKPreparedLimit(t *testing.T) {
	q := equiVectorPlanFixture(t, `prepare p from 'select a.id,b.label from equi_chunks a
		join equi_documents b on a.document_id=b.document_id
		order by l2_distance(a.v,"[0,0]") limit ?'`, false)
	require.True(t, scalarVectorPlanHasIndex(q))
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
	for _, limit := range []uint64{0, 1, 4, 0} {
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), p, []any{limit})
		require.NoError(t, err)
		cols := GetResultColumnsFromPlan(filled)
		require.Len(t, cols, 2)
		require.Equal(t, int32(types.T_varchar), cols[0].Typ.Id)
		require.Equal(t, int32(types.T_varchar), cols[1].Typ.Id)
	}
}
