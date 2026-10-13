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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func scalarVectorPlanFixture(t *testing.T, sql string, hnsw bool, configure ...func(*plan.TableDef)) *plan.Query {
	t.Helper()
	ctx := newVectorJoinMockCtx()
	table := newVectorJoinTableDef(false, true)
	table.Name, table.TblId = "scalar_items", 901
	table.Cols[0].Typ = plan.Type{Id: int32(types.T_varchar), Width: 64, NotNullable: true}
	table.Cols[1].Typ.Width = 3
	index := newVectorJoinIvfIndex()
	if hnsw {
		table.Cols[0].Typ = plan.Type{Id: int32(types.T_int64), NotNullable: true}
		index = newVectorJoinHnswIndex()
	}
	for _, def := range index.IndexDefs {
		def.TableExist = true
		if !hnsw {
			def.IndexAlgoParams = `{"op_type":"vector_l2_ops","lists":"1"}`
		}
		table.Indexes = append(table.Indexes, def)
		ctx.tables[def.IndexTableName] = &plan.TableDef{Name: def.IndexTableName, TblId: uint64(910 + len(table.Indexes))}
		ctx.objects[def.IndexTableName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: def.IndexTableName}
	}
	provider := DeepCopyTableDef(table, true)
	provider.Name, provider.TblId, provider.Indexes = "scalar_provider", 902, nil
	provider.Cols[1].Typ.NotNullable = false
	for _, configureTable := range configure {
		configureTable(table)
	}
	for _, tab := range []*plan.TableDef{table, provider} {
		ctx.tables[tab.Name] = tab
		ctx.objects[tab.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: tab.Name, Obj: int64(tab.TblId)}
	}
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	p, err := BuildPlan(&scalarVectorSnapshotContext{ctx}, stmts[0], false)
	require.NoError(t, err)
	if prepare := p.GetDcl().GetPrepare(); prepare != nil {
		return prepare.Plan.GetQuery()
	}
	return p.GetQuery()
}

type scalarVectorSnapshotContext struct{ *customMockCompilerContext }

func (c *scalarVectorSnapshotContext) ResolveSnapshotWithSnapshotName(string) (*plan.Snapshot, error) {
	return &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}}, nil
}

func TestScalarVectorSnapshotAdmission(t *testing.T) {
	for _, hnsw := range []bool{false, true} {
		for _, tc := range []struct {
			name, items, providerA, providerB string
			selectors, snapshots              int
		}{
			{name: "current", selectors: 2},
			{name: "two_snapshot_providers", providerA: "{snapshot='s'}", providerB: "{snapshot='s'}", snapshots: 2},
			{name: "mixed", providerB: "{snapshot='s'}", selectors: 1, snapshots: 1},
			{name: "snapshot_candidates", items: "{snapshot='s'}", snapshots: 2},
		} {
			t.Run(fmt.Sprintf("hnsw=%t/%s", hnsw, tc.name), func(t *testing.T) {
				key := "'ref'"
				if hnsw {
					key = "1"
				}
				q := scalarVectorPlanFixture(t, fmt.Sprintf(`with a as (select concat(id,'') as id from scalar_items %s order by l2_distance(v,(select v from scalar_provider %s where id=%s)) limit 2),
 b as (select concat(id,'') as id from scalar_items %s order by l2_distance(v,(select v from scalar_provider %s where id=%s)) limit 2)
 select a.id,b.id from a join b on a.id=b.id order by a.id`, tc.items, tc.providerA, key, tc.items, tc.providerB, key), hnsw)
				require.Len(t, scalarVectorSelectors(q), tc.selectors)
				seen, snapshots := make(map[int32]bool), 0
				var walk func(int32)
				walk = func(id int32) {
					if seen[id] {
						return
					}
					seen[id] = true
					n := q.Nodes[id]
					if n.ScanSnapshot != nil {
						snapshots++
					}
					for _, child := range n.Children {
						walk(child)
					}
				}
				for _, root := range q.Steps {
					walk(root)
				}
				require.Equal(t, tc.snapshots, snapshots, "snapshot semantics must survive fallback")
			})
		}
	}
}

func scalarVectorSelectors(q *plan.Query) []*plan.Node {
	var out []*plan.Node
	var walk func(int32)
	walk = func(id int32) {
		n := q.Nodes[id]
		if n.NodeType == plan.Node_VECTOR_QUERY_TOP {
			out = append(out, n)
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	for _, root := range q.Steps {
		walk(root)
	}
	return out
}

func TestScalarVectorPublicPlan(t *testing.T) {
	for _, hnsw := range []bool{false, true} {
		key := "'ref'"
		if hnsw {
			key = "1"
		}
		for _, pagination := range []string{"limit 2", "limit 0", "limit 2 offset 1", "limit 2 by rank with option 'mode=pre'"} {
			t.Run(fmt.Sprintf("hnsw=%t/%s", hnsw, pagination), func(t *testing.T) {
				q := scalarVectorPlanFixture(t, `select id from scalar_items order by l2_distance(v,(select v from scalar_provider where id=`+key+`)) `+pagination, hnsw)
				selectors := scalarVectorSelectors(q)
				require.Len(t, selectors, 1)
				n := selectors[0]
				require.Len(t, n.Children, 3)
				require.Equal(t, int32(1), n.ProjectList[0].GetCol().RelPos)
				require.NotNil(t, n.Limit)
				require.Equal(t, n.VectorQuerySourceId, scalarVectorSelectors(DeepCopyQuery(q))[0].VectorQuerySourceId)
				wire, err := q.Marshal()
				require.NoError(t, err)
				var decoded plan.Query
				require.NoError(t, decoded.Unmarshal(wire))
				require.Equal(t, n.VectorQuerySourceId, scalarVectorSelectors(&decoded)[0].VectorQuerySourceId)
				sources, searches := 0, 0
				var walk func(int32)
				walk = func(id int32) {
					node := q.Nodes[id]
					if node.NodeType == plan.Node_VECTOR_QUERY_SOURCE {
						sources++
						require.Equal(t, n.VectorQuerySourceId, node.VectorQuerySourceId)
					}
					if node.NodeType == plan.Node_FUNCTION_SCAN {
						searches++
						require.Len(t, node.Children, 1)
						for _, expr := range node.ProjectList {
							require.Equal(t, node.TableDef.Cols[expr.GetCol().ColPos].Typ, expr.Typ)
						}
					}
					for _, child := range node.Children {
						walk(child)
					}
				}
				walk(n.NodeId)
				require.Equal(t, 2, sources)
				require.Equal(t, 1, searches)
				cols := GetResultColumnsFromPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: q}})
				require.Len(t, cols, 1)
				require.Equal(t, "id", cols[0].Name)
				require.Equal(t, "scalar_items", cols[0].TblName)
				require.Equal(t, "tpch", cols[0].DbName)
			})
		}
	}
}

func TestScalarVectorPreparedAndNested(t *testing.T) {
	for _, hnsw := range []bool{false, true} {
		t.Run(fmt.Sprintf("hnsw=%t", hnsw), func(t *testing.T) {
			q := scalarVectorPlanFixture(t, `prepare p from 'select t.id from (select id from scalar_items order by l2_distance(v,(select v from scalar_provider where id=?)) limit 2) t limit ? offset ?'`, hnsw)
			require.Len(t, scalarVectorSelectors(q), 1)
			p := &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
			var key any = "ref"
			if hnsw {
				key = int64(1)
			}
			for _, k := range []uint64{0, 1, 5, 0} {
				filled, err := FillValuesOfParamsInPlan(context.Background(), p, []any{key, k, uint64(1)})
				require.NoError(t, err)
				query := filled.GetQuery()
				root := query.Nodes[query.Steps[0]]
				require.Equal(t, plan.Node_PROJECT, root.NodeType)
				require.NotNil(t, root.Limit)
				require.NotNil(t, root.Offset)
				require.Equal(t, uint64(2), scalarVectorSelectors(query)[0].Limit.GetLit().GetU64Val())
				require.Len(t, GetResultColumnsFromPlan(filled), 1)
			}
		})
	}
}

func TestScalarVectorCandidateNullability(t *testing.T) {
	for _, hnsw := range []bool{false, true} {
		for _, tc := range []struct {
			name, predicate, pagination string
			notNull, reversed, prepared bool
		}{
			{name: "nullable", pagination: "limit 1"},
			{name: "not_null", notNull: true, pagination: "limit 1"},
			{name: "nullable_reversed", reversed: true, pagination: "limit 1"},
			{name: "not_null_reversed", notNull: true, reversed: true, pagination: "limit 1"},
			{name: "nullable_offset", pagination: "limit 1 offset 1"},
			{name: "nullable_pre", pagination: "limit 2 by rank with option 'mode=pre'"},
			{name: "nullable_prepared", prepared: true, pagination: "limit ?"},
			{name: "nullable_filter", predicate: "where v is not null", pagination: "limit 1"},
		} {
			t.Run(fmt.Sprintf("hnsw=%t/%s", hnsw, tc.name), func(t *testing.T) {
				key := "'ref'"
				if hnsw {
					key = "1"
				}
				if tc.prepared {
					key = "?"
				}
				left, right := "v", "(select v from scalar_provider where id="+key+")"
				if tc.reversed {
					left, right = right, left
				}
				sql := fmt.Sprintf("select id from scalar_items %s order by l2_distance(%s,%s) %s", tc.predicate, left, right, tc.pagination)
				if tc.prepared {
					sql = "prepare p from '" + sql + "'"
				}
				q := scalarVectorPlanFixture(t, sql, hnsw, func(table *plan.TableDef) {
					// 实际索引列放在位置 2；无关向量列的 nullable 恰好相反。
					// 不能假定 v 总在位置 1，也不能拿 provider/其它列的非空性替代。
					v := table.Cols[1]
					v.Typ.NotNullable = tc.notNull
					other := &plan.ColDef{Name: "other", Typ: v.Typ}
					other.Typ.NotNullable = !tc.notNull
					table.Cols = []*plan.ColDef{table.Cols[0], other, v}
					table.Name2ColIndex = map[string]int32{"id": 0, "other": 1, "v": 2}
				})
				if tc.notNull {
					require.Len(t, scalarVectorSelectors(q), 1)
				} else {
					require.Empty(t, scalarVectorSelectors(q))
				}
			})
		}
	}
}

func TestScalarVectorConservativeBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"multirow", `select id from scalar_items order by l2_distance(v,(select v from scalar_provider where id>'a')) limit 2`},
		{"correlated", `select t.id from scalar_items t order by l2_distance(t.v,(select v from scalar_provider where id=t.id)) limit 2`},
		{"two_keys", `select id from scalar_items order by l2_distance(v,(select v from scalar_provider where id='ref')),id limit 2`},
		{"found_rows", `select sql_calc_found_rows id from scalar_items order by l2_distance(v,(select v from scalar_provider where id='ref')) limit 2`},
		{"descending", `select id from scalar_items order by l2_distance(v,(select v from scalar_provider where id='ref')) desc limit 2`},
		{"no_limit", `select id from scalar_items order by l2_distance(v,(select v from scalar_provider where id='ref'))`},
		{"volatile", `select id,rand() from scalar_items order by l2_distance(v,(select v from scalar_provider where id='ref')) limit 2`},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Empty(t, scalarVectorSelectors(scalarVectorPlanFixture(t, tc.sql, false))) })
	}
	for _, mode := range []string{"force", "post", "auto", "include"} {
		t.Run(mode, func(t *testing.T) {
			q := scalarVectorPlanFixture(t, `select id from scalar_items order by l2_distance(v,(select v from scalar_provider where id='ref')) limit 2 by rank with option 'mode=`+mode+`'`, false)
			require.Empty(t, scalarVectorSelectors(q))
		})
	}
}

func TestScalarVectorGuardDoesNotMutateProvider(t *testing.T) {
	v := newVectorJoinPlanCase(t, vectorJoinPlanOptions{joinType: plan.Node_SINGLE, providerSingle: true})
	scan := v.builder.qry.Nodes[v.mainScanNodeID]
	scan.TableDef.Indexes = nil
	for _, def := range newVectorJoinIvfIndex().IndexDefs {
		scan.TableDef.Indexes = append(scan.TableDef.Indexes, def)
	}
	scan.TableDef.Cols[1].Typ.NotNullable = true
	v.projNode.ProjectList = []*plan.Expr{newVectorJoinColExpr(scan.BindingTags[0], 0, "id", scan.TableDef.Cols[0].Typ)}
	sort := v.builder.qry.Nodes[v.projNode.Children[0]]
	sort.OrderBy[0].Expr.GetF().Args[0].Typ.NotNullable = true
	for _, notNull := range []bool{true, false} {
		// 表达式仍标注 NOT NULL，真实 schema 列可以为空时也必须拒绝。
		scan.TableDef.Cols[1].Typ.NotNullable = notNull
		before := v.builder.qry.String()
		if notNull {
			require.NotEmpty(t, v.builder.detectScalarVectorGuard(v.projNode))
		} else {
			require.Empty(t, v.builder.detectScalarVectorGuard(v.projNode))
		}
		require.Equal(t, before, v.builder.qry.String())
	}
}

func TestScalarVectorJoinConsumers(t *testing.T) {
	for _, hnsw := range []bool{false, true} {
		for _, filtered := range []bool{false, true} {
			t.Run(fmt.Sprintf("hnsw=%t/filtered=%t", hnsw, filtered), func(t *testing.T) {
				keyA, keyB, filterA, filterB := "'ref'", "'ref'", "", ""
				if hnsw {
					keyA, keyB = "1", "2"
				}
				if filtered {
					filterA, filterB, keyB = "where id>'a'", "where id<'c'", "'missing'"
					if hnsw {
						filterA, filterB, keyB = "where id>1", "where id<3", "2"
					}
				}
				q := scalarVectorPlanFixture(t, fmt.Sprintf(`with a as (select concat(id,'') as id from scalar_items %s order by l2_distance(v,(select v from scalar_provider where id=%s)) limit 2),
 b as (select concat(id,'') as id from scalar_items %s order by l2_distance(v,(select v from scalar_provider where id=%s)) limit 2)
 select a.id,b.id from a join b on a.id=b.id order by a.id`, filterA, keyA, filterB, keyB), hnsw)
				selectors := scalarVectorSelectors(q)
				require.Len(t, selectors, 2)
				require.NotEqual(t, selectors[0].VectorQuerySourceId, selectors[1].VectorQuerySourceId)
			})
		}
	}
}
