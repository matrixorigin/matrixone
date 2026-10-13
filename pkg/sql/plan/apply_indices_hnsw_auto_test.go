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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func hnswAutoPlanFixture(t *testing.T, sql string) *plan.Query {
	t.Helper()
	ctx := newVectorJoinMockCtx(t)
	table := newVectorJoinTableDef(true, true)
	table.Name, table.TblId = "hnsw_auto", 901
	table.Cols[0].Typ = plan.Type{Id: int32(types.T_int64), NotNullable: true}
	table.Cols[1].Typ.Width = 2
	table.Cols = append(table.Cols, &plan.ColDef{Name: "category", Typ: plan.Type{Id: int32(types.T_int32)}})
	table.Name2ColIndex["category"] = 2
	for i, def := range table.Indexes {
		def.TableExist = true
		ctx.tables[def.IndexTableName] = &plan.TableDef{Name: def.IndexTableName, TblId: uint64(910 + i)}
		ctx.objects[def.IndexTableName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: def.IndexTableName}
	}
	ctx.tables[table.Name] = table
	ctx.objects[table.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()
	p, err := BuildPlan(ctx, statements[0], false)
	require.NoError(t, err)
	return p.GetQuery()
}

func findNodeOfType(query *plan.Query, typ plan.Node_NodeType) *plan.Node {
	for _, node := range query.Nodes {
		if node.NodeType == typ {
			return node
		}
	}
	return nil
}

func countHnswSearches(query *plan.Query, root int32) int {
	node := query.Nodes[root]
	n := 0
	if node.NodeType == plan.Node_INDEX_SEARCH_SCAN {
		n++
	}
	for _, child := range node.Children {
		n += countHnswSearches(query, child)
	}
	return n
}

func TestHnswAutoBuildsAdaptiveTop(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		branches int
		searches []int
	}{
		{"filter", `select id from hnsw_auto where category = 1
			order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=auto'`, 3, []int{1, 1, 0}},
		{"no_filter", `select id from hnsw_auto
			order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=auto'`, 2, []int{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := hnswAutoPlanFixture(t, tc.sql)
			adaptive := findNodeOfType(query, plan.Node_ADAPTIVE_TOP)
			require.NotNil(t, adaptive)
			require.Len(t, adaptive.Children, tc.branches)
			require.False(t, adaptive.AdaptiveTopFallbackOnEmpty)
			for i, child := range adaptive.Children {
				require.Equal(t, tc.searches[i], countHnswSearches(query, child), "branch %d", i)
			}
			if tc.branches == 3 {
				require.NotNil(t, findReachableSemiJoin(query, adaptive.Children[1]), "pre branch")
				require.Nil(t, findReachableSemiJoin(query, adaptive.Children[0]), "post branch")
			}
		})
	}
}

func findReachableSemiJoin(query *plan.Query, root int32) *plan.Node {
	node := query.Nodes[root]
	if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_SEMI {
		return node
	}
	for _, child := range node.Children {
		if found := findReachableSemiJoin(query, child); found != nil {
			return found
		}
	}
	return nil
}

func TestIndexSearchAlias(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	ctx := NewBindContext(builder, nil)
	require.Equal(t, "mo_hnsw_alias_0", indexSearchAlias(ctx, "mo_hnsw_alias"))
	ctx.bindingByTable["mo_hnsw_alias_0"] = &Binding{}
	require.Equal(t, "mo_hnsw_alias_1", indexSearchAlias(ctx, "mo_hnsw_alias"))
}
