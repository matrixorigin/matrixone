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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	cagraplan "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/plan"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
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

// findNodeUnder returns the first node of type typ in the subtree of root.
func findNodeUnder(query *plan.Query, root int32, typ plan.Node_NodeType) *plan.Node {
	node := query.Nodes[root]
	if node.NodeType == typ {
		return node
	}
	for _, child := range node.Children {
		if found := findNodeUnder(query, child, typ); found != nil {
			return found
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

func TestHnswIterativeWidens(t *testing.T) {
	for _, tc := range []struct {
		mode      string
		semi      bool
		iterative bool
	}{
		{"pre", true, false},
		{"iterative", true, true},
		{"post", false, false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			query := hnswAutoPlanFixture(t, `select id from hnsw_auto where category = 1
				order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=`+tc.mode+`'`)
			search := findNodeOfType(query, plan.Node_INDEX_SEARCH_SCAN)
			require.NotNil(t, search)
			opts, err := hnswplan.DecodeScanOptions(search.IndexSearchScan.AlgoOptions)
			require.NoError(t, err)
			require.Equal(t, tc.iterative, opts.Iterative)
			require.Equal(t, tc.semi, findNodeOfType(query, plan.Node_JOIN) != nil && findSemiJoin(query) != nil)
		})
	}
}

func findSemiJoin(query *plan.Query) *plan.Node {
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_SEMI {
			return node
		}
	}
	return nil
}

func cagraPlanFixture(t *testing.T, sql string) *plan.Query {
	t.Helper()
	return cuvsPlanFixture(t, catalog.MoIndexCagraAlgo.ToString(),
		[]string{catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage}, sql)
}

// cuvsPlanFixture plans sql over table cagra_iter with an index of algo whose hidden
// tables have roles.
func cuvsPlanFixture(t *testing.T, algo string, roles []string, sql string) *plan.Query {
	t.Helper()
	ctx := newVectorJoinMockCtx(t)
	table := newVectorJoinTableDef(false, true)
	table.Name, table.TblId = "cagra_iter", 971
	table.Cols[0].Typ = plan.Type{Id: int32(types.T_int64), NotNullable: true}
	table.Cols[1].Typ.Width = 2
	table.Cols = append(table.Cols,
		&plan.ColDef{Name: "category", Typ: plan.Type{Id: int32(types.T_int32)}},
		&plan.ColDef{Name: "note", Typ: plan.Type{Id: int32(types.T_int32)}})
	table.Name2ColIndex["category"] = 2
	table.Name2ColIndex["note"] = 3
	params := `{"op_type":"vector_l2_ops","included_columns":"category"}`
	for i, role := range roles {
		def := &plan.IndexDef{
			IndexName: "idx_cagra_v", IndexAlgo: algo, IndexAlgoTableType: role,
			IndexTableName: "cagra_" + role, Parts: []string{"v"}, IndexAlgoParams: params, TableExist: true,
		}
		table.Indexes = append(table.Indexes, def)
		ctx.tables[def.IndexTableName] = &plan.TableDef{Name: def.IndexTableName, TblId: uint64(980 + i)}
		ctx.objects[def.IndexTableName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: def.IndexTableName}
	}
	ctx.tables[table.Name] = table
	ctx.objects[table.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	defer statements[0].Free()
	p, err := BuildPlan(ctx, statements[0], false)
	require.NoError(t, err)
	return p.GetQuery()
}

func TestCagraFilterPushdownByMode(t *testing.T) {
	for _, tc := range []struct {
		name, filter, mode     string
		iterative, pushed, pre bool
	}{
		{"covered, iterative", "category = 1", "iterative", true, true, false},
		{"covered, pre", "category = 1", "pre", false, true, false},
		{"covered, no mode", "category = 1", "", false, true, false},
		{"covered, post", "category = 1", "post", false, false, false},
		{"primary key, post", "id < 5", "post", false, true, false},
		{"residual, iterative", "category = 1 and note = 2", "iterative", true, true, true},
		{"residual only, pre", "note = 2", "pre", false, false, true},
		{"residual, no mode", "category = 1 and note = 2", "", false, true, false},
		{"residual, post", "note = 2", "post", false, false, false},
		{"no filter, iterative", "", "iterative", true, false, false},
		{"distance only, pre", "l2_distance(v,'[0,0]') < 1", "pre", false, false, false},
		{"distance only, iterative", "l2_distance(v,'[0,0]') < 1", "iterative", false, false, false},
		{"distance and residual, pre", "note = 2 and l2_distance(v,'[0,0]') < 1", "pre", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			where := ""
			if tc.filter != "" {
				where = " where " + tc.filter
			}
			option := ""
			if tc.mode != "" {
				option = " by rank with option 'mode=" + tc.mode + "'"
			}
			query := cagraPlanFixture(t, "select id from cagra_iter"+where+
				" order by l2_distance(v,'[0,0]') limit 2"+option)
			search := findNodeOfType(query, plan.Node_INDEX_SEARCH_SCAN)
			require.NotNil(t, search)
			opts, err := cagraplan.DecodeScanOptions(search.IndexSearchScan.AlgoOptions)
			require.NoError(t, err)
			require.Equal(t, tc.iterative, opts.Iterative)
			require.Equal(t, tc.pushed, opts.FilterJSON != "")
			// a filter outside the search goes through membership: the PRE shape,
			// whose producer applies every filter, and no post-filter over-fetch
			semi := findSemiJoin(query)
			require.Equal(t, tc.pre, semi != nil)
			if tc.pre {
				require.False(t, search.IndexSearchScan.PostFilterOverFetch)
				require.Len(t, search.RuntimeFilterProbeList, 1)
				require.True(t, search.RuntimeFilterProbeList[0].UseMembershipFilter)
				producer := findNodeUnder(query, semi.Children[1], plan.Node_TABLE_SCAN)
				require.NotNil(t, producer)
				// distance range predicates move to the search node, not the producer
				want := 0
				for _, term := range strings.Split(tc.filter, " and ") {
					if !strings.HasPrefix(term, "l2_distance") {
						want++
					}
				}
				require.Len(t, producer.FilterList, want)
			}
		})
	}
}

func TestCuvsAutoBuildsAdaptiveTop(t *testing.T) {
	for _, tc := range []struct {
		name, filter string
		branches     int
	}{
		{"include filter", " where category = 1", 3},
		{"no include filter", " where note = 2", 3},
		{"no filter", "", 2},
		{"distance filter only", " where l2_distance(v,'[0,0]') < 1", 2},
	} {
		for _, idx := range []struct {
			algo  string
			roles []string
		}{
			{catalog.MoIndexCagraAlgo.ToString(), []string{catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage}},
			{catalog.MoIndexIvfpqAlgo.ToString(), []string{catalog.Ivfpq_TblType_Metadata, catalog.Ivfpq_TblType_Storage}},
		} {
			t.Run(idx.algo+"/"+tc.name, func(t *testing.T) {
				query := cuvsPlanFixture(t, idx.algo, idx.roles, "select id from cagra_iter"+tc.filter+
					" order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=auto'")
				adaptive := findNodeOfType(query, plan.Node_ADAPTIVE_TOP)
				require.NotNil(t, adaptive)
				require.Len(t, adaptive.Children, tc.branches)
			})
		}
	}
}

// A mode the index does not support is an error only when that index serves the query.
func TestVectorModeCheckedOnlyForServingIndex(t *testing.T) {
	for _, option := range []string{"'nprobe=4'", "'max_nprobe=4'"} {
		query := cagraPlanFixture(t, "select id from cagra_iter order by cosine_distance(v,'[0,0]') limit 2 by rank with option "+option)
		require.Nil(t, findNodeOfType(query, plan.Node_INDEX_SEARCH_SCAN), option)
	}
}
