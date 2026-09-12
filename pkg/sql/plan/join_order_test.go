// Copyright 2022 Matrix Origin
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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func TestSetParentRejectsJoinGraphCycle(t *testing.T) {
	vertices := makeJoinVertices(3)
	setParent(0, 1, vertices)
	setParent(1, 2, vertices)

	setParent(2, 0, vertices)

	require.Equal(t, int32(-1), vertices[2].parent)
	require.False(t, vertices[0].children[2])
	require.True(t, findParent(0, 2, vertices))
	require.False(t, findParent(2, 0, vertices))
}

func TestJoinGraphWalksTerminateOnExistingCycle(t *testing.T) {
	vertices := makeJoinVertices(3)
	vertices[0].parent = 1
	vertices[1].parent = 2
	vertices[2].parent = 1
	vertices[1].children[2] = true
	vertices[2].children[1] = true

	require.False(t, findParent(0, 0, vertices))
	require.False(t, findSelectivityInChildren(1, vertices))
}

func TestJoinGraphWalksIgnoreInvalidVertexIDs(t *testing.T) {
	vertices := makeJoinVertices(2)
	vertices[0].parent = 3
	vertices[0].children[3] = true

	require.False(t, findParent(0, 1, vertices))
	require.False(t, findSelectivityInChildren(0, vertices))
}

func TestDimensionOrderingUsesParentKeyActiveDomain(t *testing.T) {
	dateDimension := &joinVertex{
		node: &plan.Node{Stats: &plan.Stats{
			Selectivity: 365.0 / 73049.0, Outcnt: 365,
		}},
		selectivityOnParent: 365.0 / 1827.0,
	}
	itemDimension := &joinVertex{
		node: &plan.Node{Stats: &plan.Stats{
			Selectivity: 15406.0 / 300000.0, Outcnt: 15406,
		}},
		selectivityOnParent: 15406.0 / 300000.0,
	}

	// Raw dimension selectivity incorrectly prefers the date table because its
	// catalog covers about 200 years. Relative to the fact table's five-year
	// key domain, the item predicate is the more selective join.
	require.Less(t, dateDimension.node.Stats.Selectivity, itemDimension.node.Stats.Selectivity)
	require.Greater(t, compareJoinVertexStats(dateDimension, itemDimension), 0)

	dateDimension.selectivityOnParent = -1
	itemDimension.selectivityOnParent = -1
	require.Less(t, compareJoinVertexStats(dateDimension, itemDimension), 0,
		"missing parent-key NDV must retain the existing ordering fallback")
}

func TestSetSelectivityOnParentUsesActiveJoinKeyDomain(t *testing.T) {
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableCnt = 2_880_000_000
	stats.NdvMap["sold_date_sk"] = 1827
	statsCache.Set(1, stats)
	builder := NewQueryBuilder(plan.Query_SELECT, &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}, false, false)
	parentTable := &plan.TableDef{
		TblId: 1,
		Cols:  []*plan.ColDef{{Name: "sold_date_sk"}},
	}
	vertices := makeJoinVertices(2)
	vertices[0].node.NodeId = 0
	vertices[0].node.BindingTags = []int32{0}
	vertices[0].node.Stats.Outcnt = 91
	vertices[1].node.NodeId = 1
	vertices[1].node.BindingTags = []int32{1}
	vertices[1].node.Stats.Outcnt = 2_880_000_000
	setParent(0, 1, vertices)

	childTable := &plan.TableDef{TblId: 2, Cols: []*plan.ColDef{{Name: "dimension_key"}}}
	childStats := NewStatsInfo()
	childStats.TableCnt = 73_049
	childStats.NdvMap["dimension_key"] = 73_049
	statsCache.Set(2, childStats)
	builder.tag2Table[0] = childTable
	builder.tag2Table[1] = parentTable
	builder.qry.Nodes = []*plan.Node{vertices[0].node, vertices[1].node}

	builder.setSelectivityOnParent(
		0, 1, []int32{0}, []int32{0}, childTable, parentTable, vertices)
	require.InDelta(t, 91.0/1827.0, vertices[0].selectivityOnParent, 1e-12)

	stats.NdvMap["sold_date_sk"] = 100_000
	childStats.NdvMap["dimension_key"] = 1000
	vertices[0].node.Stats.Outcnt = 100
	setParent(0, 1, vertices)
	builder.setSelectivityOnParent(
		0, 1, []int32{0}, []int32{0}, childTable, parentTable, vertices)
	require.InDelta(t, 0.1, vertices[0].selectivityOnParent, 1e-12,
		"the active domain cannot exceed the dimension key domain")

	delete(stats.NdvMap, "sold_date_sk")
	setParent(0, 1, vertices)
	builder.setSelectivityOnParent(
		0, 1, []int32{0}, []int32{0}, childTable, parentTable, vertices)
	require.Equal(t, float64(-1), vertices[0].selectivityOnParent)
}

func TestJoinGraphOrdersDimensionsByFactKeyActiveDomain(t *testing.T) {
	statsCache := NewStatsCache()
	for tableID, stats := range map[uint64]*statsinfo.StatsInfo{
		1: {TableCnt: 2_880_000_000, NdvMap: map[string]float64{
			"ss_item_sk": 289_000, "ss_sold_date_sk": 1823,
		}},
		2: {TableCnt: 300_000, NdvMap: map[string]float64{"i_item_sk": 300_000}},
		3: {TableCnt: 73_049, NdvMap: map[string]float64{"d_date_sk": 73_049}},
	} {
		statsCache.Set(tableID, stats)
	}
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	factTable := &plan.TableDef{TblId: 1, Cols: []*plan.ColDef{
		{Name: "ss_item_sk"}, {Name: "ss_sold_date_sk"},
	}}
	itemTable := &plan.TableDef{TblId: 2, Cols: []*plan.ColDef{{Name: "i_item_sk"}}}
	dateTable := &plan.TableDef{TblId: 3, Cols: []*plan.ColDef{{Name: "d_date_sk"}}}
	builder.tag2Table[0] = factTable
	builder.tag2Table[1] = itemTable
	builder.tag2Table[2] = dateTable
	builder.qry.Nodes = []*plan.Node{
		{NodeId: 0, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{0}, TableDef: factTable,
			Stats: &plan.Stats{Cost: 2_880_000_000, Outcnt: 2_880_000_000, Selectivity: 1}},
		{NodeId: 1, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{1}, TableDef: itemTable,
			Stats: &plan.Stats{Cost: 300_000, Outcnt: 3282, Selectivity: 0.0109}},
		{NodeId: 2, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{2}, TableDef: dateTable,
			Stats: &plan.Stats{Cost: 73_049, Outcnt: 365, Selectivity: 0.005}},
	}
	joinCondition := func(leftTag, leftCol, rightTag, rightCol int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: leftTag, ColPos: leftCol}}},
				{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rightTag, ColPos: rightCol}}},
			},
		}}}
	}

	vertices := builder.getJoinGraph(builder.qry.Nodes, []*plan.Expr{
		joinCondition(0, 0, 1, 0),
		joinCondition(0, 1, 2, 0),
	})
	require.Equal(t, int32(0), vertices[1].parent)
	require.Equal(t, int32(0), vertices[2].parent)
	require.InDelta(t, 3282.0/289_000.0, vertices[1].selectivityOnParent, 1e-12)
	require.InDelta(t, 365.0/1823.0, vertices[2].selectivityOnParent, 1e-12)

	builder.buildSubJoinTree(vertices, 0)
	require.Equal(t, []int32{0, 1}, builder.qry.Nodes[3].Children,
		"the item restriction must be applied before the one-year date restriction")
}

func TestSemiJoinPushdownUsesFactKeyActiveDomain(t *testing.T) {
	statsCache := NewStatsCache()
	for tableID, stats := range map[uint64]*statsinfo.StatsInfo{
		1: {TableCnt: 2_880_000_000, NdvMap: map[string]float64{
			"ss_item_sk": 289_000, "ss_sold_date_sk": 1823,
		}},
		2: {TableCnt: 300_000, NdvMap: map[string]float64{"i_item_sk": 300_000}},
		3: {TableCnt: 73_049, NdvMap: map[string]float64{"d_date_sk": 73_049}},
	} {
		statsCache.Set(tableID, stats)
	}
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	factTable := &plan.TableDef{TblId: 1, Cols: []*plan.ColDef{
		{Name: "ss_item_sk"}, {Name: "ss_sold_date_sk"},
	}}
	itemTable := &plan.TableDef{TblId: 2, Cols: []*plan.ColDef{{Name: "i_item_sk"}}}
	dateTable := &plan.TableDef{TblId: 3, Cols: []*plan.ColDef{{Name: "d_date_sk"}}}
	builder.tag2Table[0] = factTable
	builder.tag2Table[1] = itemTable
	builder.tag2Table[2] = dateTable
	joinCondition := func(leftTag, leftCol, rightTag, rightCol int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: leftTag, ColPos: leftCol}}},
				{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rightTag, ColPos: rightCol}}},
			},
		}}}
	}
	builder.qry.Nodes = []*plan.Node{
		{NodeId: 0, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{0}, TableDef: factTable,
			Stats: &plan.Stats{Outcnt: 2_880_000_000, Selectivity: 1}},
		{NodeId: 1, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{1}, TableDef: itemTable,
			Stats: &plan.Stats{Outcnt: 300, Selectivity: 0.001}},
		{NodeId: 2, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{2}, TableDef: dateTable,
			Stats: &plan.Stats{Outcnt: 91, Selectivity: 0.0012}},
		{NodeId: 3, NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{0, 2},
			OnList: []*plan.Expr{joinCondition(0, 1, 2, 0)},
			Stats:  &plan.Stats{Outcnt: 143_000_000, Selectivity: 0.0012}},
		{NodeId: 4, NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI, Children: []int32{3, 1},
			OnList: []*plan.Expr{joinCondition(0, 0, 1, 0)},
			Stats:  &plan.Stats{Outcnt: 143_000, Selectivity: 0.001}},
	}
	activeSelectivity, ok := builder.getJoinActiveDomainSelectivity(4)
	require.True(t, ok)
	require.InDelta(t, 300.0/289_000.0, activeSelectivity, 1e-12)
	builder.qry.Nodes[4].JoinType = plan.Node_ANTI
	activeSelectivity, ok = builder.getJoinActiveDomainSelectivity(4)
	require.True(t, ok)
	require.InDelta(t, 1-300.0/289_000.0, activeSelectivity, 1e-12,
		"ANTI selectivity is the complement of the matching key fraction")
	builder.qry.Nodes[4].JoinType = plan.Node_LEFT
	_, ok = builder.getJoinActiveDomainSelectivity(4)
	require.False(t, ok, "unsupported join semantics must retain the existing fallback")
	builder.qry.Nodes[4].JoinType = plan.Node_SEMI

	root := builder.pushdownSemiAntiJoins(4)
	require.Equal(t, int32(3), root)
	require.Equal(t, []int32{4, 2}, builder.qry.Nodes[3].Children)
	require.Equal(t, []int32{0, 1}, builder.qry.Nodes[4].Children)
}

func TestBoundPlanOrdersDimensionsByFactKeyActiveDomain(t *testing.T) {
	mock := NewEmptyCompilerContext()
	mock.dbs = map[string]bool{"tpch": true}
	registerTable := func(tableID uint64, name string, columns ...string) *plan.TableDef {
		defs := make([]*plan.ColDef, len(columns))
		for i, column := range columns {
			defs[i] = &plan.ColDef{Name: column, Typ: plan.Type{Id: int32(types.T_int32)}}
		}
		table := &plan.TableDef{
			TblId: tableID, Name: name, TableType: catalog.SystemOrdinaryRel, Cols: defs,
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		}
		mock.tables[name] = table
		mock.objects[name] = &plan.ObjectRef{Obj: int64(tableID), ObjName: name, SchemaName: "tpch"}
		return table
	}
	fact := registerTable(1001, "active_fact", "f_item_sk", "f_date_sk")
	item := registerTable(1002, "active_item", "i_item_sk", "i_filter")
	date := registerTable(1003, "active_date", "d_date_sk", "d_filter")

	statsCache := NewStatsCache()
	for tableID, stats := range map[uint64]*statsinfo.StatsInfo{
		fact.TblId: {TableName: fact.Name, TableCnt: 2_880_000_000, NdvMap: map[string]float64{
			"f_item_sk": 289_000, "f_date_sk": 1823,
		}},
		item.TblId: {TableName: item.Name, TableCnt: 300_000, NdvMap: map[string]float64{
			"i_item_sk": 300_000, "i_filter": 100,
		}},
		date.TblId: {TableName: date.Name, TableCnt: 73_049, NdvMap: map[string]float64{
			"d_date_sk": 73_049, "d_filter": 200,
		}},
	} {
		statsCache.Set(tableID, stats)
	}
	ctx := &fixedStatsCompilerContext{statsCacheCompilerContext: &statsCacheCompilerContext{
		MockCompilerContext: mock, statsCache: statsCache,
	}}
	stmts, err := mysql.Parse(ctx.GetContext(), `
		select count(*)
		from active_fact, active_item, active_date
		where f_item_sk = i_item_sk and f_date_sk = d_date_sk
		  and i_filter = 1 and d_filter = 1`, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	built, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)

	var firstDimension string
	var scanTables func(int32) []string
	scanTables = func(nodeID int32) []string {
		node := built.GetQuery().Nodes[nodeID]
		if node.NodeType == plan.Node_TABLE_SCAN {
			return []string{node.TableDef.Name}
		}
		var tables []string
		for _, child := range node.Children {
			tables = append(tables, scanTables(child)...)
		}
		if node.NodeType == plan.Node_JOIN && len(tables) == 2 {
			if tables[0] == fact.Name {
				firstDimension = tables[1]
			} else if tables[1] == fact.Name {
				firstDimension = tables[0]
			}
		}
		return tables
	}
	for _, step := range built.GetQuery().Steps {
		scanTables(step)
	}
	require.Equal(t, item.Name, firstDimension)
}

func makeJoinVertices(n int) []*joinVertex {
	vertices := make([]*joinVertex, n)
	for i := range vertices {
		vertices[i] = &joinVertex{
			node: &plan.Node{
				Stats: &plan.Stats{
					Selectivity: 1,
					Outcnt:      1,
				},
			},
			children:            make(map[int32]bool),
			parent:              -1,
			selectivityOnParent: -1,
		}
	}
	return vertices
}
