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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// #31689: a select-list MATCH over a JOIN drives its own fulltext search stream on the join
// child it references, when that child is not null-extended.

type projectedJoinCase struct {
	joinType    planpb.Node_JoinType
	isRightJoin bool
	docsChild   int    // join child index of the fulltext-indexed scan
	filterTerm  string // "" = no WHERE MATCH on the docs scan
	projTerm    string
}

// buildProjectedJoinPlan builds PROJECT(match(projTerm)) -> SORT -> JOIN(docs, q) and runs
// attachProjectedFullTextMatches and applyIndices on it.
func buildProjectedJoinPlan(t *testing.T, c projectedJoinCase) *QueryBuilder {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(t), false, true)
	ctx := NewBindContext(builder, nil)

	docsDef := makeFullTextJoinTestTableDef("ft", true)
	registerFullTextJoinRegularIndexTable(builder, docsDef.Indexes[0].IndexTableName)
	docsTag := builder.genNewBindTag()
	var filters []*planpb.Expr
	if c.filterTerm != "" {
		filters = append(filters, makeFullTextMatchExpr(c.filterTerm, 0, docsDef, docsTag, []int32{2, 3}))
	}
	docsID := builder.appendNode(makeFullTextJoinTestScan(docsDef, docsTag, filters), ctx)

	qDef := makeFullTextJoinTestTableDef("q", false)
	qID := builder.appendNode(makeJoinIndexTestScan(qDef, builder.genNewBindTag()), ctx)

	children := []int32{docsID, qID}
	if c.docsChild == 1 {
		children = []int32{qID, docsID}
	}
	joinID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_JOIN,
		JoinType:    c.joinType,
		IsRightJoin: c.isRightJoin,
		Children:    children,
	}, ctx)
	sortID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_SORT,
		Children: []int32{joinID},
		OrderBy:  []*planpb.OrderBySpec{{Expr: ftjColExpr(docsDef, docsTag, 0)}},
	}, ctx)
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{sortID},
		BindingTags: []int32{builder.genNewBindTag()},
		ProjectList: []*planpb.Expr{
			ftjColExpr(docsDef, docsTag, 0),
			makeFullTextMatchExpr(c.projTerm, 0, docsDef, docsTag, []int32{2, 3}),
		},
	}, ctx)

	builder.attachProjectedFullTextMatches(projID)
	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}
	return builder
}

func TestFullTextProjectedMatchDrivesJoinChild(t *testing.T) {
	tests := []struct {
		name      string
		c         projectedJoinCase
		wantScans int
	}{
		{name: "SEMI, projection only",
			c:         projectedJoinCase{joinType: planpb.Node_SEMI, projTerm: "world"},
			wantScans: 1},
		{name: "SEMI right-swapped, docs at child 1",
			c:         projectedJoinCase{joinType: planpb.Node_SEMI, isRightJoin: true, docsChild: 1, projTerm: "world"},
			wantScans: 1},
		{name: "SEMI, different filter term",
			c:         projectedJoinCase{joinType: planpb.Node_SEMI, filterTerm: "hello", projTerm: "world"},
			wantScans: 2},
		{name: "SEMI, same term as filter",
			c:         projectedJoinCase{joinType: planpb.Node_SEMI, filterTerm: "hello", projTerm: "hello"},
			wantScans: 1},
		{name: "INNER, docs at child 1, different filter term",
			c:         projectedJoinCase{joinType: planpb.Node_INNER, docsChild: 1, filterTerm: "hello", projTerm: "world"},
			wantScans: 2},
		{name: "MARK preserved child",
			c:         projectedJoinCase{joinType: planpb.Node_MARK, filterTerm: "hello", projTerm: "world"},
			wantScans: 2},
		{name: "ANTI preserved child",
			c:         projectedJoinCase{joinType: planpb.Node_ANTI, projTerm: "world"},
			wantScans: 1},
		{name: "LEFT preserved child",
			c:         projectedJoinCase{joinType: planpb.Node_LEFT, projTerm: "world"},
			wantScans: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := buildProjectedJoinPlan(t, tt.c)
			require.Zero(t, countReachableFullTextMatches(builder.qry))
			require.Equal(t, tt.wantScans, countReachableFullTextScans(builder.qry))
		})
	}
}

func TestFullTextProjectedMatchNotDrivenOnNullExtendedOrProbeChild(t *testing.T) {
	tests := []struct {
		name string
		c    projectedJoinCase
	}{
		{name: "LEFT null-extended child",
			c: projectedJoinCase{joinType: planpb.Node_LEFT, docsChild: 1, projTerm: "world"}},
		{name: "RIGHT null-extended child",
			c: projectedJoinCase{joinType: planpb.Node_RIGHT, docsChild: 0, projTerm: "world"}},
		{name: "ANTI probe child",
			c: projectedJoinCase{joinType: planpb.Node_ANTI, docsChild: 1, projTerm: "world"}},
		{name: "MARK probe child",
			c: projectedJoinCase{joinType: planpb.Node_MARK, docsChild: 1, projTerm: "world"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := buildProjectedJoinPlan(t, tt.c)
			require.Equal(t, 1, countReachableFullTextMatches(builder.qry))
			require.Zero(t, countReachableFullTextScans(builder.qry))
		})
	}
}

func TestAggPullupSkipsRightScanColumnsUnderFullTextMatch(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	for _, withMatch := range []bool{false, true} {
		leftScan := &planpb.Node{
			NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10},
			TableDef: groupHashKeyTestTable("id", "id"), Stats: &planpb.Stats{Outcnt: 1},
		}
		agg := &planpb.Node{
			NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0},
			BindingTags: []int32{20, 21},
			GroupBy:     []*planpb.Expr{GetColExpr(intType, 10, 1)},
			Stats:       &planpb.Stats{Outcnt: 1},
		}
		rightTable := groupHashKeyTestTable("id", "id")
		rightTable.Cols[0].Typ = intType
		rightScan := &planpb.Node{
			NodeId: 2, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{30},
			TableDef: rightTable, Stats: &planpb.Stats{Outcnt: 1},
		}
		join := &planpb.Node{
			NodeId: 3, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{1, 2},
			OnList: []*planpb.Expr{{
				Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
					Args: []*planpb.Expr{GetColExpr(intType, 20, 0), GetColExpr(intType, 30, 0)},
				}},
			}},
			Stats: &planpb.Stats{Outcnt: 1},
		}
		projected := GetColExpr(planpb.Type{Id: int32(types.T_text)}, 30, 1)
		if withMatch {
			projected = &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "fulltext_match"},
					Args: []*planpb.Expr{
						makePlan2StringConstExprWithType("anchor", false),
						makePlan2Int64ConstExprWithType(0),
						GetColExpr(planpb.Type{Id: int32(types.T_text)}, 30, 1),
					},
				}},
			}
		}
		proj := &planpb.Node{
			NodeId: 4, NodeType: planpb.Node_PROJECT, Children: []int32{3},
			ProjectList: []*planpb.Expr{projected},
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(t), false, true)
		builder.qry.Nodes = []*planpb.Node{leftScan, agg, rightScan, join, proj}

		require.Equal(t, !withMatch, applyAggPullup(4, join, agg, leftScan, rightScan, builder),
			"withMatch=%v", withMatch)
		if withMatch {
			require.Equal(t, int32(30), projected.GetF().Args[2].GetCol().RelPos)
		} else {
			require.Equal(t, int32(21), projected.GetCol().RelPos)
		}
	}
}

func TestAttachProjectedFullTextMatchesSkipsBooleanCompositionScan(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(t), false, true)
	ctx := NewBindContext(builder, nil)

	docsDef := makeFullTextJoinTestTableDef("ft", true)
	docsTag := builder.genNewBindTag()
	match := func(term string) *planpb.Expr {
		return makeFullTextMatchExpr(term, 0, docsDef, docsTag, []int32{2, 3})
	}
	zero := makePlan2Float64ConstExprWithType(0)
	filters := map[string]*planpb.Expr{
		"or":   booleanFullTextBind(t, "or", match("hello"), match("world")),
		"not":  booleanFullTextBind(t, "not", match("hello")),
		"=0":   booleanFullTextBind(t, "=", match("hello"), zero),
		"bare": match("world"),
	}
	for name, filter := range filters {
		t.Run(name, func(t *testing.T) {
			docsID := builder.appendNode(makeFullTextJoinTestScan(docsDef, docsTag, []*planpb.Expr{filter}), ctx)
			qID := builder.appendNode(makeJoinIndexTestScan(makeFullTextJoinTestTableDef("q", false), builder.genNewBindTag()), ctx)
			joinID := builder.appendNode(&planpb.Node{
				NodeType: planpb.Node_JOIN, JoinType: planpb.Node_SEMI, Children: []int32{docsID, qID},
			}, ctx)
			projID := builder.appendNode(&planpb.Node{
				NodeType: planpb.Node_PROJECT, Children: []int32{joinID},
				ProjectList: []*planpb.Expr{match("hello")},
			}, ctx)

			builder.attachProjectedFullTextMatches(projID)

			want := 1
			if name == "bare" {
				want = 2
			}
			require.Len(t, builder.qry.Nodes[docsID].FilterList, want)
		})
	}
}

// A projected MATCH is not served below the LIMIT or OFFSET of a derived table: the index would
// filter the rows before the derived table paginates them. Pagination of the MATCH's own SELECT,
// and of a SELECT over an unpaginated derived table, applies after it and is served.
func TestFullTextProjectedMatchStopsAtDerivedPagination(t *testing.T) {
	queries := []struct {
		sql    string
		served bool
	}{
		{"select f.id, match(f.title, f.body) against('hello') from ft f join base b on f.base_id = b.id order by f.id limit 1 offset 1", true},
		{"select q.id, match(q.title, q.body) against('hello') from (select f.id, f.title, f.body from ft f join base b on f.base_id = b.id) q order by q.id limit 1", true},
		{"select q.id, match(q.title, q.body) against('hello') from (select f.id, f.title, f.body from ft f join base b on f.base_id = b.id order by f.id limit 1) q", false},
		{"select q.id, match(q.title, q.body) against('hello') from (select f.id, f.title, f.body from ft f join base b on f.base_id = b.id order by f.id limit 1 offset 1) q", false},
		{"select f.id, match(f.title, f.body) against('hello') from (select * from ft limit 1) f join base b on f.base_id = b.id", false},
		{"select f.id, match(f.title, f.body) against('hello') from (select * from ft limit 1 offset 1) f join base b on f.base_id = b.id", false},
	}
	for _, ft2 := range []bool{false, true} {
		for _, q := range queries {
			cc := newFullTextJoinMockCompilerContext(t)
			builder := NewQueryBuilder(planpb.Query_SELECT, cc, false, true)
			def := makeFullTextJoinTestTableDef("ft", true)
			scan := makeFullTextJoinTestScan(def, 0, nil)
			registerFullTextJoinRegularIndexTable(builder, def.Indexes[0].IndexTableName)
			if ft2 {
				convertFullTextJoinTestToFulltext2(builder, scan)
				for _, idx := range def.Indexes {
					registerFullTextJoinRegularIndexTable(builder, idx.IndexTableName)
				}
			}
			cc.tables["ft"], cc.objects["ft"] = def, &planpb.ObjectRef{SchemaName: "test", ObjName: "ft"}
			cc.tables["base"], cc.objects["base"] = makeFullTextJoinTestTableDef("base", false), &planpb.ObjectRef{SchemaName: "test", ObjName: "base"}
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, q.sql, 1)
			require.NoError(t, err)
			p, err := BuildPlan(cc, stmt, false, nil)
			stmt.Free()
			require.NoError(t, err)
			query := p.GetQuery()
			scans := countReachableFullTextScans(query) + countReachableFullText2Scans(query)
			if q.served {
				require.Equal(t, 1, scans, "ft2=%v: %s", ft2, q.sql)
				require.Zero(t, countReachableFullTextMatches(query), "ft2=%v: %s", ft2, q.sql)
			} else {
				require.Zero(t, scans, "ft2=%v: %s", ft2, q.sql)
				require.Equal(t, 1, countReachableFullTextMatches(query), "ft2=%v: %s", ft2, q.sql)
			}
		}
	}
}
