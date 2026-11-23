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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// #31715: a view, derived table or CTE keeps a pass-through PROJECT above the
// MATCH-filtered scan. An outer PROJECT that also selects a MATCH reads the
// inner PROJECT's columns, so the inner PROJECT is removed when a PROJECT above
// can take over as the fulltext rewrite boundary.

type passThroughPlan struct {
	between    planpb.Node_NodeType // node between the outer and inner PROJECT
	sortAsRoot bool                 // the SORT is the root; no outer PROJECT
	nested     bool                 // a second column pass-through PROJECT above the inner one
	noMatch    bool                 // the outer PROJECT selects no MATCH
	scanLimit  bool
	scanOffset bool
	filterTerm string
	projTerm   string
}

// build returns the builder, the root id and the inner PROJECT id of
//
//	outer PROJECT(id[, match(projTerm)]) -> [between] -> [nested PROJECT] -> inner PROJECT -> SCAN(match(filterTerm))
func (p passThroughPlan) build(t *testing.T) (*QueryBuilder, int32, int32) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(t), false, true)
	ctx := NewBindContext(builder, nil)

	docsDef := makeFullTextJoinTestTableDef("ft", true)
	registerFullTextJoinRegularIndexTable(builder, docsDef.Indexes[0].IndexTableName)
	scanTag := builder.genNewBindTag()
	scan := makeFullTextJoinTestScan(docsDef, scanTag,
		[]*planpb.Expr{makeFullTextMatchExpr(p.filterTerm, 0, docsDef, scanTag, []int32{2, 3})})
	if p.scanLimit {
		scan.Limit = makePlan2Uint64ConstExprWithType(2)
	}
	if p.scanOffset {
		scan.Offset = makePlan2Uint64ConstExprWithType(1)
	}
	scanID := builder.appendNode(scan, ctx)

	passThrough := func(childID, childTag int32) (int32, int32) {
		tag := builder.genNewBindTag()
		list := make([]*planpb.Expr, len(docsDef.Cols))
		for i := range docsDef.Cols {
			list[i] = ftjColExpr(docsDef, childTag, int32(i))
		}
		id := builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_PROJECT, Children: []int32{childID},
			BindingTags: []int32{tag}, ProjectList: list,
		}, ctx)
		return id, tag
	}
	innerID, topTag := passThrough(scanID, scanTag)
	childID := innerID
	if p.nested {
		childID, topTag = passThrough(innerID, topTag)
	}
	topCol := func(pos int32) *planpb.Expr {
		return ftjColExpr(docsDef, topTag, pos)
	}
	topMatch := func(term string) *planpb.Expr {
		match := makeFullTextMatchExpr(term, 0, docsDef, topTag, nil)
		match.GetF().Args = append(match.GetF().Args, topCol(2), topCol(3))
		return match
	}

	switch p.between {
	case planpb.Node_SORT:
		childID = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_SORT, Children: []int32{childID},
			OrderBy: []*planpb.OrderBySpec{{Expr: topCol(0)}},
		}, ctx)
	case planpb.Node_WINDOW:
		childID = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_WINDOW, Children: []int32{childID},
			BindingTags: []int32{builder.genNewBindTag()},
		}, ctx)
	case planpb.Node_JOIN:
		qID := builder.appendNode(makeJoinIndexTestScan(makeFullTextJoinTestTableDef("q", false), builder.genNewBindTag()), ctx)
		childID = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{childID, qID},
		}, ctx)
	}
	if p.sortAsRoot {
		return builder, childID, innerID
	}

	list := []*planpb.Expr{topCol(0)}
	if !p.noMatch {
		list = append(list, topMatch(p.projTerm))
	}
	rootID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{childID},
		BindingTags: []int32{builder.genNewBindTag()}, ProjectList: list,
	}, ctx)
	return builder, rootID, innerID
}

func reachableNodeIDs(query *planpb.Query, rootID int32) map[int32]bool {
	seen := map[int32]bool{}
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		for _, child := range query.Nodes[id].Children {
			visit(child)
		}
	}
	visit(rootID)
	return seen
}

func removePassThroughProjects(builder *QueryBuilder, rootID int32) int32 {
	colRefCnt := map[[2]int32]int{}
	builder.countColRefs(rootID, colRefCnt)
	rootID, _ = builder.removeSimpleProjections(rootID, planpb.Node_UNKNOWN, false, colRefCnt)
	return rootID
}

func TestFullTextPassThroughProjectUnderProject(t *testing.T) {
	for _, tt := range []struct {
		name              string
		p                 passThroughPlan
		wantFullTextScans int
	}{
		{name: "PROJECT parent, same term",
			p: passThroughPlan{filterTerm: "hello", projTerm: "hello"}, wantFullTextScans: 1},
		{name: "PROJECT parent, different term",
			p: passThroughPlan{filterTerm: "hello", projTerm: "world"}, wantFullTextScans: 2},
		{name: "SORT under PROJECT",
			p: passThroughPlan{between: planpb.Node_SORT, filterTerm: "hello", projTerm: "hello"}, wantFullTextScans: 1},
		{name: "nested pass-through PROJECT",
			p: passThroughPlan{nested: true, filterTerm: "hello", projTerm: "hello"}, wantFullTextScans: 1},
		{name: "scan LIMIT under PROJECT",
			p: passThroughPlan{scanLimit: true, filterTerm: "hello", projTerm: "hello"}, wantFullTextScans: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			builder, rootID, innerID := tt.p.build(t)
			rootID = removePassThroughProjects(builder, rootID)
			require.False(t, reachableNodeIDs(builder.qry, rootID)[innerID],
				"the pass-through PROJECT is removed")

			newID, err := builder.applyIndices(rootID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			builder.qry.Steps = []int32{newID}
			require.Zero(t, countReachableFullTextMatches(builder.qry))
			require.Equal(t, tt.wantFullTextScans, countReachableFullTextScans(builder.qry))
		})
	}
}

func TestFullTextPassThroughProjectKeptWithoutMatchAbove(t *testing.T) {
	for _, tt := range []struct {
		name string
		p    passThroughPlan
	}{
		{name: "WINDOW parent", p: passThroughPlan{between: planpb.Node_WINDOW}},
		{name: "JOIN parent", p: passThroughPlan{between: planpb.Node_JOIN}},
		{name: "SORT not under PROJECT", p: passThroughPlan{between: planpb.Node_SORT, sortAsRoot: true}},
		{name: "PROJECT parent without MATCH", p: passThroughPlan{noMatch: true}},
		{name: "SORT under PROJECT without MATCH", p: passThroughPlan{between: planpb.Node_SORT, noMatch: true}},
		{name: "scan LIMIT under SORT", p: passThroughPlan{between: planpb.Node_SORT, scanLimit: true}},
		{name: "scan OFFSET under SORT", p: passThroughPlan{between: planpb.Node_SORT, scanOffset: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.p.filterTerm, tt.p.projTerm = "hello", "hello"
			builder, rootID, innerID := tt.p.build(t)
			rootID = removePassThroughProjects(builder, rootID)
			require.True(t, reachableNodeIDs(builder.qry, rootID)[innerID],
				"the pass-through PROJECT stays the fulltext rewrite boundary")
		})
	}
}
