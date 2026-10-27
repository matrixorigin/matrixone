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

// ScanSnapshot propagation into the fulltext2 index search scan built by the covered
// fast path (#27941).

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// coveredFulltext2Fixture builds the minimal shape that clears every tryApplyCoveredFulltext2
// guard: one MATCH filter, one fulltext2 index with an INCLUDE column, no residual predicate,
// and a pk-only projection. snapshot is attached to the base scan.
func coveredFulltext2Fixture(t *testing.T, snapshot *plan.Snapshot) (
	builder *QueryBuilder, nodeID int32, projNode, sortNode, scanNode *plan.Node, idxdef *plan.IndexDef,
) {
	t.Helper()

	builder = NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	bindCtx := NewBindContext(builder, nil)

	scanTag := builder.genNewBindTag()
	scanNode = &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef:   &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
				{Name: "body", Typ: plan.Type{Id: int32(types.T_varchar), Width: 256}},
				{Name: "tag", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
			Name2ColIndex: map[string]int32{"id": 0, "body": 1, "tag": 2},
			Indexes: []*plan.IndexDef{
				{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store"},
				{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__meta"},
			},
		},
		BindingTags:  []int32{scanTag},
		ScanSnapshot: snapshot,
		// match(body, 'x' IN NATURAL LANGUAGE MODE)
		FilterList: []*plan.Expr{{
			Typ: plan.Type{Id: int32(types.T_float32)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &ObjectRef{ObjName: "match_against"},
				Args: []*plan.Expr{
					makePlan2StringConstExprWithType("x"),
					{
						Typ:  plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(tree.FULLTEXT_NL)}}},
					},
				},
			}},
		}},
	}
	nodeID = builder.appendNode(scanNode, bindCtx)
	for i := 0; i < 10; i++ {
		builder.ctxByNode = append(builder.ctxByNode, bindCtx)
	}

	projNode = &plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{nodeID},
		// pk only
		ProjectList: []*plan.Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64), Width: 64},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 0}},
		}},
	}
	sortNode = &plan.Node{NodeType: plan.Node_SORT, Children: []int32{nodeID}}

	idxdef = &plan.IndexDef{
		IndexName:       "ft2idx",
		IndexAlgo:       catalog.MoIndexFullText2Algo.ToString(),
		IncludedColumns: []string{"tag"},
	}
	return
}

// The covered path's fulltext2 index search scan carries a deep copy of the base scan's
// snapshot, on the node and on its IndexSearchScan.
func TestTryApplyCoveredFulltext2PropagatesScanSnapshot(t *testing.T) {
	snapshot := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}}
	builder, nodeID, projNode, sortNode, scanNode, idxdef := coveredFulltext2Fixture(t, snapshot)

	handled, err := builder.tryApplyCoveredFulltext2(nodeID, projNode, sortNode, scanNode,
		[]int32{0}, []*plan.IndexDef{idxdef}, nil, nil, map[int32]int32{}, nil, nil, false)
	require.NoError(t, err)
	require.True(t, handled, "the fixture must clear every covered-path guard")

	node := findCoveredFulltext2Scan(t, builder)
	for _, got := range []*plan.Snapshot{node.ScanSnapshot, node.IndexSearchScan.ScanSnapshot} {
		require.NotNil(t, got)
		require.NotNil(t, got.TS)
		assert.Equal(t, snapshot.TS.PhysicalTime, got.TS.PhysicalTime)
		assert.Equal(t, snapshot.TS.LogicalTime, got.TS.LogicalTime)
		assert.NotSame(t, snapshot, got, "must be a deep copy, not the scan node's own Snapshot")
		assert.NotSame(t, snapshot.TS, got.TS, "the TS must be deep-copied too")
	}
}

// No snapshot on the base scan leaves the search scan's ScanSnapshot nil.
func TestTryApplyCoveredFulltext2NoSnapshotLeavesSearchUnsnapshotted(t *testing.T) {
	builder, nodeID, projNode, sortNode, scanNode, idxdef := coveredFulltext2Fixture(t, nil)

	handled, err := builder.tryApplyCoveredFulltext2(nodeID, projNode, sortNode, scanNode,
		[]int32{0}, []*plan.IndexDef{idxdef}, nil, nil, map[int32]int32{}, nil, nil, false)
	require.NoError(t, err)
	require.True(t, handled)

	node := findCoveredFulltext2Scan(t, builder)
	assert.Nil(t, node.ScanSnapshot)
	assert.Nil(t, node.IndexSearchScan.ScanSnapshot)
}

func TestCoveredFulltext2ScoreLimitOnlyForEquivalentSort(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keys      []*plan.OrderBySpec
		foundRows bool
		wantLimit uint64
	}{
		{name: "score descending", wantLimit: 200},
		{name: "score ascending", keys: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_ASC}}},
		{name: "different projected key", keys: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC,
			Expr: GetColExpr(plan.Type{Id: int32(types.T_int64)}, 0, 0)}}},
		{name: "second key", keys: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}, {Flag: plan.OrderBySpec_DESC}}},
		{name: "explicit null placement", keys: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_FIRST}}},
		{name: "found rows", foundRows: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, nodeID, projNode, sortNode, scanNode, idxdef := coveredFulltext2Fixture(t, nil)
			ctx := builder.ctxByNode[nodeID]
			ctx.projectTag = builder.genNewBindTag()
			projNode.ProjectList = append(projNode.ProjectList, DeepCopyExpr(scanNode.FilterList[0]))
			scoreKey := &plan.OrderBySpec{
				Expr: GetColExpr(plan.Type{Id: int32(types.T_float32)}, ctx.projectTag, 1),
				Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_INTERNAL,
			}
			scanNode.FilterList = append(scanNode.FilterList, fnExpr("is_null", &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 2, Name: "tag"}},
			}))
			if tc.keys == nil {
				sortNode.OrderBy = []*plan.OrderBySpec{scoreKey}
			} else {
				for _, key := range tc.keys {
					if key.Expr == nil {
						key.Expr = DeepCopyExpr(scoreKey.Expr)
					} else {
						key.Expr.GetCol().RelPos = ctx.projectTag
					}
				}
				sortNode.OrderBy = tc.keys
			}
			builder.sqlCalcFoundRows = tc.foundRows
			handled, err := builder.tryApplyCoveredFulltext2(nodeID, projNode, sortNode, scanNode,
				[]int32{0}, []*plan.IndexDef{idxdef}, []int32{1}, nil,
				map[int32]int32{0: 0}, makePlan2Uint64ConstExprWithType(200), nil, false)
			require.NoError(t, err)
			require.True(t, handled)
			search := findCoveredFulltext2Scan(t, builder)
			opts, err := ft2plan.DecodeScanOptions(search.IndexSearchScan.AlgoOptions)
			require.NoError(t, err)
			assert.Contains(t, opts.IncludePreds, `"op":"is_null"`)
			if tc.wantLimit == 0 {
				require.Nil(t, fulltextScanLimit(search))
			} else {
				require.NotNil(t, fulltextScanLimit(search))
				assert.Equal(t, tc.wantLimit, fulltextScanLimit(search).GetLit().GetU64Val())
			}
		})
	}
}

func TestCoveredFulltext2ScoreLimitFromSortShapes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		innerSort  bool
		ascending  bool
		innerLimit bool
		wantLimit  bool
	}{
		{name: "overwritten implicit sort", wantLimit: true},
		{name: "retained equivalent sort", innerSort: true, wantLimit: true},
		{name: "non-equivalent inner sort", innerSort: true, ascending: true},
		{name: "bounded inner sort", innerSort: true, innerLimit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scoreType := plan.Type{Id: int32(types.T_float32)}
			search := &plan.Node{
				NodeType: plan.Node_INDEX_SEARCH_SCAN, BindingTags: []int32{7},
				IndexSearchScan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexAlgo: catalog.MoIndexFullText2Algo.ToString()}},
			}
			inner := &plan.Node{
				NodeType: plan.Node_SORT, Children: []int32{0},
				OrderBy: []*plan.OrderBySpec{{Expr: GetColExpr(scoreType, 7, 1), Flag: plan.OrderBySpec_DESC}},
			}
			project := &plan.Node{
				NodeType: plan.Node_PROJECT, Children: []int32{0}, BindingTags: []int32{8},
				ProjectList: []*plan.Expr{GetColExpr(scoreType, 7, 1)},
			}
			if tc.innerSort {
				project.Children[0] = 1
			}
			if tc.ascending {
				inner.OrderBy[0].Flag = plan.OrderBySpec_ASC
			}
			if tc.innerLimit {
				inner.Limit = makePlan2Uint64ConstExprWithType(1)
			}
			outer := &plan.Node{
				NodeType: plan.Node_SORT, Children: []int32{2},
				OrderBy: []*plan.OrderBySpec{{Expr: GetColExpr(scoreType, 8, 0), Flag: plan.OrderBySpec_DESC}},
				Limit:   makePlan2Uint64ConstExprWithType(2), Offset: makePlan2Uint64ConstExprWithType(1),
			}
			builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{search, inner, project}}}
			builder.pushCoveredFulltext2ScoreLimitFromSort(outer)
			if tc.wantLimit {
				require.NotNil(t, search.IndexSearchScan.CandidateLimit)
				assert.Equal(t, uint64(3), search.IndexSearchScan.CandidateLimit.GetLit().GetU64Val())
			} else {
				assert.Nil(t, search.IndexSearchScan.CandidateLimit)
			}
		})
	}
}

func TestCoveredFulltext2ScoreLimitOnBoundSQL(t *testing.T) {
	for _, tc := range []struct {
		name       string
		orderBy    string
		pagination string
		prefix     string
		from       string
		residual   string
		wrap       bool
		wantLimit  uint64
	}{
		{name: "implicit score order", pagination: "LIMIT 2", wantLimit: 2},
		{name: "implicit score order offset", pagination: "LIMIT 2 OFFSET 1", wantLimit: 3},
		{name: "explicit match score", orderBy: "MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE) DESC", pagination: "LIMIT 2", wantLimit: 2},
		{name: "score alias", orderBy: "sc DESC", pagination: "LIMIT 2", wantLimit: 2},
		{name: "score alias offset", orderBy: "sc DESC", pagination: "LIMIT 2 OFFSET 1", wantLimit: 3},
		{name: "nested score page", orderBy: "sc DESC", pagination: "LIMIT 2 OFFSET 1", wrap: true, wantLimit: 3},
		{name: "ascending score", orderBy: "sc ASC", pagination: "LIMIT 2"},
		{name: "different sort", orderBy: "id DESC", pagination: "LIMIT 2"},
		{name: "score with second key", orderBy: "sc DESC, id", pagination: "LIMIT 2"},
		{name: "id before score", orderBy: "id, sc DESC", pagination: "LIMIT 2"},
		{name: "score expression", orderBy: "sc + 1 DESC", pagination: "LIMIT 2"},
		{name: "no limit", orderBy: "sc DESC"},
		{name: "overflow", orderBy: "sc DESC", pagination: "LIMIT 18446744073709551615 OFFSET 1"},
		{name: "found rows", prefix: "SQL_CALC_FOUND_ROWS ", orderBy: "sc DESC", pagination: "LIMIT 2"},
		{name: "residual filter", residual: " AND title <> 'excluded'", orderBy: "sc DESC", pagination: "LIMIT 2"},
		{name: "join", from: "ft JOIN t ON ft.id = t.id", orderBy: "sc DESC", pagination: "LIMIT 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := newIssue24822FullText2Optimizer(t)
			for _, idx := range optimizer.ctx.tables["ft"].Indexes {
				idx.IncludedColumns = []string{"base_id"}
			}
			from := tc.from
			if from == "" {
				from = "ft"
			}
			statement := `SELECT ` + tc.prefix + `ft.id, MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE) AS sc
				FROM ` + from + ` WHERE MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE)
				AND base_id IS NULL` + tc.residual
			if tc.orderBy != "" {
				statement += " ORDER BY " + tc.orderBy
			}
			statement += " " + tc.pagination
			if tc.wrap {
				statement = "SELECT id FROM (" + statement + ") AS page"
			}
			p, err := runOneStmt(optimizer, t, statement)
			require.NoError(t, err)
			query := p.GetQuery()
			planned := &QueryBuilder{qry: query}
			scans := collectFullTextFunctionScans(planned, query.Steps[0])
			require.Len(t, scans, 1)
			search := scans[0]
			if tc.residual == "" && tc.from == "" {
				require.True(t, isFulltext2SearchScan(search), "the query must use the covered path")
				opts, err := ft2plan.DecodeScanOptions(search.IndexSearchScan.AlgoOptions)
				require.NoError(t, err)
				assert.Contains(t, opts.IncludePreds, `"op":"is_null"`)
				require.Equal(t, 1, reachableFullTextOrderSorts(planned, query.Steps[0]),
					"the explicit sort must replace, rather than duplicate, implicit relevance sorting")
				assert.False(t, search.IndexSearchScan.PostFilterOverFetch, "a covered search has no join to drop candidates")
			}
			if tc.wantLimit != 0 {
				require.NotNil(t, fulltextScanLimit(search))
				assert.Equal(t, tc.wantLimit, fulltextScanLimit(search).GetLit().GetU64Val())
			} else {
				require.Nil(t, fulltextScanLimit(search))
			}
		})
	}
}

func TestJoinFulltext2ScoreLimitOnBoundSQL(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prefix     string
		orderBy    string
		pagination string
		from       string
		residual   string
		wantLimit  uint64
	}{
		{name: "implicit order", pagination: "LIMIT 2", wantLimit: 2},
		{name: "score alias", orderBy: "sc DESC", pagination: "LIMIT 2", wantLimit: 2},
		{name: "score alias offset", orderBy: "sc DESC", pagination: "LIMIT 2 OFFSET 1", wantLimit: 3},
		{name: "explicit match score", orderBy: "MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE) DESC", pagination: "LIMIT 2", wantLimit: 2},
		{name: "ascending score", orderBy: "sc ASC", pagination: "LIMIT 2"},
		{name: "different sort", orderBy: "id DESC", pagination: "LIMIT 2"},
		{name: "score with second key", orderBy: "sc DESC, id", pagination: "LIMIT 2"},
		{name: "id before score", orderBy: "id, sc DESC", pagination: "LIMIT 2"},
		{name: "no limit", orderBy: "sc DESC"},
		{name: "residual filter", residual: " AND title <> 'excluded'", orderBy: "sc DESC", pagination: "LIMIT 2"},
		{name: "user join", from: "ft JOIN t ON ft.id = t.id", orderBy: "sc DESC", pagination: "LIMIT 2"},
		{name: "found rows", prefix: "SQL_CALC_FOUND_ROWS ", orderBy: "sc DESC", pagination: "LIMIT 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := newIssue24822FullText2Optimizer(t)
			from := tc.from
			if from == "" {
				from = "ft"
			}
			statement := `SELECT ` + tc.prefix + `ft.id, ft.title, MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE) AS sc
				FROM ` + from + ` WHERE MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE)` + tc.residual
			if tc.orderBy != "" {
				statement += " ORDER BY " + tc.orderBy
			}
			statement += " " + tc.pagination
			p, err := runOneStmt(optimizer, t, statement)
			require.NoError(t, err)
			query := p.GetQuery()
			planned := &QueryBuilder{qry: query}
			scans := collectFullTextFunctionScans(planned, query.Steps[0])
			require.Len(t, scans, 1)
			require.True(t, isFulltext2SearchScan(scans[0]))
			if tc.wantLimit != 0 {
				require.NotNil(t, fulltextScanLimit(scans[0]))
				assert.Equal(t, tc.wantLimit, fulltextScanLimit(scans[0]).GetLit().GetU64Val())
			} else {
				require.Nil(t, fulltextScanLimit(scans[0]))
			}
			assert.Equal(t, tc.wantLimit != 0, scans[0].IndexSearchScan.PostFilterOverFetch,
				"a search bounded below the base-table join over-fetches")
		})
	}
}

func TestFulltext2PeeledIncludeFiltersStayOnBaseScans(t *testing.T) {
	for _, tc := range []struct {
		name       string
		residual   string
		pagination string
		pushdown   int8
		baseScans  int
	}{
		{name: "include only", pagination: "LIMIT 2", baseScans: 1},
		{name: "include and residual", residual: " AND title <> 'excluded'", baseScans: 1},
		{name: "include and residual with membership", residual: " AND title <> 'excluded'", pushdown: 1, baseScans: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := newIssue24822FullText2Optimizer(t)
			for _, idx := range optimizer.ctx.tables["ft"].Indexes {
				idx.IncludedColumns = []string{"base_id"}
			}
			optimizer.ctx.fulltextBloomFilterPushdown = tc.pushdown
			statement := `SELECT ft.id, ft.title FROM ft
				WHERE MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE)
				AND base_id IS NULL` + tc.residual + " " + tc.pagination
			p, err := runOneStmt(optimizer, t, statement)
			require.NoError(t, err)
			query := p.GetQuery()
			planned := &QueryBuilder{qry: query}
			scans := collectFullTextFunctionScans(planned, query.Steps[0])
			require.Len(t, scans, 1)
			opts, err := ft2plan.DecodeScanOptions(scans[0].IndexSearchScan.AlgoOptions)
			require.NoError(t, err)
			require.Contains(t, opts.IncludePreds, `"op":"is_null"`)

			baseScans := 0
			for _, node := range query.Nodes {
				if node.NodeType != plan.Node_TABLE_SCAN || node.TableDef.GetName() != "ft" {
					continue
				}
				baseScans++
				require.Contains(t, FormatExprs(node.FilterList, FormatOption{}), "base_id",
					"base scan %d must re-check the peeled include predicate", node.NodeId)
			}
			require.Equal(t, tc.baseScans, baseScans)
		})
	}
}

func TestFulltext2ScoreLimitParityWithIncludeFilters(t *testing.T) {
	for _, tc := range []struct {
		name      string
		prefix    string
		orderBy   string
		residual  string
		wantLimit uint64
	}{
		{name: "implicit order", wantLimit: 2},
		{name: "explicit score order", orderBy: "sc DESC", wantLimit: 2},
		{name: "explicit match score", orderBy: "MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE) DESC", wantLimit: 2},
		{name: "implicit order with residual", residual: " AND title <> 'excluded'"},
		{name: "explicit score order with residual", orderBy: "sc DESC", residual: " AND title <> 'excluded'"},
		{name: "explicit score order found rows", prefix: "SQL_CALC_FOUND_ROWS ", orderBy: "sc DESC"},
		{name: "explicit score order with tiebreak", orderBy: "sc DESC, id"},
		{name: "id before score", orderBy: "id, sc DESC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := newIssue24822FullText2Optimizer(t)
			for _, idx := range optimizer.ctx.tables["ft"].Indexes {
				idx.IncludedColumns = []string{"base_id"}
			}
			statement := `SELECT ` + tc.prefix + `ft.id, ft.title, MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE) AS sc
				FROM ft WHERE MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE)
				AND base_id IS NULL` + tc.residual
			if tc.orderBy != "" {
				statement += " ORDER BY " + tc.orderBy
			}
			statement += " LIMIT 2"
			p, err := runOneStmt(optimizer, t, statement)
			require.NoError(t, err)
			query := p.GetQuery()
			planned := &QueryBuilder{qry: query}
			scans := collectFullTextFunctionScans(planned, query.Steps[0])
			require.Len(t, scans, 1)
			if tc.wantLimit != 0 {
				require.NotNil(t, fulltextScanLimit(scans[0]))
				assert.Equal(t, tc.wantLimit, fulltextScanLimit(scans[0]).GetLit().GetU64Val())
			} else {
				require.Nil(t, fulltextScanLimit(scans[0]))
			}
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.GetName() == "ft" {
					require.Contains(t, FormatExprs(node.FilterList, FormatOption{}), "base_id")
				}
			}
		})
	}
}

func TestFulltext2ScoreLimitParityWithMembershipPrefilter(t *testing.T) {
	scanLimit := func(t *testing.T, pattern string, pushdown int8, orderBy, limit string) *plan.Expr {
		optimizer := newIssue24822FullText2Optimizer(t)
		optimizer.ctx.fulltextBloomFilterPushdown = pushdown
		pkType := plan.Type{Id: int32(types.T_int64)}
		ftDef := optimizer.ctx.tables["ft"]
		ftDef.Cols[ftDef.Name2ColIndex["id"]].Typ = pkType
		for _, idx := range ftDef.Indexes {
			optimizer.ctx.tables[idx.IndexTableName].Cols[1].Typ = pkType
		}
		statement := `SELECT ft.id, ft.title, MATCH(title, body) AGAINST("` + pattern + `" IN BOOLEAN MODE) AS sc
			FROM ft WHERE MATCH(title, body) AGAINST("` + pattern + `" IN BOOLEAN MODE)
			AND title <> "excluded"`
		if orderBy != "" {
			statement += " ORDER BY " + orderBy
		}
		statement += " " + limit
		if strings.Contains(limit, "?") {
			statement = "prepare p from '" + statement + "'"
		}
		p, err := runOneStmt(optimizer, t, statement)
		require.NoError(t, err)
		query := p.GetQuery()
		if prepare := p.GetDcl().GetPrepare(); prepare != nil {
			query = prepare.Plan.GetQuery()
		}
		scans := collectFullTextFunctionScans(&QueryBuilder{qry: query}, query.Steps[0])
		require.Len(t, scans, 1)
		return fulltextScanLimit(scans[0])
	}
	for _, tc := range []struct {
		name      string
		pattern   string
		pushdown  int8
		limit     string
		wantLimit uint64
	}{
		{name: "exact membership", pattern: "+hello", pushdown: 1, limit: "LIMIT 2 OFFSET 1", wantLimit: 3},
		{name: "membership off", pattern: "+hello", limit: "LIMIT 2"},
		{name: "should term", pattern: "hello", pushdown: 1, limit: "LIMIT 2"},
		{name: "prepared limit", pattern: "+hello", pushdown: 1, limit: "LIMIT ?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			implicit := scanLimit(t, tc.pattern, tc.pushdown, "", tc.limit)
			explicit := scanLimit(t, tc.pattern, tc.pushdown, "sc DESC", tc.limit)
			if tc.wantLimit == 0 {
				require.Nil(t, implicit)
				require.Nil(t, explicit)
				return
			}
			require.NotNil(t, implicit)
			require.NotNil(t, explicit)
			assert.Equal(t, tc.wantLimit, implicit.GetLit().GetU64Val())
			assert.Equal(t, tc.wantLimit, explicit.GetLit().GetU64Val())
		})
	}
}

// The new direct SEARCH shape is covered by the bound-SQL tests above. Keep
// the legacy implicit-sort shape and its pagination/key barriers valid too.
func TestPushCoveredFulltext2ScoreLimitLegacySort(t *testing.T) {
	for _, mode := range []string{"equivalent", "paginated", "non_column_key", "different_score_tag"} {
		t.Run(mode, func(t *testing.T) {
			score := plan.Type{Id: int32(types.T_float32)}
			search := &plan.Node{NodeType: plan.Node_INDEX_SEARCH_SCAN, BindingTags: []int32{7},
				IndexSearchScan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexAlgo: catalog.MoIndexFullText2Algo.ToString()}}}
			inner := &plan.Node{NodeType: plan.Node_SORT, Children: []int32{0},
				OrderBy: []*plan.OrderBySpec{{Expr: GetColExpr(score, 7, 1), Flag: plan.OrderBySpec_DESC}}}
			project := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{1}, BindingTags: []int32{8},
				ProjectList: []*plan.Expr{GetColExpr(score, 7, 1)}}
			outer := &plan.Node{NodeType: plan.Node_SORT, Children: []int32{2}, Limit: makePlan2Uint64ConstExprWithType(2),
				OrderBy: []*plan.OrderBySpec{{Expr: GetColExpr(score, 8, 0), Flag: plan.OrderBySpec_DESC}}}
			switch mode {
			case "paginated":
				inner.Limit = makePlan2Uint64ConstExprWithType(1)
			case "non_column_key":
				inner.OrderBy[0].Expr = makePlan2Uint64ConstExprWithType(1)
			case "different_score_tag":
				inner.OrderBy[0].Expr.GetCol().RelPos = 9
			}
			builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{search, inner, project, outer}}}
			builder.pushCoveredFulltext2ScoreLimitFromSort(outer)
			if mode == "equivalent" {
				require.NotNil(t, search.IndexSearchScan.CandidateLimit)
				require.Equal(t, uint64(2), search.IndexSearchScan.CandidateLimit.GetLit().GetU64Val())
			} else {
				require.Nil(t, search.IndexSearchScan.CandidateLimit)
			}
		})
	}
}

// The score sort bounds a recorded join's search only while that join itself still
// carries no limit, offset, filter or non-INNER type.
func TestJoinFulltext2ScoreLimitRechecksRecordedJoin(t *testing.T) {
	for _, mode := range []string{"clean", "join limit", "join offset", "join filter", "left join", "literal only prepared"} {
		t.Run(mode, func(t *testing.T) {
			score := plan.Type{Id: int32(types.T_float32)}
			search := &plan.Node{NodeType: plan.Node_INDEX_SEARCH_SCAN, BindingTags: []int32{7},
				IndexSearchScan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexAlgo: catalog.MoIndexFullText2Algo.ToString()}}}
			base := &plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{6}}
			join := &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{1, 0}}
			project := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{2}, BindingTags: []int32{8},
				ProjectList: []*plan.Expr{GetColExpr(score, 7, 1)}}
			outer := &plan.Node{NodeType: plan.Node_SORT, Children: []int32{3}, Limit: makePlan2Uint64ConstExprWithType(2),
				OrderBy: []*plan.OrderBySpec{{Expr: GetColExpr(score, 8, 0), Flag: plan.OrderBySpec_DESC}}}
			builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{search, base, join, project, outer}}}
			recorded := fulltextBoundableJoin{search: 0}
			switch mode {
			case "join limit":
				join.Limit = makePlan2Uint64ConstExprWithType(1)
			case "join offset":
				join.Offset = makePlan2Uint64ConstExprWithType(1)
			case "join filter":
				join.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)}
			case "left join":
				join.JoinType = plan.Node_LEFT
			case "literal only prepared":
				recorded.literalOnly = true
				outer.Limit = &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
			}
			builder.fulltextBoundableJoins = map[int32]fulltextBoundableJoin{2: recorded}
			builder.pushJoinFulltext2ScoreLimitFromSort(outer)
			if mode == "clean" {
				require.NotNil(t, search.IndexSearchScan.CandidateLimit)
				require.Equal(t, uint64(2), search.IndexSearchScan.CandidateLimit.GetLit().GetU64Val())
			} else {
				require.Nil(t, search.IndexSearchScan.CandidateLimit)
			}
		})
	}
}

// The SQL tests prove reachability; these typed controls exercise each barrier
// around the optional inner sort without constructing a service or corpus.
func TestCoveredFulltext2ScoreLimitSortBarriers(t *testing.T) {
	type nodes struct{ outer, project, inner, search *plan.Node }
	type testCase struct {
		name   string
		mutate func(nodes)
	}
	for _, withInner := range []bool{false, true} {
		cases := []testCase{
			{name: "equivalent"},
			{name: "ascending", mutate: func(n nodes) { n.outer.OrderBy[0].Flag = plan.OrderBySpec_ASC }},
			{name: "secondary key", mutate: func(n nodes) { n.outer.OrderBy = append(n.outer.OrderBy, n.outer.OrderBy[0]) }},
			{name: "non-score key first", mutate: func(n nodes) {
				first := &plan.OrderBySpec{Expr: GetColExpr(n.outer.OrderBy[0].Expr.Typ, n.outer.OrderBy[0].Expr.GetCol().RelPos, 0), Flag: plan.OrderBySpec_DESC}
				n.outer.OrderBy = append([]*plan.OrderBySpec{first}, n.outer.OrderBy...)
			}},
			{name: "different score", mutate: func(n nodes) { n.project.ProjectList[1].GetCol().ColPos = 0 }},
			{name: "expression projection", mutate: func(n nodes) { n.project.ProjectList[0] = makePlan2Uint64ConstExprWithType(1) }},
			{name: "foreign projection", mutate: func(n nodes) { n.project.ProjectList[0].GetCol().RelPos++ }},
			{name: "project filter", mutate: func(n nodes) { n.project.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)} }},
			{name: "search filter", mutate: func(n nodes) { n.search.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)} }},
			{name: "join", mutate: func(n nodes) { n.search.NodeType = plan.Node_JOIN }},
			{name: "project limit", mutate: func(n nodes) { n.project.Limit = makePlan2Uint64ConstExprWithType(1) }},
			{name: "search offset", mutate: func(n nodes) { n.search.Offset = makePlan2Uint64ConstExprWithType(1) }},
			{name: "outer rank", mutate: func(n nodes) { n.outer.RankOption = &plan.RankOption{} }},
			{name: "project rank", mutate: func(n nodes) { n.project.RankOption = &plan.RankOption{} }},
			{name: "search rank", mutate: func(n nodes) { n.search.RankOption = &plan.RankOption{} }},
			{name: "overflow", mutate: func(n nodes) { n.outer.Limit = makePlan2Uint64ConstExprWithType(^uint64(0)) }},
		}
		if withInner {
			cases = append(cases,
				testCase{name: "inner ascending", mutate: func(n nodes) { n.inner.OrderBy[0].Flag = plan.OrderBySpec_ASC }},
				testCase{name: "inner different key", mutate: func(n nodes) { n.inner.OrderBy[0].Expr.GetCol().ColPos = 0 }},
				testCase{name: "inner constant ordering", mutate: func(n nodes) { n.inner.OrderBy[0].Expr = makePlan2Int64ConstExprWithType(1) }},
				testCase{name: "inner secondary key", mutate: func(n nodes) { n.inner.OrderBy = append(n.inner.OrderBy, n.inner.OrderBy[0]) }},
				testCase{name: "inner filter", mutate: func(n nodes) { n.inner.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)} }},
				testCase{name: "inner limit", mutate: func(n nodes) { n.inner.Limit = makePlan2Uint64ConstExprWithType(1) }},
				testCase{name: "inner rank", mutate: func(n nodes) { n.inner.RankOption = &plan.RankOption{} }},
			)
		}
		shape := "direct/"
		if withInner {
			shape = "implicit sort/"
		}
		for _, tc := range cases {
			t.Run(shape+tc.name, func(t *testing.T) {
				typ := plan.Type{Id: int32(types.T_float32)}
				search := &plan.Node{NodeType: plan.Node_INDEX_SEARCH_SCAN, BindingTags: []int32{1},
					IndexSearchScan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexAlgo: catalog.MoIndexFullText2Algo.ToString()}}}
				b := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{search}}}
				child := int32(0)
				var inner *plan.Node
				if withInner {
					inner = &plan.Node{NodeType: plan.Node_SORT, Children: []int32{child},
						OrderBy: []*plan.OrderBySpec{{Expr: GetColExpr(typ, 1, 1), Flag: plan.OrderBySpec_DESC}}}
					child = int32(len(b.qry.Nodes))
					b.qry.Nodes = append(b.qry.Nodes, inner)
				}
				project := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{child}, BindingTags: []int32{2},
					ProjectList: []*plan.Expr{GetColExpr(typ, 1, 0), GetColExpr(typ, 1, 1)}}
				projectID := int32(len(b.qry.Nodes))
				b.qry.Nodes = append(b.qry.Nodes, project)
				outer := &plan.Node{NodeType: plan.Node_SORT, Children: []int32{projectID},
					OrderBy: []*plan.OrderBySpec{{Expr: GetColExpr(typ, 2, 1), Flag: plan.OrderBySpec_DESC}},
					Limit:   makePlan2Uint64ConstExprWithType(2), Offset: makePlan2Uint64ConstExprWithType(1)}
				if tc.mutate != nil {
					tc.mutate(nodes{outer, project, inner, search})
				}
				b.pushCoveredFulltext2ScoreLimitFromSort(outer)
				if tc.mutate == nil {
					require.NotNil(t, search.IndexSearchScan.CandidateLimit)
					require.Equal(t, uint64(3), search.IndexSearchScan.CandidateLimit.GetLit().GetU64Val())
				} else {
					require.Nil(t, search.IndexSearchScan.CandidateLimit)
				}
				// Pagination still belongs to the outer sort in either outcome.
				require.NotNil(t, outer.Limit)
				require.Equal(t, uint64(1), outer.Offset.GetLit().GetU64Val())
			})
		}
	}
}

// findCoveredFulltext2Scan returns the single fulltext2 index search scan appended by the
// rewrite.
func findCoveredFulltext2Scan(t *testing.T, builder *QueryBuilder) *plan.Node {
	t.Helper()
	var found *plan.Node
	for _, n := range builder.qry.Nodes {
		if isFulltext2SearchScan(n) {
			require.Nil(t, found, "expected exactly one fulltext2 index search scan")
			found = n
		}
	}
	require.NotNil(t, found, "the covered rewrite must append a fulltext2 index search scan")
	return found
}
