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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func reachableIndexOrderNodes(p *Plan) []*planpb.Node {
	if p == nil || p.GetQuery() == nil {
		return nil
	}
	q := p.GetQuery()
	seen := make(map[int32]bool)
	var result []*planpb.Node
	var walk func(int32)
	walk = func(id int32) {
		if id < 0 || int(id) >= len(q.Nodes) || seen[id] || q.Nodes[id] == nil {
			return
		}
		seen[id] = true
		n := q.Nodes[id]
		result = append(result, n)
		for _, child := range n.Children {
			walk(child)
		}
	}
	for _, root := range q.Steps {
		walk(root)
	}
	return result
}

func TestForceOrderPortPlans(t *testing.T) {
	tests := []struct {
		name, sql, index string
		wantSort         bool
	}{
		{"unusable noncovering forced order", "select id,a,b from index_hint_t force index(idx_a) order by b,id limit 2", "", true},
		{"nonindex filter cannot invent backfill", "select id,a,b from index_hint_t force index(idx_a) where b=1 order by b,id", "", true},
		{"covering remains useful without matching order", "select id,a from index_hint_t force index(idx_a) order by id", "idx_a", true},
		{"matching order keeps noncovering access", "select id,a,b from index_hint_t force index(idx_a) order by a,id limit 2", "idx_a", true},
		{"forced range remains usable despite low selectivity", "select id,a,b from index_hint_t force index(idx_a) where a>=0 order by b,id", "idx_a", true},
		{"bounded plain force", "select id,a,b from index_hint_t force index(idx_ab) where a=1 and b in (1,2) order by id", "idx_ab", true},
		{"fixed prefix order scope", "select id,a,b from index_hint_t force index for order by(idx_ab) where a=1 order by b", "idx_ab", true},
		{"ignored primary", "select id,a,b from index_hint_t force index(primary,idx_ab) ignore index(primary) where a=1 and b in (1,2) order by id", "idx_ab", true},
		{"nullable unique incomplete", "select id from index_hint_t force index for order by(uk_ab) where a=1 order by a", "", true},
		{"nullable unique group", "select a,b,count(*) from index_hint_t force index for group by(uk_ab) group by a,b", "", false},
		{"nullable unique fully constrained", "select id,a,b from index_hint_t force index for order by(uk_ab) where a=1 and b=2 order by a,b", "uk_ab", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t, tt.sql)
			require.NoError(t, err)
			var index string
			var hasSort bool
			for _, n := range reachableIndexOrderNodes(p) {
				if n.IndexScanInfo.IsIndexScan {
					index = n.IndexScanInfo.IndexName
				}
				hasSort = hasSort || n.NodeType == planpb.Node_SORT
			}
			require.Equal(t, tt.index, index)
			require.Equal(t, tt.wantSort, hasSort)
		})
	}
}

func TestForceOrderPortJoinCoveringColumns(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	queryPlan, err := runOneStmt(mock, t, `
		select r.id, r.a, r.b
		from index_hint_t l join index_hint_t r force index(idx_ab) on l.id = r.id
		where r.a = 1 and r.b in (1,2)
		order by r.b, r.id`)
	require.NoError(t, err)
	require.Contains(t, reachableIndexScanNames(queryPlan.GetQuery()), "idx_ab")
}

func TestIndexOrderFixedPrefixProof(t *testing.T) {
	const tag int32 = 41
	eq := func(rel int32) *planpb.Expr {
		e := makeEqFilterExpr(1)
		e.GetF().Args[0].GetCol().RelPos = rel
		return e
	}
	commuted := eq(tag)
	commuted.GetF().Args[0], commuted.GetF().Args[1] = commuted.GetF().Args[1], commuted.GetF().Args[0]
	parameter := eq(tag)
	parameter.GetF().Args[1] = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	otherColumn := eq(tag)
	otherColumn.GetF().Args[1] = GetColExpr(planpb.Type{Id: int32(types.T_int32)}, tag, 2)
	rangeFilter := eq(tag)
	rangeFilter.GetF().Func.ObjName = ">"
	volatileValue, err := BindFuncExprImplByPlanExpr(context.Background(), "rand", nil)
	require.NoError(t, err)
	require.False(t, isRuntimeConstExpr(volatileValue))
	volatileFilter := eq(tag)
	volatileFilter.GetF().Args[1] = volatileValue
	for _, tc := range []struct {
		name     string
		filter   *planpb.Expr
		params   string
		floatKey bool
		order    []int32
		want     bool
	}{
		{name: "literal", filter: eq(tag), order: []int32{2, 0}, want: true},
		{name: "commuted", filter: commuted, order: []int32{2}, want: true},
		{name: "parameter", filter: parameter, order: []int32{2}, want: true},
		{name: "fixed order key", filter: eq(tag), order: []int32{1, 2}, want: true},
		{name: "other binding", filter: eq(tag + 1), order: []int32{2}},
		{name: "column comparison", filter: otherColumn, order: []int32{2}},
		{name: "volatile value", filter: volatileFilter, order: []int32{2}},
		{name: "range", filter: rangeFilter, order: []int32{2}},
		{name: "unconstrained gap", filter: eq(tag), order: []int32{0}},
		{name: "float signed zero", filter: eq(tag), floatKey: true, order: []int32{2}},
		{name: "truncated prefix", filter: eq(tag), params: `{"prefix_lengths":"a:4"}`, order: []int32{2}},
		{name: "invalid metadata", filter: eq(tag), params: `{bad`, order: []int32{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intType := planpb.Type{Id: int32(types.T_int32)}
			table := &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "id", Typ: intType}, {Name: "a", Typ: intType}, {Name: "b", Typ: intType}}, Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2}}
			if tc.floatKey {
				table.Cols[1].Typ.Id = int32(types.T_float64)
			}
			scan := &planpb.Node{TableDef: table, BindingTags: []int32{tag}, FilterList: []*planpb.Expr{tc.filter}}
			idx := &planpb.IndexDef{Parts: []string{"a", "b", "id"}, IndexAlgoParams: tc.params}
			require.Equal(t, tc.want, indexOrderColumnsMatch(idx, scan, tc.order))
		})
	}
}

func TestForceOrderScopeDoesNotFixVolatilePrefix(t *testing.T) {
	for _, predicate := range []string{"a = cast(floor(rand() * 2) as signed)", "cast(floor(rand() * 2) as signed) = a"} {
		t.Run(predicate, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t, "select id,a,b from index_hint_t force index for order by(idx_ab) where "+predicate+" order by b,id")
			require.NoError(t, err)
			require.NotContains(t, reachableIndexScanNames(p.GetQuery()), "idx_ab")
		})
	}
}

func TestForceOrderRetainsBoundedKeySet(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	table.Name2ColIndex["payload"] = int32(len(table.Cols))
	table.Cols = append(table.Cols, &planpb.ColDef{Name: "payload", OriginName: "payload", Typ: planpb.Type{Id: int32(types.T_int32)}, Default: &planpb.Default{NullAbility: true}})
	p, err := runOneStmt(mock, t, `select id,payload from index_hint_t force index(idx_ab)
		where a=1 and b in (1,2) order by payload,id`)
	require.NoError(t, err)
	var index *planpb.Node
	for _, n := range reachableIndexOrderNodes(p) {
		if n.IndexScanInfo.IsIndexScan && n.IndexScanInfo.IndexName == "idx_ab" {
			index = n
		}
	}
	require.NotNil(t, index)
	found := false
	for _, filter := range index.FilterList {
		if f := filter.GetF(); f != nil && f.Func != nil && f.Func.ObjName == "prefix_in" {
			found = true
		}
	}
	require.True(t, found, "forced key-set access should remain bounded")
	require.Empty(t, index.OrderBy)
	require.Nil(t, index.IndexReaderParam)
}

func TestForceOrderAndJoinScopedHintsKeepBindings(t *testing.T) {
	for _, joinIndex := range []string{"idx_a", "idx_ab"} {
		t.Run(joinIndex, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			query := `select r.id,r.a,r.b from index_hint_t r force index for join(` + joinIndex + `)
				force index for order by(idx_ab) join index_hint_t l on r.id=l.id
				where r.a=1 order by r.b,r.id`
			_, err := runOneStmt(mock, t, query)
			require.NoError(t, err)
		})
	}
}

func TestForceOrderSharedScanUsesOneAccess(t *testing.T) {
	for _, tc := range []struct {
		name         string
		firstOrdered bool
		single       bool
	}{
		{name: "both ordered", firstOrdered: true},
		{name: "unordered then ordered"},
		{name: "single unordered", single: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
			ctx := NewBindContext(builder, nil)
			table := mock.ctxt.tables["index_hint_t"]
			tag := builder.genNewBindTag()
			a := GetColExpr(table.Cols[1].Typ, tag, 1)
			b := GetColExpr(table.Cols[2].Typ, tag, 2)
			equal, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{a, MakePlan2Int32ConstExprWithType(1)})
			require.NoError(t, err)
			greater, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{b, MakePlan2Int32ConstExprWithType(1)})
			require.NoError(t, err)
			scanID := builder.appendNode(&planpb.Node{
				NodeType: planpb.Node_TABLE_SCAN, TableDef: table, ObjRef: mock.ctxt.objects["index_hint_t"],
				BindingTags: []int32{tag}, FilterList: []*planpb.Expr{equal, greater},
				Stats: &planpb.Stats{TableCnt: 1000, Outcnt: 10, Selectivity: 0.01, Cost: 1000},
			}, ctx)
			require.NoError(t, builder.recordIndexHints(scanID, table, []*tree.IndexHint{{HintType: tree.HintForce, HintScope: tree.HintForScan, IndexNames: []string{"idx_ab"}}}))
			id := GetColExpr(table.Cols[0].Typ, tag, 0)
			firstLimit, secondLimit := MakePlan2Int64ConstExprWithType(1), MakePlan2Int64ConstExprWithType(2)
			firstNode := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{scanID}, ProjectList: []*planpb.Expr{id}, Limit: firstLimit}
			if tc.firstOrdered {
				firstNode = &planpb.Node{NodeType: planpb.Node_SORT, Children: []int32{scanID}, OrderBy: []*planpb.OrderBySpec{{Expr: id}}, Limit: firstLimit}
			}
			first := builder.appendNode(firstNode, ctx)
			second := builder.appendNode(&planpb.Node{NodeType: planpb.Node_SORT, Children: []int32{scanID}, OrderBy: []*planpb.OrderBySpec{{Expr: a}, {Expr: b}}, Limit: secondLimit}, ctx)
			root := builder.appendNode(&planpb.Node{NodeType: planpb.Node_UNION_ALL, Children: []int32{first, second}}, ctx)
			if tc.single {
				root = first
			}
			refs := map[[2]int32]int{{tag, 0}: 1, {tag, 1}: 1, {tag, 2}: 1}
			colMap := make(map[[2]int32]*planpb.Expr)
			_, err = builder.applyForceIndexHints(root, nil, refs, colMap)
			require.NoError(t, err)
			if tc.single {
				require.False(t, builder.isScanProtected(scanID))
				_, err = builder.applyIndices(root, refs, colMap)
				require.NoError(t, err)
				require.NotEqual(t, scanID, builder.qry.Nodes[first].Children[0])
				return
			}
			firstAccess := builder.qry.Nodes[first].Children[0]
			secondAccess := builder.qry.Nodes[second].Children[0]
			require.Equal(t, firstAccess, secondAccess, "a shared scan must not publish two incompatible column mappings")
			if tc.firstOrdered {
				require.NotEqual(t, scanID, firstAccess)
				indexScan := builder.qry.Nodes[firstAccess]
				require.True(t, indexScan.IndexScanInfo.IsIndexScan)
				require.Empty(t, indexScan.OrderBy)
				require.Nil(t, indexScan.IndexReaderParam)
			} else {
				require.Equal(t, scanID, firstAccess)
				require.True(t, builder.isScanProtected(scanID))
				require.Empty(t, colMap)
				_, err = builder.applyIndices(root, refs, colMap)
				require.NoError(t, err)
				require.Empty(t, colMap)
			}
			require.Equal(t, firstLimit, builder.qry.Nodes[first].Limit)
			require.Equal(t, secondLimit, builder.qry.Nodes[second].Limit)
		})
	}
}

func TestForceJoinTruncatedKeyPrefix(t *testing.T) {
	for _, tc := range []struct {
		name, params           string
		matchSecond, wantIndex bool
	}{
		{name: "matched leading prefix", params: `{"prefix_lengths":"a:4"}`},
		{name: "unmatched suffix prefix", params: `{"prefix_lengths":"b:4"}`, wantIndex: true},
		{name: "matched suffix prefix uses earlier full part", params: `{"prefix_lengths":"b:4"}`, matchSecond: true, wantIndex: true},
		{name: "malformed metadata", params: `{bad`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, joinID, scanID, table := makeIndexHintJoinBuilder(t)
			table.Indexes = []*planpb.IndexDef{{IndexName: "ix_pref", IndexTableName: "idx_join_a_table", Parts: []string{"a", "b", catalog.CreateAlias("id")}, IndexAlgoParams: tc.params, TableExist: true}}
			join := builder.qry.Nodes[joinID]
			right := builder.qry.Nodes[join.Children[1]]
			strType := planpb.Type{Id: int32(types.T_varchar), Width: 20}
			if tc.params == `{"prefix_lengths":"a:4"}` {
				table.Cols[1].Typ, right.TableDef.Cols[1].Typ = strType, strType
				join.OnList[0].GetF().Args[0].Typ, join.OnList[0].GetF().Args[1].Typ = strType, strType
			}
			if tc.params == `{"prefix_lengths":"b:4"}` {
				table.Cols[2].Typ, right.TableDef.Cols[0].Typ = strType, strType
			}
			if tc.matchSecond {
				join.OnList = append(join.OnList, ftjMakeEqExpr(t,
					ftjColExpr(table, builder.qry.Nodes[scanID].BindingTags[0], 2),
					ftjColExpr(right.TableDef, right.BindingTags[0], 0)))
			}
			require.NoError(t, builder.recordIndexHints(scanID, table, []*tree.IndexHint{{HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"ix_pref"}}}))
			_, err := builder.applyIndicesForJoins(joinID, join, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			if tc.wantIndex {
				require.NotEqual(t, scanID, join.Children[0])
				access := builder.qry.Nodes[join.Children[0]]
				require.Equal(t, planpb.Node_INDEX, access.JoinType)
				require.NotEmpty(t, builder.qry.Nodes[access.Children[1]].RuntimeFilterProbeList)
				require.Len(t, join.RuntimeFilterBuildList, 1)
				require.Len(t, join.RuntimeFilterBuildList[0].KeyComponentProbeTypes, 1)
			} else {
				require.Equal(t, scanID, join.Children[0])
				require.Empty(t, join.RuntimeFilterBuildList)
			}
		})
	}
}

func TestForcedJoinAccessUsesSupportedPaths(t *testing.T) {
	tests := []struct {
		name, index, where, order string
		want                      bool
	}{
		{"covering without order", "idx_ab", "t.b in (1,2,3)", "", true},
		{"covering with order", "idx_ab", "t.b in (1,2,3)", " order by t.b,t.id", true},
		{"unusable without order", "idx_a", "t.b in (1,2,3)", "", false},
		{"unusable with order", "idx_a", "t.b in (1,2,3)", " order by t.b,t.id", false},
		{"bounded static filter after JOIN lookup declines", "idx_a", "t.a=7 and t.b in (1,2,3)", " order by t.b,t.id", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t, "select t.id,t.a,t.b from (values row(7),row(8)) k(a) join index_hint_t t force index("+tc.index+") on t.a=k.a where "+tc.where+tc.order)
			require.NoError(t, err)
			indexes := reachableIndexScanNames(p.GetQuery())
			if tc.want {
				require.Contains(t, indexes, tc.index)
			} else {
				require.NotContains(t, indexes, tc.index)
			}
		})
	}
}
func TestPlainForceCoveringOrderKeepsReaderLimit(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	p, err := runOneStmt(mock, t, "select id,a,b from index_hint_t force index(idx_ab) order by a,b,id limit 2")
	require.NoError(t, err)
	var index *planpb.Node
	for _, node := range reachableIndexOrderNodes(p) {
		if node.IndexScanInfo.IsIndexScan && node.IndexScanInfo.IndexName == "idx_ab" {
			index = node
		}
	}
	require.NotNil(t, index)
	require.NotEmpty(t, index.OrderBy)
	require.NotNil(t, index.IndexReaderParam)
}

func TestForceOrderReaderLimitDoesNotCrossJoin(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	p, err := runOneStmt(mock, t, `select t.id,t.a from index_hint_t t force index for order by(idx_a)
		join index_hint_t u on t.id=u.id order by t.a limit 1`)
	require.NoError(t, err)
	var index *planpb.Node
	for _, node := range reachableIndexOrderNodes(p) {
		if node.IndexScanInfo.IsIndexScan && node.IndexScanInfo.IndexName == "idx_a" {
			index = node
		}
	}
	require.NotNil(t, index)
	require.Nil(t, index.IndexReaderParam, "a JOIN can discard rows before the final LIMIT")
}

func TestPlainForceAndOrderScopeUseAllowedIndex(t *testing.T) {
	for _, query := range []string{
		"select id,a,b from index_hint_t force index(idx_ab) force index for order by(idx_a) order by a limit 2",
		"select id,a,b from index_hint_t force index(idx_ab) force index for order by(primary) order by id limit 2",
	} {
		t.Run(query, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t, query)
			require.NoError(t, err)
			require.Equal(t, []string{"idx_ab"}, reachableIndexScanNames(p.GetQuery()))
		})
	}
}

type backfillTopStatsContext struct {
	*MockCompilerContext
	rows float64
}

func (c *backfillTopStatsContext) Stats(_ *ObjectRef, _ *Snapshot) (*statsinfo.StatsInfo, error) {
	if c.rows == 0 {
		return nil, nil
	}
	return &statsinfo.StatsInfo{
		TableCnt: c.rows, BlockNumber: 128,
		NdvMap: map[string]float64{"id": c.rows, "a": 1000, "b": 1000,
			catalog.IndexTableIndexColName: c.rows, catalog.IndexTablePrimaryColName: c.rows},
	}, nil
}

func TestRegularIndexBackfillTop(t *testing.T) {
	tests := []struct {
		name, sql string
		want      uint64
	}{
		{"prefix", "where a=1 order by b,id limit 2", 2},
		{"full index order", "order by a,b,id limit 2", 2},
		{"wide trailing index value", "where a=1 order by b,id limit 2", 2},
		{"descending", "where a=1 order by b desc,id desc limit 2", 2},
		{"mixed", "where a=1 order by b asc,id desc limit 2", 2},
		{"offset", "where a=1 order by b,id limit 2 offset 3", 5},
		{"mapped residual", "where a=1 and mod(b,2)=0 order by b,id limit 2", 2},
		{"unmapped residual", "where a=1 and mod(payload,2)=0 order by b,id limit 2", 0},
		{"incomplete order", "where a=1 order by b limit 2", 0},
		{"unmapped order", "where a=1 order by payload,id limit 2", 0},
		{"volatile residual", "where a=1 and rand()>0.5 order by b,id limit 2", 0},
		{"unlimited", "where a=1 order by b,id", 0},
		{"bound overflow", "where a=1 order by b,id limit 18446744073709551615 offset 1", 0},
		{"unknown cardinality", "where a=1 order by b,id limit 2", 0},
		{"small input", "where a=1 order by b,id limit 2", 0},
		{"large candidate budget", "where a=1 order by b,id limit 2000", 0},
		{"wide order key", "where a=1 order by b,id limit 2", 0},
		{"wide filter key", "where a='key' order by b,id limit 2", 0},
		{"repeated order key", "where a=1 order by b desc,b asc,id limit 2", 0},
		{"volatile projection", "where a=1 order by b,id limit 2", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			table.Cols = append(table.Cols, &planpb.ColDef{ColId: 4, Name: "payload", OriginName: "payload", Typ: planpb.Type{Id: int32(types.T_int32)}, Default: &planpb.Default{NullAbility: true}})
			table.Name2ColIndex["payload"] = 4
			ctx := &backfillTopStatsContext{MockCompilerContext: &mock.ctxt, rows: 1000000}
			selectList := "id,a,b,payload"
			switch tc.name {
			case "wide trailing index value":
				table.Cols[4].Typ = planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
				for _, index := range table.Indexes {
					if index.IndexName == "idx_ab" {
						index.Parts = []string{"a", "b", "id", "payload", index.Parts[len(index.Parts)-1]}
					}
				}
				other := int32(len(table.Cols))
				table.Name2ColIndex["other"] = other
				table.Cols = append(table.Cols, &planpb.ColDef{ColId: uint64(other), Name: "other", OriginName: "other", Typ: planpb.Type{Id: int32(types.T_int32)}, Default: &planpb.Default{NullAbility: true}})
				selectList += ",other"
			case "unknown cardinality":
				ctx.rows = 0
			case "small input":
				ctx.rows = 1
			case "large candidate budget":
				ctx.rows = 2000
			case "wide order key":
				table.Cols[2].Typ = planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
			case "wide filter key":
				table.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
			case "volatile projection":
				selectList += ",rand()"
			}
			stmts, err := mysql.Parse(ctx.GetContext(), "select "+selectList+" from index_hint_t force index(idx_ab) "+tc.sql, 1)
			require.NoError(t, err)
			p, err := BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)
			q := p.GetQuery()
			var bound uint64
			var readerLimit uint64
			var hasReaderLimit bool
			var joins int
			seen := map[int32]bool{}
			var walk func(int32)
			walk = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				n := q.Nodes[id]
				if n.IndexScanInfo.IsIndexScan && n.IndexReaderParam != nil {
					var ok bool
					readerLimit, ok = getLiteralUint64(n.IndexReaderParam.Limit)
					require.True(t, ok)
					hasReaderLimit = true
				}
				if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_INDEX {
					joins++
					var inspect func(int32)
					inspect = func(s int32) {
						v := q.Nodes[s]
						if v.NodeType == planpb.Node_SORT {
							if tc.name == "mapped residual" {
								require.Len(t, v.OrderBy, 1)
								require.Equal(t, int32(types.T_varchar), v.OrderBy[0].Expr.Typ.Id, "reuse the exact serialized order after residual evaluation")
							}
							x, ok := getLiteralUint64(v.Limit)
							require.True(t, ok)
							require.Nil(t, v.Offset)
							bound = x
						}
						for _, c := range v.Children {
							inspect(c)
						}
					}
					inspect(n.Children[1])
				}
				for _, c := range n.Children {
					walk(c)
				}
			}
			for _, root := range q.Steps {
				walk(root)
			}
			if tc.name == "mapped residual" {
				calls := 0
				decoded := 0
				var countModulo func(*planpb.Expr)
				countModulo = func(expr *planpb.Expr) {
					if f := expr.GetF(); f != nil {
						if f.Func.ObjName == "serial_extract" {
							decoded++
						}
						if f.Func.ObjName == "mod" || f.Func.ObjName == "%" {
							calls++
						}
						for _, arg := range f.Args {
							countModulo(arg)
						}
					}
				}
				for id := range seen {
					if n := q.Nodes[id]; n.IndexScanInfo.IsIndexScan {
						for _, filter := range n.FilterList {
							countModulo(filter)
						}
					}
				}
				require.LessOrEqual(t, calls, 1, "an index-side SQL predicate must not be evaluated twice")
				require.LessOrEqual(t, decoded, 1, "the exact encoded prefix already establishes a; only the residual needs b decoded")
			}
			require.GreaterOrEqual(t, joins, 1, "the control must exercise real index backfill")
			require.Equal(t, tc.want, bound)
			wantNatural := tc.name == "prefix" || tc.name == "descending" || tc.name == "offset" || tc.name == "full index order"
			require.Equal(t, wantNatural, hasReaderLimit, "static limits require complete SQL predicate and natural-order proofs")
			if wantNatural {
				require.Equal(t, tc.want, readerLimit)
			}
		})
	}
}

func TestRegularIndexBackfillTopRequiresPrivateAccess(t *testing.T) {
	for _, shared := range []string{"orphan only", "base", "index", "join"} {
		t.Run(shared, func(t *testing.T) {
			builder, _, scanID, table := makeIndexHintJoinBuilder(t)
			scan := builder.qry.Nodes[scanID]
			scan.Stats = &planpb.Stats{TableCnt: 1000000, Outcnt: 1000000, Selectivity: 1}
			var def *planpb.IndexDef
			for _, idx := range table.Indexes {
				if usableRegularHintIndex(idx) && !idx.Unique {
					def = idx
					break
				}
			}
			require.NotNil(t, def)
			// The reused JOIN fixture stores VARCHAR PKs by default. This
			// table declares an INT PK, so use matching index metadata.
			mock := builder.compCtx.(*fullTextJoinMockCompilerContext)
			mock.tables[def.IndexTableName].Cols[1].Typ = table.Cols[table.Name2ColIndex[table.Pkey.PkeyColName]].Typ
			access, indexID, err := builder.buildHintedIndexBackfillJoin(def, scan)
			require.NoError(t, err)
			require.NotEqual(t, int32(-1), access)
			ctx := builder.ctxByNode[scanID]
			tag := scan.BindingTags[0]
			pkPos := table.Name2ColIndex[table.Pkey.PkeyColName]
			aPos := table.Name2ColIndex["a"]
			sortID := builder.appendNode(&planpb.Node{
				NodeType: planpb.Node_SORT, Children: []int32{access},
				OrderBy: []*planpb.OrderBySpec{
					{Expr: GetColExpr(table.Cols[aPos].Typ, tag, aPos)},
					{Expr: GetColExpr(table.Cols[pkPos].Typ, tag, pkPos)},
				},
				Limit: MakePlan2Uint64ConstExprWithType(2),
			}, ctx)
			projectID := builder.appendNode(&planpb.Node{
				NodeType: planpb.Node_PROJECT, Children: []int32{sortID},
				BindingTags: []int32{builder.genNewBindTag()},
				ProjectList: []*planpb.Expr{GetColExpr(table.Cols[pkPos].Typ, tag, pkPos)},
			}, ctx)
			builder.qry.Steps = []int32{projectID}
			var other int32
			switch shared {
			case "base":
				other = scanID
			case "index":
				other = indexID
			case "join":
				other = access
			}
			if shared != "orphan only" {
				extra := builder.appendNode(&planpb.Node{
					NodeType: planpb.Node_PROJECT, Children: []int32{other},
					BindingTags: []int32{builder.genNewBindTag()},
				}, ctx)
				builder.qry.Steps = append(builder.qry.Steps, extra)
			}
			before := len(builder.qry.Nodes)
			builder.pushRegularIndexBackfillTop(builder.qry.Nodes[projectID], map[[2]int32]int{})
			build := builder.qry.Nodes[builder.qry.Nodes[access].Children[1]]
			if shared == "orphan only" {
				require.Equal(t, planpb.Node_SORT, build.NodeType)
			} else {
				require.Equal(t, indexID, build.NodeId, "another consumer must retain the complete scan")
				require.Len(t, builder.qry.Nodes, before, "decline must not publish partial nodes")
			}
		})
	}
}
