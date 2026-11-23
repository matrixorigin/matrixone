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
	"errors"
	"math"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func newIndexMergePlanContext(t *testing.T) *encodedIndexPlanTestContext {
	t.Helper()
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	table.Indexes = []*planpb.IndexDef{table.Indexes[0], {
		IndexName: "idx_b", Parts: []string{"b", catalog.CreateAlias("id")},
		IndexTableName: "idx_hint_b", TableExist: true,
	}}
	addIndexHintIndexTableForTest(mock, "idx_hint_b", 25361)
	ctx := &encodedIndexPlanTestContext{
		MockCompilerContext: &mock.ctxt, statsCache: NewStatsCache(),
		statsByID: make(map[uint64]*statspb.StatsInfo),
	}
	for _, name := range []string{"index_hint_t", "idx_hint_a", "idx_hint_b"} {
		def := ctx.tables[name]
		stats := NewStatsInfo()
		stats.TableName, stats.TableCnt = name, 200000
		stats.BlockNumber, stats.AccurateObjectNumber = 25, 1
		stats.NdvMap = map[string]float64{"id": 200000, "a": 500, "b": 200000,
			catalog.IndexTableIndexColName: 200000, catalog.IndexTablePrimaryColName: 200000}
		stats.SizeMap = map[string]uint64{"id": 800000, "a": 800000, "b": 800000,
			catalog.IndexTableIndexColName: 2400000, catalog.IndexTablePrimaryColName: 800000}
		stats.DataTypeMap = map[string]uint64{"id": uint64(types.T_int32), "a": uint64(types.T_int32), "b": uint64(types.T_int32)}
		stats.MinValMap = map[string]float64{"id": 1, "a": 0, "b": 1}
		stats.MaxValMap = map[string]float64{"id": 200000, "a": 499, "b": 200000}
		ctx.statsByID[def.TblId] = stats
		ctx.statsCache.Set(def.TblId, stats)
	}
	return ctx
}

func indexMergePlanNodes(q *planpb.Query, kind planpb.Node_NodeType) []*planpb.Node {
	var nodes []*planpb.Node
	var visit func(int32)
	visit = func(id int32) {
		n := q.Nodes[id]
		if n.NodeType == kind {
			nodes = append(nodes, n)
		}
		for _, child := range n.Children {
			visit(child)
		}
	}
	for _, root := range q.Steps {
		visit(root)
	}
	return nodes
}

func TestRegularIndexMergePublicPlans(t *testing.T) {
	for _, projection := range []string{"count(*)", "id"} {
		t.Run(projection, func(t *testing.T) {
			ctx := newIndexMergePlanContext(t)
			p, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t,
				"select "+projection+" from index_hint_t where a=3 or b=10")
			require.NoError(t, err)
			q := p.GetQuery()
			require.ElementsMatch(t, []string{"idx_a", "idx_b"}, reachableIndexScanNames(q))
			require.Len(t, indexMergePlanNodes(q, planpb.Node_UNION), 1, "主键并集必须去重")
			for _, scan := range indexMergePlanNodes(q, planpb.Node_TABLE_SCAN) {
				require.True(t, scan.IndexScanInfo.IsIndexScan, "精确主键覆盖路径不能回扫基表")
				require.Equal(t, "index_hint_t", scan.ParentObjRef.ObjName)
			}
		})
	}
}

func TestRegularIndexMergePublicControls(t *testing.T) {
	for _, tc := range []struct {
		name, sql       string
		merge, backfill bool
		indexes         []string
		change          func(*encodedIndexPlanTestContext)
	}{
		{name: "非覆盖回表", sql: "select a,b from index_hint_t force index(idx_a,idx_b) where a=3 or b=10", merge: true, backfill: true},
		{name: "回表物理成本拒绝", sql: "select a,b from index_hint_t where a=3 or b=10"},
		{name: "稀疏回表正对照", sql: "select a,b from index_hint_t where a=3 or b=10", merge: true, backfill: true, change: func(ctx *encodedIndexPlanTestContext) {
			ctx.statsByID[25356].NdvMap["a"] = 200000
			ctx.statsByID[25356].MaxValMap["a"] = 200000
		}},
		{name: "非排序主键回表", sql: "select a,b from index_hint_t force index(idx_a,idx_b) where a=3 or b=10", merge: true, backfill: true, change: func(ctx *encodedIndexPlanTestContext) {
			ctx.tables["index_hint_t"].ClusterBy = &planpb.ClusterByDef{Name: "a"}
		}},
		{name: "宽主键传输成本", sql: "select count(*) from index_hint_t where a=3 or b=10", change: func(ctx *encodedIndexPlanTestContext) {
			pkType := planpb.Type{Id: int32(types.T_varchar), Width: 4096, NotNullable: true}
			ctx.tables["index_hint_t"].Cols[0].Typ = pkType
			ctx.statsByID[25356].SizeMap["id"] = 200000 * 4096
			ctx.statsByID[25356].DataTypeMap["id"] = uint64(types.T_varchar)
			for _, name := range []string{"idx_hint_a", "idx_hint_b"} {
				ctx.tables[name].Cols[1].Typ = pkType
			}
		}},
		{name: "同列 OR", sql: "select id from index_hint_t where a=3 or a=7"},
		{name: "跨列 AND", sql: "select id from index_hint_t where a=3 and b=10"},
		{name: "未索引分支", sql: "select id from index_hint_t where a=3 or id=10"},
		{name: "忽略一个索引", sql: "select id from index_hint_t ignore index(idx_b) where a=3 or b=10"},
		{name: "只允许一个索引", sql: "select id from index_hint_t use index(idx_a) where a=3 or b=10"},
		{name: "不存在的表达式索引", sql: "select id from index_hint_t where a=3 or abs(b)=10"},
		{name: "非稳定分支", sql: "select id from index_hint_t where a=3 or b=cast(rand()*20 as int)"},
		{name: "IN 分支", sql: "select id from index_hint_t where a in (3,7) or b=10", merge: true},
		{name: "可靠统计范围", sql: "select id from index_hint_t where a<3 or b=10", merge: true},
		{name: "越域范围估算回退", sql: "select count(*) from index_hint_t where a<1000 or b=10"},
		{name: "退化范围统计", sql: "select count(*) from index_hint_t where a<1 or b=10", change: func(ctx *encodedIndexPlanTestContext) {
			ctx.statsByID[25356].MaxValMap["a"] = 0
		}},
		{name: "精确范围分支", sql: "select id from index_hint_t force index(idx_a,idx_b) where a between 3 and 7 or b=10", merge: true},
		{name: "NULL 范围残余", sql: "select id from index_hint_t force index(idx_a,idx_b) where a<3 or b=10", merge: true},
		{name: "分支内 AND", sql: "select id from index_hint_t force index(idx_a,idx_b) where (a>=3 and a<7) or b=10", merge: true},
		{name: "全局排序分页", sql: "select id from index_hint_t where a=3 or b=10 order by id limit 2 offset 1", merge: true},
		{name: "额外回表条件", sql: "select id from index_hint_t force index(idx_a,idx_b) where (a=3 or b=10) and a<>7", merge: true, backfill: true},
		{name: "主键残余", sql: "select id from index_hint_t where (a=3 or b=10) and id<>7", merge: true},
		{name: "多个 OR 残余", sql: "select id from index_hint_t force index(idx_a,idx_b) where (a=3 or b=10) and (a=7 or b=20)", merge: true, backfill: true},
		{name: "三路主键并集", sql: "select id from index_hint_t force index(idx_a,idx_b,idx_ab) where a=3 or b=10 or (a=7 and b=20)", merge: true,
			indexes: []string{"idx_a", "idx_b", "idx_ab"}, change: func(ctx *encodedIndexPlanTestContext) {
				table := ctx.tables["index_hint_t"]
				table.Indexes = append(table.Indexes, &planpb.IndexDef{IndexName: "idx_ab", IndexTableName: "idx_hint_ab", Parts: []string{"a", "b", catalog.CreateAlias("id")}, TableExist: true})
				ctx.statsByID[25358] = ctx.statsByID[25357]
				ctx.statsCache.Set(25358, ctx.statsByID[25358])
			}},
		{name: "唯一二级索引", sql: "select id from index_hint_t where a=3 or b=10", merge: true, change: func(ctx *encodedIndexPlanTestContext) {
			idx := ctx.tables["index_hint_t"].Indexes[1]
			idx.Unique = true
			idx.Parts = []string{"b"}
			ctx.tables["idx_hint_b"].Cols[0].Typ = ctx.tables["index_hint_t"].Cols[2].Typ
		}},
		{name: "缺少列统计", sql: "select count(*) from index_hint_t where a=3 or b=10", change: func(ctx *encodedIndexPlanTestContext) {
			delete(ctx.statsByID[25356].NdvMap, "a")
		}},
		{name: "低选择率", sql: "select count(*) from index_hint_t where a=3 or b=10", change: func(ctx *encodedIndexPlanTestContext) {
			stats := ctx.statsByID[25356]
			stats.NdvMap["a"], stats.NdvMap["b"] = 2, 2
		}},
		{name: "无统计", sql: "select count(*) from index_hint_t where a=3 or b=10", change: func(ctx *encodedIndexPlanTestContext) {
			delete(ctx.statsByID, 25356)
			ctx.statsCache = NewStatsCache()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newIndexMergePlanContext(t)
			if tc.change != nil {
				tc.change(ctx)
			}
			p, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t, tc.sql)
			require.NoError(t, err)
			q := p.GetQuery()
			require.Equal(t, tc.merge, len(indexMergePlanNodes(q, planpb.Node_UNION)) != 0)
			if !tc.merge {
				return
			}
			indexes := tc.indexes
			if indexes == nil {
				indexes = []string{"idx_a", "idx_b"}
			}
			require.ElementsMatch(t, indexes, reachableIndexScanNames(q))
			if len(indexes) == 3 {
				require.Len(t, indexMergePlanNodes(q, planpb.Node_UNION_ALL), 1)
			}
			base := 0
			for _, scan := range indexMergePlanNodes(q, planpb.Node_TABLE_SCAN) {
				if !scan.IndexScanInfo.IsIndexScan {
					base++
				}
				require.Nil(t, scan.Limit, "LIMIT 不能推到 OR 分支或去重之前")
				require.Nil(t, scan.Offset)
			}
			require.Equal(t, tc.backfill, base != 0)
			if tc.backfill {
				joins := indexMergePlanNodes(q, planpb.Node_JOIN)
				require.Len(t, joins, 1)
				require.Equal(t, planpb.Node_INDEX, joins[0].JoinType)
				require.NotEmpty(t, joins[0].RuntimeFilterBuildList)
			}
		})
	}
}

func TestRegularIndexMergeCompositeIdentity(t *testing.T) {
	for _, projection := range []string{"count(*)", "tenant_id,activity_id"} {
		t.Run(projection, func(t *testing.T) {
			ctx := newEncodedExistsPlanTestContext(t, 600000)
			ctx.tables["cost_activity"].Indexes = nil
			addCostActivityRegularIndex(t, ctx, "idx_amount", []string{"amount", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, false)
			addCostActivityRegularIndex(t, ctx, "idx_activity", []string{"activity_id", catalog.CreateAlias(catalog.CPrimaryKeyColName)}, false)
			p, err := runOneStmt(&encodedIndexPlanTestOptimizer{ctx: ctx}, t,
				"select "+projection+" from cost_activity where amount=10 or activity_id=7")
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"idx_amount", "idx_activity"}, reachableIndexScanNames(p.GetQuery()))
			unions := indexMergePlanNodes(p.GetQuery(), planpb.Node_UNION)
			require.Len(t, unions, 1)
			require.Len(t, unions[0].ProjectList, 1)
			require.Equal(t, int32(types.T_varchar), unions[0].ProjectList[0].Typ.Id)
		})
	}
}

func TestRegularIndexMergeBranchBudget(t *testing.T) {
	ctx := newIndexMergePlanContext(t)
	makeOR := func(a, b *planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{a, b})
		require.NoError(t, err)
		return expr
	}
	leaf := MakePlan2BoolConstExprWithType(true)
	for _, n := range []int{2, 7, 8, 9} {
		expr := leaf
		for i := 1; i < n; i++ {
			expr = makeOR(expr, leaf)
		}
		var arms []*planpb.Expr
		require.Equal(t, n <= 8, regularIndexMergeArms(expr, &arms))
		if n <= 8 {
			require.Len(t, arms, n)
		}
	}
	var arms []*planpb.Expr
	require.False(t, regularIndexMergeArms(nil, &arms))
	and, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "and", []*planpb.Expr{makeOR(leaf, leaf), makeOR(leaf, leaf)})
	require.NoError(t, err)
	require.True(t, regularIndexMergeArms(and, &arms))
	require.Len(t, arms, 1, "AND 不能展开为笛卡尔积")
}

func TestRegularIndexOnlyAliasedSinglePartMapping(t *testing.T) {
	ctx := newIndexMergePlanContext(t)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	table := ctx.tables["index_hint_t"]
	tag := builder.genNewBindTag()
	typ := table.Cols[2].Typ
	bound := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 10}}}}
	filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{GetColExpr(typ, tag, 2), bound})
	require.NoError(t, err)
	index := &planpb.IndexDef{IndexName: "idx_b", IndexTableName: "idx_hint_b", Unique: true, Parts: []string{catalog.CreateAlias("b")}}
	ctx.tables[index.IndexTableName].Cols[0].Typ = typ
	node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: table, ObjRef: ctx.objects[table.Name],
		BindingTags: []int32{tag}, FilterList: []*planpb.Expr{filter}, Limit: MakePlan2Uint64ConstExprWithType(5)}
	build := func(index *planpb.IndexDef, mapping map[[2]int32]*planpb.Expr) *planpb.Node {
		scan, err := builder.buildRegularIndexOnlyScan(index, node, mapping, &Snapshot{}, &regularIndexOnlyMatch{
			filterIdx: []int32{0}, filterType: EqualIndexCondition, leadingEqual: true,
		})
		require.NoError(t, err)
		return scan
	}
	mapping := make(map[[2]int32]*planpb.Expr)
	scan := build(index, mapping)
	require.NotNil(t, scan)
	require.NotNil(t, mapping[[2]int32{tag, 2}], "the alias resolves to b rather than the primary key")
	require.NotContains(t, mapping, [2]int32{tag, 0})
	require.Same(t, node.Limit, scan.Limit)
	require.Equal(t, int32(0), scan.FilterList[0].GetF().Args[0].GetCol().ColPos)

	t.Run("nil base context", func(t *testing.T) {
		ctx.SetContext(nil)
		mapping := make(map[[2]int32]*planpb.Expr)
		scan := build(index, mapping)
		require.NotNil(t, scan)
		require.NotNil(t, mapping[[2]int32{tag, 2}])
	})

	t.Run("cancellation cause", func(t *testing.T) {
		cancelled, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("cancel regular index mapping")
		cancel(cause)
		ctx.SetContext(cancelled)
		original := GetColExpr(typ, tag, 2)
		mapping := map[[2]int32]*planpb.Expr{{tag, 2}: original}
		scan, err := builder.buildRegularIndexOnlyScan(index, node, mapping, &Snapshot{}, &regularIndexOnlyMatch{
			filterIdx: []int32{0}, filterType: EqualIndexCondition, leadingEqual: true,
		})
		require.Nil(t, scan)
		require.ErrorIs(t, err, cause)
		require.Same(t, original, mapping[[2]int32{tag, 2}])
	})

	t.Run("deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		ctx.SetContext(expired)
		mapping := make(map[[2]int32]*planpb.Expr)
		scan, err := builder.buildRegularIndexOnlyScan(index, node, mapping, &Snapshot{}, &regularIndexOnlyMatch{
			filterIdx: []int32{0}, filterType: EqualIndexCondition, leadingEqual: true,
		})
		require.Nil(t, scan)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Empty(t, mapping)
	})

	t.Run("missing index column", func(t *testing.T) {
		ctx.SetContext(context.Background())
		invalid := *index
		invalid.Parts = []string{"missing"}
		original := GetColExpr(typ, tag, 2)
		mapping := map[[2]int32]*planpb.Expr{{tag, 2}: original}
		scan := build(&invalid, mapping)
		require.Nil(t, scan)
		require.Len(t, mapping, 1)
		require.Same(t, original, mapping[[2]int32{tag, 2}])
	})
}

func TestRegularIndexMergeConstructionFailure(t *testing.T) {
	ctx := newIndexMergePlanContext(t)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	table := ctx.tables["index_hint_t"]
	tag := builder.genNewBindTag()
	eq := func(col, value int32) *planpb.Expr {
		literal := &planpb.Expr{Typ: table.Cols[col].Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: value}}}}
		expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{GetColExpr(table.Cols[col].Typ, tag, col), literal})
		require.NoError(t, err)
		return expr
	}
	left, right := eq(1, 3), eq(2, 10)
	predicate, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*planpb.Expr{left, right})
	require.NoError(t, err)
	node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: table, ObjRef: ctx.objects[table.Name],
		BindingTags: []int32{tag}, FilterList: []*planpb.Expr{predicate}, Limit: MakePlan2Uint64ConstExprWithType(5)}
	id := builder.appendNode(node, NewBindContext(builder, nil))
	candidate := &regularIndexMergeCandidate{covering: true}
	for _, arm := range []*planpb.Expr{left, right} {
		branch := builder.matchRegularIndexMergeBranch(node, arm, table.Indexes, 4)
		require.NotNil(t, branch)
		candidate.branches = append(candidate.branches, branch)
	}
	for _, branch := range candidate.branches {
		cancelCtx, cancel := context.WithCancelCause(context.Background())
		ctx.SetContext(cancelCtx)
		stop := errors.New("index merge stopped after resolution")
		compiler := &controlledIndexResolver{
			CompilerContext: ctx,
			target:          branch.index.IndexTableName,
			after:           func() { cancel(stop) },
		}
		builder.compCtx = compiler
		original := proto.Clone(node)
		nameCount := len(builder.nameByColRef)
		mapping := map[[2]int32]*planpb.Expr{}
		got, err := builder.buildRegularIndexMerge(node, 0, candidate, mapping)
		require.Equal(t, id, got)
		require.ErrorIs(t, err, stop)
		require.Equal(t, 1, compiler.resolved)
		require.Len(t, builder.qry.Nodes, 1, "no branch may be published on cancellation")
		require.Len(t, builder.ctxByNode, 1)
		require.Len(t, builder.nameByColRef, nameCount)
		require.Empty(t, mapping)
		require.Empty(t, cmp.Diff(original, node))
		cancel(nil)
		ctx.SetContext(context.Background())
		builder.compCtx = ctx
	}
	indexType := ctx.tables["idx_hint_b"].Cols[0].Typ
	ctx.tables["idx_hint_b"].Cols[0].Typ = planpb.Type{Id: int32(types.T_json)}
	before := proto.Clone(node)
	nameCount := len(builder.nameByColRef)
	mapping := map[[2]int32]*planpb.Expr{}
	got, err := builder.buildRegularIndexMerge(node, 0, candidate, mapping)
	require.NoError(t, err)
	require.Equal(t, id, got)
	require.Len(t, builder.qry.Nodes, 1, "第一分支不能提前发布")
	require.Len(t, builder.ctxByNode, 1)
	require.Len(t, builder.nameByColRef, nameCount)
	require.Empty(t, mapping)
	require.Empty(t, cmp.Diff(before, node))

	// 失败后的同一构造器仍可成功发布，并把快照与分页放在正确边界上。
	ctx.tables["idx_hint_b"].Cols[0].Typ = indexType
	node.ScanSnapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	for _, branch := range candidate.branches {
		branch.scan.ScanSnapshot = node.ScanSnapshot
	}
	root, err := builder.buildRegularIndexMerge(node, 0, candidate, mapping)
	require.NoError(t, err)
	require.NotEqual(t, id, root)
	require.Same(t, node.Limit, builder.qry.Nodes[root].Limit)
	published := 0
	for _, scan := range builder.qry.Nodes {
		if scan.IndexScanInfo.IsIndexScan {
			published++
			require.Same(t, node.ScanSnapshot, scan.ScanSnapshot)
			require.Nil(t, scan.Limit)
			require.Nil(t, scan.Offset)
		}
	}
	require.Equal(t, 2, published)
	require.NotNil(t, mapping[[2]int32{tag, 0}])
}

func TestRegularIndexMergeInvalidCost(t *testing.T) {
	ctx := newIndexMergePlanContext(t)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	table := ctx.tables["index_hint_t"]
	tag := builder.genNewBindTag()
	makeEq := func(col int32, value int64) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
			GetColExpr(table.Cols[col].Typ, tag, col), MakePlan2Int64ConstExprWithType(value),
		})
		require.NoError(t, err)
		return expr
	}
	expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*planpb.Expr{makeEq(1, 3), makeEq(2, 10)})
	require.NoError(t, err)
	node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: table, ObjRef: ctx.objects[table.Name], BindingTags: []int32{tag}, FilterList: []*planpb.Expr{expr}}
	id := builder.appendNode(node, NewBindContext(builder, nil))
	refs := map[[2]int32]int{{tag, 1}: 1, {tag, 2}: 1}
	for _, cost := range []float64{0, math.NaN(), math.Inf(1)} {
		node.Stats = &planpb.Stats{TableCnt: 200000, Cost: cost, Outcnt: 401, Selectivity: 0.002005}
		original := proto.Clone(node)
		mapping := map[[2]int32]*planpb.Expr{}
		got, err := builder.applyRegularIndexMerge(node, table.Indexes, refs, mapping, builder.newEncodedRegularIndexCostContext(node, refs))
		require.NoError(t, err)
		require.Equal(t, id, got)
		require.Empty(t, cmp.Diff(original, node, cmpopts.EquateNaNs()))
		require.Empty(t, mapping)
		require.Len(t, builder.qry.Nodes, 1)
	}
}
