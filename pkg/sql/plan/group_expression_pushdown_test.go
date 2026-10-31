// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newGroupingExpressionTestBuilder(t *testing.T, proc *process.Process) *QueryBuilder {
	t.Helper()
	integer, text := types.T_int64.ToType().PlanType(), types.New(types.T_varchar, 64, 0).PlanType()
	substring, err := BindFuncExprImplByPlanExpr(proc.Ctx, "substr", []*planpb.Expr{
		GetColExpr(text, 0, 2), makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(2),
	})
	require.NoError(t, err)
	substring.Typ.Width = 2
	annotateStringLengthSource(substring, proc)
	require.NotNil(t, substring.PreparedNumeric.GetStringDomainSource())
	on, err := BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*planpb.Expr{
		GetColExpr(integer, 0, 0), GetColExpr(integer, 1, 0),
	})
	require.NoError(t, err)
	count, err := BindFuncExprImplByPlanExpr(proc.Ctx, "count", []*planpb.Expr{GetColExpr(integer, 0, 1)})
	require.NoError(t, err)
	query := &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{3}, Nodes: []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
			Stats: &planpb.Stats{Outcnt: 1000000, Cost: 1000000, BlockNum: 1000}},
		{NodeId: 1, NodeType: planpb.Node_TABLE_SCAN, TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
			{Name: "key", Typ: integer}, {Name: "payload", Typ: text},
		}}, ProjectList: []*planpb.Expr{GetColExpr(integer, 0, 0), GetColExpr(text, 0, 1)},
			Stats: &planpb.Stats{Outcnt: 100, Cost: 100, Rowsize: 80}},
		{NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{0, 1},
			ProjectList: []*planpb.Expr{GetColExpr(integer, 0, 1), GetColExpr(integer, 1, 0), GetColExpr(text, 1, 1)},
			OnList:      []*planpb.Expr{on}, SendMsgList: []planpb.MsgHeader{{MsgTag: 10, MsgType: int32(message.MsgJoinMap)}},
			Stats: &planpb.Stats{Outcnt: 1000000, HashmapStats: &planpb.HashMapStats{HashOnPK: false}}},
		{NodeId: 3, NodeType: planpb.Node_AGG, Children: []int32{2},
			GroupBy:      []*planpb.Expr{substring, GetColExpr(integer, 0, 0)},
			GroupingFlag: []bool{true, true}, AggList: []*planpb.Expr{count}},
	}}
	return &QueryBuilder{qry: query, ctxByNode: make([]*BindContext, len(query.Nodes))}
}

func TestPushdownGroupingExpressionPreservesRowsAndSlots(t *testing.T) {
	proc := newPlanTestProcess(t)
	for _, consumer := range []string{"only_derived", "raw_group_key", "hidden_metadata", "nonnullable_input", "bound_collation"} {
		t.Run(consumer, func(t *testing.T) {
			builder := newGroupingExpressionTestBuilder(t, proc)
			agg, join, scan := builder.qry.Nodes[3], builder.qry.Nodes[2], builder.qry.Nodes[1]
			original := DeepCopyExpr(agg.GroupBy[0])
			if consumer == "raw_group_key" {
				agg.GroupBy = append(agg.GroupBy, GetColExpr(scan.ProjectList[1].Typ, 0, 2))
				agg.GroupingFlag = append(agg.GroupingFlag, true)
			} else if consumer == "hidden_metadata" {
				agg.GroupBy[1].PreparedNumeric = &planpb.PreparedNumericMetadata{
					StringDomainSource: GetColExpr(scan.ProjectList[1].Typ, 0, 2),
				}
			} else if consumer == "nonnullable_input" {
				scan.ProjectList[1].Typ.NotNullable = true
				join.ProjectList[2].Typ.NotNullable = true
				agg.GroupBy[0].GetF().Args[0].Typ.NotNullable = true
			} else if consumer == "bound_collation" {
				agg.GroupBy[0].Typ.CollationCoercibility = 2
				agg.GroupBy[0].Typ.CollationCoercibilitySet = true
				agg.GroupBy[0].PreparedNumeric.StringDomainSource.Typ = agg.GroupBy[0].Typ
			}
			beforeAgg, beforeJoin, beforeScan := DeepCopyNode(agg), DeepCopyNode(join), DeepCopyNode(scan)
			builder.pushdownGroupingExpressions()
			require.Len(t, builder.qry.Nodes, 5)
			require.Len(t, builder.ctxByNode, 5)
			project := builder.qry.Nodes[join.Children[1]]
			require.Equal(t, planpb.Node_PROJECT, project.NodeType)
			require.Equal(t, []int32{1}, project.Children)
			require.Len(t, project.ProjectList, 3)
			require.True(t, proto.Equal(beforeScan, scan), "raw build scan must not change")
			for i, source := range scan.ProjectList {
				require.True(t, proto.Equal(GetColExpr(source.Typ, 0, int32(i)), project.ProjectList[i]))
			}
			require.Equal(t, 80+float64(types.VarlenaSize)+4*2, project.Stats.Rowsize)
			require.Nil(t, project.Stats.HashmapStats)
			output := int32(2)
			if consumer == "raw_group_key" || consumer == "hidden_metadata" {
				output = 3
				require.True(t, proto.Equal(beforeJoin.ProjectList[2], join.ProjectList[2]))
			}
			require.Len(t, join.ProjectList, int(output)+1)
			require.Equal(t, &planpb.ColRef{RelPos: 1, ColPos: 2}, join.ProjectList[output].GetCol())
			require.Equal(t, &planpb.ColRef{RelPos: 0, ColPos: output}, agg.GroupBy[0].GetCol())
			require.True(t, proto.Equal(&original.Typ, &agg.GroupBy[0].Typ))
			require.True(t, proto.Equal(original.PreparedNumeric, agg.GroupBy[0].PreparedNumeric))
			require.True(t, proto.Equal(original.PreparedNumeric, project.ProjectList[2].PreparedNumeric))
			// All other annotations, keys, aggregate owners and public slots stay
			// byte-for-byte the same; only the three approved edges may differ.
			beforeJoin.Children, beforeJoin.ProjectList = join.Children, join.ProjectList
			beforeAgg.GroupBy = agg.GroupBy
			require.True(t, proto.Equal(beforeJoin, join))
			require.True(t, proto.Equal(beforeAgg, agg))
			if consumer != "only_derived" {
				return
			}
			// A duplicate build key and duplicate probes expose fanout. Include
			// NULL, Unicode, and an unmatched build row: moving the expression
			// must retain the grouped multiset, not only the projected values.
			payloads := []string{"abcd", "abEF", "你好世界", "", "unmatched"}
			keys, probes := []int{1, 1, 2, 3, 4}, []int{1, 1, 2, 3, 9}
			var joined []string
			var matched []int
			for _, probe := range probes {
				for row, key := range keys {
					if key == probe {
						joined, matched = append(joined, payloads[row]), append(matched, row)
					}
				}
			}
			evaluate := func(expr *planpb.Expr, values []string) []string {
				input := batch.NewWithSize(1)
				defer input.Clean(proc.Mp())
				input.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 64, 0))
				for _, value := range values {
					require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value), value == "", proc.Mp()))
				}
				input.SetRowCount(len(values))
				expr = DeepCopyExpr(expr)
				expr.GetF().Args[0].GetCol().ColPos = 0
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{input})
				require.NoError(t, err)
				defer free()
				output := make([]string, len(values))
				for i := range output {
					if result.IsNull(uint64(i)) {
						output[i] = "<NULL>"
					} else {
						output[i] = result.GetStringAt(i)
					}
				}
				return output
			}
			before := make(map[string]int)
			for _, value := range evaluate(original, joined) {
				before[value]++
			}
			after, built := make(map[string]int), evaluate(project.ProjectList[2], payloads)
			for _, row := range matched {
				after[built[row]]++
			}
			require.Equal(t, map[string]int{"ab": 4, "你好": 1, "<NULL>": 1}, before)
			require.Equal(t, before, after)
			// Extreme starts are total too; no signed negation/addition overflow.
			for _, start := range []int64{math.MinInt64, math.MaxInt64} {
				expr := DeepCopyExpr(original)
				expr.GetF().Args[1] = makePlan2Int64ConstExprWithType(start)
				expr.PreparedNumeric = nil
				require.True(t, groupExpressionSubstring(expr))
				require.Equal(t, []string{"", ""}, evaluate(expr, []string{"abcd", "你好"}))
			}
		})
	}
}

func TestPushdownGroupingExpressionGuards(t *testing.T) {
	proc := newPlanTestProcess(t)
	for _, tc := range []struct {
		name string
		edit func(*QueryBuilder, *planpb.Node, *planpb.Node, *planpb.Expr)
	}{
		{"prepare", func(b *QueryBuilder, _, _ *planpb.Node, _ *planpb.Expr) { b.isPrepareStatement = true }},
		{"outer_join", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) { j.JoinType = planpb.Node_LEFT }},
		{"shuffle", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) { j.Stats.HashmapStats.Shuffle = true }},
		{"probe_source", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) { j.ProjectList[2].GetCol().RelPos = 0 }},
		{"invalid_slot", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) { e.GetF().Args[0].GetCol().ColPos = 99 }},
		{"type_mismatch", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) { j.ProjectList[2].Typ.Width++ }},
		{"different_string_domain", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) {
			e.Typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI)
		}},
		{"fallible_cast", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) {
			cast, err := appendCastBeforeExpr(proc.Ctx, e.GetF().Args[0], types.T_int64.ToType().PlanType())
			require.NoError(t, err)
			*e = *cast
		}},
		{"volatile", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) {
			random, err := BindFuncExprImplByPlanExpr(proc.Ctx, "rand", nil)
			require.NoError(t, err)
			*e = *random
		}},
		{"sourced_length", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) {
			e.GetF().Args[2].GetLit().Src = makePlan2Int64ConstExprWithType(2)
		}},
		{"variable_length", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) {
			e.GetF().Args[2].GetLit().StringSource = uint32(types.StringSourceUserVariable)
		}},
		{"open_witness", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) {
			e.PreparedNumeric.StringDomainSource.GetF().Args[0] = DeepCopyExpr(e.GetF().Args[0])
		}},
		{"shared_join", func(b *QueryBuilder, _, _ *planpb.Node, _ *planpb.Expr) { b.qry.Steps = append(b.qry.Steps, 2) }},
		{"weak_fanout", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) { j.Stats.Outcnt = 399 }},
		{"unknown_fanout", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) { j.Stats.Outcnt = math.Inf(1) }},
		{"unbounded_width", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) { e.Typ.Width = 0 }},
		{"widening", func(_ *QueryBuilder, _, _ *planpb.Node, e *planpb.Expr) { e.Typ.Width = 65 }},
		{"inactive_key", func(_ *QueryBuilder, a, _ *planpb.Node, _ *planpb.Expr) { a.GroupingFlag[0] = false }},
		{"join_observes_payload", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) {
			j.OnList[0].GetF().Args[1].GetCol().ColPos = 1
		}},
		{"reverse_key_is_not_reoriented", func(_ *QueryBuilder, _, j *planpb.Node, _ *planpb.Expr) {
			args := j.OnList[0].GetF().Args
			args[0], args[1] = args[1], args[0]
		}},
		{"unknown_owner", func(_ *QueryBuilder, a, _ *planpb.Node, _ *planpb.Expr) { a.Limit = makePlan2Int64ConstExprWithType(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := newGroupingExpressionTestBuilder(t, proc)
			agg, join := builder.qry.Nodes[3], builder.qry.Nodes[2]
			tc.edit(builder, agg, join, agg.GroupBy[0])
			before := proto.Clone(builder.qry).(*planpb.Query)
			builder.pushdownGroupingExpressions()
			require.True(t, proto.Equal(before, builder.qry), "rejected plans must not be mutated")
		})
	}
}

func TestOptimizePushesGroupingExpressionBeforeJoin(t *testing.T) {
	proc := newPlanTestProcess(t)
	mock, cache := NewMockCompilerContext(true, proc), NewStatsCache()
	for name, rows := range map[string]float64{"lineitem": 10000000, "supplier": 100} {
		table := mock.tables[name]
		stats := NewStatsInfo()
		stats.TableName, stats.TableCnt = name, rows
		stats.AccurateObjectNumber, stats.BlockNumber = 1, int64(rows/8192)+1
		for _, col := range table.Cols {
			ndv := rows
			if col.Name == "l_suppkey" {
				ndv = 100
			}
			stats.NdvMap[col.Name] = ndv
			stats.MinValMap[col.Name], stats.MaxValMap[col.Name] = 1, ndv
			stats.DataTypeMap[col.Name] = uint64(col.Typ.Id)
			stats.SizeMap[col.Name] = uint64(rows) * 16
		}
		cache.Set(table.TblId, stats)
	}
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{MockCompilerContext: mock, statsCache: cache}}
	stmt, err := mysql.ParseOne(proc.Ctx, `select substr(s.s_comment,1,2), l.l_shipdate, count(*)
		from lineitem l join supplier s on l.l_suppkey=s.s_suppkey
		group by substr(s.s_comment,1,2), l.l_shipdate having count(*) > 1`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	// This includes ConstantFold and node compaction after createQuery, not
	// merely BuildPlan or direct invocation of the new rewrite.
	query, err := NewBaseOptimizer(ctx).Optimize(stmt, false)
	require.NoError(t, err)
	wire, err := query.Marshal()
	require.NoError(t, err)
	restored := new(planpb.Query)
	require.NoError(t, restored.Unmarshal(wire))
	require.True(t, proto.Equal(query, restored))
	found := 0
	for _, agg := range restored.Nodes {
		if agg.NodeType != planpb.Node_AGG || len(agg.Children) != 1 {
			continue
		}
		join := restored.Nodes[agg.Children[0]]
		if join.NodeType != planpb.Node_JOIN || len(join.Children) != 2 {
			continue
		}
		project := restored.Nodes[join.Children[1]]
		if project.NodeType != planpb.Node_PROJECT {
			continue
		}
		derived := project.ProjectList[len(project.ProjectList)-1]
		require.True(t, groupExpressionSubstring(derived))
		require.Equal(t, "supplier", restored.Nodes[project.Children[0]].TableDef.Name)
		require.Len(t, agg.GroupBy, 2)
		require.NotNil(t, agg.GroupBy[0].GetCol())
		require.True(t, proto.Equal(&derived.Typ, &agg.GroupBy[0].Typ))
		require.True(t, proto.Equal(derived.PreparedNumeric, agg.GroupBy[0].PreparedNumeric))
		require.Len(t, restored.Nodes[restored.Steps[len(restored.Steps)-1]].ProjectList, 3)
		found++
	}
	if found == 0 {
		t.Logf("final plan: %s", proto.MarshalTextString(restored))
	}
	require.Equal(t, 1, found, "the final public plan must retain the build-side computation")
}
