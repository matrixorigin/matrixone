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
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

type scalarCTETestContext struct {
	*MockCompilerContext
	selectLimit uint64
}

func (c *scalarCTETestContext) ResolveVariable(name string, system, global bool) (interface{}, error) {
	if name == "sql_select_limit" {
		return c.selectLimit, nil
	}
	return c.MockCompilerContext.ResolveVariable(name, system, global)
}

func scalarCTETestPlan(t *testing.T, sql string, limit uint64) *Query {
	t.Helper()
	ctx := &scalarCTETestContext{MockCompilerContext: NewMockCompilerContext(true), selectLimit: limit}
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	})
	p, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err, sql)
	return p.GetQuery()
}

func scalarCTEReachable(q *Query, typ planpb.Node_NodeType) []*Node {
	var result []*Node
	seen := make(map[int32]bool)
	var walk func(int32)
	walk = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		n := q.Nodes[id]
		if n.NodeType == typ {
			result = append(result, n)
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	for _, root := range q.Steps {
		walk(root)
	}
	return result
}

func TestScalarCTEReuseUnionLimit(t *testing.T) {
	for _, n := range []int{2, 4, 8} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("branches=%d/limit=%d", n, n+delta), func(t *testing.T) {
				branches := make([]string, n)
				for i := range branches {
					branches[i] = fmt.Sprintf("select a+%d v from c", i)
				}
				sql := "with c as (select max(l_orderkey) a, max(l_linenumber) b from lineitem) " + strings.Join(branches, " union all ") + fmt.Sprintf(" limit %d", n+delta)
				q := scalarCTETestPlan(t, sql, math.MaxUint64)
				if delta < 0 {
					require.Len(t, scalarCTEReachable(q, planpb.Node_TABLE_SCAN), n)
					return
				}
				require.Len(t, scalarCTEReachable(q, planpb.Node_TABLE_SCAN), 1)
				require.Len(t, scalarCTEReachable(q, planpb.Node_SINK), 1)
				require.Len(t, scalarCTEReachable(q, planpb.Node_SINK_SCAN), n)
			})
		}
	}
}

func TestScalarCTEReuseNestedAndProjection(t *testing.T) {
	for _, suffix := range []string{"", " limit 2", " limit 2 offset 1", " order by 1 limit 2"} {
		q := scalarCTETestPlan(t, "with p as (select max(l_orderkey) a, max(l_linenumber) b, max(l_partkey) c from lineitem), c as (select * from p) select a from c union all select c from c"+suffix, math.MaxUint64)
		require.Len(t, scalarCTEReachable(q, planpb.Node_TABLE_SCAN), 1)
		sinks := scalarCTEReachable(q, planpb.Node_SINK)
		require.Len(t, sinks, 1)
		require.Len(t, sinks[0].ProjectList, 2, "the unused middle column must not be published")
		readers := scalarCTEReachable(q, planpb.Node_SINK_SCAN)
		require.Len(t, readers, 2)
		for _, reader := range readers {
			for _, expr := range reader.ProjectList {
				require.Less(t, expr.GetCol().ColPos, int32(2))
			}
		}
	}
	q := scalarCTETestPlan(t, "with c as (select max(l_orderkey) a, max(l_linenumber) b from lineitem) select count(*) from (select 1 from c union all select b from c) t", math.MaxUint64)
	require.Len(t, scalarCTEReachable(q, planpb.Node_SINK_SCAN), 2)
	for _, reader := range scalarCTEReachable(q, planpb.Node_SINK_SCAN) {
		require.NotEmpty(t, reader.ProjectList, "count-only readers need a published row carrier")
	}
	q = scalarCTETestPlan(t, "with c as (select max(l_orderkey) a, max(l_linenumber) b from lineitem) select 1 from c union all select b from c", math.MaxUint64)
	require.Len(t, scalarCTEReachable(q, planpb.Node_SINK_SCAN), 2)
	for _, reader := range scalarCTEReachable(q, planpb.Node_SINK_SCAN) {
		require.NotEmpty(t, reader.ProjectList)
	}
}

func TestScalarCTESourceIdentity(t *testing.T) {
	base := &Node{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{1},
		ObjRef: &planpb.ObjectRef{Obj: 42}, TableDef: &planpb.TableDef{Name: "t"},
		ScanSnapshot: &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}},
		ProjectList:  []*Expr{{Typ: Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1}}}}}
	mutations := map[string]func(*Node){
		"object":     func(n *Node) { n.ObjRef.Obj++ },
		"snapshot":   func(n *Node) { n.ScanSnapshot.TS.PhysicalTime++ },
		"schema":     func(n *Node) { n.TableDef.Name = "other" },
		"column":     func(n *Node) { n.ProjectList[0].GetCol().ColPos++ },
		"type":       func(n *Node) { n.ProjectList[0].Typ.Id = int32(types.T_int32) },
		"expression": func(n *Node) { n.ProjectList[0] = MakePlan2Int64ConstExprWithType(0) },
		"limit":      func(n *Node) { n.Limit = MakePlan2Uint64ConstExprWithType(1) },
		"children":   func(n *Node) { n.Children = []int32{0} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			other := proto.Clone(base).(*Node)
			other.BindingTags[0] = 2
			other.ProjectList[0].GetCol().RelPos = 2
			builder := &QueryBuilder{qry: &Query{Nodes: []*Node{base, other}}}
			require.True(t, builder.scalarCTESourcesEqual(0, 1, make(map[int32]int32)))
			require.False(t, builder.scalarCTESourcesEqual(0, 1, map[int32]int32{1: 3}))
			mutate(other)
			require.False(t, builder.scalarCTESourcesEqual(0, 1, make(map[int32]int32)))
		})
	}
	q := scalarCTETestPlan(t, `select a from (with c as (select max(l_orderkey) a from lineitem) select a from c) x
		union all select a from (with c as (select max(l_orderkey) a from lineitem) select a from c) y`, math.MaxUint64)
	require.Len(t, scalarCTEReachable(q, planpb.Node_TABLE_SCAN), 2, "identical text in different CTE scopes is not declaration identity")
}

func TestScalarCTENestedNumericAggregate(t *testing.T) {
	sql := `with p as (
		select l_returnflag period, -sum(case when l_comment like '%/x/%' then cast(l_orderkey as double) else 0 end) a
		from lineitem where l_returnflag in ('A', 'B') and not (l_comment like 'E%' or l_comment like 'F%') group by l_returnflag),
		c as (select max(case when period='A' then a else null end) a, max(case when period='B' then a else null end) b from p),
		d as (select c.*, a/nullif(b,0) ratio from c)
		select row_number() over(), v from (select a v from d union all select ratio from d) s limit 2`
	for _, amount := range []string{"cast(l_orderkey as double)", "cast(l_extendedprice as decimal(20,2))"} {
		q := scalarCTETestPlan(t, strings.ReplaceAll(sql, "cast(l_orderkey as double)", amount), math.MaxUint64)
		require.Len(t, scalarCTEReachable(q, planpb.Node_TABLE_SCAN), 1, amount)
		require.Len(t, scalarCTEReachable(q, planpb.Node_SINK_SCAN), 2, amount)
	}
}

func TestCTEScalarUnionRowBound(t *testing.T) {
	builder := &QueryBuilder{qry: &Query{Nodes: []*Node{
		{NodeType: planpb.Node_TABLE_SCAN},
		{NodeType: planpb.Node_AGG, Children: []int32{0}},
		{NodeType: planpb.Node_PROJECT, Children: []int32{1}},
		{NodeType: planpb.Node_UNION_ALL, Children: []int32{2, 2}},
	}}}
	bound := func(id int32) uint64 { return builder.cteScalarUnionRowBound(id, make(map[int32]uint64)) }
	require.Equal(t, uint64(1), bound(1))
	require.Equal(t, uint64(2), bound(3))
	for _, typ := range []planpb.Node_NodeType{planpb.Node_PROJECT, planpb.Node_FILTER, planpb.Node_SORT, planpb.Node_WINDOW} {
		builder.qry.Nodes[2].NodeType = typ
		builder.qry.Nodes[2].Limit = MakePlan2Uint64ConstExprWithType(0)
		builder.qry.Nodes[2].Offset = MakePlan2Uint64ConstExprWithType(100)
		require.Equal(t, uint64(2), bound(3), "LIMIT/OFFSET must not tighten the bound")
	}
	for _, id := range []int32{-1, 99, 0} {
		require.Zero(t, bound(id))
	}
	builder.qry.Nodes[0] = nil
	require.Zero(t, bound(0))
	builder.qry.Nodes[1].GroupBy = []*Expr{{}}
	require.Zero(t, bound(3))
	builder.qry.Nodes[1].GroupBy = nil
	builder.qry.Nodes[1].GroupingFlag = []bool{true}
	require.Zero(t, bound(3))
	builder.qry.Nodes[1].GroupingFlag = nil
	builder.qry.Nodes[2].Children = []int32{2}
	require.Zero(t, bound(3), "cycles are unknown")
	builder.qry.Nodes[2].Children = []int32{1}
	builder.qry.Nodes[3].Children = []int32{2}
	require.Zero(t, bound(3))
	// A small DAG represents exponential fan-out without allocating 2^64 nodes.
	for i := 4; i < 68; i++ {
		builder.qry.Nodes = append(builder.qry.Nodes, &Node{NodeType: planpb.Node_UNION_ALL, Children: []int32{int32(i - 1), int32(i - 1)}})
	}
	builder.qry.Nodes[3].Children = []int32{1, 1}
	require.Zero(t, bound(67), "overflow is unknown, not a truncated upper bound")
}

func TestScalarCTEUnsafeConsumersInline(t *testing.T) {
	for _, sql := range []string{
		"with c as (select max(l_orderkey) a from lineitem) select a from c union all select a from c limit 0",
		"with c as (select max(l_orderkey) a from lineitem limit 1) select a from c union all select a from c",
		"with c as (select l_orderkey a from lineitem group by l_orderkey) select a from c union all select a from c",
		"with c as (select max(l_orderkey) a, rand() b from lineitem) select a from c union all select a from c",
		"with c as (select max(l_orderkey) a from lineitem) select a from c where a > 1 union all select a from c",
		"with c as (select max(l_orderkey) a from lineitem) select c.a from c join c d on c.a=d.a",
		"with c as (select sum(l_orderkey) a from lineitem) select a from c union all select a from c",
		"with c as (select sum(cast(l_extendedprice as decimal(38,2))) a from lineitem) select a from c union all select a from c",
		"with c as (select max(abs(l_orderkey)) a from lineitem) select a from c union all select a from c",
		"with c as (select max(l_comment) a from lineitem) select a from c union all select a from c",
	} {
		q := scalarCTETestPlan(t, sql, math.MaxUint64)
		require.Empty(t, scalarCTEReachable(q, planpb.Node_SINK_SCAN), sql)
	}
	branches := make([]string, 65)
	for i := range branches {
		branches[i] = "select a from c"
	}
	q := scalarCTETestPlan(t, "with c as (select max(l_orderkey) a from lineitem) "+strings.Join(branches, " union all "), math.MaxUint64)
	require.Empty(t, scalarCTEReachable(q, planpb.Node_SINK_SCAN), "fan-out metadata must remain bounded")
	q = scalarCTETestPlan(t, "with c as (select max(l_orderkey) a from lineitem) select a from c union all select a from c", 1)
	require.Empty(t, scalarCTEReachable(q, planpb.Node_SINK_SCAN))
}
