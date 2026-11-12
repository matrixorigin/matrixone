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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func scalarCTEUnionSQL(branches, limit int) string {
	arms := make([]string, branches)
	for i := range arms {
		metric := "x + r"
		if i%2 != 0 {
			metric = "y - r"
		}
		arms[i] = "select " + metric + " as v from calc"
	}
	return fmt.Sprintf(`with grouped as (
		select l_orderkey, sum(l_extendedprice) as x, sum(l_discount) as y
		from lineitem group by l_orderkey
	), scalar as (
		select max(x) as x, max(y) as y from grouped
	), calc as (
		select scalar.*, x / nullif(y, 0) as r from scalar
	)
	select row_number() over () as rn, u.v from (%s) u limit %d`, strings.Join(arms, " union all "), limit)
}

func TestCTEBoundedNestedScalarReuse(t *testing.T) {
	for _, branches := range []int{2, 4, 8} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("branches_%d_limit_%d", branches, branches+delta), func(t *testing.T) {
				logicPlan, err := runOneStmt(NewMockOptimizer(false), t, scalarCTEUnionSQL(branches, branches+delta))
				require.NoError(t, err)
				query := logicPlan.GetQuery()
				wantScans, wantReaders := branches, 0
				if delta >= 0 {
					wantScans, wantReaders = 1, branches
				}
				require.Equal(t, wantScans, countReachableNodeType(query, planpb.Node_TABLE_SCAN))
				require.Equal(t, wantReaders, countReachableNodeType(query, planpb.Node_SINK_SCAN))
			})
		}
	}
}

func TestCTEBoundedScalarFormattedCTAS(t *testing.T) {
	mock := NewMockOptimizer(false)
	for _, col := range mock.ctxt.tables["lineitem"].Cols {
		if col.Name == "l_extendedprice" || col.Name == "l_discount" {
			col.Typ = planpb.Type{Id: int32(types.T_float64)}
		}
	}
	sql := strings.ReplaceAll(scalarCTEUnionSQL(4, 4), "x + r", "concat(round(r*100,2),'%')")
	sql = strings.ReplaceAll(sql, "y - r", "format(y,2)")
	logicPlan, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	require.Equal(t, 1, countReachableNodeType(logicPlan.GetQuery(), planpb.Node_TABLE_SCAN))
	ctas, err := buildSingleStmt(mock, t, "create table scalar_metrics as "+sql)
	require.NoError(t, err)
	create := ctas.GetDdl().GetCreateTable()
	table := create.GetTableDef()
	table.TblId = 99102
	// The catalog assigns relkind when CREATE executes; a raw DDL TableDef
	// without it would exercise the unrelated legacy INSERT fallback.
	table.TableType = catalog.SystemOrdinaryRel
	table.Cols = append(table.Cols, MakeRowIdColDef())
	table.Name2ColIndex = make(map[string]int32, len(table.Cols))
	for i, col := range table.Cols {
		table.Name2ColIndex[col.Name] = int32(i)
	}
	mock.ctxt.objects[table.Name] = &ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	mock.ctxt.tables[table.Name] = table
	mock.ctxt.id2name[table.TblId] = table.Name
	mock.ctxt.pks[table.Name] = nil
	insertPlan, err := runOneStmt(mock, t, create.GetCreateAsSelectSql())
	require.NoError(t, err)
	baseScans := 0
	for id := range cteReachablePlanNodes(insertPlan.GetQuery()) {
		node := insertPlan.GetQuery().Nodes[id]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef.GetName() == "lineitem" {
			baseScans++
		}
	}
	require.Equal(t, 1, baseScans, "the exact SQL emitted by CTAS must use the shared producer")
}

func TestCTEBoundedNestedScalarKeepsUnsafeShapesInline(t *testing.T) {
	prefix := `with g as (select l_orderkey, sum(l_extendedprice) x, sum(l_discount) y
		from lineitem group by l_orderkey), s as (select max(x) x, max(y) y from g) `
	for _, tc := range []struct {
		name, sql string
	}{
		{"unused producer column", prefix + `select x from s union all select x from s limit 2`},
		{"dead union projection", prefix + `select u.x from (select x, abs(y) unused from s union all select x, abs(y) from s) u limit 2`},
		{"conditional demand", prefix + `select if(false, x, 0) from s union all select y from s limit 2`},
		{"nested constant null", prefix + `select x + cast(null as decimal(38,2)) from s union all select y from s limit 2`},
		{"zero limit", prefix + `select x from s union all select y from s limit 0`},
		{"offset", prefix + `select x from s union all select y from s limit 2 offset 1`},
		{"consumer filter", prefix + `select x from s where y > 0 union all select y from s limit 2`},
		{"conditional join", prefix + `select s.x from s join nation on s.y=n_nationkey union all select y from s limit 2`},
		{"count only", prefix + `select count(*) from (select x from s union all select y from s) u`},
		{"distinct", prefix + `select x from s union select y from s limit 2`},
		{"unsupported window", prefix + `select sum(x) over () from s union all select y from s limit 2`},
		{"producer having", strings.Replace(prefix, "max(y) y from g)", "max(y) y from g having max(x)>0)", 1) + `select x from s union all select y from s limit 2`},
		{"internal limit", strings.Replace(prefix, "max(y) y from g)", "max(y) y from g limit 1)", 1) + `select x from s union all select y from s limit 2`},
		{"grouped producer", strings.Replace(prefix, "max(y) y from g)", "max(y) y from g group by l_orderkey)", 1) + `select x from s union all select y from s limit 2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			require.Zero(t, countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN))
		})
	}
}

func TestCTEScalarLimitCovers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit *planpb.Expr
		bound uint64
		want  bool
	}{
		{"absent", nil, 8, true},
		{"equal", makePlan2Int64ConstExprWithType(8), 8, true},
		{"smaller", makePlan2Int64ConstExprWithType(7), 8, false},
		{"negative", makePlan2Int64ConstExprWithType(-1), 8, false},
		{"zero", makePlan2Int64ConstExprWithType(0), 0, false},
		{"unsigned", makePlan2Uint64ConstExprWithType(math.MaxUint64), math.MaxUint64, true},
		{"signed overflow", makePlan2Int64ConstExprWithType(math.MaxInt64), math.MaxUint64, false},
		{"null", makePlan2NullConstExprWithType(), 1, false},
		{"parameter", &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}, 1, false},
		{"wrong type", makePlan2BoolConstExprWithType(true), 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, cteScalarLimitCovers(tc.limit, tc.bound)) })
	}
}

func TestCTEScalarIdentity(t *testing.T) {
	col := func(tag int32) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: tag}}}
	}
	makeBuilder := func() *QueryBuilder {
		return &QueryBuilder{qry: &Query{Nodes: []*Node{
			{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{1}},
			{NodeType: planpb.Node_AGG, BindingTags: []int32{2}, Children: []int32{0}, AggList: []*planpb.Expr{col(1)}},
			{NodeType: planpb.Node_PROJECT, BindingTags: []int32{3}, Children: []int32{1}, ProjectList: []*planpb.Expr{col(2)}},
		}}}
	}
	t.Run("semantic identity", func(t *testing.T) {
		original, ok := makeBuilder().cteScalarIdentity(2)
		require.True(t, ok)
		renamed := makeBuilder()
		for i, node := range renamed.qry.Nodes {
			node.NodeId = int32(i + 10)
			node.BindingTags[0] += 10
		}
		renamed.qry.Nodes[1].AggList[0] = col(11)
		renamed.qry.Nodes[2].ProjectList[0] = col(12)
		other, ok := renamed.cteScalarIdentity(2)
		require.True(t, ok)
		for i := range original {
			require.True(t, proto.Equal(original[i], other[i]))
		}
		renamed.qry.Nodes[0].ScanSnapshot = &planpb.Snapshot{Tenant: &planpb.SnapshotTenant{TenantID: 7}}
		other, ok = renamed.cteScalarIdentity(2)
		require.True(t, ok)
		require.False(t, proto.Equal(original[2], other[2]), "scan snapshot belongs to producer identity")
		renamed.qry.Nodes[1].AggList[0].Typ.Scale = 2
		other, ok = renamed.cteScalarIdentity(2)
		require.True(t, ok)
		require.False(t, proto.Equal(original[1], other[1]), "internal expression types belong to producer identity")
	})
	for _, tc := range []struct {
		name   string
		mutate func(*QueryBuilder)
	}{
		{"cycle", func(b *QueryBuilder) { b.qry.Nodes[1].Children[0] = 2 }},
		{"invalid child", func(b *QueryBuilder) { b.qry.Nodes[1].Children[0] = 99 }},
		{"nil node", func(b *QueryBuilder) { b.qry.Nodes[0] = nil }},
		{"nested shared source", func(b *QueryBuilder) { b.qry.Nodes[0].NodeType = planpb.Node_SINK_SCAN }},
		{"duplicate tag", func(b *QueryBuilder) { b.qry.Nodes[1].BindingTags[0] = 1 }},
		{"correlated tag", func(b *QueryBuilder) { b.qry.Nodes[1].AggList[0] = col(99) }},
		{"computed output", func(b *QueryBuilder) { b.qry.Nodes[2].ProjectList[0] = makePlan2Int64ConstExprWithType(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := makeBuilder()
			_, ok := b.cteScalarIdentity(2)
			require.True(t, ok)
			tc.mutate(b)
			_, ok = b.cteScalarIdentity(2)
			require.False(t, ok)
		})
	}
}

func TestCTEBoundedNestedScalarInsert(t *testing.T) {
	source := strings.Replace(scalarCTEUnionSQL(4, 4), "as rn, u.v", "as rn, 'metric', u.v, 'scalar'", 1)
	for _, tc := range []struct {
		name, prefix, suffix string
		shared               bool
	}{
		{"plain insert", "insert into", "", true},
		{"insert ignore", "insert ignore into", "", false},
		{"on duplicate", "insert into", " on duplicate key update n_comment='same'", false},
		{"replace", "replace into", "", false},
		{"returning", "insert into", " returning n_nationkey", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
				tc.prefix+" nation (n_nationkey, n_name, n_regionkey, n_comment) "+source+tc.suffix)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			baseScans, sharedReaders := 0, 0
			for nodeID := range cteReachablePlanNodes(query) {
				node := query.Nodes[nodeID]
				if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef.GetName() == "lineitem" {
					baseScans++
				}
				if node.NodeType == planpb.Node_SINK_SCAN && node.TableDef.GetName() == "scalar" {
					sharedReaders++
				}
			}
			if tc.shared {
				require.Equal(t, 1, baseScans)
				require.Equal(t, 4, sharedReaders)
			} else {
				require.Equal(t, 4, baseScans)
				require.Zero(t, sharedReaders)
			}
		})
	}
}
