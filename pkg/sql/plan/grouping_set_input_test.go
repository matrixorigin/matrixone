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
	"math"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func useLegacyGroupingSetPlan(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	proc := mock.CurrentContext().GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion47)
}

func TestGroupingSetInputSharingProtocolGate(t *testing.T) {
	const sql = `select l_returnflag, l_linestatus,
		grouping(l_returnflag), grouping(l_linestatus), count(*)
		from lineitem
		group by l_returnflag, l_linestatus with rollup`

	ctx := NewMockCompilerContext(true)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	})

	build := func(version int64) *planpb.Query {
		t.Helper()
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		stmt, err := mysql.ParseOne(context.Background(), sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		return built.GetQuery()
	}

	legacy := reachableGroupingSetShape(build(defines.MORPCVersion48))
	require.Equal(t, 3, legacy.tableScans)
	require.Zero(t, legacy.expandProjects)

	shared := reachableGroupingSetShape(build(defines.MORPCVersion49))
	require.Equal(t, 1, shared.tableScans)
	require.Equal(t, 1, shared.aggregates)
	require.Equal(t, 1, shared.expandProjects)
	require.Equal(t, 1, shared.aggregatesOnExpand)
	require.Equal(t, 3, shared.sinkScans)
	require.Equal(t, 1, shared.materializedSinks)
	require.Equal(t, []bool{true, true, true, false, false, false}, shared.flags)
	require.True(t, shared.hasEmptyRowMarker)

	rt.SetGlobalVariables("optimizer_hints", "sharedComputation=1")
	rolledBack := reachableGroupingSetShape(build(defines.MORPCVersion49))
	require.Equal(t, 3, rolledBack.tableScans)
	require.Zero(t, rolledBack.expandProjects)
}

func TestGroupingSetInputSharingRejectsInheritedGroupingSentinel(t *testing.T) {
	const sql = `select d.l_returnflag, grouping(d.l_returnflag), count(*)
		from (
			select l_returnflag
			from lineitem
			group by rollup(l_returnflag)
		) d
		group by rollup(d.l_returnflag)`

	ctx := NewMockCompilerContext(true)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion49)
	rt.SetGlobalVariables("optimizer_hints", "")

	stmt, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	shape := reachableGroupingSetShape(built.GetQuery())
	// Dynamic grouping distinguishes its own sentinel from SQL NULL. The outer
	// ROLLUP therefore cannot accept the inner sentinel, and sharing the inner
	// ROLLUP cannot change the sentinel's legacy representation before the outer
	// active grouping branch consumes it.
	require.Zero(t, shape.expandProjects)
	require.Zero(t, shape.sinkScans)
}

func TestGroupingSetInputSharingRequiresLegacyDrainWitness(t *testing.T) {
	const sql = `select cast('' as varchar) as g, cast(0 as unsigned) as n
		union all
		select l_returnflag, count(*)
		from lineitem
		group by rollup(l_returnflag)
		limit 1`

	ctx := NewMockCompilerContext(true)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion49)
	rt.SetGlobalVariables("optimizer_hints", "")

	stmt, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	shape := reachableGroupingSetShape(built.GetQuery())
	require.Zero(t, shape.expandProjects)
	require.Zero(t, shape.sinkScans)
}

func TestGroupingSetSentinelDetectionFollowsMaterializedSource(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{
		Nodes: []*planpb.Node{
			{NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}},
			{NodeType: planpb.Node_SINK, Children: []int32{2}},
			{NodeType: planpb.Node_PROJECT, Children: []int32{3}},
			{
				NodeType:     planpb.Node_PROJECT,
				GroupingFlag: []bool{true, false},
				ExtraOptions: groupingSetExpandOptionPrefix + "2",
			},
			{NodeType: planpb.Node_TABLE_SCAN},
		},
		Steps: []int32{1},
	}}

	require.True(t, builder.subtreeMayExposeGroupingSentinel(0, make(map[int32]bool)))
	require.False(t, builder.subtreeMayExposeGroupingSentinel(4, make(map[int32]bool)))
}

func TestRewriteGroupingSetExprPreservesSQLBitOrder(t *testing.T) {
	groupingExpr := func() *planpb.Expr {
		args := make([]*planpb.Expr, 3)
		for i := range args {
			args[i] = groupingSetCol(
				planpb.Type{Id: int32(types.T_int64)}, 7, int32(i))
		}
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "grouping"},
				Args: args,
			}},
		}
	}

	for _, test := range []struct {
		name  string
		flags []bool
		want  int64
	}{
		{name: "all active", flags: []bool{true, true, true}, want: 0},
		{name: "rightmost inactive", flags: []bool{true, true, false}, want: 1},
		{name: "middle inactive", flags: []bool{true, false, true}, want: 2},
		{name: "leftmost inactive", flags: []bool{false, true, true}, want: 4},
		{name: "outer inactive", flags: []bool{false, true, false}, want: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr := groupingExpr()
			agg := &planpb.Node{
				BindingTags:  []int32{7, 8},
				GroupingFlag: test.flags,
			}

			rewriteGroupingSetExpr(expr, agg, 9, len(test.flags))

			require.Equal(t, test.want, expr.GetLit().GetI64Val())
		})
	}
}

func TestGroupingSetDeclaredRowSize(t *testing.T) {
	for _, test := range []struct {
		name string
		typs []planpb.Type
		want float64
		ok   bool
	}{
		{
			name: "fixed and nullable",
			typs: []planpb.Type{
				{Id: int32(types.T_int64), NotNullable: true},
				{Id: int32(types.T_int64)},
			},
			want: 17,
			ok:   true,
		},
		{
			name: "varchar declared characters",
			typs: []planpb.Type{{Id: int32(types.T_varchar), Width: 10}},
			want: float64(types.VarlenaSize + 40 + 1),
			ok:   true,
		},
		{
			name: "array declared elements",
			typs: []planpb.Type{{
				Id: int32(types.T_array_float32), Width: 3, NotNullable: true,
			}},
			want: float64(types.VarlenaSize + 12),
			ok:   true,
		},
		{name: "missing variable capacity", typs: []planpb.Type{{Id: int32(types.T_text)}}, ok: false},
		{name: "unsupported tuple", typs: []planpb.Type{{Id: int32(types.T_tuple), Width: 8}}, ok: false},
		{name: "future type", typs: []planpb.Type{{Id: 256, Width: 8}}, ok: false},
		{name: "empty schema", ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := materializedDeclaredRowSize(test.typs)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.want, got)
		})
	}
}

func TestGroupingSetSharingFitsCostAndStorage(t *testing.T) {
	for _, test := range []struct {
		name                                   string
		producerCost, inputSize, rows, outSize float64
		branches                               int
		want                                   bool
	}{
		{name: "clear byte-work win", producerCost: 1000, inputSize: 8, rows: 10, outSize: 16, branches: 3, want: true},
		{name: "wide aggregate output loses", producerCost: 1000, inputSize: 8, rows: 10, outSize: 1000, branches: 3},
		{name: "single output row exceeds record safety bound", producerCost: 1e15, inputSize: 8, rows: 1, outSize: float64(materialized.MaxSpillBatchBytes)/2 + 1, branches: 3},
		{name: "branch scan traffic loses", producerCost: 40, inputSize: 8, rows: 10, outSize: 16, branches: 20},
		{name: "spill ceiling", producerCost: math.MaxFloat64 / 16, inputSize: 8, rows: groupingSetEstimatedSpillBytesLimit/8 + 1, outSize: 8, branches: 2},
		{name: "single branch", producerCost: 1000, inputSize: 8, rows: 10, outSize: 16, branches: 1},
		{name: "unknown input width", producerCost: 1000, rows: 10, outSize: 16, branches: 3},
		{name: "nan producer", producerCost: math.NaN(), inputSize: 8, rows: 10, outSize: 16, branches: 3},
		{name: "overflowed traffic", producerCost: math.MaxFloat64, inputSize: 2, rows: math.MaxFloat64, outSize: 2, branches: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, groupingSetSharingFitsCostAndStorage(
				test.producerCost, test.inputSize, test.rows, test.outSize, test.branches))
		})
	}
}

type groupingSetShape struct {
	tableScans         int
	aggregates         int
	expandProjects     int
	aggregatesOnExpand int
	sinkScans          int
	materializedSinks  int
	flags              []bool
	hasEmptyRowMarker  bool
}

func reachableGroupingSetShape(query *planpb.Query) groupingSetShape {
	shape := groupingSetShape{}
	seen := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || seen[nodeID] {
			return
		}
		seen[nodeID] = true
		node := query.Nodes[nodeID]
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			shape.tableScans++
		case planpb.Node_AGG:
			shape.aggregates++
			if len(node.Children) == 1 {
				if _, ok := DecodeGroupingSetExpandOption(query.Nodes[node.Children[0]].ExtraOptions); ok {
					shape.aggregatesOnExpand++
				}
			}
		case planpb.Node_SINK_SCAN:
			shape.sinkScans++
		case planpb.Node_SINK:
			if node.ExtraOptions == materialized.CTESinkOption {
				shape.materializedSinks++
			}
		case planpb.Node_PROJECT:
			if _, ok := DecodeGroupingSetExpandOption(node.ExtraOptions); ok {
				shape.expandProjects++
				shape.flags = append([]bool(nil), node.GroupingFlag...)
				if len(node.ProjectList) >= 2 {
					marker := node.ProjectList[len(node.ProjectList)-2]
					setID := node.ProjectList[len(node.ProjectList)-1]
					shape.hasEmptyRowMarker = types.T(marker.Typ.Id) == types.T_bool &&
						types.T(setID.Typ.Id) == types.T_int64
				}
			}
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, stepID := range query.Steps {
		visit(stepID)
	}
	return shape
}
