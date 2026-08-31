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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func useLegacyGroupingSetPlan(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	proc := mock.CurrentContext().GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
}

func TestGroupingSetInputSharingProtocolGate(t *testing.T) {
	const sql = `select l_returnflag, l_linestatus,
		grouping(l_returnflag), grouping(l_linestatus), count(*)
		from lineitem
		group by l_returnflag, l_linestatus with rollup`

	ctx := NewMockCompilerContext(true)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
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

	legacy := reachableGroupingSetShape(build(defines.MORPCVersion40))
	require.Equal(t, 3, legacy.tableScans)
	require.Zero(t, legacy.expandProjects)

	shared := reachableGroupingSetShape(build(defines.MORPCVersion43))
	require.Equal(t, 1, shared.tableScans)
	require.Equal(t, 1, shared.aggregates)
	require.Equal(t, 1, shared.expandProjects)
	require.Equal(t, 1, shared.aggregatesOnExpand)
	require.Equal(t, 3, shared.sinkScans)
	require.Equal(t, []bool{true, true, true, false, false, false}, shared.flags)
}

type groupingSetShape struct {
	tableScans         int
	aggregates         int
	expandProjects     int
	aggregatesOnExpand int
	sinkScans          int
	flags              []bool
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
		case planpb.Node_PROJECT:
			if _, ok := DecodeGroupingSetExpandOption(node.ExtraOptions); ok {
				shape.expandProjects++
				shape.flags = append([]bool(nil), node.GroupingFlag...)
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
