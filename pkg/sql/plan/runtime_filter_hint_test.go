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

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRuntimeFilterDisableHintOptionalKeys(t *testing.T) {
	for _, composite := range []bool{false, true} {
		name := "single"
		if composite {
			name = "composite"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				hints    *OptimizerHints
				disabled bool
			}{
				{name: "default"},
				{name: "zero", hints: &OptimizerHints{}},
				{name: "one", hints: &OptimizerHints{runtimeFilter: 1}, disabled: true},
				{name: "other nonzero", hints: &OptimizerHints{runtimeFilter: 2}, disabled: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					builder := newRuntimeFilterSingleTestBuilder(t, false)
					builder.optimizerHints = tc.hints
					join := builder.qry.Nodes[2]
					join.JoinType = planpb.Node_INNER
					probe := builder.qry.Nodes[0]
					if composite {
						configureRuntimeFilterCompositePK(builder)
					}
					setPlanTestGlobalVariable(t, builder.compCtx.GetProcess().GetService(),
						moruntime.MOProtocolVersion, defines.MORPCVersion43)
					builder.generateRuntimeFilters(2)
					if tc.disabled {
						require.Empty(t, join.RuntimeFilterBuildList)
						require.Empty(t, probe.RuntimeFilterProbeList)
						return
					}
					require.Len(t, join.RuntimeFilterBuildList, 1, "enabled control must admit RF")
					require.Len(t, probe.RuntimeFilterProbeList, 1)
					require.Equal(t, join.RuntimeFilterBuildList[0].Tag, probe.RuntimeFilterProbeList[0].Tag)
					if composite {
						require.Equal(t, planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_V1,
							join.RuntimeFilterBuildList[0].KeyEncoding)
						require.Equal(t, catalog.CPrimaryKeyColName, probe.RuntimeFilterProbeList[0].Expr.GetCol().Name)
					}
				})
			}
		})
	}
}

func TestScalarPredicateRuntimeFilterDisableHint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hint     string
		disabled bool
	}{
		{name: "default"},
		{name: "zero", hint: "runtimeFilter=0"},
		{name: "one", hint: "runtimeFilter=1", disabled: true},
		{name: "other nonzero", hint: "runtimeFilter=2", disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(true, newPlanTestProcess(t))
			service := optimizer.CurrentContext().GetProcess().GetService()
			setPlanTestGlobalVariable(t, service, moruntime.MOProtocolVersion, defines.MORPCVersion43)
			setPlanTestGlobalVariable(t, service, "optimizer_hints", tc.hint)
			logicPlan, err := runOneStmt(optimizer, t, `
				select n.n_name from tpch.nation n
				where n.n_regionkey = (
					select r.r_regionkey from tpch.region r where r.r_name = 'EUROPE'
				)`)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			var single *planpb.Node
			var filter *planpb.Node
			for _, node := range query.Nodes {
				if node.NodeType == planpb.Node_FILTER && len(node.Children) == 1 {
					child := query.Nodes[node.Children[0]]
					if child.NodeType == planpb.Node_JOIN && child.JoinType == planpb.Node_SINGLE && !child.IsRightJoin {
						filter, single = node, child
					}
				}
			}
			require.NotNil(t, filter, "logical FILTER/SINGLE must remain with either hint")
			require.NotEmpty(t, filter.FilterList)
			require.Empty(t, single.OnList)
			if tc.disabled {
				require.Empty(t, single.RuntimeFilterBuildList)
				for _, node := range query.Nodes {
					require.Empty(t, node.RuntimeFilterProbeList)
				}
				return
			}
			require.Len(t, single.RuntimeFilterBuildList, 1, "enabled scalar control must admit RF")
			spec := single.RuntimeFilterBuildList[0]
			require.True(t, spec.ScalarPredicate)
			require.Equal(t, int32(1), spec.UpperLimit)
			probes := 0
			for _, node := range query.Nodes {
				for _, probe := range node.RuntimeFilterProbeList {
					if probe.Tag == spec.Tag {
						require.Equal(t, planpb.Node_TABLE_SCAN, node.NodeType)
						probes++
					}
				}
			}
			require.Equal(t, 1, probes)
		})
	}
}

func TestRuntimeFilterDisableHintPreservesRequiredPairs(t *testing.T) {
	for _, joinType := range []planpb.Node_JoinType{planpb.Node_INDEX, planpb.Node_DEDUP} {
		t.Run(joinType.String(), func(t *testing.T) {
			for _, disabled := range []bool{false, true} {
				builder := newRuntimeFilterSingleTestBuilder(t, false)
				if disabled {
					builder.optimizerHints = &OptimizerHints{runtimeFilter: 1}
				}
				builder.qry.Nodes[2].JoinType = joinType
				setPlanTestGlobalVariable(t, builder.compCtx.GetProcess().GetService(),
					moruntime.MOProtocolVersion, defines.MORPCVersion43)
				builder.generateRuntimeFilters(2)
				require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
				require.Len(t, builder.qry.Nodes[0].RuntimeFilterProbeList, 1)
				require.Equal(t, builder.qry.Nodes[2].RuntimeFilterBuildList[0].Tag,
					builder.qry.Nodes[0].RuntimeFilterProbeList[0].Tag)
			}
		})
	}
	t.Run("preassigned MustApply", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(t, true)
		builder.optimizerHints = &OptimizerHints{runtimeFilter: 1}
		build := &planpb.RuntimeFilterSpec{Tag: 42, MustApply: true}
		probe := &planpb.RuntimeFilterSpec{Tag: 42, MustApply: true}
		builder.qry.Nodes[2].RuntimeFilterBuildList = []*planpb.RuntimeFilterSpec{build}
		builder.qry.Nodes[0].RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{probe}
		builder.generateRuntimeFilters(2)
		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, builder.qry.Nodes[0].RuntimeFilterProbeList, 1)
		require.Same(t, build, builder.qry.Nodes[2].RuntimeFilterBuildList[0])
		require.Same(t, probe, builder.qry.Nodes[0].RuntimeFilterProbeList[0])
		require.True(t, build.MustApply)
		require.True(t, probe.MustApply)
	})
	t.Run("shuffle parent optional child", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(t, false)
		builder.optimizerHints = &OptimizerHints{runtimeFilter: 1}
		builder.qry.Nodes[2].JoinType = planpb.Node_INNER
		parent := &planpb.Node{
			NodeId: 3, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{2, 1},
			Stats:    &planpb.Stats{HashmapStats: &planpb.HashMapStats{Shuffle: true}},
		}
		builder.qry.Nodes = append(builder.qry.Nodes, parent)
		builder.generateRuntimeFilters(3)
		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
		require.Len(t, parent.RuntimeFilterBuildList, 1)
		require.Len(t, parent.RuntimeFilterProbeList, 1)
		require.Equal(t, parent.RuntimeFilterBuildList[0].Tag, parent.RuntimeFilterProbeList[0].Tag)
		require.Nil(t, parent.RuntimeFilterBuildList[0].Expr)
		require.Nil(t, parent.RuntimeFilterProbeList[0].Expr)
	})
}
