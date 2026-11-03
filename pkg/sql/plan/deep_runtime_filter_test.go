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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func newDeepRuntimeFilterTestBuilder(t *testing.T) *QueryBuilder {
	b := newRuntimeFilterSingleTestBuilder(t, false)
	probe, _ := configureRuntimeFilterCompositePK(b)
	outer := b.qry.Nodes[2]
	outer.JoinType = planpb.Node_INNER
	outer.OnList = outer.OnList[:1]
	outer.OnList[0].GetF().Args[0].GetCol().Name = "a"
	probe.ProjectList = []*planpb.Expr{GetColExpr(probe.TableDef.Cols[0].Typ, 1, 0)}
	sibling := DeepCopyNode(probe)
	sibling.NodeId, sibling.BindingTags = 3, []int32{3}
	sibling.ProjectList[0].GetCol().RelPos = 3
	lower := &planpb.Node{NodeId: 4, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{3, 0}, OnList: []*planpb.Expr{makeRuntimeFilterTestEq(probe.TableDef.Cols[0].Typ, 3, 1, 0, 0)},
		Stats: &planpb.Stats{Outcnt: 1000000, Cost: 2000000, HashmapStats: &planpb.HashMapStats{Shuffle: true}},
	}
	filter := &planpb.Node{NodeId: 5, NodeType: planpb.Node_FILTER, Children: []int32{4}}
	b.qry.Nodes = append(b.qry.Nodes, sibling, lower, filter)
	outer.Children[0] = 5
	b.qry.StmtType, b.qry.Steps = planpb.Query_SELECT, []int32{2}
	return b
}

func TestDeepRuntimeFilterLineageAndGuards(t *testing.T) {
	setPlanTestGlobalVariable(t, "", "runtime_filter_limit_in", int64(3))
	for _, tc := range []struct {
		name string
		edit func(*QueryBuilder)
		want bool
	}{
		{name: "inner build descent and transparent filter", want: true},
		{name: "commuted equality retains original ON", want: true, edit: func(b *QueryBuilder) {
			args := b.qry.Nodes[2].OnList[0].GetF().Args
			args[0], args[1] = args[1], args[0]
		}},
		{name: "total widening text predicate on probe path", want: true, edit: func(b *QueryBuilder) {
			charType := planpb.Type{Id: int32(types.T_char), Width: 1}
			varcharType := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
			for _, id := range []int{0, 3} {
				b.qry.Nodes[id].TableDef.Cols[1].Typ = charType
			}
			left, err := appendCastBeforeExpr(b.GetContext(), GetColExpr(charType, 1, 1), varcharType)
			require.NoError(t, err)
			right, err := appendCastBeforeExpr(b.GetContext(), GetColExpr(charType, 3, 1), varcharType)
			require.NoError(t, err)
			predicate, err := BindFuncExprImplByPlanExpr(b.GetContext(), "<>", []*planpb.Expr{left, right})
			require.NoError(t, err)
			b.qry.Nodes[5].FilterList = []*planpb.Expr{predicate}
		}},
		{name: "fallible skipped sibling", edit: func(b *QueryBuilder) {
			b.qry.Nodes[3].FilterList = []*planpb.Expr{makeMixedSideRuntimeFilterResidual(b.qry.Nodes[0].TableDef.Cols[0].Typ, 3, 3)}
		}},
		{name: "barrier", edit: func(b *QueryBuilder) { b.qry.Nodes[5].FilterIsBarrier = true }},
		{name: "local limit", edit: func(b *QueryBuilder) { b.qry.Nodes[5].Limit = makePlan2Int64ConstExprWithType(1) }},
		{name: "outer join", edit: func(b *QueryBuilder) { b.qry.Nodes[4].JoinType = planpb.Node_LEFT }},
		{name: "shared subtree", edit: func(b *QueryBuilder) { b.qry.Nodes[4].Children[0] = 0 }},
		{name: "source step", edit: func(b *QueryBuilder) { b.qry.Nodes[3].SourceStep = []int32{0} }},
		{name: "unknown binding", edit: func(b *QueryBuilder) { b.qry.Nodes[2].OnList[0].GetF().Args[0].GetCol().RelPos = 99 }},
		{name: "ordinary key limit", edit: func(b *QueryBuilder) { b.qry.Nodes[2].Stats.HashmapStats.HashmapSize = 4 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newDeepRuntimeFilterTestBuilder(t)
			if tc.edit != nil {
				tc.edit(b)
			}
			before := proto.Clone(b.qry).(*planpb.Query)
			b.generateRuntimeFilters(2)
			outer, scan := b.qry.Nodes[2], b.qry.Nodes[0]
			if !tc.want {
				require.Empty(t, outer.RuntimeFilterBuildList)
				require.Empty(t, scan.RuntimeFilterProbeList)
				return
			}
			require.Len(t, outer.RuntimeFilterBuildList, 1)
			require.Len(t, scan.RuntimeFilterProbeList, 1)
			require.True(t, scan.RuntimeFilterProbeList[0].NotOnPk)
			require.Equal(t, int32(3), outer.RuntimeFilterBuildList[0].UpperLimit)
			tags, safe := DeepOrdinaryRuntimeFilterTags(b.qry)
			require.True(t, safe)
			require.Equal(t, []int32{outer.RuntimeFilterBuildList[0].Tag}, tags)
			// Keep the original estimates and distribution; compile may remove
			// the optional pair without leaving optimistic RF-only statistics.
			for i, node := range b.qry.Nodes {
				copy := DeepCopyNode(node)
				copy.RuntimeFilterBuildList, copy.RuntimeFilterProbeList = nil, nil
				require.True(t, proto.Equal(before.Nodes[i], copy))
			}
		})
	}
}

func TestDeepRuntimeFilterRejectsCyclicScanWait(t *testing.T) {
	b := newDeepRuntimeFilterTestBuilder(t)
	const tag int32 = 7
	b.qry.Nodes[2].RuntimeFilterBuildList = []*planpb.RuntimeFilterSpec{{Tag: tag, BuildExpr: makePlan2Int64ConstExprWithType(1)}}
	// The build scan cannot wait for the join whose build it supplies. An
	// unsafe candidate must be returned for physical fallback, not ignored.
	b.qry.Nodes[1].RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{{Tag: tag, Expr: makePlan2Int64ConstExprWithType(1)}}
	tags, safe := DeepOrdinaryRuntimeFilterTags(b.qry)
	require.Equal(t, []int32{tag}, tags)
	require.False(t, safe)
	b.qry.Nodes[0].RuntimeFilterProbeList = b.qry.Nodes[1].RuntimeFilterProbeList
	b.qry.Nodes[1].RuntimeFilterProbeList = nil
	_, safe = DeepOrdinaryRuntimeFilterTags(b.qry)
	require.True(t, safe)
}
