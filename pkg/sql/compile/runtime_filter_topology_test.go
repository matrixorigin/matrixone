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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func makeRightSingleTopologyPlan(tag int32) *plan.Query {
	probeSpec := &plan.RuntimeFilterSpec{Tag: tag, Expr: &plan.Expr{}}
	buildSpec := &plan.RuntimeFilterSpec{Tag: tag, Expr: &plan.Expr{}}
	return &plan.Query{Nodes: []*plan.Node{
		{
			NodeType:               plan.Node_TABLE_SCAN,
			NodeId:                 0,
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{probeSpec},
		},
		{
			NodeType:               plan.Node_JOIN,
			NodeId:                 1,
			JoinType:               plan.Node_SINGLE,
			IsRightJoin:            true,
			Children:               []int32{0},
			Stats:                  &plan.Stats{HashmapStats: &plan.HashMapStats{}},
			RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{buildSpec},
		},
	}}
}

func makeRuntimeFilterConsumerScope(tag int32, cn engine.Node) *Scope {
	return &Scope{
		NodeInfo: cn,
		DataSource: &Source{node: &plan.Node{
			NodeType:               plan.Node_TABLE_SCAN,
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Tag: tag}},
		}},
	}
}

func makeRuntimeFilterProducerScope(tag int32, cn engine.Node) (*Scope, *hashbuild.HashBuild) {
	build := hashbuild.NewArgument()
	build.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tag}
	return &Scope{NodeInfo: cn, RootOp: build}, build
}

func TestValidateRightSingleRuntimeFilterTopology(t *testing.T) {
	const tag int32 = 7
	cn1 := engine.Node{Id: "cn1", Addr: "cn1:6001"}
	cn2 := engine.Node{Id: "cn2", Addr: "cn2:6001"}
	qry := makeRightSingleTopologyPlan(tag)
	compiled := []int32{1}

	t.Run("one local producer is reachable", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{consumer}))
	})

	t.Run("standalone scopes without CN metadata are colocated", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, engine.Node{})
		producer, build := makeRuntimeFilterProducerScope(tag, engine.Node{})
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{consumer}))
	})

	t.Run("partial CN metadata is rejected as ambiguous", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, engine.Node{})
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		err := validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "consumer <local> cannot reach colocated producer")
		require.ErrorContains(t, err, "cn1:6001")
	})

	t.Run("remote-only producer is rejected before execution", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn2)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		err := validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "tag 7")
		require.ErrorContains(t, err, "cn2:6001")
		require.ErrorContains(t, err, "cannot reach colocated producer")
		require.ErrorContains(t, err, "cn1:6001")
	})

	t.Run("missing producer is rejected", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)

		err := validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "tag 7 has no producer")
	})

	t.Run("duplicate local producers are rejected", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer1, build1 := makeRuntimeFilterProducerScope(tag, cn1)
		producer2, build2 := makeRuntimeFilterProducerScope(tag, cn1)
		defer build1.Release()
		defer build2.Release()
		consumer.PreScopes = []*Scope{producer1, producer2}

		err := validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "2 producers")
		require.ErrorContains(t, err, "expected exactly one colocated producer")
	})

	t.Run("one producer on each CN is not a colocated phase-1 topology", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer1, build1 := makeRuntimeFilterProducerScope(tag, cn1)
		producer2, build2 := makeRuntimeFilterProducerScope(tag, cn2)
		defer build1.Release()
		defer build2.Release()
		consumer.PreScopes = []*Scope{producer1, producer2}

		err := validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "2 producers")
		require.ErrorContains(t, err, "cn1:6001")
		require.ErrorContains(t, err, "cn2:6001")
	})

	t.Run("missing scan consumer is rejected", func(t *testing.T) {
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()

		err := validateRightSingleRuntimeFilterTopology(qry, compiled, []*Scope{producer})
		require.ErrorContains(t, err, "tag 7 has no scan consumer")
	})

	t.Run("fully unmaterialized tag from a pruned subtree is ignored", func(t *testing.T) {
		require.NoError(t, validateRightSingleRuntimeFilterTopology(qry, nil, nil))
	})

	t.Run("fully missing topology for a compiled subtree is rejected", func(t *testing.T) {
		err := validateRightSingleRuntimeFilterTopology(qry, compiled, nil)
		require.ErrorContains(t, err, "tag 7 has no scan consumer")
	})

	t.Run("pruned tag does not hide a partial executed topology", func(t *testing.T) {
		const prunedTag int32 = 8
		mixedQry := makeRightSingleTopologyPlan(tag)
		prunedJoin := makeRightSingleTopologyPlan(prunedTag).Nodes[1]
		mixedQry.Nodes = append(mixedQry.Nodes, prunedJoin)
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)

		err := validateRightSingleRuntimeFilterTopology(mixedQry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "tag 7 has no producer")
	})

	t.Run("pruned tag does not invalidate a complete executed topology", func(t *testing.T) {
		const prunedTag int32 = 8
		mixedQry := makeRightSingleTopologyPlan(tag)
		prunedJoin := makeRightSingleTopologyPlan(prunedTag).Nodes[1]
		mixedQry.Nodes = append(mixedQry.Nodes, prunedJoin)
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateRightSingleRuntimeFilterTopology(mixedQry, compiled, []*Scope{consumer}))
	})

	t.Run("unrelated and shuffle runtime filters are ignored", func(t *testing.T) {
		noRightSingle := &plan.Query{Nodes: []*plan.Node{{
			NodeType:               plan.Node_TABLE_SCAN,
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Tag: tag}},
		}}}
		require.NoError(t, validateRightSingleRuntimeFilterTopology(noRightSingle, nil, nil))

		shuffle := makeRightSingleTopologyPlan(tag)
		shuffle.Nodes[1].Stats.HashmapStats.Shuffle = true
		require.NoError(t, validateRightSingleRuntimeFilterTopology(shuffle, compiled, nil))
	})
}

func TestCollectRuntimeFilterTopologyVisitsSharedScopeOnce(t *testing.T) {
	const tag int32 = 9
	cn := engine.Node{Id: "cn1", Addr: "cn1:6001"}
	consumer := makeRuntimeFilterConsumerScope(tag, cn)
	producer, build := makeRuntimeFilterProducerScope(tag, cn)
	defer build.Release()
	consumer.PreScopes = []*Scope{producer}

	topology := collectRuntimeFilterTopology([]*Scope{consumer, producer}, []int32{tag})
	require.Len(t, topology.consumers[tag], 1)
	require.Len(t, topology.producers[tag], 1)
}

func BenchmarkValidateRightSingleRuntimeFilterTopologyNoFilter(b *testing.B) {
	qry := &plan.Query{Nodes: make([]*plan.Node, 64)}
	for i := range qry.Nodes {
		qry.Nodes[i] = &plan.Node{NodeType: plan.Node_TABLE_SCAN}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := validateRightSingleRuntimeFilterTopology(qry, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}
