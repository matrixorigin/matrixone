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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileLocalShuffleFilterTopology(t *testing.T) {
	for _, scenario := range []string{"eligible", "reuse", "multiple-cn", "disabled", "unselective"} {
		t.Run(scenario, func(t *testing.T) {
			cn := engine.Node{Addr: "cn1:6001", Mcpu: 4}
			c := newCompileForShuffleJoinTest(t, engine.Nodes{cn})
			node := newShuffleJoinTestNode(4)
			node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			buildExpr := node.OnList[0].GetF().Args[1]
			probeType := node.OnList[0].GetF().Args[0].Typ
			node.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: 77}}
			node.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{{Tag: 77, BuildExpr: buildExpr, ProbeType: &probeType}}
			left := &plan.Node{Stats: &plan.Stats{Dop: 4, Outcnt: 1000}}
			right := &plan.Node{Stats: &plan.Stats{Dop: 4, Outcnt: 10}}
			if scenario == "reuse" {
				node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Reuse
			}
			if scenario == "multiple-cn" {
				c.cnList = append(c.cnList, engine.Node{Addr: "cn2:6001", Mcpu: 4})
			}
			if scenario == "disabled" {
				node.RuntimeFilterBuildList[0].BuildExpr = nil
			}
			if scenario == "unselective" {
				right.Stats.Outcnt = 1000
			}
			probe := newShuffleJoinTestScope(t, cn, 4)
			build := newShuffleJoinTestScope(t, cn, 4)
			result := c.compileLocalShuffleJoin(node, left, right, []*Scope{probe}, []*Scope{build})
			producer := build.RootOp.(*hashbuild.HashBuild)
			if scenario != "eligible" {
				require.Nil(t, producer.LocalFilter)
				return
			}
			require.NotNil(t, producer.LocalFilter)
			router := result[0].RootOp.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
			require.Same(t, producer.LocalFilter, router.LocalFilter)
			require.Nil(t, router.RuntimeFilterSpec, "optional membership must not add a global probe barrier")
			copiedBuild := dupOperator(producer, 2, 4).(*hashbuild.HashBuild)
			copiedProbe := dupOperator(router, 2, 4).(*shuffle.Shuffle)
			t.Cleanup(copiedBuild.Release)
			t.Cleanup(copiedProbe.Release)
			require.Same(t, producer.LocalFilter, copiedBuild.LocalFilter)
			require.Same(t, producer.LocalFilter, copiedProbe.LocalFilter)
			require.Equal(t, int32(2), copiedBuild.ShuffleIdx)
			require.Equal(t, int32(2), copiedBuild.GetParalleID(), "duplicated producers need distinct registration IDs")
			require.Equal(t, int32(4), copiedBuild.GetMaxParallel(), "completion counts actual duplicated producers")
		})
	}
}
