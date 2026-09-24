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

package compile

import (
	"slices"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func vectorPlacementCompile(t *testing.T, workers engine.Nodes) (*Compile, *expressionVersionClient) {
	t.Helper()
	c, client := expressionProtocolTestCompile(t)
	t.Cleanup(c.proc.Free)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{isFirst: true}
	c.cnList = slices.Clone(workers)
	c.addr = workers[0].Addr
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	value, _ := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster := value.(*schedulerTestCluster)
	cluster.cns = nil
	for _, worker := range workers {
		cluster.cns = append(cluster.cns, metadata.CNService{
			ServiceID: worker.Id, PipelineServiceAddress: worker.Addr, QueryAddress: worker.Addr,
		})
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion94)
	client.version = defines.MORPCVersion94
	return c, client
}

func vectorPlacementNode() *plan.Node {
	return &plan.Node{
		NodeType:        plan.Node_VECTOR_INDEX_SCAN,
		ObjRef:          &plan.ObjectRef{SchemaName: "db", ObjName: "docs"},
		TableDef:        &plan.TableDef{Name: "docs"},
		Stats:           &plan.Stats{Outcnt: 10},
		VectorIndexScan: &plan.VectorIndexScan{},
	}
}

func TestVectorScanPlacementKeepsObjectOwnersAcrossIngress(t *testing.T) {
	workers := engine.Nodes{
		{Id: "a", Addr: "z:6001", Mcpu: 4},
		{Id: "b", Addr: "y:6001", Mcpu: 4},
		{Id: "c", Addr: "x:6001", Mcpu: 4},
	}
	for _, count := range []int{2, 3} {
		var reference map[types.Objectid]string
		for ingress := 0; ingress < count; ingress++ {
			selected := slices.Clone(workers[:count])
			selected[0], selected[ingress] = selected[ingress], selected[0]
			c, _ := vectorPlacementCompile(t, selected)
			scopes, err := c.compileVectorIndexScan(vectorPlacementNode())
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes(scopes) })
			require.Equal(t, selected, c.cnList, "scan numbering must not mutate query placement")
			owners := make(map[types.Objectid]string)
			buckets := make(map[int32]bool)
			for _, scope := range scopes {
				require.False(t, buckets[scope.NodeInfo.CNIDX])
				buckets[scope.NodeInfo.CNIDX] = true
				require.Equal(t, int32(count), scope.NodeInfo.CNCNT)
			}
			require.Len(t, buckets, count)
			// Fixed, complete ObjectIDs exercise the real storage ownership predicate.
			for object := byte(0); object < 32; object++ {
				id := types.Objectid{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, object, 0}
				stats := objectio.NewObjectStatsWithObjectID(&id, object%2 == 0, false, true)
				for _, scope := range scopes {
					rsp := &engine.RangesShuffleParam{Node: vectorPlacementNode(),
						CNCNT: int32(count), CNIDX: scope.NodeInfo.CNIDX, ShuffleByObjectID: true}
					if !plan2.ShouldSkipObjByShuffle(rsp, stats) {
						require.Empty(t, owners[id], "object must have exactly one owner")
						owners[id] = scope.NodeInfo.Id
					}
				}
				require.NotEmpty(t, owners[id])
			}
			if reference == nil {
				reference = owners
			} else {
				require.Equal(t, reference, owners, "changing ingress must preserve physical object owners")
			}
		}
	}
}

func TestVectorScanPlacementUsesAddressOnlyForMissingIdentity(t *testing.T) {
	workers := engine.Nodes{{Addr: "z:6001"}, {Id: "b", Addr: "b:6001"}, {Addr: "a:6001"}}
	c, client := vectorPlacementCompile(t, workers)
	scopes, err := c.compileVectorIndexScan(vectorPlacementNode())
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	addresses := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		addresses = append(addresses, scope.NodeInfo.Addr)
	}
	require.Equal(t, []string{"a:6001", "z:6001", "b:6001"}, addresses)
	require.Equal(t, workers, c.cnList)
	require.Equal(t, client.calls, client.releases)
}
