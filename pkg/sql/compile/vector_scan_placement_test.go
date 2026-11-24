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

	"github.com/matrixorigin/matrixone/pkg/catalog"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
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
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, vectorscan.StablePartitionOwnershipProtocolVersion)
	client.version = vectorscan.StablePartitionOwnershipProtocolVersion
	return c, client
}

func vectorPlacementNode() *plan.Node {
	return &plan.Node{
		NodeType:        plan.Node_INDEX_SEARCH_SCAN,
		ObjRef:          &plan.ObjectRef{SchemaName: "db", ObjName: "docs"},
		TableDef:        &plan.TableDef{Name: "docs"},
		Stats:           &plan.Stats{Outcnt: 10},
		IndexSearchScan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString()}, AlgoOptions: []byte(`{}`)},
	}
}

func TestVectorScanPlacementKeepsObjectOwnersAcrossIngress(t *testing.T) {
	workers := engine.Nodes{
		{Id: "a", Addr: "z:6001", Mcpu: 4},
		{Id: "b", Addr: "y:6001", Mcpu: 4},
		{Id: "c", Addr: "x:6001", Mcpu: 4},
	}
	for _, count := range []int{2, 3} {
		var legacyReference map[types.Objectid]string
		for ingress := 0; ingress < count; ingress++ {
			selected := slices.Clone(workers[:count])
			selected[0], selected[ingress] = selected[ingress], selected[0]
			c, _ := vectorPlacementCompile(t, selected)
			scopes, err := c.compileIndexSearchScan(vectorPlacementNode())
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes(scopes) })
			require.Equal(t, selected, c.cnList, "scan numbering must not mutate query placement")
			legacyOwners := make(map[types.Objectid]string)
			buckets := make(map[int32]bool)
			for _, scope := range scopes {
				require.False(t, buckets[scope.NodeInfo.CNIDX])
				buckets[scope.NodeInfo.CNIDX] = true
				require.Equal(t, int32(count), scope.NodeInfo.CNCNT)
			}
			require.Len(t, buckets, count)
			// The reader owns memory based on the actual execution route,
			// not on the stable sort ordinal used for object/cache partitions.
			for object := byte(0); object < 32; object++ {
				id := types.Objectid{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, object, 0}
				appendable := object%2 == 0
				stats := objectio.NewObjectStatsWithObjectID(&id, appendable, false, true)
				var owner string
				for _, scope := range scopes {
					node := scope.DataSource.node
					rsp := &engine.RangesShuffleParam{
						Node: node, CNCNT: scope.NodeInfo.CNCNT, CNIDX: scope.NodeInfo.CNIDX,
						IsLocalCN: scope.ipAddrMatch(c.addr), ShuffleByObjectID: true,
						AppendableOnMemoryOwner: node.IndexSearchScan.GetStablePartitionOwnership(),
					}
					require.True(t, rsp.AppendableOnMemoryOwner)
					if !plan2.ShouldSkipObjByShuffle(rsp, stats) {
						require.Empty(t, owner, "object must have exactly one owner")
						owner = scope.NodeInfo.Id
					}
					rsp.AppendableOnMemoryOwner = false
					if !plan2.ShouldSkipObjByShuffle(rsp, stats) {
						require.Empty(t, legacyOwners[id])
						legacyOwners[id] = scope.NodeInfo.Id
					}
				}
				require.NotEmpty(t, legacyOwners[id])
				if appendable {
					require.Equal(t, selected[0].Id, owner, "flushed aobj follows the actual ingress")
				} else {
					require.Equal(t, legacyOwners[id], owner, "immutable object/cache locality is unchanged")
				}
			}
			if legacyReference == nil {
				legacyReference = legacyOwners
			} else {
				require.Equal(t, legacyReference, legacyOwners, "unmarked old plans retain object-ID ownership")
			}
		}
	}
}

func TestVectorScanPlacementUsesAddressOnlyForMissingIdentity(t *testing.T) {
	workers := engine.Nodes{{Addr: "z:6001"}, {Id: "b", Addr: "b:6001"}, {Addr: "a:6001"}}
	c, client := vectorPlacementCompile(t, workers)
	scopes, err := c.compileIndexSearchScan(vectorPlacementNode())
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
