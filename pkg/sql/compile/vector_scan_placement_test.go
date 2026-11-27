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
	"errors"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

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
		var reference map[types.Objectid]string
		for ingress := 0; ingress < count; ingress++ {
			selected := slices.Clone(workers[:count])
			selected[0], selected[ingress] = selected[ingress], selected[0]
			c, _ := vectorPlacementCompile(t, selected)
			scopes, err := c.compileIndexSearchScan(vectorPlacementNode())
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes(scopes) })
			require.Equal(t, selected, c.cnList, "scan numbering must not mutate query placement")
			immutableOwners := make(map[types.Objectid]string)
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
				var owner string
				for _, scope := range scopes {
					rsp := &engine.RangesShuffleParam{Node: scope.DataSource.node,
						CNCNT: scope.NodeInfo.CNCNT, CNIDX: scope.NodeInfo.CNIDX,
						IsLocalCN: scope.ipAddrMatch(c.addr), ShuffleByObjectID: true}
					if !plan2.ShouldSkipObjByShuffle(rsp, stats) {
						require.Empty(t, owner, "object must have exactly one owner")
						owner = scope.NodeInfo.Id
					}
				}
				require.NotEmpty(t, owner)
				if stats.GetAppendable() {
					require.Equal(t, selected[0].Id, owner, "appendable rows follow ingress")
				} else {
					immutableOwners[id] = owner
				}
			}
			if reference == nil {
				reference = immutableOwners
			} else {
				require.Equal(t, reference, immutableOwners, "changing ingress must preserve immutable object owners")
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

func TestPrunedVectorScanDoesNotProbePeer(t *testing.T) {
	workers := engine.Nodes{{Addr: "local:6001", Mcpu: 2}, {Id: "peer", Addr: "peer:6001", Mcpu: 2}}
	for _, tc := range []struct {
		name  string
		limit uint64
	}{{"pruned", 0}, {"executed", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := vectorPlacementCompile(t, workers)
			c.e = &schedulerTestEngine{nodes: workers}
			c.anal = nil // compileQuery owns the analyze module it allocates.
			t.Cleanup(func() {
				if c.anal != nil {
					c.anal.release()
				}
			})
			client.customResponse, client.sendErr = true, errors.New("peer query service unavailable")
			project := &plan.Node{NodeType: plan.Node_PROJECT, NodeId: 1, Children: []int32{0},
				Limit: plan2.MakePlan2Uint64ConstExprWithType(tc.limit), Stats: &plan.Stats{}}
			q := &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{1}, Nodes: []*plan.Node{vectorPlacementNode(), project}}
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
			scopes, err := c.compileQuery(q)
			defer ReleaseScopes(scopes)
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.Len(t, c.cnList, 2)
			require.NoError(t, err)
			require.Zero(t, client.calls, "placement probes no peer")
		})
	}
}

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
	return c, client
}
