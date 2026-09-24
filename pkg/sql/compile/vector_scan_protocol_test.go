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
	"context"
	"errors"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestVectorScanPlacementCapabilityFallback(t *testing.T) {
	workers := engine.Nodes{{Id: "b", Addr: "b:6001"}, {Id: "a", Addr: "a:6001"}}
	for _, mode := range []string{"supported", "old worker", "old coordinator", "unknown worker", "probe failure", "canceled", "forced local"} {
		t.Run(mode, func(t *testing.T) {
			c, client := vectorPlacementCompile(t, workers)
			node := vectorPlacementNode()
			switch mode {
			case "old worker":
				client.version = defines.MORPCVersion93
			case "old coordinator":
				moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion93)
			case "unknown worker":
				client.customResponse = true
			case "probe failure":
				client.customResponse, client.sendErr = true, errors.New("unavailable")
			case "canceled":
				ctx, cancel := context.WithCancel(c.proc.Ctx)
				cancel()
				c.proc.Ctx = ctx
			case "forced local":
				node.Stats.ForceOneCN = true
			}
			scopes, err := c.compileVectorIndexScan(node)
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, scopes)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes(scopes) })
			if mode == "forced local" {
				require.Len(t, scopes, 1)
				require.Equal(t, c.addr, scopes[0].NodeInfo.Addr)
				require.Zero(t, client.calls)
			} else {
				require.Len(t, scopes, 2, "unknown capability must retain legacy distributed execution")
				want := "b"
				if mode == "supported" {
					want = "a"
				}
				require.Equal(t, want, scopes[0].NodeInfo.Id)
			}
			for i, scope := range scopes {
				require.Equal(t, int32(len(scopes)), scope.NodeInfo.CNCNT)
				require.Equal(t, int32(i), scope.NodeInfo.CNIDX)
			}
		})
	}
}

func TestVectorScanPartitionTransportAndRollback(t *testing.T) {
	c, client := vectorPlacementCompile(t, engine.Nodes{{Id: "b", Addr: "b:6001"}, {Id: "a", Addr: "a:6001"}})
	scopes, err := c.compileVectorIndexScan(vectorPlacementNode())
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	remote := scopes[0]
	require.Equal(t, "a", remote.NodeInfo.Id)
	require.Zero(t, remote.NodeInfo.CNIDX)
	data, err := encodeRemoteScope(remote, c.proc)
	require.NoError(t, err)
	decoded, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(decoded.release)
	require.True(t, decoded.IsRemote)
	require.Equal(t, remote.NodeInfo.CNIDX, decoded.NodeInfo.CNIDX)
	require.Equal(t, remote.NodeInfo.CNCNT, decoded.NodeInfo.CNCNT)

	// The execution mapping stays frozen if the destination rolls back.
	client.version = defines.MORPCVersion93
	_, err = encodeRemoteScope(remote, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.Equal(t, "a", remote.NodeInfo.Id)
	require.Zero(t, remote.NodeInfo.CNIDX)
	require.Equal(t, client.calls, client.releases)
	client.version = defines.MORPCVersion94
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion93)
	_, err = decodeScope(data, c.proc, true, nil)
	require.ErrorContains(t, err, "version 94")
	_, err = encodeRemoteScope(remote, c.proc)
	require.ErrorContains(t, err, "remote destination")

	// Local decoding and the old nonzero remote layout need no new capability.
	local, err := decodeScope(data, c.proc, false, nil)
	require.NoError(t, err)
	t.Cleanup(local.release)
	require.False(t, local.IsRemote)
	remote.NodeInfo.CNIDX = 1
	legacy, err := encodeRemoteScope(remote, c.proc)
	require.NoError(t, err)
	oldLayout, err := decodeScope(legacy, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(oldLayout.release)
	require.True(t, oldLayout.IsRemote)
	require.Equal(t, int32(1), oldLayout.NodeInfo.CNIDX)
}

func TestVectorScanPartitionProtocolNestedScopes(t *testing.T) {
	c, _ := vectorPlacementCompile(t, engine.Nodes{{Id: "b", Addr: "b:6001"}, {Id: "a", Addr: "a:6001"}})
	leaf := &pipeline.Pipeline{
		Node:       &pipeline.NodeInfo{CnCnt: 2, CnIdx: 0},
		DataSource: &pipeline.Source{Node: vectorPlacementNode()},
	}
	root := &pipeline.Pipeline{
		Node:     &pipeline.NodeInfo{Id: "a", Addr: "a:6001"},
		Children: []*pipeline.Pipeline{nil, {Children: []*pipeline.Pipeline{leaf}}},
	}
	require.True(t, hasRemoteVectorPartitionZero(root))
	require.NoError(t, validateVectorPartitionDestination(c.proc, root))
	require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, root))
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	require.ErrorIs(t, validateVectorPartitionDestination(c.proc, root), context.Canceled)
	require.Error(t, validateRemoteVectorPartitionProtocol(nil, root))
	require.Error(t, validateVectorPartitionDestination(nil, root))
	root.Node = nil
	require.Error(t, validateVectorPartitionDestination(c.proc, root))
	leaf.Node.CnCnt = 1
	require.NoError(t, validateRemoteVectorPartitionProtocol(nil, root))
	leaf.Node.CnCnt = 2
	leaf.DataSource.Node.NodeType = plan.Node_TABLE_SCAN
	require.NoError(t, validateVectorPartitionDestination(nil, root))
	require.NoError(t, validateRemoteVectorPartitionProtocol(nil, nil))
}
