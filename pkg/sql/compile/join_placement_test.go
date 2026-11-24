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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBroadcastJoinStageRetainsInputOwnership(t *testing.T) {
	local := engine.Node{Id: "coord", Addr: "cn-coord:6001", Mcpu: 1}
	remote := engine.Node{Id: "worker", Addr: "cn-worker:6001", Mcpu: 1}
	peer := engine.Node{Id: "peer", Addr: "cn-peer:6001", Mcpu: 1}
	for _, tc := range []struct {
		name             string
		probes           []engine.Node
		build            engine.Node
		sink             bool
		fullOuter        bool
		wantLocal        bool
		preserveProbe    bool
		wantRemoteInputs int
		windowBuild      bool
		distributed      bool
		extraBuild       bool
	}{
		{name: "remote_probe_local_sink", probes: []engine.Node{remote}, sink: true, wantLocal: true, wantRemoteInputs: 1},
		{name: "local_parallel_probe_local_sink", probes: []engine.Node{scopeNodeWithMcpu(local, 4)}, sink: true, wantLocal: true, preserveProbe: true},
		{name: "remote_probe_and_build", probes: []engine.Node{remote}, build: remote, preserveProbe: true},
		{name: "local_probe_remote_build", probes: []engine.Node{local}, build: remote, wantLocal: true, preserveProbe: true, wantRemoteInputs: 1},
		{name: "forced_single_cn_full_outer", probes: []engine.Node{local, remote}, build: remote, fullOuter: true, wantLocal: true, wantRemoteInputs: 2},
		{name: "remote_probe_local_window", probes: []engine.Node{remote}, build: local, windowBuild: true, wantLocal: true, wantRemoteInputs: 1},
		{name: "sink_owner_first", probes: []engine.Node{local, remote}, sink: true, distributed: true},
		{name: "sink_owner_last", probes: []engine.Node{remote, local}, sink: true, distributed: true},
		{name: "sink_and_peer_build_input", probes: []engine.Node{remote, local}, sink: true, distributed: true, extraBuild: true},
		{name: "no_probe_owns_sink", probes: []engine.Node{remote, peer}, sink: true, wantLocal: true, wantRemoteInputs: 2},
		{name: "forced_single_with_sink_owner", probes: []engine.Node{remote, local}, sink: true, fullOuter: true, wantLocal: true, wantRemoteInputs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCompileForShuffleJoinTest(t, engine.Nodes{remote})
			c.addr = local.Addr
			c.execType = plan2.ExecTypeAP_MULTICN
			c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
			c.stepRegs = make(map[int32][][2]int32)
			colType := types.T_int64.ToType()
			col := func(rel int32) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(colType.Oid)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: 0}}}
			}
			eq, err := planfunction.GetFunctionByName(context.Background(), "=", []types.Type{colType, colType})
			require.NoError(t, err)
			join := newShuffleJoinTestNode(1)
			join.Stats.HashmapStats.Shuffle = false
			if tc.fullOuter {
				join.JoinType, join.IsRightJoin = plan.Node_OUTER, true
			}
			join.OnList[0].GetF().Func = &plan.ObjectRef{ObjName: "=", Obj: eq.GetEncodedOverloadID()}
			join.OnList[0].GetF().Args = []*plan.Expr{col(0), col(1)}
			left := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ProjectList: []*plan.Expr{col(0)}}
			right := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ProjectList: []*plan.Expr{col(0)}}
			newScan := func(worker engine.Node) *Scope {
				s := newScope(Remote)
				s.NodeInfo = worker
				s.Proc = c.proc.NewNoContextChildProc(0)
				s.setRootOperator(table_scan.NewArgument().WithTypes([]plan.Type{{Id: int32(colType.Oid)}}))
				return s
			}
			var probes, result []*Scope
			for _, node := range tc.probes {
				probes = append(probes, newScan(node))
			}
			var build, producer, extra *Scope
			var edge *process.WaitRegister
			t.Cleanup(func() {
				inputs := append([]*Scope{build, extra}, probes...)
				for _, input := range inputs {
					if input == nil {
						continue
					}
					owned := false
					for _, root := range result {
						owned = owned || walkScopeTree(root, func(s *Scope) bool { return s == input })
					}
					if !owned {
						input.release()
					}
				}
				for _, root := range result {
					root.release()
				}
			})
			if tc.sink {
				right.NodeType = plan.Node_SINK_SCAN
				right.SourceStep = []int32{0}
				sink := &plan.Node{NodeType: plan.Node_SINK}
				qry := &plan.Query{Nodes: []*plan.Node{sink, right}, Steps: []int32{0, 1}}
				c.anal.qry = qry
				require.NoError(t, c.compileSinkScan(qry, 1))
				builds, err := c.compileSinkScanNode(right, 1)
				require.NoError(t, err)
				require.Len(t, builds, 1)
				build = builds[0]
				producer = newScan(local)
				t.Cleanup(producer.release)
				_, err = c.compileSinkNode(sink, []*Scope{producer}, 0)
				require.NoError(t, err)
				edge = build.Proc.Reg.MergeReceivers[0]
				require.Same(t, edge, producer.RootOp.(*dispatch.Dispatch).LocalRegs[0])
			} else {
				build = newScan(tc.build)
				if tc.windowBuild {
					build.Magic = Merge
					build.setRootOperator(window.NewArgument())
				}
			}
			builds := []*Scope{build}
			if tc.extraBuild {
				extra = newScan(remote)
				builds = append(builds, extra)
			}
			result = c.compileJoin(join, left, right, probes, builds)
			if tc.sink {
				require.Same(t, edge, build.Proc.Reg.MergeReceivers[0])
				require.Same(t, edge, producer.RootOp.(*dispatch.Dispatch).LocalRegs[0], "step sender must target retained live register")
			}
			if tc.distributed {
				checkTwoCNBroadcast(t, c.addr, probes, builds, result)
				return
			}
			require.Len(t, result, 1)
			stage := result[0]
			require.IsType(t, &hashjoin.HashJoin{}, stage.RootOp)
			require.Equal(t, tc.wantLocal, stage.ipAddrMatch(c.addr), "JoinMap is process-local")
			if tc.preserveProbe {
				require.Same(t, probes[0], stage)
				if tc.sink {
					require.Equal(t, 4, stage.NodeInfo.Mcpu, "local parallelism must be retained")
				}
			}
			buildCount := 0
			for _, scope := range stage.PreScopes {
				if _, isBuild := scope.RootOp.(*hashbuild.HashBuild); isBuild {
					buildCount++
					require.Equal(t, stage.NodeInfo.Addr, scope.NodeInfo.Addr)
				}
			}
			require.Equal(t, 1, buildCount)
			if tc.sink {
				require.IsType(t, &merge.Merge{}, build.RootOp.GetOperatorBase().GetChildren(0))
			}
			remoteInputs := 0
			walkScopeTree(stage, func(s *Scope) bool {
				if s == stage || s.ipAddrMatch(c.addr) || !tc.wantLocal {
					return false
				}
				remoteInputs++
				require.IsType(t, &connector.Connector{}, s.RootOp, "ship batches instead of a JoinMap or local step channel")
				input, withoutOutput := getScopeForRemoteRunEncoding(s)
				require.False(t, withoutOutput)
				wire, err := encodeScope(input)
				require.NoError(t, err)
				decoded, err := decodeScope(wire, testutil.NewProcess(t), true, nil)
				require.NoError(t, err)
				t.Cleanup(decoded.release)
				require.IsType(t, &table_scan.TableScan{}, decoded.RootOp)
				require.Empty(t, decoded.Proc.Reg.MergeReceivers, "remote scan must not require an unshipped producer")
				return false
			})
			require.Equal(t, tc.wantRemoteInputs, remoteInputs)
		})
	}
}

func checkTwoCNBroadcast(t *testing.T, coordinator string, probes, builds, stages []*Scope) {
	t.Helper()
	require.Len(t, stages, 2)
	var local, peer *Scope
	for i, stage := range stages {
		require.Same(t, probes[i], stage)
		require.IsType(t, &hashjoin.HashJoin{}, stage.RootOp)
		if stage.ipAddrMatch(coordinator) {
			local = stage
		} else {
			peer = stage
		}
	}
	require.NotNil(t, local)
	require.NotNil(t, peer)
	require.Len(t, local.PreScopes, 2)
	var source, localBuild *Scope
	for _, s := range local.PreScopes {
		switch s.RootOp.(type) {
		case *dispatch.Dispatch:
			source = s
		case *hashbuild.HashBuild:
			localBuild = s
		}
	}
	require.NotNil(t, source, "broadcast build batches, not peer probe rows")
	require.NotNil(t, localBuild)
	if len(builds) == 1 {
		require.Same(t, builds[0], source)
	} else {
		require.Len(t, source.PreScopes, len(builds))
		for i := range builds {
			require.Same(t, builds[i], source.PreScopes[i])
		}
	}
	sender := source.RootOp.(*dispatch.Dispatch)
	require.Len(t, sender.LocalRegs, 1)
	require.Same(t, sender.LocalRegs[0], localBuild.Proc.Reg.MergeReceivers[0])
	require.Len(t, sender.RemoteRegs, 1)
	require.Equal(t, peer.NodeInfo.Addr, sender.RemoteRegs[0].NodeAddr)

	wire, err := encodeScope(peer)
	require.NoError(t, err)
	decoded, err := decodeScope(wire, testutil.NewProcess(t), true, nil)
	require.NoError(t, err)
	t.Cleanup(decoded.release)
	require.IsType(t, &hashjoin.HashJoin{}, decoded.RootOp)
	require.Len(t, decoded.PreScopes, 1)
	peerBuild := decoded.PreScopes[0]
	require.IsType(t, &hashbuild.HashBuild{}, peerBuild.RootOp)
	require.Equal(t, peer.NodeInfo.Addr, peerBuild.NodeInfo.Addr)
	require.Equal(t, decoded.RootOp.(*hashjoin.HashJoin).JoinMapTag, peerBuild.RootOp.(*hashbuild.HashBuild).JoinMapTag)
	require.Len(t, peerBuild.RemoteReceivRegInfos, 1)
	receiver := peerBuild.RemoteReceivRegInfos[0]
	require.Equal(t, sender.RemoteRegs[0].Uuid, receiver.Uuid)
	require.Equal(t, source.NodeInfo.Addr, receiver.FromAddr)
}
