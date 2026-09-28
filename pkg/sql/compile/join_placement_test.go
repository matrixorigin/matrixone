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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	windowop "github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestBroadcastJoinInputColocation(t *testing.T) {
	nodes := engine.Nodes{{Id: "local", Addr: "local:6001", Mcpu: 1}, {Id: "remote", Addr: "remote:6001", Mcpu: 1}, {Id: "other", Addr: "other:6001", Mcpu: 1}}
	for _, tc := range []struct {
		name                      string
		probe, build              int
		sink, multiple, wantLocal bool
	}{
		{name: "already local", wantLocal: true},
		{name: "same remote", probe: 1, build: 1},
		{name: "remote build local probe", build: 1, wantLocal: true},
		{name: "different remote build", probe: 1, build: 2, wantLocal: true},
		{name: "relocatable local build", probe: 1},
		{name: "local sink build", probe: 1, sink: true, wantLocal: true},
		{name: "local sink multiple probes", probe: 1, sink: true, multiple: true, wantLocal: true},
		{name: "ordinary distributed probes", probe: 1, build: 2, multiple: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCompileForShuffleJoinTest(t, nodes)
			makeScope := func(index int, sink bool) *Scope {
				s := newScope(Remote)
				if index == 0 {
					s.Magic = Merge
				}
				s.NodeInfo = scopeNodeWithMcpu(nodes[index], 1)
				s.Proc = c.proc.NewNoContextChildProc(0)
				s.setRootOperator(merge.NewArgument().WithSinkScan(sink))
				return s
			}
			probe := []*Scope{makeScope(tc.probe, false)}
			build := []*Scope{makeScope(tc.build, tc.sink)}
			defer func() { ReleaseScopes(probe); ReleaseScopes(build) }()
			if tc.multiple {
				probe = append(probe, makeScope(tc.probe, false))
			}
			oldProbe, oldBuild := probe[0], build[0]
			probe, build = c.colocateBroadcastJoinInputs(probe, build)
			if tc.wantLocal {
				require.True(t, c.scopesRunOnCoordinator(probe))
				require.True(t, c.scopesRunOnCoordinator(build))
				if tc.probe != 0 {
					require.Contains(t, probe[0].PreScopes, oldProbe)
				}
				if tc.build != 0 {
					require.Contains(t, build[0].PreScopes, oldBuild)
				}
			} else {
				require.Same(t, oldProbe, probe[0])
				require.Same(t, oldBuild, build[0])
			}
		})
	}
}

func TestCompileJoinKeepsWindowStageLocal(t *testing.T) {
	for _, side := range []string{"probe", "build"} {
		t.Run(side, func(t *testing.T) {
			nodes := engine.Nodes{{Addr: "local:6001", Mcpu: 1}, {Addr: "remote:6001", Mcpu: 1}}
			c := newCompileForShuffleJoinTest(t, nodes)
			probeScan := newRemoteMergeInputForTest(c, nodes[1], 0)
			buildScan := newRemoteMergeInputForTest(c, nodes[1], 0)
			probe, build := []*Scope{probeScan}, []*Scope{buildScan}
			if side == "probe" {
				probe = c.compileWin(newRowNumberWindowNodeForTest(), probe)
			} else {
				build = c.compileWin(newRowNumberWindowNodeForTest(), build)
			}
			node := newShuffleJoinTestNode(2)
			node.OnList = []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, true)}
			left := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(0, 0, true)}}
			right := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(1, 0, true)}}
			result := c.compileJoin(node, left, right, probe, build)
			defer ReleaseScopes(result)
			require.Len(t, result, 1)
			require.False(t, node.Stats.HashmapStats.Shuffle)
			require.True(t, c.scopesRunOnCoordinator(result))
			require.True(t, scopesContainOperator(result, vm.Window))
			require.Equal(t, nodes[1].Addr, probeScan.NodeInfo.Addr)
			require.Equal(t, nodes[1].Addr, buildScan.NodeInfo.Addr)
		})
	}
}

func TestJoinWindowStagePlacement(t *testing.T) {
	nodes := engine.Nodes{{Id: "local", Addr: "local:6001", Mcpu: 1}, {Id: "remote", Addr: "remote:6001", Mcpu: 1}}
	c := newCompileForShuffleJoinTest(t, nodes)
	makeScope := func(index int) *Scope {
		s := newScope(Remote)
		s.NodeInfo = scopeNodeWithMcpu(nodes[index], 1)
		s.Proc = c.proc.NewNoContextChildProc(0)
		s.setRootOperator(merge.NewArgument())
		return s
	}
	local, remote := makeScope(0), makeScope(1)
	defer ReleaseScopes([]*Scope{local, remote})
	node := newShuffleJoinTestNode(1)
	node.Stats.HashmapStats.Shuffle = false
	require.False(t, c.joinNeedsLocalWindow(node, []*Scope{local}, []*Scope{remote}))
	local.setRootOperator(windowop.NewArgument())
	require.False(t, c.joinNeedsLocalWindow(node, []*Scope{local}, []*Scope{local}))
	require.True(t, c.joinNeedsLocalWindow(node, []*Scope{local}, []*Scope{remote}))
	require.True(t, c.joinNeedsLocalWindow(node, []*Scope{remote}, []*Scope{local}))
	node.Stats.HashmapStats.Shuffle = true
	require.True(t, c.joinNeedsLocalWindow(node, []*Scope{local}, []*Scope{local}))
}
