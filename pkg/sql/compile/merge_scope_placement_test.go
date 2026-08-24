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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestNewMergeScopeGroupsRemoteCrossTreeDependenciesByCN(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 2},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 2},
	}

	tests := []struct {
		name       string
		operator   vm.OpType
		node       engine.Node
		wantInputs int
	}{
		{
			name:       "remote dispatch dependency",
			operator:   vm.Dispatch,
			node:       nodes[1],
			wantInputs: 1,
		},
		{
			name:       "remote connector dependency",
			operator:   vm.Connector,
			node:       nodes[1],
			wantInputs: 1,
		},
		{
			name:       "local dependency needs no remote container",
			operator:   vm.Dispatch,
			node:       nodes[0],
			wantInputs: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newCompileForShuffleJoinTest(t, nodes)
			owner := newRemoteMergeInputForTest(c, test.node, 1)
			producer := newRemoteMergeInputForTest(c, test.node, 0)
			dependency := &Scope{
				Magic:    Remote,
				NodeInfo: scopeNodeWithMcpu(test.node, 1),
				Proc:     c.proc.NewNoContextChildProc(0),
			}
			switch test.operator {
			case vm.Dispatch:
				op := dispatch.NewArgument()
				op.LocalRegs = []*process.WaitRegister{owner.Proc.Reg.MergeReceivers[0]}
				dependency.setRootOperator(op)
			case vm.Connector:
				dependency.setRootOperator(connector.NewArgument().WithReg(owner.Proc.Reg.MergeReceivers[0]))
			default:
				t.Fatalf("unsupported dependency operator %s", test.operator)
			}
			producer.PreScopes = append(producer.PreScopes, dependency)

			result := c.newMergeScope([]*Scope{producer, owner})
			require.Len(t, result.PreScopes, test.wantInputs)
			if test.wantInputs == 1 {
				cnGroup := result.PreScopes[0]
				require.Len(t, cnGroup.PreScopes, 2)
				cnGroup.Proc.Base.TxnOperator = fakeTxnOperator{}
				require.True(t, checkPipelineStandaloneExecutableAtRemote(cnGroup))
			}
		})
	}
}

func TestNewMergeScopePreservesIndependentRemoteInputs(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 2},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 2},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	inputs := []*Scope{
		newRemoteMergeInputForTest(c, nodes[1], 0),
		newRemoteMergeInputForTest(c, nodes[1], 0),
	}

	result := c.newMergeScope(inputs)

	require.Equal(t, inputs, result.PreScopes,
		"independent remote pipelines should not pay for an extra per-CN merge")
}

func TestCompileWindowKeepsDistributedShuffleJoinTreesStandalone(t *testing.T) {
	const dop = int32(2)
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: int(dop)},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: int(dop)},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	c.execType = plan2.ExecTypeAP_MULTICN

	joinNode := newShuffleJoinTestNode(dop)
	joinNode.JoinType = plan.Node_OUTER
	joinNode.IsRightJoin = true
	joinNode.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
	left := &plan.Node{Stats: &plan.Stats{Dop: dop}}
	right := &plan.Node{Stats: &plan.Stats{Dop: dop}}
	probeScopes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}
	buildScopes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}

	buckets := c.compileShuffleJoin(joinNode, left, right, probeScopes, buildScopes)
	require.Len(t, buckets, len(nodes)*int(dop))

	windowScopes := c.compileWin(newRowNumberWindowNodeForTest(), buckets)
	require.Len(t, windowScopes, 1)
	require.Len(t, windowScopes[0].PreScopes, len(nodes),
		"a global window must send one standalone shuffle tree per CN")
	for _, pre := range windowScopes[0].PreScopes {
		pre.Proc.Base.TxnOperator = fakeTxnOperator{}
		require.True(t, checkPipelineStandaloneExecutableAtRemote(pre),
			"window input on %s retains an out-of-tree local receiver", pre.NodeInfo.Addr)
	}
}

func newRemoteMergeInputForTest(c *Compile, node engine.Node, receivers int) *Scope {
	s := &Scope{
		Magic:    Remote,
		NodeInfo: scopeNodeWithMcpu(node, 1),
		Proc:     c.proc.NewNoContextChildProc(receivers),
	}
	s.setRootOperator(merge.NewArgument())
	return s
}

func newRowNumberWindowNodeForTest() *plan.Node {
	return &plan.Node{WinSpecList: []*plan.Expr{{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			WindowFunc: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     function.EncodeOverloadID(function.ROW_NUMBER, 0),
					ObjName: "row_number",
				},
			}}},
		}},
	}}}
}
