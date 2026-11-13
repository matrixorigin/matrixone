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
	"strconv"
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

func TestNewMergeScopeGroupsRemoteReceiverDependencies(t *testing.T) {
	for _, opType := range []vm.OpType{vm.Dispatch, vm.Connector} {
		for _, local := range []bool{false, true} {
			name := opType.String() + "/remote"
			if local {
				name = opType.String() + "/local"
			}
			t.Run(name, func(t *testing.T) {
				c := newMergeOwnershipCompile(t)
				node := c.cnList[1]
				if local {
					node = c.cnList[0]
				}
				owner := newMergeOwnershipInput(c, node, 1)
				producer := newMergeOwnershipInput(c, node, 0)
				dependency := newMergeOwnershipInput(c, node, 0)
				reg := owner.Proc.Reg.MergeReceivers[0]
				if opType == vm.Dispatch {
					op := dispatch.NewArgument()
					op.LocalRegs = []*process.WaitRegister{reg}
					dependency.setRootOperator(op)
				} else {
					dependency.setRootOperator(connector.NewArgument().WithReg(reg))
				}
				producer.PreScopes = []*Scope{dependency}
				c.scopes = []*Scope{producer, owner}

				result := c.newMergeScope(c.scopes)
				c.scopes = []*Scope{result}
				if local {
					require.Equal(t, []*Scope{producer, owner}, result.PreScopes)
					return
				}
				require.Len(t, result.PreScopes, 1)
				container := result.PreScopes[0]
				require.Equal(t, node.Addr, container.NodeInfo.Addr)
				require.Equal(t, []*Scope{producer, owner}, container.PreScopes)
				require.Same(t, reg, owner.Proc.Reg.MergeReceivers[0])
				require.Equal(t, 2, container.Proc.Reg.MergeReceivers[0].NilBatchCnt)
				assertMergeReceiverOwnership(t, container)
			})
		}
	}
}

func TestNewMergeScopePreservesIndependentInputs(t *testing.T) {
	for _, count := range []int{0, 1, 3} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			c := newMergeOwnershipCompile(t)
			inputs := make([]*Scope, 0, count)
			for i := 0; i < count; i++ {
				input := newMergeOwnershipInput(c, c.cnList[1], 0)
				inputs = append(inputs, input)
			}
			result := c.newMergeScope(inputs)
			c.scopes = []*Scope{result}
			require.Equal(t, inputs, result.PreScopes)
			require.Len(t, result.Proc.Reg.MergeReceivers, count)
			for i, input := range inputs {
				require.Same(t, result.Proc.Reg.MergeReceivers[i], input.RootOp.(*connector.Connector).Reg)
			}
		})
	}
}

func TestRemoteStandaloneAllowsExternalRootOutput(t *testing.T) {
	for _, opType := range []vm.OpType{vm.Dispatch, vm.Connector} {
		t.Run(opType.String(), func(t *testing.T) {
			c := newMergeOwnershipCompile(t)
			s := newMergeOwnershipInput(c, c.cnList[1], 0)
			reg := process.NewPipelineEdge(1, 1)
			if opType == vm.Dispatch {
				op := dispatch.NewArgument()
				op.LocalRegs = []*process.WaitRegister{reg}
				s.setRootOperator(op)
			} else {
				s.setRootOperator(connector.NewArgument().WithReg(reg))
			}
			// RemoteRun retains this root on the caller; only its children are serialized.
			require.True(t, checkPipelineStandaloneExecutableAtRemote(s))
		})
	}
}

func TestNewMergeScopeGroupingPreservesInputsOutsideCNList(t *testing.T) {
	c := newMergeOwnershipCompile(t)
	local := newMergeOwnershipInput(c, c.cnList[0], 0)
	other := engine.Node{Addr: "other:6001", Mcpu: 1}
	owner := newMergeOwnershipInput(c, other, 1)
	producer := newMergeOwnershipInput(c, other, 0)
	dependency := newMergeOwnershipInput(c, other, 0)
	dependency.setRootOperator(connector.NewArgument().WithReg(owner.Proc.Reg.MergeReceivers[0]))
	producer.PreScopes = []*Scope{dependency}
	c.scopes = []*Scope{local, producer, owner}

	result := c.newMergeScope(c.scopes)
	c.scopes = []*Scope{result}
	require.Len(t, result.PreScopes, 2)
	require.Equal(t, c.addr, result.PreScopes[0].NodeInfo.Addr)
	require.Equal(t, []*Scope{local}, result.PreScopes[0].PreScopes)
	require.Equal(t, other.Addr, result.PreScopes[1].NodeInfo.Addr)
	require.Equal(t, []*Scope{producer, owner}, result.PreScopes[1].PreScopes)
	assertMergeReceiverOwnership(t, result.PreScopes[1])
}

func TestCompileWindowPreservesShuffleGroupReceiverOwnership(t *testing.T) {
	for _, shuffleType := range []plan.ShuffleType{plan.ShuffleType_Range, plan.ShuffleType_Hash} {
		t.Run(shuffleType.String(), func(t *testing.T) {
			c := newMergeOwnershipCompile(t)
			agg, nodes := newShuffleGroupV2TestNodes(2)
			agg.Stats.HashmapStats.ShuffleType = shuffleType
			sum, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "sum", agg.GroupBy)
			require.NoError(t, err)
			agg.AggList = []*plan.Expr{sum}
			inputs := []*Scope{
				newMergeOwnershipInput(c, c.cnList[0], 0),
				newMergeOwnershipInput(c, c.cnList[1], 0),
			}
			buckets := c.compileShuffleGroup(agg, inputs, nodes)
			c.scopes = buckets
			require.Len(t, buckets, 4)
			originalInfos := make([][]RemoteReceivRegInfo, len(buckets))
			originalReceivers := make([][]*process.WaitRegister, len(buckets))
			for i, bucket := range buckets {
				originalInfos[i] = append([]RemoteReceivRegInfo(nil), bucket.RemoteReceivRegInfos...)
				originalReceivers[i] = append([]*process.WaitRegister(nil), bucket.Proc.Reg.MergeReceivers...)
			}

			result := c.compileWin(newMergeOwnershipWindowNode(), buckets)
			c.scopes = result
			require.Len(t, result, 1)
			require.Len(t, result[0].PreScopes, 2, "each CN's dependent buckets must travel together")
			seen := make(map[*Scope]int)
			for _, container := range result[0].PreScopes {
				assertMergeReceiverOwnership(t, container)
				require.Equal(t, Remote, container.Magic)
				require.Len(t, container.PreScopes, 2)
				for _, bucket := range container.PreScopes {
					seen[bucket]++
					require.Equal(t, container.NodeInfo.Addr, bucket.NodeInfo.Addr)
					require.True(t, hasOperatorType(bucket.RootOp, vm.Group), "group operator must not be replaced")
				}
			}
			for i, bucket := range buckets {
				require.Equal(t, 1, seen[bucket], "every bucket must retain one owner")
				require.Equal(t, originalInfos[i], bucket.RemoteReceivRegInfos)
				for j, reg := range originalReceivers[i] {
					require.Same(t, reg, bucket.Proc.Reg.MergeReceivers[j])
				}
				for _, info := range bucket.RemoteReceivRegInfos {
					matched := false
					for _, source := range inputs {
						for _, remote := range source.RootOp.(*dispatch.Dispatch).RemoteRegs {
							if remote.Uuid == info.Uuid {
								matched = true
								require.Equal(t, source.NodeInfo.Addr, info.FromAddr)
								require.Equal(t, bucket.NodeInfo.Addr, remote.NodeAddr)
							}
						}
					}
					require.True(t, matched, "each remote receiver must keep its original dispatch owner")
				}
			}
		})
	}
}

func newMergeOwnershipCompile(t *testing.T) *Compile {
	t.Helper()
	c := NewMockCompile(t)
	t.Cleanup(func() {
		ReleaseScopes(c.scopes)
		c.proc.Free()
	})
	c.addr = "local:6001"
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 2}, {Addr: "remote:6001", Mcpu: 2}}
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{qry: &plan.Query{}}
	return c
}

func newMergeOwnershipInput(c *Compile, node engine.Node, receivers int) *Scope {
	s := newScope(Remote)
	s.NodeInfo = node
	s.NodeInfo.Mcpu = 1
	s.Proc = c.proc.NewNoContextChildProc(receivers)
	s.setRootOperator(merge.NewArgument())
	c.scopes = append(c.scopes, s)
	return s
}

func newMergeOwnershipWindowNode() *plan.Node {
	return &plan.Node{WinSpecList: []*plan.Expr{{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			WindowFunc: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.ROW_NUMBER, 0), ObjName: "row_number"},
			}}},
		}},
	}}}
}

// Inspect receiver identity independently of the production standalone detector.
func assertMergeReceiverOwnership(t *testing.T, root *Scope) {
	t.Helper()
	owned := make(map[*process.WaitRegister]bool)
	var all []*Scope
	var collect func(*Scope)
	collect = func(s *Scope) {
		all = append(all, s)
		for _, reg := range s.Proc.Reg.MergeReceivers {
			owned[reg] = true
		}
		for _, pre := range s.PreScopes {
			collect(pre)
		}
	}
	collect(root)
	for _, s := range all[1:] {
		switch op := s.RootOp.(type) {
		case *dispatch.Dispatch:
			for _, reg := range op.LocalRegs {
				require.True(t, owned[reg], "dispatch targets a receiver outside its remote tree")
			}
		case *connector.Connector:
			require.True(t, owned[op.Reg], "connector targets a receiver outside its remote tree")
		}
	}
}
