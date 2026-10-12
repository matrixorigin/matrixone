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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minusall"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestMinusAllTransportRoundTrip(t *testing.T) {
	for _, outerType := range []plan.Node_NodeType{
		plan.Node_MINUS_ALL, plan.Node_MINUS, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
	} {
		t.Run(outerType.String(), func(t *testing.T) {
			checkMinusAllTransportRoundTrip(t, outerType)
		})
	}
}

func checkMinusAllTransportRoundTrip(t *testing.T, outerType plan.Node_NodeType) {
	t.Helper()
	c, original := newMinusAllTransportFixture(t, outerType)
	data, err := encodeRemoteScope(original, c.proc)
	require.NoError(t, err)
	restored, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		restored.FreeOperator(c)
		restored.release()
	})

	owners := 0
	var checkScope func(*Scope, *Scope)
	checkScope = func(want, got *Scope) {
		require.True(t, got.IsRemote)
		require.Equal(t, want.NodeInfo, got.NodeInfo)
		require.Len(t, got.PreScopes, len(want.PreScopes))
		require.Len(t, got.Proc.Reg.MergeReceivers, len(want.Proc.Reg.MergeReceivers))
		for i, reg := range got.Proc.Reg.MergeReceivers {
			require.Equal(t, want.Proc.Reg.MergeReceivers[i].NilBatchCnt, reg.NilBatchCnt)
			require.Equal(t, cap(want.Proc.Reg.MergeReceivers[i].Ch2), cap(reg.Ch2))
		}
		var checkOp func(vm.Operator, vm.Operator)
		checkOp = func(wantOp, gotOp vm.Operator) {
			require.Equal(t, wantOp.OpType(), gotOp.OpType())
			require.Equal(t, wantOp.GetOperatorBase().NumChildren(), gotOp.GetOperatorBase().NumChildren())
			switch wantOp.OpType() {
			case vm.MinusAll, vm.Minus, vm.Intersect, vm.IntersectAll:
				owners++
				arg := gotOp.GetOperatorBase()
				require.Equal(t, minusAllTransportKeys(wantOp), minusAllTransportKeys(gotOp))
				require.Equal(t, 2, arg.NumChildren())
				for i := 0; i < 2; i++ {
					input, ok := arg.GetChildren(i).(*merge.Merge)
					require.True(t, ok)
					require.True(t, input.Partial)
					require.Equal(t, int32(i), input.StartIDX)
					require.Equal(t, int32(i+1), input.EndIDX)
					require.Zero(t, input.NumChildren())
				}
			case vm.Limit:
				require.Equal(t, wantOp.(*limit.Limit).LimitExpr, gotOp.(*limit.Limit).LimitExpr)
			}
			for i := 0; i < wantOp.GetOperatorBase().NumChildren(); i++ {
				checkOp(wantOp.GetOperatorBase().GetChildren(i), gotOp.GetOperatorBase().GetChildren(i))
			}
		}
		checkOp(want.RootOp, got.RootOp)
		for i, child := range got.PreScopes {
			out, ok := child.RootOp.(*connector.Connector)
			require.True(t, ok)
			require.Same(t, got.Proc.Reg.MergeReceivers[i], out.Reg)
			checkScope(want.PreScopes[i], child)
		}
	}
	checkScope(original, restored)
	require.Equal(t, 2, owners, "nested MinusAll and its outer set operator must survive transport")
}

func minusAllTransportKeys(op vm.Operator) []*plan.Expr {
	switch arg := op.(type) {
	case *minusall.MinusAll:
		return arg.KeyExprs
	case *minus.Minus:
		return arg.KeyExprs
	case *intersect.Intersect:
		return arg.KeyExprs
	case *intersectall.IntersectAll:
		return arg.KeyExprs
	default:
		panic("unexpected binary set operator")
	}
}

func TestMinusAllTransportRejectsMalformedNestedShape(t *testing.T) {
	for _, missing := range []bool{true, false} {
		name := "wrong_left_merge"
		if missing {
			name = "missing_left_merge"
		}
		t.Run(name, func(t *testing.T) {
			c, original := newMinusAllTransportFixture(t, plan.Node_INTERSECT_ALL)
			data, err := encodeRemoteScope(original, c.proc)
			require.NoError(t, err)
			wire := new(pipeline.Pipeline)
			require.NoError(t, wire.Unmarshal(data))
			// Find the innermost owner through real serialized PreScopes.
			var corrupt func(*pipeline.Pipeline) bool
			corrupt = func(p *pipeline.Pipeline) bool {
				for _, child := range p.Children {
					if corrupt(child) {
						return true
					}
				}
				for i, instruction := range p.InstructionList {
					if instruction.Op != int32(vm.MinusAll) {
						continue
					}
					require.NotSame(t, wire, p)
					require.GreaterOrEqual(t, i, 2)
					require.Equal(t, int32(vm.Merge), p.InstructionList[i-2].Op)
					require.Equal(t, int32(vm.Merge), p.InstructionList[i-1].Op)
					if missing {
						p.InstructionList = append(p.InstructionList[:i-2], p.InstructionList[i-1:]...)
					} else {
						p.InstructionList[i-2] = &pipeline.Instruction{
							Op: int32(vm.Limit), Limit: plan2.MakePlan2Uint64ConstExprWithType(1),
						}
					}
					return true
				}
				return false
			}
			require.True(t, corrupt(wire))
			data, err = wire.Marshal()
			require.NoError(t, err)
			restored, err := decodeScope(data, c.proc, true, nil)
			if restored != nil {
				t.Cleanup(func() {
					restored.FreeOperator(c)
					restored.release()
				})
			}
			input := "left"
			if missing {
				input = "right"
			}
			require.ErrorContains(t, err, "invalid remote binary set operator")
			require.ErrorContains(t, err, input+" input")
			require.Nil(t, restored)
		})
	}
}

func newMinusAllTransportFixture(t *testing.T, outerType plan.Node_NodeType) (*Compile, *Scope) {
	t.Helper()
	nodes := engine.Nodes{{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 1}}
	c := newCompileForShuffleJoinTest(t, nodes)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	leaf := func() *Scope { return newRemoteMergeInputForTest(c, nodes[0], 0) }
	build := func(left, right *Scope, nodeType plan.Node_NodeType, key int64) *Scope {
		node := newParallelDistinctSetTestNode(nodeType)
		node.PhysicalEqualityKeyList = []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(key)}
		return c.compileMinusAndIntersect(node, []*Scope{left}, []*Scope{right}, nodeType)[0]
	}
	inner := build(leaf(), leaf(), plan.Node_MINUS_ALL, 7)
	outer := build(inner, leaf(), outerType, 11)
	outer.setRootOperator(limit.NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(3)))
	root := c.newMergeScope([]*Scope{outer})
	root.Magic = Remote
	t.Cleanup(func() {
		root.FreeOperator(c)
		root.release()
		c.proc.Free()
	})
	return c, root
}
