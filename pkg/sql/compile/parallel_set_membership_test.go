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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestParallelSetMembership(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*Compile, *plan.Node, []*Scope)
		owners int
	}{
		{"intersect identity subset", nil, 8},
		{"except", func(_ *Compile, n *plan.Node, _ []*Scope) { n.NodeType = plan.Node_MINUS }, 8},
		{"four cpu", func(c *Compile, _ *plan.Node, _ []*Scope) { c.ncpu = 4 }, 4},
		{"one cpu", func(c *Compile, _ *plan.Node, _ []*Scope) { c.ncpu = 1 }, 1},
		{"session max dop", func(c *Compile, _ *plan.Node, _ []*Scope) {
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{MaxDop: 2}}}
		}, 2},
		{"explicit headroom", func(_ *Compile, n *plan.Node, _ []*Scope) { n.SpillMem = 256 << 20 }, 2},
		{"automatic unchanged", func(_ *Compile, n *plan.Node, _ []*Scope) { n.SpillMem = 0 }, 1},
		{"all unchanged", func(_ *Compile, n *plan.Node, _ []*Scope) { n.NodeType = plan.Node_INTERSECT_ALL }, 1},
		{"prepared unchanged", func(c *Compile, _ *plan.Node, _ []*Scope) { c.isPrepare = true }, 1},
		{"remote unchanged", func(_ *Compile, _ *plan.Node, s []*Scope) { s[0].NodeInfo.Addr = "remote" }, 1},
		{"no identity key", func(_ *Compile, n *plan.Node, _ []*Scope) {
			n.PhysicalEqualityKeyList[1] = n.PhysicalEqualityKeyList[0]
		}, 1},
		{"different physical domain", func(_ *Compile, n *plan.Node, _ []*Scope) { n.PhysicalEqualityKeyList[1].Typ.Id = int32(types.T_int32) }, 1},
		{"limit fallback", func(_ *Compile, n *plan.Node, _ []*Scope) { n.Limit = plan2.MakePlan2Int64ConstExprWithType(1) }, 1},
		{"nullable annotation", func(_ *Compile, n *plan.Node, _ []*Scope) { n.PhysicalEqualityKeyList[1].Typ.NotNullable = true }, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			t.Cleanup(c.proc.Free)
			c.isPrepare = false
			c.addr = "127.0.0.1:18000"
			c.ncpu = 64
			c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 64}}
			inputs := newDistinctSetTestScopes(c, 2)
			for _, s := range inputs {
				s.NodeInfo.Addr = c.addr
			}
			visible := []*plan.Expr{plan2.GetColExpr(types.T_char.ToType().PlanType(), 0, 0), plan2.GetColExpr(types.T_date.ToType().PlanType(), 0, 1)}
			physical := plan2.DeepCopyExprList(visible)
			physical[0] = &plan.Expr{Typ: visible[0].Typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "cast"}, Args: []*plan.Expr{visible[0]}}}}
			node := &plan.Node{NodeType: plan.Node_INTERSECT, Stats: &plan.Stats{Dop: 64}, SpillMem: 4 << 30, ProjectList: visible, PhysicalEqualityKeyList: physical}
			if tc.edit != nil {
				tc.edit(c, node, inputs)
			}
			before := proto.Clone(node)
			keys, dop := c.parallelSetMembership(node, inputs[:1], inputs[1:])
			require.Equal(t, tc.owners, dop)
			if dop == 1 {
				for _, s := range inputs {
					s.FreeOperator(c)
					s.release()
				}
				return
			}
			require.Equal(t, []int32{1}, keys, "computed text domain must not be used for routing")
			result := c.compileParallelSetMembership(node, inputs[:1], inputs[1:], keys, dop)
			t.Cleanup(func() {
				for _, s := range result {
					s.FreeOperator(c)
					s.release()
				}
			})
			require.True(t, proto.Equal(before, node))
			require.Len(t, result, 1)
			require.IsType(t, &merge.Merge{}, result[0].RootOp)
			require.Len(t, result[0].PreScopes, dop)
			for i, owner := range result[0].PreScopes {
				op := owner.RootOp.GetOperatorBase().GetChildren(0)
				var actualKeys []*plan.Expr
				if node.NodeType == plan.Node_INTERSECT {
					actualKeys = op.(*intersect.Intersect).KeyExprs
				} else {
					actualKeys = op.(*minus.Minus).KeyExprs
				}
				require.Len(t, actualKeys, len(physical))
				for k := range actualKeys {
					require.True(t, proto.Equal(actualKeys[k], node.PhysicalEqualityKeyList[k]))
				}
				require.Equal(t, 2, op.GetOperatorBase().NumChildren())
				require.Len(t, owner.Proc.Reg.MergeReceivers, 2)
				if i != 0 {
					require.Empty(t, owner.PreScopes)
					continue
				}
				require.Len(t, owner.PreScopes, 2)
				for _, feeder := range owner.PreScopes {
					router := feeder.RootOp.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
					require.Equal(t, keys, router.ShuffleKeyColumns)
					require.True(t, router.DrainAllBuckets)
					require.Equal(t, int32(dop), router.BucketNum)
				}
			}
		})
	}
}
