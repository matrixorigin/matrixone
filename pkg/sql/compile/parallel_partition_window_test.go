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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileParallelPartitionWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*Compile, *plan.Node, *Scope)
		owners int
	}{
		{"complete partitions", nil, 4},
		{"auto unchanged", func(_ *Compile, n *plan.Node, _ *Scope) { n.SpillMem = 0 }, 1},
		{"small threshold", func(_ *Compile, n *plan.Node, _ *Scope) { n.SpillMem = 255 << 20 }, 1},
		{"threshold boundary", func(_ *Compile, n *plan.Node, _ *Scope) { n.SpillMem = 256 << 20 }, 2},
		{"max dop", func(c *Compile, _ *plan.Node, _ *Scope) {
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{MaxDop: 2}}}
		}, 2},
		{"one cpu", func(c *Compile, _ *plan.Node, _ *Scope) { c.ncpu = 1 }, 1},
		{"prepared", func(c *Compile, _ *plan.Node, _ *Scope) { c.isPrepare = true }, 1},
		{"global window", func(_ *Compile, n *plan.Node, _ *Scope) { n.WinSpecList[0].GetW().PartitionBy = nil }, 1},
		{"remote input", func(_ *Compile, _ *plan.Node, s *Scope) { s.NodeInfo.Addr = "remote:18000" }, 1},
		{"multiple producers", func(_ *Compile, _ *plan.Node, s *Scope) { s.NodeInfo.Mcpu = 2 }, 1},
		{"topn", func(_ *Compile, _ *plan.Node, s *Scope) {
			p := s.RootOp.(*partition.Partition)
			p.PreReduce, p.Limit = true, plan2.MakePlan2Uint64ConstExprWithType(1)
		}, 1},
		{"computed order", func(_ *Compile, n *plan.Node, _ *Scope) {
			n.WinSpecList[0].GetW().OrderBy[0].Expr = n.WinSpecList[0].GetW().WindowFunc
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			t.Cleanup(c.proc.Free)
			c.isPrepare, c.addr, c.ncpu = false, "127.0.0.1:18000", 16
			c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 16}}
			col := plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 0)
			fn, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "sum", []*plan.Expr{col})
			require.NoError(t, err)
			node := &plan.Node{NodeType: plan.Node_WINDOW, SpillMem: 512 << 20,
				WinSpecList: []*plan.Expr{{Expr: &plan.Expr_W{W: &plan.WindowSpec{
					Name: "sum", WindowFunc: fn, PartitionBy: []*plan.Expr{col},
					OrderBy: []*plan.OrderBySpec{{Expr: col}},
				}}}}}
			source := newDistinctSetTestScope(c)
			source.NodeInfo.Addr = c.addr
			p := partition.NewArgument()
			p.OrderBySpecs = []*plan.OrderBySpec{{Expr: col}}
			source.setRootOperator(p)
			if tc.edit != nil {
				tc.edit(c, node, source)
			}
			before := proto.Clone(node)
			result := c.compileWin(node, []*Scope{source})
			t.Cleanup(func() { result[0].FreeOperator(c); result[0].release() })
			require.True(t, proto.Equal(before, node))
			require.Len(t, result, 1)
			if tc.owners == 1 {
				require.IsType(t, &window.Window{}, result[0].RootOp)
				return
			}
			require.IsType(t, &merge.Merge{}, result[0].RootOp)
			require.Len(t, result[0].PreScopes, tc.owners)
			var sum int64
			for i, owner := range result[0].PreScopes {
				w := owner.RootOp.GetOperatorBase().GetChildren(0).(*window.Window)
				require.False(t, w.PartitionTopN)
				sum += w.SpillThreshold
				require.Len(t, owner.Proc.Reg.MergeReceivers, 1)
				if i == 0 {
					require.Equal(t, []*Scope{source}, owner.PreScopes)
					require.Equal(t, dispatch.SendToAnyLocalFunc, source.RootOp.(*dispatch.Dispatch).FuncId)
				} else {
					require.Empty(t, owner.PreScopes)
				}
			}
			require.LessOrEqual(t, sum, node.SpillMem)
		})
	}
}
