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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// Ordinary Partition emits a complete logical partition in each batch. Such
// batches can be assigned to independent Window owners without repartitioning
// rows or changing the frame evaluator. Keep automatic spill policy unchanged.
func (c *Compile) parallelPartitionWindow(node *plan.Node, ss []*Scope) int {
	const minOwnerSpill = int64(128 << 20)
	if node == nil || node.NodeType != plan.Node_WINDOW || c.IsTpQuery() ||
		c.isPrepare || c.ncpu <= 1 || node.SpillMem < 2*minOwnerSpill ||
		len(node.WinSpecList) == 0 || len(ss) != 1 || ss[0] == nil ||
		ss[0].Proc == nil || ss[0].RootOp == nil || ss[0].NodeInfo.Mcpu != 1 || len(c.cnList) != 1 ||
		c.addr == "" || c.cnList[0].Addr != c.addr ||
		!sameExecutionNode(ss[0].NodeInfo, c.cnList[0]) {
		return 1
	}
	w := node.WinSpecList[0].GetW()
	if w == nil || len(w.PartitionBy) == 0 || w.WindowFunc.GetF() == nil {
		return 1
	}
	for _, expr := range w.PartitionBy {
		if expr.GetCol() == nil {
			return 1
		}
	}
	for _, spec := range w.OrderBy {
		if spec == nil || spec.Expr.GetCol() == nil {
			return 1
		}
	}
	for _, expr := range node.WinSpecList {
		if expr.GetW() == nil || expr.GetW().WindowFunc.GetF() == nil {
			return 1
		}
		for _, arg := range expr.GetW().WindowFunc.GetF().Args {
			if arg == nil || (arg.GetCol() == nil && arg.GetLit() == nil) {
				return 1
			}
		}
	}
	if w.Frame != nil {
		for _, bound := range []*plan.FrameBound{w.Frame.Start, w.Frame.End} {
			if bound != nil && bound.Val != nil && bound.Val.GetLit() == nil {
				return 1
			}
		}
	}
	upstream := ss[0].RootOp
	for upstream.OpType() == vm.Projection && upstream.GetOperatorBase().NumChildren() == 1 {
		upstream = upstream.GetOperatorBase().GetChildren(0)
	}
	p, ok := upstream.(*partition.Partition)
	if !ok || p.PreReduce || p.Limit != nil || p.WithTies {
		return 1
	}
	return min(c.capFanoutByMaxDop(c.ncpu), int(node.SpillMem/minOwnerSpill))
}

func (c *Compile) compileParallelPartitionWindow(node *plan.Node, source *Scope, dop int) []*Scope {
	owners := c.newScopeListWithNode(dop, 1, c.cnList[0])
	send, err := constructLocalDispatchFromScopes(0, owners, source)
	if err != nil {
		panic(err) // selection established coordinator-local receiver placement
	}
	// Dispatch transfers whole batches. Row-level shuffle would split a logical
	// partition and invalidate cumulative, ranking and bounded frame semantics.
	send.FuncId = dispatch.SendToAnyLocalFunc
	send.SetAnalyzeControl(c.anal.curNodeIdx, false)
	source.setRootOperator(send)
	source.IsEnd = true
	owners[0].PreScopes = append(owners[0].PreScopes, source)
	for _, owner := range owners {
		arg := constructWindow(c.proc.Ctx, node, c.proc)
		arg.SpillThreshold = node.SpillMem / int64(dop)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
		owner.setRootOperator(arg)
	}
	c.anal.isFirst = false
	return []*Scope{c.newMergeScope(owners)}
}
