// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minusall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// tryCompileOwnedSetMembership changes no input scope until all admission and
// graph checks pass. A nil result means the existing execution remains valid.
func (c *Compile) tryCompileOwnedSetMembership(nodeID int32, node *plan.Node, left, right []*Scope) ([]*Scope, error) {
	if err := process.PipelineContextError(c.proc.Ctx); err != nil {
		return nil, err
	}
	if node == nil || node.Stats == nil || c.IsTpQuery() || c.isPrepare || c.ncpu <= 1 ||
		c.addr == "" || len(node.ProjectList) == 0 || len(left) == 0 || len(right) == 0 ||
		node.Limit != nil || node.Offset != nil || len(node.OrderBy) != 0 || len(node.FilterList) != 0 ||
		node.FilterIsBarrier || nodeHasUserLevelLockFunction(node) ||
		math.IsNaN(node.Stats.Cost) || math.IsInf(node.Stats.Cost, 0) || node.Stats.Cost < 250000 {
		return nil, nil
	}
	keys := setMembershipIdentityKeys(node)
	query := c.pn.GetQuery()
	if len(keys) == 0 || !setMembershipInputsAreTotal(query, nodeID) {
		return nil, nil
	}
	worker, err := c.currentCNWorkerWithRuntimeState(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	if worker.State == schedule.WorkerStateDraining || worker.State == schedule.WorkerStateDrained {
		return nil, nil
	}
	local := c.materializeScheduledWorker(worker)
	admitted := false
	for _, candidate := range c.cnList {
		if sameExecutionNode(candidate, local) && schedulableEngineWorkState(candidate.WorkState) {
			admitted = true
			break
		}
	}
	if !admitted {
		return nil, nil
	}
	inputWorkers, ok := setMembershipProducerCount(local, c.addr, left, right)
	if !ok || inputWorkers <= 1 {
		return nil, nil
	}
	consolidate := inputWorkers > 8
	routers := inputWorkers
	if consolidate {
		routers = 2
	}
	width, ok := setMembershipRowWidth(query, node)
	if !ok {
		return nil, nil
	}
	unit := math.Ceil(float64(8<<20) + 2*float64(routers)*8192*width)
	if math.IsNaN(unit) || math.IsInf(unit, 0) || unit <= 0 || unit >= float64(math.MaxUint64/2) {
		return nil, nil
	}
	budget, err := c.proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	headroom, err := budget.AvailableMemory()
	if err != nil {
		return nil, err
	}
	// Consolidation reduces router count, not the input work available to
	// membership owners: each router can send to every owner.
	workerLimit := c.capFanoutByMaxDop(c.ncpu)
	dop := min(4, workerLimit, inputWorkers)
	for dop > 1 && uint64(unit) > headroom/uint64(2*dop) {
		dop--
	}
	if dop == 4 && workerLimit >= shuffle.MaxOwnedBuckets && inputWorkers >= shuffle.MaxOwnedBuckets &&
		uint64(unit) <= headroom/(2*shuffle.MaxOwnedBuckets) {
		dop = shuffle.MaxOwnedBuckets
	}
	if dop < 2 {
		return nil, nil
	}
	return c.compileOwnedSetMembership(node, left, right, keys, dop, consolidate, local)
}

// Only scopes that actually clone through ParallelRun may publish multiple
// terminal signals. Remote scopes are first merged into one local stream.
func setMembershipProducerCount(local engine.Node, addr string, sides ...[]*Scope) (int, bool) {
	seenScopes := make(map[*Scope]bool)
	seenOps := make(map[vm.Operator]bool)
	var uniqueOp func(vm.Operator) bool
	uniqueOp = func(op vm.Operator) bool {
		if op == nil {
			return true
		}
		if seenOps[op] {
			return false
		}
		seenOps[op] = true
		for _, child := range op.GetOperatorBase().Children {
			if !uniqueOp(child) {
				return false
			}
		}
		return true
	}
	var unique func(*Scope) bool
	unique = func(s *Scope) bool {
		if s == nil || seenScopes[s] || s.RootOp == nil {
			return false
		}
		seenScopes[s] = true
		if !uniqueOp(s.RootOp) {
			return false
		}
		for _, child := range s.PreScopes {
			if !unique(child) {
				return false
			}
		}
		return true
	}
	count := 0
	for _, scopes := range sides {
		for _, s := range scopes {
			if s == nil || s.Proc == nil || s.NodeInfo.Mcpu < 1 || s.IsEnd || !unique(s) {
				return 0, false
			}
			switch s.RootOp.(type) {
			case *connector.Connector, *dispatch.Dispatch:
				return 0, false
			case *top.Top:
				if s.RootOp.(*top.Top).OrderedOutput {
					return 0, false
				}
			}
			if !sameExecutionNode(s.NodeInfo, local) || !s.ipAddrMatch(addr) {
				count++
				continue
			}
			if s.NodeInfo.Mcpu > 1 && (s.Magic == Normal || !(s.isTableScan() || s.IsLoad || s.IsTbFunc)) {
				return 0, false
			}
			count += s.NodeInfo.Mcpu
		}
	}
	return count, true
}

func setMembershipRowWidth(query *plan.Query, node *plan.Node) (float64, bool) {
	const maxProfitableRowBytes = 72
	rowTypes := make([]plan.Type, 0, len(node.ProjectList))
	for _, expr := range node.ProjectList {
		if expr == nil {
			return 0, false
		}
		rowTypes = append(rowTypes, expr.Typ)
	}
	declared, ok := plan2.MaterializedDeclaredRowSize(rowTypes)
	if !ok {
		return 0, false
	}
	// Exchange and transport both copy the row. Projection stats are commonly
	// the synthetic value 100, so only use scan statistics when every projection
	// preserves distinct input columns. Otherwise the declared upper estimate
	// gives a conservative profitability check.
	estimated := float64(0)
	for _, id := range node.Children {
		w, known := setMembershipDirectScanRowWidth(query, id)
		if !known {
			estimated = declared
			break
		}
		estimated = math.Max(estimated, w)
	}
	if estimated <= 0 || estimated > maxProfitableRowBytes {
		return 0, false
	}
	// The wider declared bound also drives the budget heuristic; the live
	// allocation controller remains authoritative if statistics are stale.
	return math.Max(estimated, declared), true
}

// This estimate is valid only along a chain of one-to-one column projections.
// A scan's complete row statistic may overestimate a narrower projection, but
// never needs to infer the bytes added by a computed or repeated expression.
func setMembershipDirectScanRowWidth(query *plan.Query, id int32) (float64, bool) {
	seen := make(map[int32]bool)
	for id >= 0 && int(id) < len(query.Nodes) && query.Nodes[id] != nil && !seen[id] {
		seen[id] = true
		node := query.Nodes[id]
		if len(node.ProjectList) == 0 {
			return 0, false
		}
		columns := make(map[int32]bool, len(node.ProjectList))
		for _, expr := range node.ProjectList {
			if expr == nil || expr.GetCol() == nil || expr.GetCol().RelPos != 0 || columns[expr.GetCol().ColPos] {
				return 0, false
			}
			columns[expr.GetCol().ColPos] = true
		}
		switch node.NodeType {
		case plan.Node_TABLE_SCAN:
			if node.Stats == nil || node.Stats.Rowsize <= 0 || math.IsNaN(node.Stats.Rowsize) || math.IsInf(node.Stats.Rowsize, 0) {
				return 0, false
			}
			return node.Stats.Rowsize, true
		case plan.Node_PROJECT, plan.Node_FILTER:
			if len(node.Children) != 1 {
				return 0, false
			}
			id = node.Children[0]
		default:
			return 0, false
		}
	}
	return 0, false
}

func (c *Compile) compileOwnedSetMembership(node *plan.Node, left, right []*Scope, keys []int32, dop int, consolidate bool, local engine.Node) ([]*Scope, error) {
	// Construct all dispatches before changing any source tree. A source's
	// merge wrapper can itself group remote dependencies, so trying to detach a
	// partially built wrapper on error would lose ownership of those groups.
	counts := make([]int, 0, len(left)+len(right))
	leftCount := 0
	for _, side := range [][]*Scope{left, right} {
		start := len(counts)
		if consolidate {
			counts = append(counts, 1)
		} else {
			for _, source := range side {
				count := source.NodeInfo.Mcpu
				if !sameExecutionNode(source.NodeInfo, local) || !source.ipAddrMatch(c.addr) {
					count = 1
				}
				counts = append(counts, count)
			}
		}
		if start == 0 {
			leftCount = len(counts)
		}
	}
	owners := c.newScopeListWithNode(dop, len(counts), local)
	sends := make([]*dispatch.Dispatch, 0, len(counts))
	for i, count := range counts {
		source := &Scope{NodeInfo: scopeNodeWithMcpu(local, count)}
		send, err := constructLocalDispatchFromScopes(i, owners, source)
		if err != nil {
			for _, send := range sends {
				send.Release()
			}
			ReleaseScopes(owners)
			return nil, err
		}
		sends = append(sends, send)
	}
	feeders := make([]*Scope, 0, len(counts))
	for _, side := range [][]*Scope{left, right} {
		if consolidate {
			feeders = append(feeders, c.newMergeScope(side))
			continue
		}
		for _, source := range side {
			if !sameExecutionNode(source.NodeInfo, local) || !source.ipAddrMatch(c.addr) {
				source = c.newMergeScope([]*Scope{source})
			}
			feeders = append(feeders, source)
		}
	}
	for i, source := range feeders {
		router := shuffle.NewArgument()
		router.ShuffleType, router.ShuffleKeyColumns = int32(plan.ShuffleType_Hash), append([]int32(nil), keys...)
		router.BucketNum, router.DrainAllBuckets, router.OwnedOutput = int32(dop), true, true
		router.OwnedColumnCount = len(node.ProjectList)
		router.SetAnalyzeControl(c.anal.curNodeIdx, false)
		source.setRootOperator(router)
		send := sends[i]
		send.FuncId, send.ShuffleType = dispatch.ShuffleToAllFunc, plan2.ShuffleToRegIndex
		send.SetAnalyzeControl(c.anal.curNodeIdx, false)
		source.setRootOperator(send)
		source.IsEnd = true
	}
	owners[0].PreScopes = append(owners[0].PreScopes, feeders...)
	for _, owner := range owners {
		owner.RootOp.(*merge.Merge).WithPartial(0, int32(leftCount))
		rightMerge := merge.NewArgument().WithPartial(int32(leftCount), int32(len(feeders)))
		var op vm.Operator
		switch node.NodeType {
		case plan.Node_INTERSECT:
			a := intersect.NewArgument()
			a.KeyExprs = node.PhysicalEqualityKeyList
			op = a
		case plan.Node_INTERSECT_ALL:
			a := intersectall.NewArgument()
			a.KeyExprs = node.PhysicalEqualityKeyList
			op = a
		case plan.Node_MINUS:
			a := minus.NewArgument()
			a.KeyExprs = node.PhysicalEqualityKeyList
			op = a
		case plan.Node_MINUS_ALL:
			a := minusall.NewArgument()
			a.KeyExprs = node.PhysicalEqualityKeyList
			op = a
		}
		op.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
		owner.setRootOperator(op)
		op.GetOperatorBase().AppendChild(rightMerge)
	}
	c.hasMergeOp = true
	c.anal.isFirst = false
	return []*Scope{c.newMergeScope(owners)}, nil
}
