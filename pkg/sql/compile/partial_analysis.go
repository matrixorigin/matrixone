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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// capturePartialExecution runs after runOnce joins the local roots, before a
// retry compile or its analyzers can be released. Remote plans were decoded
// and published by this attempt's scope senders.
func (c *Compile) capturePartialExecution(runC *Compile, attempt uint64) {
	if c.anal == nil || runC.anal == nil || runC.anal.qry == nil {
		return
	}
	partial := models.NewPhyPlan()
	partial.RetryTime = int(attempt)
	partial.Partial = models.PartialExecutionCapture{
		Interrupted:    true,
		Attempt:        attempt,
		RemoteExpected: countExpectedRemoteScopes(runC.scopes, runC.addr),
	}
	seen := make(map[vm.Operator]struct{})
	for _, scope := range runC.scopes {
		if local, ok := partialLocalScope(scope, runC.addr, seen); ok {
			partial.LocalScope = append(partial.LocalScope, local)
		}
	}
	// The lock protects the append-only list. Its decoded plan payloads are
	// immutable, so copying the slice is enough before walking descendants.
	runC.anal.mu.RLock()
	reports := slices.Clone(runC.anal.remotePhyPlans)
	runC.anal.mu.RUnlock()
	for _, report := range reports {
		if len(report.LocalScope) == 0 {
			continue
		}
		partial.Partial.RemoteObserved++
		for _, scope := range report.LocalScope {
			if observed, ok := partialRemoteScope(scope); ok {
				partial.RemoteScope = append(partial.RemoteScope, observed)
			}
		}
		for _, scope := range report.RemoteScope {
			if observed, ok := partialRemoteScope(scope); ok {
				partial.RemoteScope = append(partial.RemoteScope, observed)
			}
		}
	}
	partial.Partial.RemoteMissing = partial.Partial.RemoteObserved < partial.Partial.RemoteExpected
	if len(partial.LocalScope)+len(partial.RemoteScope) == 0 {
		return
	}

	// Keep prepared placeholders and success topology intact. The frontend
	// consumes this attempt's logical projection synchronously during export.
	current := runC.anal.qry
	query := *current
	query.BackgroundQueries = nil
	query.Nodes = make([]*plan.Node, len(current.Nodes))
	nodes := make([]plan.Node, len(current.Nodes))
	for i, node := range current.Nodes {
		nodes[i] = *node
		nodes[i].AnalyzeInfo = nil
		query.Nodes[i] = &nodes[i]
	}
	for i := range partial.LocalScope {
		processPhyScope(&partial.LocalScope[i], nil, query.Nodes, nil)
	}
	for i := range partial.RemoteScope {
		processPhyScope(&partial.RemoteScope[i], nil, query.Nodes, nil)
	}
	c.attachResourceSummary(partial)
	c.anal.partialExecutionPlan, c.anal.partialExecutionQuery = partial, &query
}

func partialLocalScope(scope *Scope, addr string, seen map[vm.Operator]struct{}) (models.PhyScope, bool) {
	if !scopeRunsLocally(scope, addr) {
		return models.PhyScope{}, false
	}
	out := models.PhyScope{
		Magic:        scope.Magic.String(),
		Mcpu:         int8(scope.NodeInfo.Mcpu),
		RootOperator: partialLocalOperator(scope.RootOp, seen),
	}
	for _, pre := range scope.PreScopes {
		if observed, ok := partialLocalScope(pre, addr, seen); ok {
			out.PreScopes = append(out.PreScopes, observed)
		}
	}
	return out, out.RootOperator != nil || len(out.PreScopes) != 0
}

func partialLocalOperator(op vm.Operator, seen map[vm.Operator]struct{}) *models.PhyOperator {
	if op == nil {
		return nil
	}
	if _, exists := seen[op]; exists {
		return nil
	}
	seen[op] = struct{}{}

	base := op.GetOperatorBase()
	var out models.PhyOperator
	for _, child := range base.Children {
		if observed := partialLocalOperator(child, seen); observed != nil {
			out.Children = append(out.Children, observed)
		}
	}
	if base.OpAnalyzer != nil {
		out.OpStats = partialOperatorStats(base.OpAnalyzer.GetOpStats())
	}
	if out.OpStats == nil && len(out.Children) == 0 {
		return nil
	}
	out.OpName, out.NodeIdx = op.OpType().String(), base.Idx
	out.IsFirst, out.IsLast = base.IsFirst, base.IsLast
	return &out
}

func partialRemoteScope(scope models.PhyScope) (models.PhyScope, bool) {
	out := models.PhyScope{Magic: scope.Magic, Mcpu: scope.Mcpu, RootOperator: partialRemoteOperator(scope.RootOperator)}
	for _, pre := range scope.PreScopes {
		if observed, ok := partialRemoteScope(pre); ok {
			out.PreScopes = append(out.PreScopes, observed)
		}
	}
	return out, out.RootOperator != nil || len(out.PreScopes) != 0
}

func partialRemoteOperator(op *models.PhyOperator) *models.PhyOperator {
	if op == nil {
		return nil
	}
	out := models.PhyOperator{OpName: op.OpName, NodeIdx: op.NodeIdx, IsFirst: op.IsFirst, IsLast: op.IsLast, OpStats: partialOperatorStats(op.OpStats)}
	for _, child := range op.Children {
		if observed := partialRemoteOperator(child); observed != nil {
			out.Children = append(out.Children, observed)
		}
	}
	if out.OpStats == nil && len(out.Children) == 0 {
		return nil
	}
	return &out
}

func partialOperatorStats(stats *process.OperatorStats) *process.OperatorStats {
	if stats == nil || stats.CallNum == 0 {
		return nil
	}
	// Copy scalar facts only. Live maps and recursive background queries are
	// not needed for scan attribution or compact operator diagnostics.
	out := *stats
	out.OperatorMetrics = nil
	out.ExtraStats = nil
	out.BackgroundQueries = nil
	return &out
}
