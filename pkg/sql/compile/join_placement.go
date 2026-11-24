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
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func broadcastJoinForcesOneCN(node *plan.Node, isEq bool) bool {
	switch node.JoinType {
	case plan.Node_OUTER, plan.Node_DEDUP:
		return true
	case plan.Node_LEFT, plan.Node_RIGHT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE:
		return isEq && node.IsRightJoin
	default:
		return false
	}
}

// joinInputsOnCoordinator checks CN ownership, not local parallelism.
func (c *Compile) joinInputsOnCoordinator(probe, build []*Scope) bool {
	for _, scopes := range [2][]*Scope{probe, build} {
		for _, scope := range scopes {
			if !scope.ipAddrMatch(c.addr) {
				return false
			}
		}
	}
	return true
}

func (c *Compile) singleCoordinatorJoinInput(scopes []*Scope) []*Scope {
	if len(scopes) == 1 && scopes[0].NodeInfo.Mcpu == 1 && scopes[0].ipAddrMatch(c.addr) {
		return scopes
	}
	return []*Scope{c.newMergeScope(scopes)}
}

// WINDOW has no remote instruction encoding. Keep only its JOIN stage local;
// its input scans can still run on other CNs and ship their batches.
func (c *Compile) joinNeedsLocalWindow(node *plan.Node, probe, build []*Scope) bool {
	if !node.Stats.HashmapStats.Shuffle && c.joinInputsOnCoordinator(probe, build) {
		return false
	}
	for _, scopes := range [2][]*Scope{probe, build} {
		for _, scope := range scopes {
			if walkScopeTree(scope, func(s *Scope) bool {
				found := false
				if s.RootOp != nil {
					_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
						found = found || op.OpType() == vm.Window
						return nil
					})
				}
				return found
			}) {
				return true
			}
		}
	}
	return false
}

// A single probe does not imply that its build publishes to the same
// MessageBoard. Likewise, a SINK_SCAN or foreign scan cannot follow a remote
// probe: its input is process-local. Keep the join on the coordinator but
// continue to send ordinary remote scan batches through their Merge edges.
// Shuffle already has its own owner-aware placement.
func (c *Compile) colocateBroadcastJoinInputs(probe, build []*Scope) ([]*Scope, []*Scope) {
	if c.joinInputsOnCoordinator(probe, build) {
		return probe, build
	}
	_, hasLocalInput := sinkScanDependencyNode(probe, build)
	remoteBuildMismatch := len(probe) == 1 && c.IsSingleScope(build) &&
		build[0].Magic == Remote && !sameExecutionNode(probe[0].NodeInfo, build[0].NodeInfo)
	if !hasLocalInput && !remoteBuildMismatch {
		return probe, build
	}
	return c.singleCoordinatorJoinInput(probe), c.singleCoordinatorJoinInput(build)
}

// Multi-probe broadcast leaves each probe in place and dispatches build batches
// to a HashBuild on each probe CN. Only the build source needs a host: attaching
// a local SINK_SCAN under a peer would disconnect it from its producer.
func (c *Compile) placeMultiProbeBroadcastBuild(probe, build []*Scope) ([]*Scope, []*Scope) {
	if c.joinInputsOnCoordinator(probe, build) {
		return probe, build
	}
	if _, hasSink := sinkScanDependencyNode(build); !hasSink {
		return probe, build
	}
	build = c.singleCoordinatorJoinInput(build)
	for _, p := range probe {
		if sameExecutionNode(p.NodeInfo, build[0].NodeInfo) {
			// compileBuildSideForBroadcastJoin attaches to this probe, even
			// if it isn't the first one. Other CNs keep their own JoinMaps.
			return probe, build
		}
	}
	return c.singleCoordinatorJoinInput(probe), build
}
