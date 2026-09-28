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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// CN ownership, not local worker count, determines colocation. This differs
// from scopesRunOnCoordinator, which requires one single-worker pipeline.
func (c *Compile) joinInputsOnCoordinator(probe, build []*Scope) bool {
	for _, scopes := range [2][]*Scope{probe, build} {
		for _, scope := range scopes {
			if !sameExecutionAddr(scope.NodeInfo.Addr, c.addr) {
				return false
			}
		}
	}
	return true
}

// A WINDOW scope cannot be encoded remotely. Preserve its coordinator stage
// before either broadcast or shuffle attaches it to a different scope tree.
func (c *Compile) joinNeedsLocalWindow(node *plan.Node, probe, build []*Scope) bool {
	if !node.Stats.HashmapStats.Shuffle && c.joinInputsOnCoordinator(probe, build) {
		return false
	}
	return scopesContainOperator(probe, vm.Window) || scopesContainOperator(build, vm.Window)
}

// JoinMap is a process-local dependency, not a remotely transported result.
// With a single probe, a remote build on another CN cannot publish its map to
// that probe. Merge batches first and construct both join operators locally.
// Local SINK_SCAN/foreign inputs must also stay with their owning process;
// attaching such a build below a remote probe does not relocate that state.
// Shuffle has its own owner-aware stage placement and does not use this path.
func (c *Compile) colocateBroadcastJoinInputs(probe, build []*Scope) ([]*Scope, []*Scope) {
	// Preserve already-local parallel pipelines and their probe fanout.
	if c.joinInputsOnCoordinator(probe, build) {
		return probe, build
	}
	_, hasLocalInput := sinkScanDependencyNode(probe, build)
	remoteBuildMismatch := len(probe) == 1 && c.IsSingleScope(build) &&
		build[0].Magic == Remote && !sameExecutionNode(probe[0].NodeInfo, build[0].NodeInfo)
	if !hasLocalInput && !remoteBuildMismatch {
		return probe, build
	}
	if !c.scopesRunOnCoordinator(probe) {
		probe = []*Scope{c.newMergeScope(probe)}
	}
	if !c.scopesRunOnCoordinator(build) {
		build = []*Scope{c.newMergeScope(build)}
	}
	return probe, build
}
