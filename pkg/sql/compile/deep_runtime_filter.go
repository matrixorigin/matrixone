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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// Called only after compileBuildSideForBroadcastJoin has made its input
// complete. A lone HashBuild elsewhere is not evidence of complete keys.
func (c *Compile) noteCompleteBroadcastRuntimeFilterBuild(op vm.Operator) vm.Operator {
	build, ok := op.(*hashbuild.HashBuild)
	if !ok || build.RuntimeFilterSpec == nil || !c.deepRuntimeFilterTags[build.RuntimeFilterSpec.Tag] {
		return op
	}
	if c.completeDeepRuntimeFilterBuilds == nil {
		c.completeDeepRuntimeFilterBuilds = make(map[vm.Operator]bool)
	}
	c.completeDeepRuntimeFilterBuilds[op] = true
	return op
}

func (c *Compile) validateDeepRuntimeFilterTopology(tags []int32, logicalSafe bool, roots []*Scope) {
	if len(tags) == 0 {
		return
	}
	topology := collectRuntimeFilterTopology(roots, tags)
	safe := logicalSafe && !c.IsTpQuery() && len(c.dependentSourceScopes) == 0 && c.proc != nil && c.proc.GetMessageBoard() != nil
	visited := make(map[*Scope]bool)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if scope == nil || visited[scope] {
			return
		}
		visited[scope] = true
		if scope.IsRemote || scope.LazyPreScopes || len(scope.lazyRemoteFragmentCounts) != 0 ||
			(scope.Magic != Normal && scope.Magic != Merge && scope.Magic != Remote) ||
			(scope.Magic == Normal && len(scope.PreScopes) != 0) ||
			(scope.NodeInfo.Addr != "" && scope.NodeInfo.Addr != c.addr) ||
			(scope.Magic == Remote && scope.NodeInfo.Addr != c.addr) ||
			scope.Proc == nil || scope.Proc.GetMessageBoard() != c.proc.GetMessageBoard() {
			safe = false
		}
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	if safe {
		for _, root := range roots {
			walk(root)
		}
	}
	for _, tag := range tags {
		admitted := safe && len(topology.producers[tag]) == 1 && len(topology.consumers[tag]) > 0
		if admitted {
			producer := topology.producers[tag][0]
			complete := false
			if producer.NodeInfo.Mcpu == 1 {
				_ = vm.HandleAllOp(producer.RootOp, func(_ vm.Operator, op vm.Operator) error {
					if build, ok := op.(*hashbuild.HashBuild); ok && build.RuntimeFilterSpec != nil &&
						build.RuntimeFilterSpec.Tag == tag && !build.IsShuffle && c.completeDeepRuntimeFilterBuilds[op] {
						complete = true
					}
					return nil
				})
			}
			admitted = complete
		}
		if !admitted {
			// This removes the publisher and installs private source-node views,
			// so initialization/remote representation cannot restore a scan wait.
			disableOptionalRuntimeFilter(tag, topology)
		}
	}
}
