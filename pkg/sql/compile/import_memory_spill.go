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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// beginSpillEpochs binds broadcast spill only for a complete admitted local
// graph. Remote and other uncontrolled attempts keep their legacy behavior.
func (c *Compile) beginSpillEpochs() {
	if c.importMemory == nil {
		for _, scope := range c.scopes {
			disableUncontrolledBroadcastSpill(scope)
		}
		return
	}
	probes := make(map[int32][]*hashjoin.HashJoin)
	builds := make(map[int32][]*hashbuild.HashBuild)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			switch join := op.(type) {
			case *hashjoin.HashJoin:
				if !join.IsShuffle {
					probes[join.JoinMapTag] = append(probes[join.JoinMapTag], join)
				}
			case *hashbuild.HashBuild:
				if !join.IsShuffle && join.CanSpill {
					builds[join.JoinMapTag] = append(builds[join.JoinMapTag], join)
				}
			}
			return nil
		})
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
	for group, joins := range probes {
		// Preserve topology retained from an earlier compiled attempt.
		if len(joins[0].SpillExpectedScopes) > 0 {
			continue
		}
		if len(builds[group]) == 0 {
			continue
		}
		ids := make([]uint32, len(joins))
		for i := range ids {
			ids[i] = uint32(i + 1)
		}
		for i, join := range joins {
			join.SpillScopeID = ids[i]
			join.SpillExpectedScopes = ids
		}
		for _, build := range builds[group] {
			build.SpillExpectedScopes = ids
		}
	}
}

// endSpillEpochs is an attempt cleanup supplement for scopes which failed before
// Prepare. Successful remote participants leave shared groups to their owners.
func (c *Compile) endSpillEpochs(failed bool) {
	if !failed {
		return
	}
	var walk func(*Scope)
	walk = func(scope *Scope) {
		vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			switch join := op.(type) {
			case *hashjoin.HashJoin:
				if join.SpillGroup != nil {
					join.SpillGroup.Abort(join.SpillEpoch)
				}
			case *hashbuild.HashBuild:
				if join.SpillGroup != nil {
					join.SpillGroup.Abort(join.SpillEpoch)
				}
			}
			return nil
		})
		for _, child := range scope.PreScopes {
			walk(child)
		}
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
}

func disableUncontrolledBroadcastSpill(scope *Scope) {
	vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
		switch join := op.(type) {
		case *hashjoin.HashJoin:
			if !join.IsShuffle {
				join.SpillExpectedScopes = nil
				join.SpillGroup = nil
				join.SpillEpoch = 0
			}
		case *hashbuild.HashBuild:
			if !join.IsShuffle {
				join.SpillExpectedScopes = nil
				join.SpillGroup = nil
				join.SpillEpoch = 0
			}
		}
		return nil
	})
	for _, child := range scope.PreScopes {
		disableUncontrolledBroadcastSpill(child)
	}
}
