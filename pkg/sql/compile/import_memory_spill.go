// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// beginSpillEpochs prepares the complete per-CN scope identities before remote
// encoding. The query's MessageBoard owns the execution epoch; individual RPCs
// must never reset a group used by another RPC. Prepare binds the local board.
func (c *Compile) beginSpillEpochs() {
	if c.importMemory == nil && len(c.importMemoryControls) == 0 {
		for _, scope := range c.scopes {
			disableUncontrolledBroadcastSpill(scope)
		}
		return
	}
	type key struct {
		addr string
		tag  int32
	}
	locations := c.memoryExecutionLocations()
	probes := make(map[key][]*hashjoin.HashJoin)
	builds := make(map[key][]*hashbuild.HashBuild)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			addr := locations[scope]
			switch join := op.(type) {
			case *hashjoin.HashJoin:
				if !join.IsShuffle {
					probes[key{addr, join.JoinMapTag}] = append(probes[key{addr, join.JoinMapTag}], join)
				}
			case *hashbuild.HashBuild:
				if !join.IsShuffle && join.CanSpill {
					builds[key{addr, join.JoinMapTag}] = append(builds[key{addr, join.JoinMapTag}], join)
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
		// Deserialized topology is authoritative and includes other RPC scopes which
		// are absent from this Compile. Missing topology on old wire stays disabled.
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
