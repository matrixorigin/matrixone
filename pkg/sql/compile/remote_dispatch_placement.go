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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
)

type remoteDispatchPlacementVisit struct {
	scope      *Scope
	callerAddr string
	forceMerge bool
}

// normalizeRemoteDispatchReceiverAddresses fixes receiver routes in a decoded
// remote scope before any pipeline goroutine starts. RemoteRun keeps a root
// Dispatch on the caller and sends only its child to the target CN, so the
// compile-time FromAddr can be stale after an outer scope moves to another CN.
func normalizeRemoteDispatchReceiverAddresses(root *Scope, localAddr string) {
	if root == nil {
		return
	}

	retainedUUIDs := make(map[uuid.UUID]struct{})
	visited := make(map[remoteDispatchPlacementVisit]struct{})
	collectRetainedRemoteDispatchOwners(root, localAddr, true, retainedUUIDs, visited)

	visitedScopes := make(map[*Scope]struct{})
	var updateReceivers func(*Scope)
	updateReceivers = func(s *Scope) {
		if s == nil {
			return
		}
		if _, ok := visitedScopes[s]; ok {
			return
		}
		visitedScopes[s] = struct{}{}
		for i := range s.RemoteReceivRegInfos {
			info := &s.RemoteReceivRegInfos[i]
			if _, ok := retainedUUIDs[info.Uuid]; ok {
				info.FromAddr = localAddr
			}
		}
		for _, pre := range s.PreScopes {
			updateReceivers(pre)
		}
	}
	updateReceivers(root)
}

// collectRetainedRemoteDispatchOwners follows only operators that RemoteRun
// will execute on callerAddr. A standalone remote scope retains its root
// Dispatch while sending the child tree away, which can make the compile-time
// FromAddr stale. Non-standalone scopes are rejected before execution, and
// ordinary remotely executed operator trees are skipped.
func collectRetainedRemoteDispatchOwners(
	s *Scope,
	callerAddr string,
	forceMerge bool,
	retainedUUIDs map[uuid.UUID]struct{},
	visited map[remoteDispatchPlacementVisit]struct{},
) {
	if s == nil {
		return
	}
	visit := remoteDispatchPlacementVisit{scope: s, callerAddr: callerAddr, forceMerge: forceMerge}
	if _, ok := visited[visit]; ok {
		return
	}
	visited[visit] = struct{}{}

	if forceMerge {
		collectRetainedRemoteDispatchOwnersInPreScopes(s.PreScopes, callerAddr, retainedUUIDs, visited)
		return
	}

	switch s.Magic {
	case Merge, MergeInsert:
		collectRetainedRemoteDispatchOwnersInPreScopes(s.PreScopes, callerAddr, retainedUUIDs, visited)

	case Remote:
		if validateRemoteRunAddress(s.NodeInfo.Addr, callerAddr) != nil {
			return
		}
		if s.ipAddrMatch(callerAddr) {
			collectRetainedRemoteDispatchOwnersInPreScopes(s.PreScopes, callerAddr, retainedUUIDs, visited)
			return
		}
		if s.holdAnyCannotRemoteOperator() != nil {
			return
		}
		if !checkPipelineStandaloneExecutableAtRemote(s) {
			return
		}

		dispatchOp, ok := s.RootOp.(*dispatch.Dispatch)
		if !ok {
			return
		}
		for i := range dispatchOp.RemoteRegs {
			retainedUUIDs[dispatchOp.RemoteRegs[i].Uuid] = struct{}{}
		}
	}
}

func collectRetainedRemoteDispatchOwnersInPreScopes(
	preScopes []*Scope,
	callerAddr string,
	retainedUUIDs map[uuid.UUID]struct{},
	visited map[remoteDispatchPlacementVisit]struct{},
) {
	for _, pre := range preScopes {
		collectRetainedRemoteDispatchOwners(pre, callerAddr, false, retainedUUIDs, visited)
	}
}
