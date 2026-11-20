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
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func workspaceHandoffParticipants(scopes []*Scope, localAddr string) (bool, []string, error) {
	var writer, missing bool
	addresses := make(map[string]struct{})
	var visit func(*Scope) error
	visit = func(s *Scope) error {
		if s == nil {
			return nil
		}
		if s.Magic == Remote {
			if s.NodeInfo.Addr == "" {
				missing = true
			} else if s.NodeInfo.Addr != localAddr {
				addresses[s.NodeInfo.Addr] = struct{}{}
			}
		}
		if s.RootOp != nil {
			if err := vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
				if producer, ok := op.(remoteWorkspaceObjectProducer); ok && producer.ProducesRemoteWorkspaceObjects() {
					writer = true
				}
				if d, ok := op.(*dispatch.Dispatch); ok {
					for _, receiver := range d.RemoteRegs {
						if receiver.NodeAddr == "" {
							missing = true
						} else if receiver.NodeAddr != localAddr {
							addresses[receiver.NodeAddr] = struct{}{}
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		for _, child := range s.PreScopes {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, scope := range scopes {
		if err := visit(scope); err != nil {
			return false, nil, err
		}
	}
	if !writer {
		return false, nil, nil
	}
	if missing {
		return true, nil, moerr.NewInvalidStateNoCtx("remote workspace handoff participant has no address")
	}
	result := make([]string, 0, len(addresses))
	for address := range addresses {
		result = append(result, address)
	}
	slices.Sort(result)
	return true, result, nil
}

func (c *Compile) preflightWorkspaceHandoff(ctx context.Context) error {
	_, addresses, err := workspaceHandoffParticipants(c.scopes, c.addr)
	if err != nil {
		return err
	}
	for _, address := range addresses {
		if err := colexec.ProbeRemoteWorkspaceHandoff(ctx, c.proc, address); err != nil {
			return err
		}
	}
	return nil
}

type remoteWorkspaceObjectProducer interface {
	ProducesRemoteWorkspaceObjects() bool
}

func remoteScopeMayPersistWorkspaceObjects(scope *Scope, executionAddress string) (bool, error) {
	if scope == nil {
		return false, nil
	}
	var found bool
	if scope.RootOp != nil {
		if err := vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if producer, ok := op.(remoteWorkspaceObjectProducer); ok && producer.ProducesRemoteWorkspaceObjects() {
				found = true
			}
			return nil
		}); err != nil {
			return false, err
		}
	}
	for _, child := range scope.PreScopes {
		if child != nil && child.Magic == Remote && !child.ipAddrMatch(executionAddress) {
			// This child is sent as a separate RPC and negotiates its own scope.
			continue
		}
		childFound, err := remoteScopeMayPersistWorkspaceObjects(child, executionAddress)
		if err != nil {
			return false, err
		}
		found = found || childFound
	}
	return found, nil
}
