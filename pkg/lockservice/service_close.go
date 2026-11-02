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

package lockservice

import (
	"context"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

// notifyAllocatorOfClose is called only after all local lock mutations,
// physical tables and transaction ownership have been destroyed. Unlike an
// in-progress drain, this instance cannot admit or resume any work.
func (s *service) notifyAllocatorOfClose(ctx context.Context) error {
	requestAllocator := s.allocatorStateSnapshot()
	attempt := "close:" + s.serviceID
	beginReq := acquireRequest()
	defer releaseRequest(beginReq)
	beginReq.Method = pb.Method_BeginDrain
	beginReq.BeginDrain = pb.BeginDrainRequest{ServiceID: s.serviceID, AttemptID: attempt}
	beginResp, err := s.remote.client.Send(ctx, beginReq)
	if err != nil {
		return err
	}
	defer releaseResponse(beginResp)
	begin := beginResp.BeginDrain
	if begin.ServiceID != s.serviceID || begin.AttemptID != attempt ||
		begin.AllocatorID == "" || begin.AllocatorVersion == 0 {
		return ErrLockTableBindChanged
	}
	epoch := allocatorState{id: begin.AllocatorID, version: begin.AllocatorVersion}
	if _, accepted := s.observeAllocatorStateWithHoldersFromSnapshot(
		"closed-service", epoch, requestAllocator, true, s.tableGroups); !accepted {
		return ErrLockTableBindChanged
	}
	// A missing allocator bind creates a non-admitting pending drain. Its
	// Begin response is not yet OK: the epoch-echoing heartbeat below is the
	// required handshake, including for an instance already completely closed.
	terminalReq := acquireRequest()
	defer releaseRequest(terminalReq)
	terminalReq.Method = pb.Method_KeepLockTableBind
	terminalReq.KeepLockTableBind = pb.KeepLockTableBindRequest{
		ServiceID: s.serviceID, Status: pb.Status_ServiceUnLockSucc,
		ObservedAllocatorID: epoch.id, ObservedAllocatorVersion: epoch.version,
	}
	terminalResp, err := s.remote.client.Send(ctx, terminalReq)
	if err != nil {
		return err
	}
	defer releaseResponse(terminalResp)
	terminal := terminalResp.KeepLockTableBind
	if !terminal.OK || terminal.Status != pb.Status_ServiceCanRestart ||
		terminal.AllocatorID != epoch.id || terminal.AllocatorVersion != epoch.version {
		return ErrLockTableBindChanged
	}
	return nil
}
