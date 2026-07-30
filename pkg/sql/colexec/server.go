// Copyright 2021 Matrix Origin
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

package colexec

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// NewServer creates and registers a colexec server owned by serviceID.
// Different CN services in the same process must always use different servers.
func NewServer(serviceID string) *Server {
	s := &Server{
		uuidCsChanMap: UuidProcMap{
			mp:      make(map[uuid.UUID]uuidProcMapItem, 1024),
			waiters: make(map[uuid.UUID]*remoteReceiverWaitState, 1024),
		},
		cnSegmentMap: CnSegmentMap{mp: make(map[string]int32, 1024)},
		receivedRunningPipeline: RunningPipelineMapForRemoteNode{
			fromRpcClientToRelatedPipeline: make(map[rpcClientItem]runningPipelineInfo, 1024),
			sessionCleanupWaiters:          make(map[morpc.ClientSession]struct{}, 128),
		},
	}
	rt := runtime.ServiceRuntime(serviceID)
	if rt == nil {
		panic("service runtime is not initialized for colexec server")
	}
	rt.SetGlobalVariables(runtime.ColexecServer, s)
	return s
}

// GetServer returns the colexec server owned by serviceID.
func GetServer(serviceID string) *Server {
	rt := runtime.ServiceRuntime(serviceID)
	if rt == nil {
		return nil
	}
	v, ok := rt.GetGlobalVariables(runtime.ColexecServer)
	if !ok {
		return nil
	}
	s, _ := v.(*Server)
	return s
}

// MustGetServer returns the colexec server owned by serviceID and panics when
// the CN has not initialized its execution service. Missing this component is
// a programming error and must not silently fall back to another CN.
func MustGetServer(serviceID string) *Server {
	s := GetServer(serviceID)
	if s == nil {
		panic(fmt.Sprintf("colexec server is not initialized for CN %s", serviceID))
	}
	return s
}

// GetProcByUuid used the uuid to get a process from the srv.
// if the process is nil, it means the process has done.
// if forcedDelete, do an action to avoid another routine to put a new item.
func (srv *Server) GetProcByUuid(u uuid.UUID, forcedDelete bool) (*process.Process, process.RemotePipelineInformationChannel, bool) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	return srv.getProcByUuidLocked(u, forcedDelete)
}

func (srv *Server) getProcByUuidLocked(u uuid.UUID, forcedDelete bool) (*process.Process, process.RemotePipelineInformationChannel, bool) {
	p, ok := srv.uuidCsChanMap.mp[u]
	if !ok {
		if forcedDelete {
			srv.uuidCsChanMap.mp[u] = uuidProcMapItem{
				state: remoteReceiverTombstone,
			}
		}
		return nil, nil, false
	}

	if p.state != remoteReceiverReady {
		if !srv.retainClosedReceiverForWaitersLocked(u, p) {
			delete(srv.uuidCsChanMap.mp, u)
		}
		return nil, nil, true
	}
	resultProc := p.proc
	resultCh := p.ch
	p.proc = nil
	p.ch = nil
	p.state = remoteReceiverAttached
	srv.uuidCsChanMap.mp[u] = p
	return resultProc, resultCh, true
}

// AttachProcByUuidOrWait atomically attaches one notify stream to a ready
// receiver. Missing receivers return a generation-scoped wait handle.
// Attached and closed receivers are terminal states and are not consumed or
// deleted by a duplicate lookup, preserving the original owner's cleanup
// generation.
func (srv *Server) AttachProcByUuidOrWait(
	u uuid.UUID,
) (
	*process.Process,
	process.RemotePipelineInformationChannel,
	RemoteReceiverAttachState,
	*RemoteReceiverWaiter,
) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()

	item, ok := srv.uuidCsChanMap.mp[u]
	if !ok {
		waiter := srv.uuidCsChanMap.waiters[u]
		if waiter == nil {
			waiter = &remoteReceiverWaitState{changed: make(chan struct{})}
			srv.uuidCsChanMap.waiters[u] = waiter
		}
		waiter.refs++
		return nil, nil, RemoteReceiverMissing, &RemoteReceiverWaiter{
			server: srv,
			uid:    u,
			state:  waiter,
		}
	}
	switch item.state {
	case remoteReceiverReady:
		proc := item.proc
		ch := item.ch
		item.proc = nil
		item.ch = nil
		item.state = remoteReceiverAttached
		srv.uuidCsChanMap.mp[u] = item
		return proc, ch, RemoteReceiverAttachedNow, nil
	case remoteReceiverAttached:
		return nil, nil, RemoteReceiverAlreadyAttached, nil
	case remoteReceiverClosed, remoteReceiverTombstone:
		if !srv.retainClosedReceiverForWaitersLocked(u, item) {
			delete(srv.uuidCsChanMap.mp, u)
		}
		return nil, nil, RemoteReceiverAlreadyClosed, nil
	default:
		panic("unknown remote receiver registry state")
	}
}

// Done returns the publication signal for this exact receiver wait generation.
func (w *RemoteReceiverWaiter) Done() <-chan struct{} {
	if w == nil || w.state == nil {
		return nil
	}
	return w.state.changed
}

// Close releases this wait reference. Pointer identity prevents a canceled old
// waiter from deleting a later wait generation for the same UUID.
func (w *RemoteReceiverWaiter) Close() {
	if w == nil || w.server == nil || w.state == nil {
		return
	}
	w.once.Do(func() {
		w.server.uuidCsChanMap.Lock()
		defer w.server.uuidCsChanMap.Unlock()
		state := w.server.uuidCsChanMap.waiters[w.uid]
		if state == nil || state != w.state {
			return
		}
		state.refs--
		if state.refs <= 0 {
			delete(w.server.uuidCsChanMap.waiters, w.uid)
			if item, ok := w.server.uuidCsChanMap.mp[w.uid]; ok &&
				item.state == remoteReceiverClosed &&
				item.ownerCh == state.ownerCh {
				delete(w.server.uuidCsChanMap.mp, w.uid)
			}
		}
	})
}

func (srv *Server) PutProcIntoUuidMap(u uuid.UUID, p *process.Process, ch process.RemotePipelineInformationChannel) error {
	srv.uuidCsChanMap.Lock()
	if item, ok := srv.uuidCsChanMap.mp[u]; ok {
		oldState := "ready"
		switch item.state {
		case remoteReceiverAttached:
			oldState = "attached"
		case remoteReceiverClosed:
			oldState = "closed"
		case remoteReceiverTombstone:
			oldState = "tombstone"
			delete(srv.uuidCsChanMap.mp, u)
		}
		srv.uuidCsChanMap.Unlock()
		return moerr.NewInternalErrorNoCtxf(
			"remote receiver %s already done (existing registry state: %s)", u.String(), oldState)
	}
	if p == nil || ch == nil {
		srv.uuidCsChanMap.Unlock()
		return moerr.NewInvalidStateNoCtxf(
			"remote receiver %s requires a non-nil process and notification channel",
			u.String(),
		)
	}

	srv.uuidCsChanMap.mp[u] = uuidProcMapItem{proc: p, ch: ch, ownerCh: ch}
	waiter := srv.uuidCsChanMap.waiters[u]
	if waiter != nil {
		waiter.ownerCh = ch
	}
	srv.uuidCsChanMap.Unlock()
	if waiter != nil {
		close(waiter.changed)
	}
	return nil
}

func (srv *Server) DeleteUuids(uuids []uuid.UUID) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	for i := range uuids {
		p, ok := srv.uuidCsChanMap.mp[uuids[i]]
		if !ok {
			continue
		}

		if p.state != remoteReceiverReady &&
			!srv.retainClosedReceiverForWaitersLocked(uuids[i], p) {
			delete(srv.uuidCsChanMap.mp, uuids[i])
		} else {
			p.proc = nil
			p.ch = nil
			p.state = remoteReceiverClosed
			srv.uuidCsChanMap.mp[uuids[i]] = p
		}
	}
}

// RemoveUuidsOwned performs final cleanup for one exact registration. The
// owner check prevents a delayed rollback from deleting a later registration
// that happens to use the same UUID.
func (srv *Server) RemoveUuidsOwned(
	uuids []uuid.UUID,
	ownerCh process.RemotePipelineInformationChannel,
) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	for i := range uuids {
		item, ok := srv.uuidCsChanMap.mp[uuids[i]]
		if ok && item.ownerCh == ownerCh {
			if srv.retainClosedReceiverForWaitersLocked(uuids[i], item) {
				item.proc = nil
				item.ch = nil
				item.state = remoteReceiverClosed
				srv.uuidCsChanMap.mp[uuids[i]] = item
			} else {
				delete(srv.uuidCsChanMap.mp, uuids[i])
			}
		}
	}
}

func (srv *Server) retainClosedReceiverForWaitersLocked(
	uid uuid.UUID,
	item uuidProcMapItem,
) bool {
	waiter := srv.uuidCsChanMap.waiters[uid]
	return waiter != nil &&
		waiter.refs > 0 &&
		waiter.ownerCh == item.ownerCh
}

func (srv *Server) PutCnSegment(txnID []byte, tableId uint64, sid *objectio.Segmentid, segmentType int32) {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	srv.cnSegmentMap.mp[segmentKey(txnID, tableId, sid)] = segmentType
}

func (srv *Server) DeleteTxnSegmentIds(txnID []byte) {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	for key := range srv.cnSegmentMap.mp {
		if segmentKeyHasTxn(key, txnID) {
			delete(srv.cnSegmentMap.mp, key)
		}
	}
}

func (srv *Server) GetCnSegmentMap() map[string]int32 {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	new_mp := make(map[string]int32)
	for k, v := range srv.cnSegmentMap.mp {
		new_mp[k] = v
	}
	return new_mp
}

func (srv *Server) GetCnSegmentType(sid *objectio.Segmentid, tableId uint64, txnID []byte) int32 {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	return srv.cnSegmentMap.mp[segmentKey(txnID, tableId, sid)]
}

// GenerateObject used to generate a new object name for CN
func (srv *Server) GenerateObject() objectio.ObjectName {
	segId := objectio.NewSegmentid()
	return objectio.BuildObjectName(segId, 0)
}
