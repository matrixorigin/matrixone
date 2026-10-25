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
	"context"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// FIXME: shit design
var srv atomic.Pointer[Server]

func Get() *Server {
	return srv.Load()
}

func Set(s *Server) {
	srv.Store(s)
}

func NewServer(client logservice.CNHAKeeperClient) *Server {
	s := Get()
	if s != nil {
		return s
	}
	s = &Server{
		hakeeper:      client,
		uuidCsChanMap: UuidProcMap{mp: make(map[uuid.UUID]uuidProcMapItem, 1024), changed: make(chan struct{})},
		cnSegmentMap:  CnSegmentMap{mp: make(map[string]int32, 1024)},
		receivedRunningPipeline: RunningPipelineMapForRemoteNode{
			fromRpcClientToRelatedPipeline: make(map[rpcClientItem]runningPipelineInfo, 1024),
			sessionCleanupWaiters:          make(map[morpc.ClientSession]struct{}, 128),
		},
	}
	if !srv.CompareAndSwap(nil, s) {
		return srv.Load()
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
			srv.uuidCsChanMap.mp[u] = uuidProcMapItem{proc: nil, ch: nil}
		}
		return nil, nil, false
	}

	result1 := p.proc
	result2 := p.ch
	if p.proc == nil {
		delete(srv.uuidCsChanMap.mp, u)
	} else {
		p.proc = nil
		p.ch = nil
		srv.uuidCsChanMap.mp[u] = p
	}
	return result1, result2, true
}

// AttachRemoteReceiver atomically claims a registration and publishes its receiver.
// DeleteUuids uses the same lock: after it returns, no notifier can enqueue into
// the retired dispatch. An unclaimed completion remains available to a late lookup.
// The queue has one slot per registered UUID, so publication must never block.
func (srv *Server) AttachRemoteReceiver(u uuid.UUID, receiver *process.WrapCs) (context.CancelCauseFunc, bool, <-chan struct{}, error) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	entry, ok := srv.uuidCsChanMap.mp[u]
	if !ok {
		return nil, false, srv.uuidCsChanMap.changed, nil
	}
	if entry.proc == nil {
		delete(srv.uuidCsChanMap.mp, u)
		return nil, true, nil, nil
	}
	cancel := entry.proc.Cancel
	select {
	case entry.ch <- receiver:
		srv.uuidCsChanMap.mp[u] = uuidProcMapItem{}
		return cancel, true, nil, nil
	default:
		return nil, true, nil, moerr.NewInternalErrorNoCtx("remote receiver notification queue is full")
	}
}

func (srv *Server) PutProcIntoUuidMap(u uuid.UUID, p *process.Process, ch process.RemotePipelineInformationChannel) error {
	srv.uuidCsChanMap.Lock()
	if _, ok := srv.uuidCsChanMap.mp[u]; ok {
		delete(srv.uuidCsChanMap.mp, u)
		srv.uuidCsChanMap.Unlock()
		return moerr.NewInternalErrorNoCtx("remote receiver already done")
	}

	srv.uuidCsChanMap.mp[u] = uuidProcMapItem{proc: p, ch: ch}
	old := srv.uuidCsChanMap.changed
	srv.uuidCsChanMap.changed = make(chan struct{})
	srv.uuidCsChanMap.Unlock()
	close(old)
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

		if p.proc == nil {
			delete(srv.uuidCsChanMap.mp, uuids[i])
		} else {
			p.proc = nil
			p.ch = nil
			srv.uuidCsChanMap.mp[uuids[i]] = p
		}
	}
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
