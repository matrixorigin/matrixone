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
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// FIXME: shit design
var srv atomic.Pointer[Server]

const (
	TxnWorkSpaceIdType = 1
	CnBlockIdType      = 2
)

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
		uuidCsChanMap: UuidProcMap{mp: make(map[uuid.UUID]uuidProcMapItem, 1024)},
		cnSegmentMap:  CnSegmentMap{mp: make(map[objectio.Segmentid]int32, 1024)},
	}
	Set(s)
	return s
}

// GetProcByUuid used the uuid to get a process from the srv.
// if the process is nil, it means the process has done.
// if forcedDelete, do an action to avoid another routine to put a new item.
func (srv *Server) GetProcByUuid(u uuid.UUID, forcedDelete bool) (*process.Process, bool) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	p, ok := srv.uuidCsChanMap.mp[u]
	if !ok {
		if forcedDelete {
			srv.uuidCsChanMap.mp[u] = uuidProcMapItem{proc: nil}
		}
		return nil, false
	}

	result := p.proc
	if p.proc == nil {
		delete(srv.uuidCsChanMap.mp, u)
	} else {
		p.proc = nil
		srv.uuidCsChanMap.mp[u] = p
	}
	return result, true
}

func (srv *Server) PutProcIntoUuidMap(u uuid.UUID, p *process.Process) error {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	if _, ok := srv.uuidCsChanMap.mp[u]; ok {
		delete(srv.uuidCsChanMap.mp, u)
		return moerr.NewInternalErrorNoCtx("remote receiver already done")
	}

	srv.uuidCsChanMap.mp[u] = uuidProcMapItem{proc: p}
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
			srv.uuidCsChanMap.mp[uuids[i]] = p
		}
	}
}

func (srv *Server) PutCnSegment(sid *objectio.Segmentid, segmentType int32) {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	srv.cnSegmentMap.mp[*sid] = segmentType
}

func (srv *Server) DeleteTxnSegmentIds(sids []objectio.Segmentid) {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	for _, segmentName := range sids {
		delete(srv.cnSegmentMap.mp, segmentName)
	}
}

func (srv *Server) GetCnSegmentMap() map[string]int32 {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	new_mp := make(map[string]int32)
	for k, v := range srv.cnSegmentMap.mp {
		new_mp[string(k[:])] = v
	}
	return new_mp
}

func (srv *Server) GetCnSegmentType(sid *objectio.Segmentid) int32 {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	return srv.cnSegmentMap.mp[*sid]
}

// GenerateObject used to generate a new object name for CN
func (srv *Server) GenerateObject() objectio.ObjectName {
	segId := objectio.NewSegmentid()
	return objectio.BuildObjectName(segId, 0)
}
