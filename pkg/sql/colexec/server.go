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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var Srv *Server

const (
	TxnWorkSpaceIdType = 1
	CnBlockIdType      = 2
)

func NewServer(client logservice.CNHAKeeperClient) *Server {
	if Srv != nil {
		return Srv
	}
	Srv = &Server{
		mp:            make(map[uint64]*process.WaitRegister),
		hakeeper:      client,
		uuidCsChanMap: UuidProcMap{mp: make(map[uuid.UUID]*process.Process)},
		cnSegmentMap:  CnSegmentMap{mp: make(map[string]int32)},
	}
	return Srv
}

func (srv *Server) GetConnector(id uint64) *process.WaitRegister {
	srv.Lock()
	defer srv.Unlock()
	defer func() { delete(srv.mp, id) }()
	return srv.mp[id]
}

func (srv *Server) RegistConnector(reg *process.WaitRegister) uint64 {
	srv.Lock()
	defer srv.Unlock()
	srv.mp[srv.id] = reg
	defer func() { srv.id++ }()
	return srv.id
}

func (srv *Server) GetNotifyChByUuid(u uuid.UUID) (*process.Process, bool) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	p, ok := srv.uuidCsChanMap.mp[u]
	if !ok {
		return nil, false
	}
	return p, true
}

func (srv *Server) PutNotifyChIntoUuidMap(u uuid.UUID, p *process.Process) error {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	srv.uuidCsChanMap.mp[u] = p
	return nil
}

func (srv *Server) PutCnSegment(segmentName string, segmentType int32) {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	srv.cnSegmentMap.mp[segmentName] = segmentType
}

func (srv *Server) DeleteTxnSegmentIds(segmentNames []string) {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	for _, segmentName := range segmentNames {
		delete(srv.cnSegmentMap.mp, segmentName)
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

func (srv *Server) GetCnSegmentType(segmentName string) int32 {
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	return srv.cnSegmentMap.mp[segmentName]
}

// SegmentId is part of Id for cn2s3 directly, for more info, refer to docs about it
func (srv *Server) GenerateSegment() objectio.ObjectName {
	srv.Lock()
	defer srv.Unlock()
	return objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	// for future fileOffset
	// if srv.InitSegmentId {
	// 	srv.incrementSegmentId()
	// } else {
	// 	srv.getNewSegmentId()
	// 	srv.currentFileOffset = 0
	// 	srv.InitSegmentId = true
	// }
	// return objectio.BuildObjectName(srv.CNSegmentId, srv.currentFileOffset)
}

// func (srv *Server) incrementSegmentId() {
// 	if srv.currentFileOffset < math.MaxUint16 {
// 		srv.currentFileOffset++
// 	} else {
// 		srv.getNewSegmentId()
// 		srv.currentFileOffset = 0
// 	}
// }

// // for now, rowId is common between CN and DN.
// func (srv *Server) getNewSegmentId() {
// 	srv.CNSegmentId = common.NewSegmentid()
// }
