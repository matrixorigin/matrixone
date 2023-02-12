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
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var Srv *Server
var CnAddr string

func NewServer(client logservice.CNHAKeeperClient) *Server {
	if Srv != nil {
		return Srv
	}
	Srv = &Server{
		mp:       make(map[uint64]*process.WaitRegister),
		hakeeper: client,

		uuidCsChanMap: UuidCsChanMap{mp: make(map[uuid.UUID]chan process.WrapCs)},
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

func (srv *Server) GetNotifyChByUuid(u uuid.UUID) (chan process.WrapCs, bool) {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	p, ok := srv.uuidCsChanMap.mp[u]
	if !ok {
		return nil, false
	}
	return p, true
}

func (srv *Server) PutNotifyChIntoUuidMap(u uuid.UUID, ch chan process.WrapCs) error {
	srv.uuidCsChanMap.Lock()
	defer srv.uuidCsChanMap.Unlock()
	srv.uuidCsChanMap.mp[u] = ch
	return nil
}

func (srv *Server) HandleRequest(ctx context.Context, req morpc.Message, _ uint64, cs morpc.ClientSession) error {
	return nil
}

// SegmentId is part of Id for cn2s3 directly, for more info, refer to docs about it
func (srv *Server) GenerateSegment() (string, error) {
	srv.Lock()
	defer srv.Unlock()
	if srv.InitSegmentId {
		if err := srv.incrementSegmentId(); err != nil {
			return "", err
		}
	} else {
		if err := srv.getNewSegmentId(); err != nil {
			return "", err
		}
		srv.InitSegmentId = true
	}
	return fmt.Sprintf("%x.seg", (srv.CNSegmentId)[:]), nil
}

func (srv *Server) incrementSegmentId() error {
	// increment SegmentId
	b := binary.BigEndian.Uint32(srv.CNSegmentId[0:4])
	// can't rise up to math.MaxUint32, we need to distinct the memory
	// data in disttae, the batch's rowId's prefix is MaxUint64
	if b < math.MaxUint32-1 {
		b++
		binary.BigEndian.PutUint32(srv.CNSegmentId[0:4], b)
	} else {
		if err := srv.getNewSegmentId(); err != nil {
			return err
		}
	}
	return nil
}

// getNewSegmentId returns Id given from hakeeper
func (srv *Server) getNewSegmentId() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	Id, err := srv.hakeeper.AllocateID(ctx)
	if err != nil {
		return err
	}
	srv.CNSegmentId[0] = 0x80
	for i := 1; i < 4; i++ {
		srv.CNSegmentId[i] = 0
	}
	binary.BigEndian.PutUint64(srv.CNSegmentId[4:12], Id)
	return nil
}
