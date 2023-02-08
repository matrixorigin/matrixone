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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

		uuidMap:     UuidMap{mp: make(map[uuid.UUID]*process.WaitRegister)},
		batchCntMap: BatchCntMap{mp: make(map[uuid.UUID]uint64)},
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

func (srv *Server) GetRegFromUuidMap(u uuid.UUID) (*process.WaitRegister, bool) {
	srv.uuidMap.RLock()
	defer srv.uuidMap.RUnlock()
	r, ok := srv.uuidMap.mp[u]
	if !ok {
		return nil, false
	}
	return r, true
}

func (srv *Server) PutRegFromUuidMap(u uuid.UUID, reg *process.WaitRegister) error {
	srv.uuidMap.Lock()
	defer srv.uuidMap.Unlock()
	if r, ok := srv.uuidMap.mp[u]; ok {
		if reg != r {
			return moerr.NewInternalErrorNoCtx("PutRegFromUuidMap error! the uuid has exsit with different reg!")
		}
		return nil
	}
	srv.uuidMap.mp[u] = reg
	return nil
}

func (srv *Server) RemoveUuidFromUuidMap(u uuid.UUID) error {
	srv.uuidMap.Lock()
	defer srv.uuidMap.Unlock()
	delete(srv.uuidMap.mp, u)
	return nil
}

func (srv *Server) ReceiveNormalBatch(u uuid.UUID) {
	srv.batchCntMap.Lock()
	defer srv.batchCntMap.Unlock()
	if _, ok := srv.batchCntMap.mp[u]; !ok {
		srv.batchCntMap.mp[u] = 0
	}
	srv.batchCntMap.mp[u]++
}

func (srv *Server) IsEndStatus(u uuid.UUID, requireCnt uint64) bool {
	srv.batchCntMap.Lock()
	defer srv.batchCntMap.Unlock()
	if cnt, ok := srv.batchCntMap.mp[u]; ok {
		if cnt == requireCnt {
			delete(srv.batchCntMap.mp, u)
			return true
		}
	}
	return false
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
