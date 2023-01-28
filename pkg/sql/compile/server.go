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

package compile

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var srv *Server
var cnAddr string

func NewServer(addr string) *Server {
	if srv != nil {
		return srv
	}
	srv = &Server{
		idMap:       RelationMap{id: 0, mp: make(map[uint64]*process.WaitRegister)},
		uuidMap:     UuidMap{mp: make(map[uuid.UUID]*process.WaitRegister)},
		batchCntMap: BatchCntMap{mp: make(map[uuid.UUID]uint64)},
		chanBufMp:   new(sync.Map),
	}
	return srv
}

func (srv *Server) GetConnector(id uint64) *process.WaitRegister {
	srv.idMap.Lock()
	defer srv.idMap.Unlock()
	defer func() { delete(srv.idMap.mp, id) }()
	return srv.idMap.mp[id]
}

func (srv *Server) RegistConnector(reg *process.WaitRegister) uint64 {
	srv.idMap.Lock()
	defer srv.idMap.Unlock()
	srv.idMap.mp[srv.idMap.id] = reg
	defer func() { srv.idMap.id++ }()
	return srv.idMap.id
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
