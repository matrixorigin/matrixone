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

func NewServer() *Server {
	if srv != nil {
		return srv
	}
	srv = &Server{
		mp:        make(map[uint64]*process.WaitRegister),
		chanBufMp: new(sync.Map),
	}
	return srv
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
	srv.Lock()
	defer srv.Unlock()
	defer func() { delete(srv.uuidMap, u) }()
	r, ok := srv.uuidMap[u]
	if !ok {
		return nil, false
	}
	return r, true
}

func (srv *Server) PutRegFromUuidMap(u uuid.UUID, reg *process.WaitRegister) error {
	srv.Lock()
	defer srv.Unlock()
	if r, ok := srv.uuidMap[u]; ok {
		if reg != r {
			return moerr.NewInternalErrorNoCtx("PutRegFromUuidMap error! the uuid has exsit with different reg!")
		}
		return nil
	}
	srv.uuidMap[u] = reg
	return nil
}

func (srv *Server) UpdateRegFromUuidMap(u uuid.UUID, reg *process.WaitRegister) {
	srv.Lock()
	defer srv.Unlock()
	srv.uuidMap[u] = reg
}

func (srv *Server) HandleRequest(ctx context.Context, req morpc.Message, _ uint64, cs morpc.ClientSession) error {
	return nil
}
