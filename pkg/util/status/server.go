// Copyright 2021 -2023 Matrix Origin
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

package status

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

const JsonIdent = "    "

type CNInstance struct {
	TxnClient     client.TxnClient
	LockService   lockservice.LockService
	logtailClient *disttae.PushClient
}

type Server struct {
	mu struct {
		sync.Mutex
		LogtailServer  *service.LogtailServer
		HAKeeperClient logservice.ClusterHAKeeperClient
		CNInstances    map[string]*CNInstance
	}
}

func NewServer() *Server {
	s := &Server{}
	s.mu.CNInstances = make(map[string]*CNInstance)
	return s
}

func (s *Server) SetLogtailServer(logtailServer *service.LogtailServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.LogtailServer = logtailServer
}

func (s *Server) SetHAKeeperClient(c logservice.ClusterHAKeeperClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.HAKeeperClient == nil {
		s.mu.HAKeeperClient = c
	}
}

func (s *Server) SetTxnClient(uuid string, c client.TxnClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.CNInstances[uuid]
	if !ok {
		s.mu.CNInstances[uuid] = &CNInstance{}
	}
	s.mu.CNInstances[uuid].TxnClient = c
}

func (s *Server) SetLockService(uuid string, l lockservice.LockService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.CNInstances[uuid]
	if !ok {
		s.mu.CNInstances[uuid] = &CNInstance{}
	}
	s.mu.CNInstances[uuid].LockService = l
}

func (s *Server) SetLogTailClient(uuid string, c *disttae.PushClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.CNInstances[uuid]
	if !ok {
		s.mu.CNInstances[uuid] = &CNInstance{}
	}
	s.mu.CNInstances[uuid].logtailClient = c
}

func (s *Server) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	data, err := s.Dump()
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(data)
}

func (s *Server) Dump() ([]byte, error) {
	var status Status
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.LogtailServer != nil {
		status.LogtailServerStatus.fill(s.mu.LogtailServer)
	}
	if s.mu.HAKeeperClient != nil {
		status.HAKeeperStatus.fill(s.mu.HAKeeperClient)
	}
	status.fillCNStatus(s.mu.CNInstances)
	return json.MarshalIndent(status, "", JsonIdent)
}
