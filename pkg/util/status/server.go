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

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

const JsonIdent = "    "

type CNInstance struct {
	UUID      string
	TxnClient client.TxnClient
}

type Server struct {
	mu struct {
		sync.Mutex
		LogtailServer  *service.LogtailServer
		HAKeeperClient logservice.ClusterHAKeeperClient
		CNInstances    []CNInstance
	}
}

func NewServer() *Server {
	return &Server{}
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
	s.mu.CNInstances = append(s.mu.CNInstances, CNInstance{
		UUID:      uuid,
		TxnClient: c,
	})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	var status Status
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.LogtailServer != nil {
		status.fillLogtail(s.mu.LogtailServer)
	}
	if s.mu.HAKeeperClient != nil {
		status.fillHAKeeper(s.mu.HAKeeperClient)
	}
	status.fillTxnClient(s.mu.CNInstances)
	data, err := json.MarshalIndent(status, "", JsonIdent)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(data)
}
