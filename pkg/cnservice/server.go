// Copyright 2021 - 2022 Matrix Origin
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

package cnservice

import (
	"sync"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
)

func NewService(cfg *Config) (Service, error) {
	srv := &service{cfg: cfg}
	srv.logger = logutil.Adjust(srv.logger)
	srv.pool = &sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}
	server, err := morpc.NewRPCServer("cn-server", cfg.ListenAddress,
		morpc.NewMessageCodec(srv.acquireMessage, 16<<20),
		morpc.WithServerGoettyOptions(goetty.WithSessionRWBUfferSize(1<<20, 1<<20)))
	if err != nil {
		return nil, err
	}
	server.RegisterRequestHandler(srv.handleRequest)
	srv.server = server
	return srv, nil
}

func (s *service) Start() error {
	return s.server.Start()
}

func (s *service) Close() error {
	return s.server.Close()
}

func (s *service) acquireMessage() morpc.Message {
	return s.pool.Get().(*pipeline.Message)
}

/*
func (s *service) releaseMessage(msg *pipeline.Message) {
	msg.Reset()
	s.pool.Put(msg)
}
*/

func (s *service) handleRequest(req morpc.Message, _ uint64, cs morpc.ClientSession) error {
	return nil
}
