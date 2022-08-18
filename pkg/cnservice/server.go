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

type Options func(*service)

const (
	MessageEnd = 1

	dfServerReadBufferSize  = 1 << 10
	dfServerWriteBufferSize = 1 << 10
)

func NewService(cfg *Config, opt ...Options) (Service, error) {
	cfg.Fill()
	srv := &service{cfg: cfg}
	srv.logger = logutil.Adjust(srv.logger)
	srv.requestPool = &sync.Pool{New: func() any { return &pipeline.Message{} }}
	srv.responsePool = &sync.Pool{New: func() any { return &pipeline.Message{} }}

	server, err := morpc.NewRPCServer("cn-server", cfg.ListenAddress,
		morpc.NewMessageCodec(srv.acquireMessage, cfg.PayLoadCopyBufferSize),
		morpc.WithServerGoettyOptions(goetty.WithSessionRWBUfferSize(cfg.ReadBufferSize, cfg.WriteBufferSize)))
	if err != nil {
		return nil, err
	}
	server.RegisterRequestHandler(srv.handleRequest)
	srv.server = server
	srv.requestHandler = defaultMessageHandler

	// usr defined.
	for _, op := range opt {
		op(srv)
	}

	return srv, nil
}

func (s *service) Start() error {
	return s.server.Start()
}

func (s *service) Close() error {
	return s.server.Close()
}

func (s *service) acquireMessage() morpc.Message {
	return s.responsePool.Get().(*pipeline.Message)
}

func (s *service) handleRequest(req morpc.Message, _ uint64, cs morpc.ClientSession) error {
	m, ok := req.(*pipeline.Message)
	if !ok {
		panic("unexpected message type for cn-server")
	}

	var errCode []byte
	err := s.requestHandler(m, cs)
	if err != nil {
		errCode = []byte(err.Error())
	}
	// send response back
	return cs.Write(&pipeline.Message{Sid: MessageEnd, Code: errCode}, morpc.SendOptions{})
}

func (cfg *Config) Fill() {
	if cfg.PayLoadCopyBufferSize < 0 {
		cfg.PayLoadCopyBufferSize = dfPayLoadCopyBufferSize
	}
	if cfg.ReadBufferSize < 0 {
		cfg.ReadBufferSize = dfServerReadBufferSize
	}
	if cfg.WriteBufferSize < 0 {
		cfg.WriteBufferSize = dfServerWriteBufferSize
	}
}

func defaultMessageHandler(_ morpc.Message, _ morpc.ClientSession) error {
	return nil
}

func WithMessageHandle(f func(message morpc.Message, cs morpc.ClientSession) error) Options {
	return func(s *service) {
		s.requestHandler = f
	}
}
