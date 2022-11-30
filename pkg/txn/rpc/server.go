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

package rpc

import (
	"context"
	"sync"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

// WithServerMaxMessageSize set max rpc message size
func WithServerMaxMessageSize(maxMessageSize int) ServerOption {
	return func(s *server) {
		s.options.maxMessageSize = maxMessageSize
	}
}

// set filter func. Requests can be modified or filtered out by the filter
// before they are processed by the handler.
func WithServerMessageFilter(filter func(*txn.TxnRequest) bool) ServerOption {
	return func(s *server) {
		s.options.filter = filter
	}
}

type server struct {
	rt       runtime.Runtime
	rpc      morpc.RPCServer
	handlers map[txn.TxnMethod]TxnRequestHandleFunc

	pool struct {
		requests  sync.Pool
		responses sync.Pool
	}

	options struct {
		filter         func(*txn.TxnRequest) bool
		maxMessageSize int
	}
}

// NewTxnServer create a txn server. One DNStore corresponds to one TxnServer
func NewTxnServer(
	address string,
	rt runtime.Runtime,
	opts ...ServerOption) (TxnServer, error) {
	s := &server{
		rt:       rt,
		handlers: make(map[txn.TxnMethod]TxnRequestHandleFunc),
	}
	s.pool.requests = sync.Pool{
		New: func() any {
			return &txn.TxnRequest{}
		},
	}
	s.pool.responses = sync.Pool{
		New: func() any {
			return &txn.TxnResponse{}
		},
	}
	for _, opt := range opts {
		opt(s)
	}

	rpc, err := morpc.NewRPCServer("txn-server", address,
		morpc.NewMessageCodec(s.acquireRequest,
			morpc.WithCodecIntegrationHLC(rt.Clock()),
			morpc.WithCodecEnableChecksum(),
			morpc.WithCodecPayloadCopyBufferSize(16*1024),
			morpc.WithCodecMaxBodySize(s.options.maxMessageSize)),
		morpc.WithServerLogger(s.rt.Logger().RawLogger()),
		morpc.WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(v interface{}) {
			m := v.(morpc.RPCMessage)
			s.releaseResponse(m.Message.(*txn.TxnResponse))
		})))
	if err != nil {
		return nil, err
	}

	rpc.RegisterRequestHandler(s.onMessage)
	s.rpc = rpc
	return s, nil
}

func (s *server) Start() error {
	return s.rpc.Start()
}

func (s *server) Close() error {
	return s.rpc.Close()
}

func (s *server) RegisterMethodHandler(m txn.TxnMethod, h TxnRequestHandleFunc) {
	s.handlers[m] = h
}

// onMessage a client connection has a separate read goroutine. The onMessage invoked in this read goroutine.
func (s *server) onMessage(ctx context.Context, request morpc.Message, sequence uint64, cs morpc.ClientSession) error {
	ctx, span := trace.Debug(ctx, "server.onMessage")
	defer span.End()
	m, ok := request.(*txn.TxnRequest)
	if !ok {
		s.rt.Logger().Fatal("received invalid message", zap.Any("message", request))
	}
	defer s.releaseRequest(m)

	if s.options.filter != nil && !s.options.filter(m) {
		return nil
	}

	handler, ok := s.handlers[m.Method]
	if !ok {
		s.rt.Logger().Fatal("missing txn request handler",
			zap.String("method", m.Method.String()))
	}

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	resp := s.acquireResponse()
	if err := handler(ctx, m, resp); err != nil {
		s.releaseResponse(resp)
		return err
	}

	resp.RequestID = m.RequestID
	return cs.Write(ctx, resp)
}

func (s *server) acquireResponse() *txn.TxnResponse {
	return s.pool.responses.Get().(*txn.TxnResponse)
}

func (s *server) releaseResponse(resp *txn.TxnResponse) {
	resp.Reset()
	s.pool.responses.Put(resp)
}

func (s *server) acquireRequest() morpc.Message {
	return s.pool.requests.Get().(*txn.TxnRequest)
}

func (s *server) releaseRequest(req *txn.TxnRequest) {
	req.Reset()
	s.pool.requests.Put(req)
}
