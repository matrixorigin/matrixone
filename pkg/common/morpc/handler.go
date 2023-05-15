// Copyright 2022 Matrix Origin
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

package morpc

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

type pool[REQ, RESP MethodBasedMessage] struct {
	request  sync.Pool
	response sync.Pool
}

// NewMessagePool create message pool
func NewMessagePool[REQ, RESP MethodBasedMessage](
	requestFactory func() REQ,
	responseFactory func() RESP) MessagePool[REQ, RESP] {
	return &pool[REQ, RESP]{
		request:  sync.Pool{New: func() any { return requestFactory() }},
		response: sync.Pool{New: func() any { return responseFactory() }},
	}
}

func (p *pool[REQ, RESP]) AcquireRequest() REQ {
	return p.request.Get().(REQ)
}
func (p *pool[REQ, RESP]) ReleaseRequest(request REQ) {
	request.Reset()
	p.request.Put(request)
}
func (p *pool[REQ, RESP]) AcquireResponse() RESP {
	return p.response.Get().(RESP)
}
func (p *pool[REQ, RESP]) ReleaseResponse(resp RESP) {
	resp.Reset()
	p.response.Put(resp)
}

type handleFuncCtx[REQ, RESP MethodBasedMessage] struct {
	handleFunc HandleFunc[REQ, RESP]
	async      bool
}

func (c *handleFuncCtx[REQ, RESP]) call(
	ctx context.Context,
	req REQ,
	resp RESP) {
	if err := c.handleFunc(ctx, req, resp); err != nil {
		resp.WrapError(err)
	}
	if getLogger().Enabled(zap.DebugLevel) {
		getLogger().Debug("handle request completed",
			zap.String("response", resp.DebugString()))
	}
}

type handler[REQ, RESP MethodBasedMessage] struct {
	cfg      *Config
	rpc      RPCServer
	pool     MessagePool[REQ, RESP]
	handlers map[uint32]handleFuncCtx[REQ, RESP]

	options struct {
		filter func(REQ) bool
	}
}

// WithHandleMessageFilter set filter func. Requests can be modified or filtered out by the filter
// before they are processed by the handler.
func WithHandleMessageFilter[REQ, RESP MethodBasedMessage](filter func(REQ) bool) HandlerOption[REQ, RESP] {
	return func(s *handler[REQ, RESP]) {
		s.options.filter = filter
	}
}

// NewMessageHandler create a message handler.
func NewMessageHandler[REQ, RESP MethodBasedMessage](
	name string,
	address string,
	cfg Config,
	pool MessagePool[REQ, RESP],
	opts ...HandlerOption[REQ, RESP]) (MessageHandler[REQ, RESP], error) {
	s := &handler[REQ, RESP]{
		cfg:      &cfg,
		pool:     pool,
		handlers: make(map[uint32]handleFuncCtx[REQ, RESP]),
	}
	s.cfg.Adjust()
	for _, opt := range opts {
		opt(s)
	}

	rpc, err := s.cfg.NewServer(name,
		address,
		getLogger().RawLogger(),
		func() Message { return pool.AcquireRequest() },
		func(m Message) { pool.ReleaseResponse(m.(RESP)) },
		WithServerDisableAutoCancelContext())
	if err != nil {
		return nil, err
	}
	rpc.RegisterRequestHandler(s.onMessage)
	s.rpc = rpc
	return s, nil
}

func (s *handler[REQ, RESP]) Start() error {
	return s.rpc.Start()
}

func (s *handler[REQ, RESP]) Close() error {
	return s.rpc.Close()
}

func (s *handler[REQ, RESP]) RegisterHandleFunc(
	method uint32,
	h HandleFunc[REQ, RESP],
	async bool) MessageHandler[REQ, RESP] {
	s.handlers[method] = handleFuncCtx[REQ, RESP]{handleFunc: h, async: async}
	return s
}

func (s *handler[REQ, RESP]) Handle(
	ctx context.Context,
	req REQ) RESP {
	resp := s.pool.AcquireResponse()
	if handlerCtx, ok := s.getHandler(ctx, req, resp); ok {
		handlerCtx.call(ctx, req, resp)
	}
	return resp
}

func (s *handler[REQ, RESP]) onMessage(
	ctx context.Context,
	request Message,
	sequence uint64,
	cs ClientSession) error {
	ctx, span := trace.Debug(ctx, "lockservice.server.handle")
	defer span.End()
	req, ok := request.(REQ)
	if !ok {
		getLogger().Fatal("received invalid message",
			zap.Any("message", request))
	}

	resp := s.pool.AcquireResponse()
	handlerCtx, ok := s.getHandler(ctx, req, resp)
	if !ok {
		s.pool.ReleaseRequest(req)
		return cs.Write(ctx, resp)
	}

	fn := func(req REQ) error {
		defer s.pool.ReleaseRequest(req)
		handlerCtx.call(ctx, req, resp)
		return cs.Write(ctx, resp)
	}

	if handlerCtx.async {
		// TODO: make a goroutine pool
		go fn(req)
		return nil
	}
	return fn(req)
}

func (s *handler[REQ, RESP]) getHandler(
	ctx context.Context,
	req REQ,
	resp RESP) (handleFuncCtx[REQ, RESP], bool) {
	if getLogger().Enabled(zap.DebugLevel) {
		getLogger().Debug("received a request",
			zap.String("request", req.DebugString()))
	}

	select {
	case <-ctx.Done():
		if getLogger().Enabled(zap.DebugLevel) {
			getLogger().Debug("skip request by timeout",
				zap.String("request", req.DebugString()))
		}
		resp.WrapError(ctx.Err())
		return handleFuncCtx[REQ, RESP]{}, false
	default:
	}

	if s.options.filter != nil &&
		!s.options.filter(req) {
		if getLogger().Enabled(zap.DebugLevel) {
			getLogger().Debug("skip request by filter",
				zap.String("request", req.DebugString()))
		}
		resp.WrapError(moerr.NewInvalidInputNoCtx("skip request by filter"))
		return handleFuncCtx[REQ, RESP]{}, false
	}

	resp.SetID(req.GetID())
	resp.SetMethod(req.Method())
	handlerCtx, ok := s.handlers[req.Method()]
	if !ok {
		resp.WrapError(moerr.NewNotSupportedNoCtx("%d not support in current service",
			req.Method()))
	}
	return handlerCtx, ok
}
