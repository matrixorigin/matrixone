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
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type pool[REQ, RESP MethodBasedMessage] struct {
	request  sync.Pool
	response sync.Pool
}

// NewMessagePool create message pool
func NewMessagePool[REQ, RESP MethodBasedMessage](
	requestFactory func() REQ,
	responseFactory func() RESP,
) MessagePool[REQ, RESP] {
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
	resp RESP,
	buf *Buffer,
) {
	if err := c.handleFunc(ctx, req, resp, buf); err != nil {
		resp.WrapError(err)
	}
	if getLogger().Enabled(zap.DebugLevel) {
		getLogger().Debug("handle request completed",
			zap.String("response", resp.DebugString()))
	}
}

type methodBasedServer[REQ, RESP MethodBasedMessage] struct {
	cfg      *Config
	rpc      RPCServer
	pool     MessagePool[REQ, RESP]
	handlers map[uint32]handleFuncCtx[REQ, RESP]

	// respReleaseFunc is the function to release response.
	respReleaseFunc func(Message)

	options struct {
		filter func(REQ) bool
	}
}

// WithHandleMessageFilter set filter func. Requests can be modified or filtered out by the filter
// before they are processed by the handler.
func WithHandleMessageFilter[REQ, RESP MethodBasedMessage](filter func(REQ) bool) HandlerOption[REQ, RESP] {
	return func(s *methodBasedServer[REQ, RESP]) {
		s.options.filter = filter
	}
}

// WithHandlerRespReleaseFunc sets the respReleaseFunc of the handler.
func WithHandlerRespReleaseFunc[REQ, RESP MethodBasedMessage](f func(message Message)) HandlerOption[REQ, RESP] {
	return func(s *methodBasedServer[REQ, RESP]) {
		s.respReleaseFunc = f
	}
}

// NewMessageHandler create a message handler.
func NewMessageHandler[REQ, RESP MethodBasedMessage](
	name string,
	address string,
	cfg Config,
	pool MessagePool[REQ, RESP],
	opts ...HandlerOption[REQ, RESP]) (MethodBasedServer[REQ, RESP], error) {
	s := &methodBasedServer[REQ, RESP]{
		cfg:      &cfg,
		pool:     pool,
		handlers: make(map[uint32]handleFuncCtx[REQ, RESP]),
	}
	s.cfg.Adjust()
	for _, opt := range opts {
		opt(s)
	}

	if s.respReleaseFunc == nil {
		s.respReleaseFunc = func(m Message) {
			pool.ReleaseResponse(m.(RESP))
		}
	}

	rpc, err := s.cfg.NewServer(
		name,
		address,
		getLogger().RawLogger(),
		func() Message { return pool.AcquireRequest() },
		s.respReleaseFunc,
		WithServerDisableAutoCancelContext())
	if err != nil {
		return nil, err
	}
	rpc.RegisterRequestHandler(s.onMessage)
	s.rpc = rpc
	return s, nil
}

func (s *methodBasedServer[REQ, RESP]) Start() error {
	return s.rpc.Start()
}

func (s *methodBasedServer[REQ, RESP]) Close() error {
	return s.rpc.Close()
}

func (s *methodBasedServer[REQ, RESP]) RegisterMethod(
	method uint32,
	h HandleFunc[REQ, RESP],
	async bool,
) MethodBasedServer[REQ, RESP] {
	s.handlers[method] = handleFuncCtx[REQ, RESP]{handleFunc: h, async: async}
	return s
}

func (s *methodBasedServer[REQ, RESP]) Handle(
	ctx context.Context,
	req REQ,
	buf *Buffer,
) RESP {
	resp := s.pool.AcquireResponse()
	if handlerCtx, ok := s.getHandleFunc(ctx, req, resp); ok {
		handlerCtx.call(ctx, req, resp, buf)
	}
	return resp
}

func (s *methodBasedServer[REQ, RESP]) onMessage(
	ctx context.Context,
	request RPCMessage,
	_ uint64,
	cs ClientSession,
) error {
	req, ok := request.Message.(REQ)
	if !ok {
		return moerr.NewNotSupported(
			ctx,
			"invalid message type %T",
			request)
	}

	resp := s.pool.AcquireResponse()
	handlerCtx, ok := s.getHandleFunc(ctx, req, resp)
	if !ok {
		s.pool.ReleaseRequest(req)
		return cs.Write(ctx, resp)
	}

	fn := func(request RPCMessage) error {
		defer request.Cancel()
		req, ok := request.Message.(REQ)
		if !ok {
			getLogger().Fatal("received invalid message",
				zap.Any("message", request))
		}

		buf := NewBuffer()
		defer buf.Close()

		defer s.pool.ReleaseRequest(req)
		handlerCtx.call(
			ctx,
			req,
			resp,
			buf,
		)
		return cs.Write(ctx, resp)
	}

	if handlerCtx.async {
		return ants.Submit(
			func() {
				fn(request)
			},
		)
	}
	return fn(request)
}

func (s *methodBasedServer[REQ, RESP]) getHandleFunc(
	ctx context.Context,
	req REQ,
	resp RESP,
) (handleFuncCtx[REQ, RESP], bool) {
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

type methodBasedClient[REQ, RESP MethodBasedMessage] struct {
	cfg    *Config
	client RPCClient
	pool   MessagePool[REQ, RESP]
	m      map[uint32]func(REQ) (string, error)
}

func NewMethodBasedClient[REQ, RESP MethodBasedMessage](
	name string,
	cfg Config,
	pool MessagePool[REQ, RESP],
) (MethodBasedClient[REQ, RESP], error) {
	c := &methodBasedClient[REQ, RESP]{
		cfg:  &cfg,
		pool: pool,
		m:    make(map[uint32]func(REQ) (string, error)),
	}
	c.cfg.Adjust()
	c.cfg.BackendOptions = append(
		c.cfg.BackendOptions,
		WithBackendReadTimeout(internalTimeout),
		WithBackendFreeOrphansResponse(
			func(m Message) {
				pool.ReleaseResponse(m.(RESP))
			},
		),
	)

	client, err := c.cfg.NewClient(
		name,
		getLogger().RawLogger(),
		func() Message { return pool.AcquireResponse() },
	)
	if err != nil {
		return nil, err
	}
	c.client = client
	return c, nil
}

func (c *methodBasedClient[REQ, RESP]) RegisterMethod(
	method uint32,
	fn func(REQ) (string, error),
) {
	c.m[method] = fn
}

func (c *methodBasedClient[REQ, RESP]) Send(
	ctx context.Context,
	request REQ,
) (RESP, error) {
	var zero RESP

	f, err := c.AsyncSend(ctx, request)
	if err != nil {
		return zero, err
	}
	defer f.Close()

	v, err := f.Get()
	if err != nil {
		return zero, err
	}
	resp := v.(RESP)
	if err := resp.UnwrapError(); err != nil {
		c.pool.ReleaseResponse(resp)
		return zero, err
	}
	return resp, nil
}

func (c *methodBasedClient[REQ, RESP]) AsyncSend(
	ctx context.Context,
	request REQ,
) (*Future, error) {
	address, err := c.m[request.Method()](request)
	if err != nil {
		return nil, err
	}
	return c.client.Send(ctx, address, request)
}

func (c *methodBasedClient[REQ, RESP]) Close() error {
	return c.client.Close()
}
