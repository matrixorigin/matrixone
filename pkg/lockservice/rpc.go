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

package lockservice

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var (
	requestPool = sync.Pool{
		New: func() any {
			return &pb.Request{}
		},
	}
	responsePool = &sync.Pool{
		New: func() any {
			return &pb.Response{}
		},
	}
)

type client struct {
	cfg     *morpc.Config
	cluster clusterservice.MOCluster
	client  morpc.RPCClient
}

func NewClient(cfg morpc.Config) (Client, error) {
	c := &client{
		cfg:     &cfg,
		cluster: clusterservice.GetMOCluster(),
	}
	c.cfg.Adjust()

	client, err := c.cfg.NewClient("lockservice.client",
		runtime.ProcessLevelRuntime().Logger().RawLogger(),
		func() morpc.Message { return acquireResponse() })
	if err != nil {
		return nil, err
	}
	c.client = client
	return c, nil
}

func (c *client) Send(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	ctx, span := trace.Debug(ctx, "lockservice.client.Send")
	defer span.End()

	var address string
	switch request.Method {
	case pb.Method_Lock, pb.Method_Unlock, pb.Method_Heartbeat:
		c.cluster.GetCNService(
			clusterservice.NewServiceIDSelector(
				request.LockTable.ServiceID),
			func(s metadata.CNService) bool {
				address = s.LockServiceAddress
				return false
			})
	default:
		c.cluster.GetDNService(
			clusterservice.NewSelector(),
			func(d metadata.DNService) bool {
				// address = d.
				return false
			})

	}

	f, err := c.client.Send(ctx, address, request)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	v, err := f.Get()
	if err != nil {
		return nil, err
	}
	resp := v.(*pb.Response)
	if err := resp.UnwrapError(); err != nil {
		releaseResponse(resp)
		return nil, err
	}

	return resp, nil
}

func (c *client) Close() error {
	return c.client.Close()
}

// WithServerMessageFilter set filter func. Requests can be modified or filtered out by the filter
// before they are processed by the handler.
func WithServerMessageFilter(filter func(*pb.Request) bool) ServerOption {
	return func(s *server) {
		s.options.filter = filter
	}
}

type server struct {
	cfg      *morpc.Config
	logger   *log.MOLogger
	rpc      morpc.RPCServer
	handlers map[pb.Method]RequestHandleFunc

	options struct {
		filter func(*pb.Request) bool
	}
}

// NewServer create a lockservice server. One LockService corresponds to one Server
func NewServer(
	address string,
	cfg morpc.Config,
	opts ...ServerOption) (Server, error) {
	s := &server{
		logger:   runtime.ProcessLevelRuntime().Logger().Named("lockservice.server"),
		cfg:      &cfg,
		handlers: make(map[pb.Method]RequestHandleFunc),
	}
	s.cfg.Adjust()
	for _, opt := range opts {
		opt(s)
	}

	rpc, err := s.cfg.NewServer("lockservice.server",
		address,
		s.logger.RawLogger(),
		func() morpc.Message { return acquireRequest() },
		releaseResponse)
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

func (s *server) RegisterMethodHandler(m pb.Method, h RequestHandleFunc) {
	s.handlers[m] = h
}

func (s *server) onMessage(
	ctx context.Context,
	request morpc.Message,
	sequence uint64,
	cs morpc.ClientSession) error {
	ctx, span := trace.Debug(ctx, "lockservice.server.onMessage")
	defer span.End()
	m, ok := request.(*pb.Request)
	if !ok {
		s.logger.Fatal("received invalid message", zap.Any("message", request))
	}
	defer releaseRequest(m)

	if s.logger.Enabled(zap.DebugLevel) {
		s.logger.Debug("handle request",
			zap.String("request", m.DebugString()))
	}

	if s.options.filter != nil &&
		!s.options.filter(m) {
		if s.logger.Enabled(zap.DebugLevel) {
			s.logger.Debug("skip request by filter",
				zap.String("request", m.DebugString()))
		}
		return nil
	}

	handler, ok := s.handlers[m.Method]
	if !ok {
		s.logger.Fatal("missing request handler",
			zap.String("method", m.Method.String()))
	}

	select {
	case <-ctx.Done():
		if s.logger.Enabled(zap.DebugLevel) {
			s.logger.Debug("skip request by timeout",
				zap.String("request", m.DebugString()))
		}
		return nil
	default:
	}

	resp := acquireResponse()
	resp.RequestID = m.RequestID
	if err := handler(ctx, m, resp); err != nil {
		resp.WrapError(err)
	}
	if s.logger.Enabled(zap.DebugLevel) {
		s.logger.Debug("handle request completed",
			zap.String("response", resp.DebugString()))
	}
	return cs.Write(ctx, resp)
}

func acquireRequest() *pb.Request {
	return requestPool.Get().(*pb.Request)
}

func releaseRequest(request *pb.Request) {
	request.Reset()
	requestPool.Put(request)
}

func acquireResponse() *pb.Response {
	return responsePool.Get().(*pb.Response)
}

func releaseResponse(v morpc.Message) {
	v.(*pb.Response).Reset()
	responsePool.Put(v)
}
