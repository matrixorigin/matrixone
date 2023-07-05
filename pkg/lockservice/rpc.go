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
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
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

	defaultRPCTimeout = time.Second * 10
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

	client, err := c.cfg.NewClient("",
		getLogger().RawLogger(),
		func() morpc.Message { return acquireResponse() })
	if err != nil {
		return nil, err
	}
	c.client = client
	return c, nil
}

func (c *client) Send(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	f, err := c.AsyncSend(ctx, request)
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

func (c *client) AsyncSend(ctx context.Context, request *pb.Request) (*morpc.Future, error) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.client.send")
	defer span.End()

	var address string
	switch request.Method {
	case pb.Method_ForwardLock:
		sid := request.Lock.Options.ForwardTo
		c.cluster.GetCNService(
			clusterservice.NewServiceIDSelector(sid),
			func(s metadata.CNService) bool {
				address = s.LockServiceAddress
				return false
			})
	case pb.Method_Lock,
		pb.Method_Unlock,
		pb.Method_GetTxnLock,
		pb.Method_KeepRemoteLock:
		sid := request.LockTable.ServiceID
		c.cluster.GetCNService(
			clusterservice.NewServiceIDSelector(sid),
			func(s metadata.CNService) bool {
				address = s.LockServiceAddress
				return false
			})
	case pb.Method_GetWaitingList:
		sid := request.GetWaitingList.Txn.CreatedOn
		c.cluster.GetCNService(
			clusterservice.NewServiceIDSelector(sid),
			func(s metadata.CNService) bool {
				address = s.LockServiceAddress
				return false
			})
	default:
		c.cluster.GetDNService(
			clusterservice.NewSelector(),
			func(d metadata.DNService) bool {
				address = d.LockServiceAddress
				return false
			})
	}
	return c.client.Send(ctx, address, request)
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
		cfg:      &cfg,
		handlers: make(map[pb.Method]RequestHandleFunc),
	}
	s.cfg.Adjust()
	for _, opt := range opts {
		opt(s)
	}

	rpc, err := s.cfg.NewServer("server",
		address,
		getLogger().RawLogger(),
		func() morpc.Message { return acquireRequest() },
		releaseResponse,
		morpc.WithServerDisableAutoCancelContext())
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
	msg morpc.RPCMessage,
	sequence uint64,
	cs morpc.ClientSession) error {
	ctx, span := trace.Debug(ctx, "lockservice.server.handle")
	defer span.End()

	request := msg.Message
	req, ok := request.(*pb.Request)
	if !ok {
		getLogger().Fatal("received invalid message",
			zap.Any("message", request))
	}

	if getLogger().Enabled(zap.DebugLevel) {
		getLogger().Debug("received a request",
			zap.String("request", req.DebugString()))
	}

	if s.options.filter != nil &&
		!s.options.filter(req) {
		if getLogger().Enabled(zap.DebugLevel) {
			getLogger().Debug("skip request by filter",
				zap.String("request", req.DebugString()))
		}
		releaseRequest(req)
		return nil
	}

	handler, ok := s.handlers[req.Method]
	if !ok {
		getLogger().Fatal("missing request handler",
			zap.String("method", req.Method.String()))
	}

	select {
	case <-ctx.Done():
		if getLogger().Enabled(zap.DebugLevel) {
			getLogger().Debug("skip request by timeout",
				zap.String("request", req.DebugString()))
		}
		releaseRequest(req)
		return nil
	default:
	}

	fn := func(req *pb.Request) error {
		defer releaseRequest(req)
		resp := acquireResponse()
		resp.RequestID = req.RequestID
		resp.Method = req.Method
		handler(ctx, req, resp, cs)
		return nil
	}
	return fn(req)
}

func writeResponse(
	ctx context.Context,
	resp *pb.Response,
	err error,
	cs morpc.ClientSession) {
	if err != nil {
		resp.WrapError(err)
	}
	detail := ""
	if getLogger().Enabled(zap.DebugLevel) {
		detail = resp.DebugString()
		getLogger().Debug("handle request completed",
			zap.String("response", detail))
	}
	// after write, response will be released by rpc
	if err := cs.Write(ctx, resp); err != nil {
		getLogger().Error("write response failed",
			zap.Error(err),
			zap.String("response", detail))
	}
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
