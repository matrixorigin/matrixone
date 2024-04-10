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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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

	defaultRPCTimeout    = time.Second * 10
	defaultHandleWorkers = 4
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
	// add read timeout for lockservice client, to avoid remote lock hung and cannot read the lock response
	// due to tcp disconnected.
	c.cfg.BackendOptions = append(c.cfg.BackendOptions,
		morpc.WithBackendReadTimeout(defaultRPCTimeout))

	client, err := c.cfg.NewClient(
		"lock-client",
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
		// uuid and ip changed, async refresh cluster
		if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
			c.cluster.ForceRefresh(false)
		}
		return nil, err
	}
	return resp, nil
}

func (c *client) AsyncSend(ctx context.Context, request *pb.Request) (*morpc.Future, error) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.client.send")
	defer span.End()

	var address string
	for i := 0; i < 2; i++ {
		switch request.Method {
		case pb.Method_ForwardLock:
			sid := getUUIDFromServiceIdentifier(request.Lock.Options.ForwardTo)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		case pb.Method_Lock,
			pb.Method_Unlock,
			pb.Method_GetTxnLock,
			pb.Method_KeepRemoteLock:
			sid := getUUIDFromServiceIdentifier(request.LockTable.ServiceID)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		case pb.Method_ValidateService:
			sid := getUUIDFromServiceIdentifier(request.ValidateService.ServiceID)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		case pb.Method_GetWaitingList:
			sid := getUUIDFromServiceIdentifier(request.GetWaitingList.Txn.CreatedOn)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		default:
			c.cluster.GetTNService(
				clusterservice.NewSelector(),
				func(d metadata.TNService) bool {
					address = d.LockServiceAddress
					return false
				})
		}
		if address != "" {
			break
		}
		if i == 0 {
			c.cluster.ForceRefresh(true)
		}
	}
	if address == "" {
		getLogger().Error("cannot find lockservice address",
			zap.String("request", request.DebugString()))

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
	address  string
	cfg      *morpc.Config
	rpc      morpc.RPCServer
	handlers map[pb.Method]RequestHandleFunc

	requests chan requestCtx
	stopper  *stopper.Stopper

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
		address:  address,
		handlers: make(map[pb.Method]RequestHandleFunc),
		requests: make(chan requestCtx, 10240),
		stopper: stopper.NewStopper("lock-service-rpc-server",
			stopper.WithLogger(getLogger().RawLogger())),
	}
	s.cfg.Adjust()
	for _, opt := range opts {
		opt(s)
	}

	rpc, err := s.cfg.NewServer(
		"lock-server",
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
	for i := 0; i < defaultHandleWorkers; i++ {
		if err := s.stopper.RunTask(s.handle); err != nil {
			panic(err)
		}
	}
	return s.rpc.Start()
}

func (s *server) Close() error {
	if err := s.rpc.Close(); err != nil {
		return err
	}
	s.stopper.Stop()
	close(s.requests)
	return nil
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
		if msg.Cancel != nil {
			msg.Cancel()
		}
		releaseRequest(req)
		return nil
	}

	handler, ok := s.handlers[req.Method]
	if !ok {
		err := moerr.NewNotSupportedNoCtx("method [%s], from %s, current %s",
			req.Method.String(),
			cs.RemoteAddress(),
			s.address)
		writeResponse(
			ctx,
			msg.Cancel,
			getResponse(req),
			err,
			cs)
		return nil
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

	s.requests <- requestCtx{
		req:     req,
		handler: handler,
		cs:      cs,
		cancel:  msg.Cancel,
		ctx:     ctx,
	}
	v2.TxnLockRPCQueueSizeGauge.Set(float64(len(s.requests)))
	return nil
}

func (s *server) handle(ctx context.Context) {
	fn := func(ctx requestCtx) {
		req := ctx.req
		defer releaseRequest(req)
		resp := getResponse(req)
		ctx.handler(ctx.ctx, ctx.cancel, req, resp, ctx.cs)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ctx := <-s.requests:
			v2.TxnLockRPCQueueSizeGauge.Set(float64(len(s.requests)))
			fn(ctx)
		}
	}
}

func getResponse(req *pb.Request) *pb.Response {
	resp := acquireResponse()
	resp.RequestID = req.RequestID
	resp.Method = req.Method
	return resp
}

func writeResponse(
	ctx context.Context,
	cancel context.CancelFunc,
	resp *pb.Response,
	err error,
	cs morpc.ClientSession) {
	if cancel != nil {
		defer cancel()
	}

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

type requestCtx struct {
	req     *pb.Request
	handler RequestHandleFunc
	cs      morpc.ClientSession
	ctx     context.Context
	cancel  context.CancelFunc
}
