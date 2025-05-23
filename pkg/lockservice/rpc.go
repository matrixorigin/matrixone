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
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

var (
	defaultRPCTimeout          = time.Second * 10
	defaultHandleWorkers       = 12
	defaultHandleGetTxnWorkers = 4
)

func acquireRequest() *pb.Request {
	return reuse.Alloc[pb.Request](nil)
}

func releaseRequest(request *pb.Request) {
	reuse.Free(request, nil)
}

func acquireResponse() *pb.Response {
	return reuse.Alloc[pb.Response](nil)
}

func releaseResponse(v morpc.Message) {
	reuse.Free(v.(*pb.Response), nil)
}

type client struct {
	service string
	logger  *log.MOLogger
	cfg     *morpc.Config
	cluster clusterservice.MOCluster
	client  morpc.RPCClient
}

type ClientOption func(c *client)

func WithMOCluster(cluster clusterservice.MOCluster) ClientOption {
	return func(c *client) {
		c.cluster = cluster
	}
}

func NewClient(
	service string,
	cfg morpc.Config,
	opts ...ClientOption,
) (Client, error) {
	c := &client{
		logger:  getLogger(service),
		service: service,
		cfg:     &cfg,
	}
	for _, applyFn := range opts {
		applyFn(c)
	}
	if c.cluster == nil {
		c.cluster = clusterservice.GetMOCluster(service)
	}
	c.cfg.Adjust()
	// add read timeout for lockservice client, to avoid remote lock hung and cannot read the lock response
	// due to tcp disconnected.
	c.cfg.BackendOptions = append(c.cfg.BackendOptions,
		morpc.WithBackendReadTimeout(defaultRPCTimeout),
		morpc.WithBackendFreeOrphansResponse(releaseResponse))

	client, err := c.cfg.NewClient(
		service,
		"lock-client",
		func() morpc.Message { return acquireResponse() })
	if err != nil {
		return nil, err
	}
	c.client = client
	return c, nil
}

func (c *client) Send(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	if err := checkMethodVersion(ctx, c.service, request); err != nil {
		return nil, err
	}
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

func checkMethodVersion(
	ctx context.Context,
	service string,
	req *pb.Request,
) error {
	return runtime.CheckMethodVersion(ctx, service, methodVersions, req)
}

func (c *client) AsyncSend(ctx context.Context, request *pb.Request) (*morpc.Future, error) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.client.send")
	defer span.End()

	var sid = ""
	var address string
	for i := 0; i < 2; i++ {
		switch request.Method {
		case pb.Method_ForwardLock:
			sid = getUUIDFromServiceIdentifier(request.Lock.Options.ForwardTo)
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
			sid = getUUIDFromServiceIdentifier(request.LockTable.ServiceID)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		case pb.Method_ValidateService:
			sid = getUUIDFromServiceIdentifier(request.ValidateService.ServiceID)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		case pb.Method_GetWaitingList:
			sid = getUUIDFromServiceIdentifier(request.GetWaitingList.Txn.CreatedOn)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		case pb.Method_GetActiveTxn:
			sid = getUUIDFromServiceIdentifier(request.GetActiveTxn.ServiceID)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		case pb.Method_AbortRemoteDeadlockTxn:
			sid = getUUIDFromServiceIdentifier(request.AbortRemoteDeadlockTxn.Txn.WaiterAddress)
			c.cluster.GetCNServiceWithoutWorkingState(
				clusterservice.NewServiceIDSelector(sid),
				func(s metadata.CNService) bool {
					address = s.LockServiceAddress
					return false
				})
		default:
			values := c.cluster.GetAllTNServices()
			if len(values) > 0 {
				address = values[0].LockServiceAddress
			}
		}
		if address != "" {
			break
		}
		if i == 0 {
			c.cluster.ForceRefresh(true)
		}
	}
	if address == "" {
		var cns []string
		c.cluster.GetCNServiceWithoutWorkingState(
			clusterservice.NewSelectAll(),
			func(s metadata.CNService) bool {
				cns = append(cns, s.ServiceID)
				return true
			})
		c.logger.Error("cannot find lockservice address",
			zap.String("target", sid),
			zap.Any("cns", cns),
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
	logger   *log.MOLogger
	address  string
	cfg      *morpc.Config
	rpc      morpc.RPCServer
	handlers map[pb.Method]RequestHandleFunc

	requests             chan requestCtx
	getActiveTxnRequests chan requestCtx
	stopper              *stopper.Stopper

	options struct {
		filter func(*pb.Request) bool
	}
}

// NewServer create a lockservice server. One LockService corresponds to one Server
func NewServer(
	service string,
	address string,
	cfg morpc.Config,
	opts ...ServerOption,
) (Server, error) {
	logger := getLogger(service)
	s := &server{
		logger:               logger,
		cfg:                  &cfg,
		address:              address,
		handlers:             make(map[pb.Method]RequestHandleFunc),
		requests:             make(chan requestCtx, 10240),
		getActiveTxnRequests: make(chan requestCtx, 10240),
		stopper: stopper.NewStopper("lock-service-rpc-server",
			stopper.WithLogger(logger.RawLogger())),
	}
	s.cfg.Adjust()
	for _, opt := range opts {
		opt(s)
	}

	rpc, err := s.cfg.NewServer(
		service,
		"lock-server",
		address,
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
	s.setupRemoteHandles(defaultHandleWorkers, s.requests)
	s.setupRemoteHandles(defaultHandleGetTxnWorkers, s.getActiveTxnRequests)
	return s.rpc.Start()
}

func (s *server) Close() error {
	if err := s.rpc.Close(); err != nil {
		return err
	}
	s.stopper.Stop()
	close(s.requests)
	close(s.getActiveTxnRequests)
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
		s.logger.Fatal("received invalid message",
			zap.Any("message", request))
	}

	if s.logger.Enabled(zap.DebugLevel) {
		s.logger.Debug("received a request",
			zap.String("request", req.DebugString()))
	}

	if s.options.filter != nil &&
		!s.options.filter(req) {
		if s.logger.Enabled(zap.DebugLevel) {
			s.logger.Debug("skip request by filter",
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
		err := moerr.NewNotSupportedNoCtxf("method [%s], from %s, current %s",
			req.Method.String(),
			cs.RemoteAddress(),
			s.address)
		writeResponse(
			s.logger,
			msg.Cancel,
			getResponse(req),
			err,
			cs,
		)
		releaseRequest(req)
		return nil
	}

	select {
	case <-ctx.Done():
		if s.logger.Enabled(zap.DebugLevel) {
			s.logger.Debug("skip request by timeout",
				zap.String("request", req.DebugString()))
		}
		releaseRequest(req)
		return nil
	default:
	}

	c := s.requests
	if req.Method == pb.Method_GetActiveTxn {
		c = s.getActiveTxnRequests
	}
	c <- requestCtx{
		req:     req,
		handler: handler,
		cs:      cs,
		cancel:  msg.Cancel,
		ctx:     ctx,
	}
	v2.TxnLockRPCQueueSizeGauge.Set(float64(len(s.requests) + len(s.getActiveTxnRequests)))
	return nil
}

func (s *server) handle(
	ctx context.Context,
	requests chan requestCtx,
) {
	fn := func(ctx requestCtx) {
		start := time.Now()
		defer func() {
			v2.TxnLockWorkerHandleDurationHistogram.Observe(time.Since(start).Seconds())
		}()

		req := ctx.req
		defer releaseRequest(req)
		resp := getResponse(req)
		ctx.handler(ctx.ctx, ctx.cancel, req, resp, ctx.cs)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ctx := <-requests:
			v2.TxnLockRPCQueueSizeGauge.Set(float64(len(requests)))
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
	logger *log.MOLogger,
	cancel context.CancelFunc,
	resp *pb.Response,
	err error,
	cs morpc.ClientSession,
) {
	if cancel != nil {
		defer cancel()
	}

	if err != nil {
		resp.WrapError(err)
	}
	detail := ""
	if logger.Enabled(zap.DebugLevel) {
		detail = resp.DebugString()
		logger.Debug("handle request completed",
			zap.String("response", detail))
	}
	// after write, response will be released by rpc
	if err := cs.AsyncWrite(resp); err != nil {
		logger.Error("write response failed",
			zap.Error(err),
			zap.String("response", detail))
	}
}

func (s *server) setupRemoteHandles(
	workers int,
	requests chan requestCtx,
) {
	for i := 0; i < workers; i++ {
		if err := s.stopper.RunTask(
			func(ctx context.Context) {
				s.handle(ctx, requests)
			}); err != nil {
			panic(err)
		}
	}
}

type requestCtx struct {
	req     *pb.Request
	handler RequestHandleFunc
	cs      morpc.ClientSession
	ctx     context.Context
	cancel  context.CancelFunc
}
