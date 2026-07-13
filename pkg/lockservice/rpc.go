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
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
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
	defaultRPCWriteTimeout     = time.Second * 3
	defaultHandleWorkers       = 12
	defaultHandleGetTxnWorkers = 4
	// Recovery endpoints are hints: eviction safely falls back to service
	// discovery and the negative-response confirmation path. Keep a hard bound
	// so historical CN UUID churn cannot grow the client for its whole lifetime.
	maxRecoveryBackendEntries = 4096
	// The system resolver may retain a container hostname across endpoint
	// recreation in a long-lived CGO process. Recovery must query DNS again;
	// the pure-Go resolver has no cross-request result cache.
	recoveryResolver = &net.Resolver{PreferGo: true, StrictErrors: true}
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
	// Active-txn identity probes may deliberately reset their transport after
	// detecting a stale CN incarnation. Keep them isolated so recovery cannot
	// interrupt concurrent Lock/Unlock traffic on the normal client.
	activeTxnClient morpc.RPCClient

	recoveryResetOnce sync.Once
	recoveryResetC    chan struct{} // context-aware serialization of slow reset work
	recoveryMu        sync.RWMutex
	recoveryBackends  map[string]recoveryBackend // CN UUID -> recovery endpoint
	resolveBackend    func(context.Context, string) (string, error)
}

type recoveryBackend struct {
	discovered string
	endpoint   string
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
		logger:           getLogger(service),
		service:          service,
		cfg:              &cfg,
		recoveryBackends: make(map[string]recoveryBackend),
	}
	c.resolveBackend = resolveTCP4Endpoint
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

	// Set bounded wait for auto-create to enable fast failure detection in lockservice.
	// This is specifically needed for orphan transaction cleanup, where we need to quickly
	// detect that a remote service is down (not just slow to start).
	// 500ms is chosen to balance between:
	// - Fast failure detection for down services (critical for orphan cleanup)
	// - Enough time for legitimate backend creation in normal cases
	// Note: This only affects auto-create wait time. Normal lock operations use their own
	// timeouts (e.g., RemoteLockTimeout) and are not affected by this setting.
	c.cfg.ClientOptions = append(c.cfg.ClientOptions,
		morpc.WithClientAutoCreateWaitTimeout(500*time.Millisecond))

	client, err := c.cfg.NewClient(
		service,
		"lock-client",
		func() morpc.Message { return acquireResponse() })
	if err != nil {
		return nil, err
	}
	c.client = client
	activeTxnClient, err := c.cfg.NewClient(
		service,
		"lock-active-txn-client",
		func() morpc.Message { return acquireResponse() })
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	c.activeTxnClient = activeTxnClient
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
		observeLockserviceRemoteRPCError(request.Method, err)
		return nil, err
	}
	resp := v.(*pb.Response)
	if err := resp.UnwrapError(); err != nil {
		observeLockserviceRemoteRPCError(request.Method, err)
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
			pb.Method_GetLockHolder,
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
		case pb.Method_CheckActiveTxn:
			sid = getUUIDFromServiceIdentifier(request.CheckActiveTxn.ServiceID)
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
	transport := c.client
	if isActiveTxnMethod(request.Method) {
		address = c.activeTxnBackend(sid, address)
		transport = c.activeTxnTransport()
	}
	f, err := transport.Send(ctx, address, request)
	if err != nil {
		observeLockserviceRemoteRPCError(request.Method, err)
	}
	return f, err
}

func observeLockserviceRemoteRPCError(method pb.Method, err error) {
	if errorType := lockserviceRemoteRPCErrorType(err); errorType != "" {
		v2.NewLockserviceRemoteRPCErrorCounter(method.String(), errorType).Inc()
	}
}

func lockserviceRemoteRPCErrorType(err error) string {
	if err == nil {
		return ""
	}
	if moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) {
		return "rpc_timeout"
	}
	if moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return "backend_cannot_connect"
	}
	if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) {
		return "backend_closed"
	}
	if moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ""
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	errText := err.Error()
	switch {
	case strings.Contains(errText, "i/o timeout"),
		strings.Contains(errText, "deadline exceeded"),
		strings.Contains(errText, "timeout"):
		return "timeout"
	case strings.Contains(errText, "unexpected EOF"):
		return "unexpected_eof"
	case strings.Contains(errText, "EOF"):
		return "eof"
	}
	return ""
}

func (c *client) Close() error {
	var normalErr, activeTxnErr error
	if c.client != nil {
		normalErr = c.client.Close()
	}
	if c.activeTxnClient != nil && c.activeTxnClient != c.client {
		activeTxnErr = c.activeTxnClient.Close()
	}
	return errors.Join(normalErr, activeTxnErr)
}

func isActiveTxnMethod(method pb.Method) bool {
	return method == pb.Method_GetActiveTxn || method == pb.Method_CheckActiveTxn
}

func (c *client) activeTxnTransport() morpc.RPCClient {
	if c.activeTxnClient != nil {
		return c.activeTxnClient
	}
	// Preserve compatibility for tests and embedders that construct client
	// directly. Production NewClient always installs the isolated transport.
	return c.client
}

// ResetBackend detaches the pooled connection for one CN incarnation. The
// address can remain unchanged across a hot recreate, so service discovery
// refresh alone is insufficient to prevent reuse of the stale backend.
func (c *client) ResetBackend(parent context.Context, serviceID string) error {
	sid := getUUIDFromServiceIdentifier(serviceID)
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeoutCause(
		parent,
		defaultRPCTimeout,
		moerr.CauseResetLockServiceBackend,
	)
	defer cancel()

	// Discovery refresh, DNS, and backend shutdown can all be slow. Serialize
	// resets separately and context-aware, so one slow recovery cannot make
	// later probes wait past their own deadline.
	if err := c.acquireRecoveryReset(ctx); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	defer c.releaseRecoveryReset()
	resetter, ok := c.activeTxnTransport().(interface {
		CloseBackendFor(string) error
	})
	if !ok {
		return moerr.NewInternalErrorNoCtx("morpc client does not support targeted backend reset")
	}

	lookupAddress := func() string {
		var address string
		c.cluster.GetCNServiceWithoutWorkingState(
			clusterservice.NewServiceIDSelector(sid),
			func(s metadata.CNService) bool {
				address = s.LockServiceAddress
				return false
			})
		return address
	}

	// A negative response proves that the route used for the first check may be
	// stale even when the cached address is non-empty. Refresh synchronously so
	// the confirming request cannot be sent to an old CN address after a hot
	// recreate or address reassignment.
	staleAddress := lookupAddress()
	refresher, ok := c.cluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"cluster service does not support authoritative refresh")
	}
	if err := refresher.Refresh(ctx); err != nil {
		return err
	}
	address := lookupAddress()

	var endpoint string
	var resolveErr error
	if address != "" {
		if c.resolveBackend == nil {
			resolveErr = moerr.NewInternalErrorNoCtx("lockservice recovery resolver is not configured")
		} else {
			endpoint, resolveErr = c.resolveBackend(ctx, address)
		}
	}

	// Detach every route that may still identify this CN incarnation: the
	// pre-refresh address, the refreshed address, and a prior pinned recovery
	// endpoint. Continue after individual close failures so reset performs as
	// much cleanup as possible, then preserve the indeterminate result.
	c.recoveryMu.Lock()
	old, hadOld := c.recoveryBackends[sid]
	if hadOld {
		delete(c.recoveryBackends, sid)
	}
	c.recoveryMu.Unlock()
	candidates := []string{staleAddress, address}
	if hadOld {
		candidates = append(candidates, old.discovered, old.endpoint)
	}
	seen := make(map[string]struct{}, len(candidates))
	var closeErr error
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		closeErr = errors.Join(closeErr, resetter.CloseBackendFor(candidate))
	}
	if address == "" {
		return errors.Join(
			closeErr,
			moerr.NewInternalErrorNoCtx("cannot find lockservice address for "+sid),
		)
	}
	if resolveErr != nil || closeErr != nil {
		return errors.Join(resolveErr, closeErr)
	}

	if endpoint == address {
		return nil
	}
	c.recoveryMu.Lock()
	c.storeRecoveryBackendLocked(sid, recoveryBackend{
		discovered: address,
		endpoint:   endpoint,
	})
	c.recoveryMu.Unlock()
	return nil
}

func (c *client) acquireRecoveryReset(ctx context.Context) error {
	c.recoveryResetOnce.Do(func() {
		c.recoveryResetC = make(chan struct{}, 1)
		c.recoveryResetC <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.recoveryResetC:
		return nil
	}
}

func (c *client) releaseRecoveryReset() {
	c.recoveryResetC <- struct{}{}
}

func (c *client) storeRecoveryBackendLocked(serviceID string, backend recoveryBackend) {
	if c.recoveryBackends == nil {
		c.recoveryBackends = make(map[string]recoveryBackend)
	}
	if _, exists := c.recoveryBackends[serviceID]; !exists &&
		len(c.recoveryBackends) >= maxRecoveryBackendEntries {
		// Any victim is safe: this cache is only a recovery hint, and a miss
		// re-enters discovery plus the reset/confirmation path.
		for victim := range c.recoveryBackends {
			delete(c.recoveryBackends, victim)
			break
		}
	}
	c.recoveryBackends[serviceID] = backend
}

func (c *client) activeTxnBackend(serviceID, discovered string) string {
	c.recoveryMu.RLock()
	backend, ok := c.recoveryBackends[serviceID]
	if ok && backend.discovered == discovered {
		endpoint := backend.endpoint
		c.recoveryMu.RUnlock()
		return endpoint
	}
	c.recoveryMu.RUnlock()
	if ok {
		c.recoveryMu.Lock()
		if current, exists := c.recoveryBackends[serviceID]; exists && current == backend {
			delete(c.recoveryBackends, serviceID)
		}
		c.recoveryMu.Unlock()
	}
	return discovered
}

func resolveTCP4Endpoint(ctx context.Context, address string) (string, error) {
	return resolveTCP4EndpointWithLookup(ctx, address, recoveryResolver.LookupIP)
}

func resolveTCP4EndpointWithLookup(
	ctx context.Context,
	address string,
	lookup func(context.Context, string, string) ([]net.IP, error),
) (string, error) {
	if strings.Contains(address, "://") {
		return address, nil
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) != nil {
		return address, err
	}
	ips, err := lookup(ctx, "ip4", host)
	if err != nil {
		return address, err
	}
	// Without a concrete service-identity endpoint, a second Valid=false may
	// still come from a stale or different CN behind the same multi-A name.
	// Preserve the caller's unknown state instead of claiming reset success.
	if len(ips) != 1 {
		return address, moerr.NewInternalErrorNoCtxf(
			"lockservice recovery requires one IPv4 endpoint for %s, got %d",
			host,
			len(ips),
		)
	}
	ip := ips[0].To4()
	if ip == nil {
		return address, moerr.NewInternalErrorNoCtxf(
			"lockservice recovery requires a valid IPv4 endpoint for %s",
			host,
		)
	}
	return net.JoinHostPort(ip.String(), port), nil
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
	if req.Method == pb.Method_GetActiveTxn ||
		req.Method == pb.Method_CheckActiveTxn {
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
	_ = writeResponseWithDeadline(logger, cancel, resp, err, cs, defaultRPCWriteTimeout, nil)
}

func writeResponseWithDeadline(
	logger *log.MOLogger,
	cancel context.CancelFunc,
	resp *pb.Response,
	err error,
	cs morpc.ClientSession,
	timeout time.Duration,
	extraFields func() []zap.Field,
) error {
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
	requestID := resp.RequestID
	method := resp.Method.String()
	remote := cs.RemoteAddress()

	writeCtx, writeCancel := context.WithTimeout(context.Background(), timeout)
	defer writeCancel()
	if sessionCtx := cs.SessionCtx(); sessionCtx != nil {
		stop := context.AfterFunc(sessionCtx, writeCancel)
		defer stop()
	}
	if err := cs.Write(writeCtx, resp); err != nil {
		var extra []zap.Field
		if extraFields != nil {
			extra = extraFields()
		}
		fields := []zap.Field{
			zap.Error(err),
			zap.Uint64("request-id", requestID),
			zap.String("method", method),
			zap.String("remote", remote),
			zap.String("response", detail),
		}
		fields = append(fields, extra...)
		logger.Error("write response failed", fields...)
		// A dropped response leaves the peer's Future waiting unless the
		// session is closed and the client-side backend fails pending futures.
		if closeErr := cs.Close(); closeErr != nil {
			closeFields := []zap.Field{
				zap.Error(closeErr),
				zap.Uint64("request-id", requestID),
				zap.String("method", method),
				zap.String("remote", remote),
			}
			closeFields = append(closeFields, extra...)
			logger.Error("close client session after write response failed", closeFields...)
		}
		return err
	}
	return nil
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
