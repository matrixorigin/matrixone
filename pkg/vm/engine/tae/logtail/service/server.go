// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"
)

const (
	LogtailServiceRPCName = "logtail-server"
)

type ServerOption func(*LogtailServer)

// WithServerMaxMessageSize sets max rpc message size
func WithServerMaxMessageSize(maxMessageSize int64) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.RpcMaxMessageSize = maxMessageSize
	}
}

// WithServerEnableChecksum enables checksum
func WithServerEnableChecksum(enable bool) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.RpcEnableChecksum = enable
	}
}

// WithServerCollectInterval sets logtail collection interval.
func WithServerCollectInterval(interval time.Duration) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.LogtailCollectInterval = interval
	}
}

// WithServerSendTimeout sets timeout for response sending.
func WithServerSendTimeout(timeout time.Duration) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.ResponseSendTimeout = timeout
	}
}

// tableLogtail describes a table's logtail.
type wrapLogtail struct {
	id   TableID
	tail logtail.TableLogtail
}

// sessionError describes error when writing via morpc client session.
type sessionError struct {
	session *Session
	err     error
}

// subscription describes new subscription.
type subscription struct {
	timeout time.Duration
	tableID TableID
	req     *logtail.SubscribeRequest
	session *Session
}

// LogtailServer handles logtail push logic.
type LogtailServer struct {
	pool struct {
		requests  LogtailRequestPool
		responses LogtailResponsePool
		segments  LogtailServerSegmentPool
	}
	maxChunkSize int

	rt     runtime.Runtime
	logger *log.MOLogger

	// FIXME: change s.cfg.LogtailCollectInterval as hearbeat interval
	cfg *options.LogtailServerCfg

	ssmgr     *SessionManager
	waterline *Waterliner

	errChan chan sessionError // errChan has no buffer in order to improve sensitivity.

	// subReqChan is the channel that contains the subscription request.
	subReqChan chan subscription

	// subTailChan is the channel that contains the pull logtail of first phase.
	// There is another goroutine, that receives these kinds of logtails and begin
	// the second phase of collecting logtails.
	subTailChan chan *LogtailPhase

	// pullWorkerPool is used to control the parallel of the pull workers.
	pullWorkerPool chan struct{}

	event *Notifier

	logtailer taelogtail.Logtailer

	rpc morpc.RPCServer

	rootCtx    context.Context
	cancelFunc context.CancelFunc
	stopper    *stopper.Stopper
}

func defaultRPCServerFactory(
	name string,
	address string,
	logtailServer *LogtailServer,
	options ...morpc.ServerOption,
) (morpc.RPCServer, error) {
	codecOpts := []morpc.CodecOption{
		morpc.WithCodecMaxBodySize(int(logtailServer.cfg.RpcMaxMessageSize)),
	}
	if logtailServer.cfg.RpcEnableChecksum {
		codecOpts = append(codecOpts, morpc.WithCodecEnableChecksum())
	}
	codec := morpc.NewMessageCodec(
		logtailServer.rt.ServiceUUID(),
		func() morpc.Message {
			return logtailServer.pool.requests.Acquire()
		},
		codecOpts...,
	)

	rpc, err := morpc.NewRPCServer(LogtailServiceRPCName, address, codec,
		morpc.WithServerLogger(logtailServer.logger.RawLogger()),
		morpc.WithServerGoettyOptions(
			goetty.WithSessionReleaseMsgFunc(func(v interface{}) {
				msg := v.(morpc.RPCMessage)
				if !msg.InternalMessage() {
					logtailServer.pool.segments.Release(msg.Message.(*LogtailResponseSegment))
				}
			}),
		),
	)

	return rpc, err
}

// NewLogtailServer initializes a server for logtail push model.
func NewLogtailServer(
	address string, cfg *options.LogtailServerCfg, logtailer taelogtail.Logtailer, rt runtime.Runtime,
	rpcServerFactory func(string, string, *LogtailServer, ...morpc.ServerOption) (morpc.RPCServer, error), opts ...ServerOption,
) (*LogtailServer, error) {
	s := &LogtailServer{
		rt:             rt,
		logger:         rt.Logger(),
		cfg:            cfg,
		ssmgr:          NewSessionManager(),
		waterline:      NewWaterliner(),
		errChan:        make(chan sessionError, 1),
		subReqChan:     make(chan subscription, 100),
		subTailChan:    make(chan *LogtailPhase, 300),
		pullWorkerPool: make(chan struct{}, cfg.PullWorkerPoolSize),
		logtailer:      logtailer,
	}

	for _, opt := range opts {
		opt(s)
	}

	uid, _ := uuid.NewV7()
	s.logger = s.logger.Named(LogtailServiceRPCName).
		With(zap.String("server-id", uid.String()))

	s.pool.requests = NewLogtailRequestPool()
	s.pool.responses = NewLogtailResponsePool()
	s.pool.segments = NewLogtailServerSegmentPool(int(s.cfg.RpcMaxMessageSize))
	s.maxChunkSize = s.pool.segments.LeastEffectiveCapacity()
	if s.maxChunkSize <= 0 {
		panic("rpc max message size isn't enough")
	}

	s.logger.Debug("max data chunk size for segment", zap.Int("value", s.maxChunkSize))

	if rpcServerFactory == nil {
		rpcServerFactory = defaultRPCServerFactory
	}

	rpc, err := rpcServerFactory(LogtailServiceRPCName, address, s)
	if err != nil {
		return nil, err
	}

	rpc.RegisterRequestHandler(s.onMessage)
	s.rpc = rpc

	// control background goroutines
	ctx, cancel := context.WithCancel(context.Background())
	s.rootCtx = ctx
	s.cancelFunc = cancel
	s.stopper = stopper.NewStopper(
		LogtailServiceRPCName, stopper.WithLogger(s.logger.RawLogger()),
	)

	// receive logtail on event
	s.event = NewNotifier(s.rootCtx, eventBufferSize)
	logtailer.RegisterCallback(s.event.NotifyLogtail)

	return s, nil
}

// onMessage is the handler for morpc client session.
func (s *LogtailServer) onMessage(
	ctx context.Context,
	value morpc.RPCMessage,
	seq uint64,
	cs morpc.ClientSession,
) error {
	ctx, span := trace.Debug(ctx, "LogtailServer.onMessage")
	defer span.End()

	logger := s.logger
	request := value.Message
	msg, ok := request.(*LogtailRequest)
	if !ok {
		logger.Fatal("receive invalid message", zap.Any("message", request))
	}
	defer s.pool.requests.Release(msg)

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	stream := morpcStream{
		streamID: msg.RequestId,
		remote:   cs.RemoteAddress(),
		limit:    s.maxChunkSize,
		logger:   s.logger,
		cs:       cs,
		segments: s.pool.segments,
	}

	if req := msg.GetSubscribeTable(); req != nil {
		logger.Debug("on subscription", zap.Any("request", req))
		return s.onSubscription(ctx, stream, req)
	}

	if req := msg.GetUnsubscribeTable(); req != nil {
		logger.Debug("on unsubscription", zap.Any("request", req))
		return s.onUnsubscription(ctx, stream, req)
	}

	return moerr.NewInvalidArg(ctx, "request", msg)
}

// onSubscription handls subscription.
func (s *LogtailServer) onSubscription(
	sendCtx context.Context, stream morpcStream, req *logtail.SubscribeRequest,
) error {
	logger := s.logger

	tableID := MarshalTableID(req.Table)
	session := s.ssmgr.GetSession(
		// FIXME: using s.cfg
		s.rootCtx, logger, s.pool.responses, s, stream,
		s.cfg.ResponseSendTimeout,
		s.cfg.RPCStreamPoisonTime,
		s.cfg.LogtailCollectInterval,
	)

	repeated := session.Register(tableID, *req.Table)
	if repeated {
		logger.Info("repeated sub request", zap.String("table ID", string(tableID)))
		return nil
	}

	sub := subscription{
		timeout: ContextTimeout(sendCtx, s.cfg.ResponseSendTimeout),
		tableID: tableID,
		req:     req,
		session: session,
	}

	for {
		select {
		case <-s.rootCtx.Done():
			logger.Error("logtail server context done", zap.Error(s.rootCtx.Err()))
			return s.rootCtx.Err()
		case <-sendCtx.Done():
			logger.Error("request context done", zap.Error(sendCtx.Err()))
			return sendCtx.Err()
		case <-time.After(time.Second):
			logger.Error("cannot send subscription request, retry",
				zap.Int("chan cap", cap(s.subReqChan)),
				zap.Int("chan len", len(s.subReqChan)),
			)
		case s.subReqChan <- sub:
			return nil
		}
	}
}

// onUnsubscription sends response for unsubscription.
func (s *LogtailServer) onUnsubscription(
	sendCtx context.Context, stream morpcStream, req *logtail.UnsubscribeRequest,
) error {
	tableID := MarshalTableID(req.Table)
	session := s.ssmgr.GetSession(
		// FIXME: using s.cfg
		s.rootCtx, s.logger, s.pool.responses, s, stream,
		s.cfg.ResponseSendTimeout,
		s.cfg.RPCStreamPoisonTime,
		s.cfg.LogtailCollectInterval,
	)

	state := session.Unregister(tableID)
	if state == TableNotFound {
		return nil
	}

	return session.SendUnsubscriptionResponse(sendCtx, *req.Table)
}

// NotifySessionError notifies session manager with session error.
func (s *LogtailServer) NotifySessionError(
	session *Session, err error,
) {
	select {
	case <-s.rootCtx.Done():
		s.logger.Error("fail to notify session error", zap.Error(s.rootCtx.Err()))
	case s.errChan <- sessionError{session: session, err: err}:
	}
}

// sessionErrorHandler handles morpc client session writing error.
func (s *LogtailServer) sessionErrorHandler(ctx context.Context) {
	logger := s.logger

	for {
		select {
		case <-ctx.Done():
			logger.Error("stop session error handler", zap.Error(ctx.Err()))
			return

		case e, ok := <-s.errChan:
			if !ok {
				logger.Info("session error channel closed")
				return
			}

			// drop session directly
			if e.err != nil && s.ssmgr.HasSession(e.session.stream) {
				e.session.PostClean()
				s.ssmgr.DeleteSession(e.session.stream)
			}
		}
	}
}

// logtailPullWorker is an independent goroutine, which pull the table asynchronously.
// It generates a response which would be sent to logtail client as the part 1 of the
// whole response.
func (s *LogtailServer) logtailPullWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Error("stop logtail pull worker", zap.Error(ctx.Err()))
			return

		case sub, ok := <-s.subReqChan:
			if !ok {
				s.logger.Info("subscription channel closed")
				return
			}
			// Start a new goroutine to handle the subscription request.
			// There are xxx goroutines working at the same time at most.
			go s.pullLogtailsPhase1(ctx, sub)
		}
	}
}

func (s *LogtailServer) pullLogtailsPhase1(ctx context.Context, sub subscription) {
	// Limit the pull workers.
	s.pullWorkerPool <- struct{}{}
	defer func() { <-s.pullWorkerPool }()

	v2.LogTailSubscriptionCounter.Inc()
	s.logger.Info("handle subscription asynchronously", zap.Any("table", sub.req.Table))

	start := time.Now()
	// Get logtail of phase1. The 'from' time is zero and the 'to' time is the
	// newest time of waterline. The Ts field in the first return value is the
	// 'from' time parameter of next phase call.
	tail, err := s.getSubLogtailPhase(
		ctx,
		sub,
		timestamp.Timestamp{},
		s.waterline.Waterline(),
	)
	if err != nil {
		s.logger.Error("failed to get logtail of phase1 of subscription",
			zap.String("table", string(sub.tableID)),
			zap.Error(err),
		)
		sub.session.Unregister(sub.tableID)
		return
	}
	v2.LogTailPullCollectionPhase1DurationHistogram.Observe(time.Since(start).Seconds())

	for {
		select {
		case <-ctx.Done():
			s.logger.Error("context done in pull table logtails", zap.Error(ctx.Err()))
			return

		case s.subTailChan <- tail:
			return

		default:
			s.logger.Warn("the queue of logtails of phase1 is full")
			time.Sleep(time.Second)
		}
	}
}

// logtailSender sends total or incremental logtail.
func (s *LogtailServer) logtailSender(ctx context.Context) {
	e, ok := <-s.event.C
	if !ok {
		s.logger.Info("publishment channel closed")
		return
	}
	s.waterline.Advance(e.to)
	s.logger.Info("init waterline", zap.String("to", e.to.String()))

	for {
		select {
		case <-ctx.Done():
			s.logger.Error("stop subscription handler", zap.Error(ctx.Err()))
			return

		case tailPhase1, ok := <-s.subTailChan:
			if !ok {
				s.logger.Info("subscription channel closed")
				return
			}

			start := time.Now()
			// Phase 2 of pulling logtails. The 'from' timestamp comes from
			// the value of phase 1.
			tailPhase2, err := s.getSubLogtailPhase(
				ctx,
				tailPhase1.sub,
				*tailPhase1.tail.Ts,
				s.waterline.Waterline(),
			)
			if err != nil {
				tailPhase1.sub.session.Unregister(tailPhase1.sub.tableID)
				s.logger.Error("fail to send subscription response", zap.Error(err))
			} else {
				v2.LogTailPullCollectionPhase2DurationHistogram.Observe(time.Since(start).Seconds())
				s.sendSubscription(ctx, tailPhase1, tailPhase2)
			}

		case e, ok := <-s.event.C:
			if !ok {
				s.logger.Info("publishment channel closed")
				return
			}
			s.publishEvent(ctx, e)
		}
	}
}

func (s *LogtailServer) getSubLogtailPhase(
	ctx context.Context, sub subscription, from, to timestamp.Timestamp,
) (*LogtailPhase, error) {
	sendCtx, cancel := context.WithTimeout(ctx, sub.timeout)
	defer cancel()

	var subErr error
	defer func() {
		if subErr != nil {
			sub.session.Unregister(sub.tableID)
		}
	}()

	table := *sub.req.Table

	var tail logtail.TableLogtail
	var closeCB func()
	moprobe.WithRegion(ctx, moprobe.SubscriptionPullLogTail, func() {
		tail, closeCB, subErr = s.logtailer.TableLogtail(sendCtx, table, from, to)
	})
	if subErr != nil {
		// if error occurs, just send the error immediately.
		if closeCB != nil {
			closeCB()
		}
		s.logger.Error("fail to fetch table total logtail", zap.Error(subErr), zap.Any("table", table))
		if err := sub.session.SendErrorResponse(
			sendCtx, table, moerr.ErrInternal, "fail to fetch table total logtail",
		); err != nil {
			s.logger.Error("fail to send error response", zap.Error(err))
		}
		return nil, subErr
	}

	return &LogtailPhase{
		tail:    tail,
		closeCB: closeCB,
		sub:     sub,
	}, nil
}

func (s *LogtailServer) sendSubscription(ctx context.Context, p1, p2 *LogtailPhase) {
	sub := p1.sub
	sendCtx, cancel := context.WithTimeout(ctx, sub.timeout)
	defer cancel()
	tail, cb := newLogtailMerger(p1, p2).Merge()
	// send subscription response
	if err := sub.session.SendSubscriptionResponse(sendCtx, tail, cb); err != nil {
		s.logger.Error("fail to send subscription response", zap.Error(err))
		sub.session.Unregister(sub.tableID)
		return
	}
	// mark table as subscribed
	sub.session.AdvanceState(sub.tableID)
}

func (s *LogtailServer) publishEvent(ctx context.Context, e event) {
	// NOTE: there's gap between multiple (e.from, e.to], so we
	// maintain waterline to make UpdateResponse monotonous.
	from := s.waterline.Waterline()
	to := e.to

	wraps := make([]wrapLogtail, 0, len(e.logtails))
	for _, tail := range e.logtails {
		// skip empty logtail
		if tail.CkpLocation == "" && len(tail.Commands) == 0 {
			continue
		}
		wraps = append(wraps, wrapLogtail{
			id:   MarshalTableID(tail.GetTable()),
			tail: tail,
		})
	}

	// publish incremental logtail for all subscribed tables
	sessions := s.ssmgr.ListSession()

	if len(sessions) == 0 {
		if e.closeCB != nil {
			e.closeCB()
		}
	} else {
		var refcount atomic.Int32
		closeCB := func() {
			if refcount.Add(-1) == 0 {
				if e.closeCB != nil {
					e.closeCB()
				}
			}
		}
		refcount.Add(int32(len(sessions)))
		for _, session := range sessions {
			if err := session.Publish(ctx, from, to, closeCB, wraps...); err != nil {
				s.logger.Error("fail to publish incremental logtail", zap.Error(err),
					zap.Uint64("stream-id", session.stream.streamID), zap.String("remote", session.stream.remote),
				)
				continue
			}
		}
	}

	// update waterline for all subscribed tables
	s.waterline.Advance(to)
}

func (s *LogtailServer) gcDeletedSessions(ctx context.Context) {
	const gcTimeout = time.Hour * 24 * 7 // one week
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				s.ssmgr.Lock()
				defer s.ssmgr.Unlock()
				var pos int
				for i := range s.ssmgr.deletedClients {
					if time.Since(s.ssmgr.deletedClients[i].deletedAt) > gcTimeout {
						pos++
					} else {
						break
					}
				}
				s.ssmgr.deletedClients = s.ssmgr.deletedClients[pos:]
			}()
		}
	}
}

// Close closes api server.
func (s *LogtailServer) Close() error {
	s.logger.Info("close logtail service")

	s.cancelFunc()
	s.stopper.Stop()
	return s.rpc.Close()
}

// Start starts logtail publishment service.
func (s *LogtailServer) Start() error {
	s.logger.Info("start logtail service")

	if err := s.stopper.RunNamedTask("session error handler", s.sessionErrorHandler); err != nil {
		s.logger.Error("fail to start session error handler", zap.Error(err))
		return err
	}

	if err := s.stopper.RunNamedTask("logtail pull worker", s.logtailPullWorker); err != nil {
		s.logger.Error("fail to start logtail pull worker", zap.Error(err))
		return err
	}

	if err := s.stopper.RunNamedTask("logtail sender", s.logtailSender); err != nil {
		s.logger.Error("fail to start logtail sender", zap.Error(err))
		return err
	}

	if err := s.stopper.RunNamedTask("session cleaner", s.gcDeletedSessions); err != nil {
		s.logger.Error("fail to start session cleaner", zap.Error(err))
		return err
	}

	return s.rpc.Start()
}

// NotifyLogtail provides incremental logtail for server.
func (s *LogtailServer) NotifyLogtail(
	from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail,
) error {
	return s.event.NotifyLogtail(from, to, closeCB, tails...)
}

func (s *LogtailServer) SessionMgr() *SessionManager {
	return s.ssmgr
}
