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
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

const (
	LogtailServiceRPCName = "logtail-push-rpc"
)

// TableID is type for api.TableID
type TableID string

type ServerOption func(*LogtailServer)

// WithServerLogger sets logger
func WithServerLogger(logger *zap.Logger) ServerOption {
	return func(s *LogtailServer) {
		s.logger = logger
	}
}

// WithServerMaxMessageSize sets max rpc message size
func WithServerMaxMessageSize(maxMessageSize int64) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.RpcMaxMessageSize = maxMessageSize
	}
}

// WithServerPayloadCopyBufferSize sets payload copy buffer size
func WithServerPayloadCopyBufferSize(size int64) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.RpcPayloadCopyBufferSize = size
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

func WithServerSendTimeout(timeout time.Duration) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.ResponseSendTimeout = timeout
	}
}

func WithServerMaxLogtailFetchFailure(max int) ServerOption {
	return func(s *LogtailServer) {
		s.cfg.MaxLogtailFetchFailure = max
	}
}

// tableLogtail describes a table's logtail.
type wrapLogtail struct {
	id   TableID
	tail logtail.TableLogtail
}

// publishment describes a batch of logtail.
type publishment struct {
	from, to timestamp.Timestamp
	wraps    []wrapLogtail
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
		requests  *sync.Pool
		responses *sync.Pool
	}

	logger *zap.Logger
	cfg    *options.LogtailServerCfg

	ssmgr      *SessionManager
	waterline  *Waterliner
	subscribed *TableStacker

	errChan chan sessionError // errChan has no buffer in order to improve sensitivity.
	pubChan chan publishment
	subChan chan subscription

	logtail taelogtail.Logtailer
	clock   clock.Clock

	rpc morpc.RPCServer

	rootCtx    context.Context
	cancelFunc context.CancelFunc
	stopper    *stopper.Stopper
}

// NewLogtailServer initializes a server for logtail push model.
func NewLogtailServer(
	address string, cfg *options.LogtailServerCfg, logtail taelogtail.Logtailer, clock clock.Clock, opts ...ServerOption,
) (*LogtailServer, error) {
	s := &LogtailServer{
		logger:     logutil.GetLogger(),
		cfg:        cfg,
		ssmgr:      NewSessionManager(),
		waterline:  NewWaterliner(clock),
		subscribed: NewTableStacker(),
		errChan:    make(chan sessionError),
		pubChan:    make(chan publishment),
		subChan:    make(chan subscription, 10),
		logtail:    logtail,
		clock:      clock,
	}

	s.pool.requests = &sync.Pool{
		New: func() any {
			return &LogtailRequest{}
		},
	}
	s.pool.responses = &sync.Pool{
		New: func() any {
			return &LogtailResponse{}
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	s.logger = s.logger.
		With(zap.String("server-id", uuid.NewString()))

	codecOpts := []morpc.CodecOption{
		morpc.WithCodecPayloadCopyBufferSize(int(s.cfg.RpcPayloadCopyBufferSize)),
		morpc.WithCodecMaxBodySize(int(s.cfg.RpcMaxMessageSize)),
	}
	if s.cfg.RpcEnableChecksum {
		codecOpts = append(codecOpts, morpc.WithCodecEnableChecksum())
	}
	codec := morpc.NewMessageCodec(s.acquireRequest, codecOpts...)

	rpc, err := morpc.NewRPCServer(LogtailServiceRPCName, address, codec,
		morpc.WithServerLogger(s.logger),
		morpc.WithServerGoettyOptions(
			goetty.WithSessionReleaseMsgFunc(func(v interface{}) {
				m := v.(morpc.RPCMessage)
				s.ReleaseResponse(m.Message.(*LogtailResponse))
			}),
		),
	)
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
		LogtailServiceRPCName, stopper.WithLogger(s.logger),
	)

	return s, nil
}

// AcquireResponse fetches LogtailResponse from pool.
func (s *LogtailServer) AcquireResponse() *LogtailResponse {
	return s.pool.responses.Get().(*LogtailResponse)
}

// ReleaseResponse gives LogtailResponse back to pool.
func (s *LogtailServer) ReleaseResponse(resp *LogtailResponse) {
	resp.Reset()
	s.pool.responses.Put(resp)
}

// acquireRequest fetches LogtailRequest from pool.
func (s *LogtailServer) acquireRequest() morpc.Message {
	return s.pool.requests.Get().(*LogtailRequest)
}

// releaseRequest gives LogtailRequest back to pool.
func (s *LogtailServer) releaseRequest(req *LogtailRequest) {
	req.Reset()
	s.pool.requests.Put(req)
}

// onMessage is the handler for morpc client session.
func (s *LogtailServer) onMessage(
	ctx context.Context, request morpc.Message, seq uint64, cs morpc.ClientSession,
) error {
	ctx, span := trace.Debug(ctx, "LogtailServer.onMessage")
	defer span.End()

	logger := s.logger

	msg, ok := request.(*LogtailRequest)
	if !ok {
		logger.Fatal("receive invalid message", zap.Any("message", request))
	}
	defer s.releaseRequest(msg)

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	stream := morpcStream{
		id: msg.RequestId,
		cs: cs,
	}

	if req := msg.GetSubscribeTable(); req != nil {
		logger.Debug("on subscritpion", zap.Any("request", req))
		return s.onSubscription(ctx, stream, req)
	}

	if req := msg.GetUnsubscribeTable(); req != nil {
		logger.Debug("on unsubscritpion", zap.Any("request", req))
		return s.onUnsubscription(ctx, stream, req)
	}

	return moerr.NewInvalidArg(ctx, "request", msg)
}

// onSubscription handls subscription.
func (s *LogtailServer) onSubscription(
	sendCtx context.Context, stream morpcStream, req *logtail.SubscribeRequest,
) error {
	logger := s.logger

	tableID := TableID(req.Table.String())
	session := s.ssmgr.GetSession(
		s.rootCtx, logger, s.cfg.ResponseSendTimeout, s, s, stream, s.streamPoisionTime(),
	)

	repeated := session.Register(tableID, *req.Table)
	if repeated {
		return nil
	}

	timeout := s.cfg.ResponseSendTimeout
	if deadline, ok := sendCtx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	sub := subscription{
		timeout: timeout,
		tableID: tableID,
		req:     req,
		session: session,
	}

	select {
	case <-s.rootCtx.Done():
		logger.Error("logtail server context done", zap.Error(s.rootCtx.Err()))
		return s.rootCtx.Err()
	case <-sendCtx.Done():
		logger.Error("request context done", zap.Error(sendCtx.Err()))
		return sendCtx.Err()
	case s.subChan <- sub:
	}

	return nil
}

// onUnsubscription sends response for unsubscription.
func (s *LogtailServer) onUnsubscription(
	sendCtx context.Context, stream morpcStream, req *logtail.UnsubscribeRequest,
) error {
	tableID := TableID(req.Table.String())
	session := s.ssmgr.GetSession(
		s.rootCtx, s.logger, s.cfg.ResponseSendTimeout, s, s, stream, s.streamPoisionTime(),
	)

	state := session.Unregister(tableID)
	if state == TableNotFound {
		return nil
	}

	if state == TableSubscribed {
		s.subscribed.Unregister(tableID)
	}

	return session.SendUnsubscriptionResponse(sendCtx, *req.Table)
}

// streamPoisionTime returns poision duration for stream.
func (s *LogtailServer) streamPoisionTime() time.Duration {
	return s.cfg.LogtailCollectInterval/2 + 1
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
			if e.err != nil {
				e.session.PostClean()
				s.ssmgr.DeleteSession(e.session.stream)
				s.subscribed.Unregister(e.session.ListSubscribedTable()...)
			}
		}
	}
}

// logtailSender sends total or additional logtail.
func (s *LogtailServer) logtailSender(ctx context.Context) {
	logger := s.logger

	for {
		select {
		case <-ctx.Done():
			logger.Error("stop subscription handler", zap.Error(ctx.Err()))
			return

		case sub, ok := <-s.subChan:
			if !ok {
				logger.Info("subscription channel closed")
				return
			}

			logger.Debug("handle subscription asynchronously", zap.Any("table", sub.req.Table))

			subscriptionFunc := func(sub subscription) {
				sendCtx, cancel := context.WithTimeout(ctx, sub.timeout)
				defer cancel()

				var subErr error
				defer func() {
					if subErr != nil {
						sub.session.Unregister(sub.tableID)
					}
				}()

				table := *sub.req.Table
				from := timestamp.Timestamp{}
				to := s.waterline.Waterline()

				// fetch total logtail for table
				tail, subErr := s.logtail.TableLogtail(sendCtx, table, from, to)
				if subErr != nil {
					logger.Error("fail to fetch table total logtail", zap.Error(subErr), zap.Any("table", table))
					if err := sub.session.SendErrorResponse(
						sendCtx, table, moerr.ErrInternal, "fail to fetch table total logtail",
					); err != nil {
						logger.Error("fail to send error response", zap.Error(err))
					}
					return
				}

				logger.Debug("send subscription response", zap.Any("table", sub.req.Table), zap.Any("To", to.String()))

				// send subscription response
				subErr = sub.session.SendSubscriptionResponse(sendCtx, tail)
				if subErr != nil {
					logger.Error("fail to send subscription response", zap.Error(subErr))
					return
				}

				// mark table as subscribed
				sub.session.AdvanceState(sub.tableID)

				// register subscribed table
				s.subscribed.Register(sub.tableID, table)
			}

			subscriptionFunc(sub)

		case pub, ok := <-s.pubChan:
			if !ok {
				logger.Info("publishment channel closed")
				return
			}

			logger.Debug("publish additional logtail", zap.Any("From", pub.from.String()), zap.Any("To", pub.to.String()))

			// publish additional logtail for all subscribed tables
			for _, session := range s.ssmgr.ListSession() {
				if err := session.Publish(ctx, pub.from, pub.to, pub.wraps...); err != nil {
					logger.Error("fail to publish additional logtail", zap.Error(err), zap.Uint64("stream-id", session.stream.id))
					continue
				}
			}

			// update waterline for all subscribed tables
			s.waterline.Advance(pub.to)
		}
	}
}

// collector collects logtail by interval.
func (s *LogtailServer) collector(ctx context.Context) {
	logger := s.logger

	ticker := time.NewTicker(s.cfg.LogtailCollectInterval)
	defer ticker.Stop()

	risk := 0
	for {
		select {
		case <-ctx.Done():
			logger.Error("stop logtail collector", zap.Error(ctx.Err()))
			return

		case <-ticker.C:
			collectFunc := func() {
				defer func() {
					if risk >= s.cfg.MaxLogtailFetchFailure {
						panic("fail to fetch additional logtail many times")
					}
				}()

				from := s.waterline.Waterline()
				to, _ := s.clock.Now()

				tables := s.subscribed.ListTable()
				wraps := make([]wrapLogtail, 0, len(tables))
				for _, t := range tables {
					tail, err := s.logtail.TableLogtail(ctx, t.table, from, to)
					if err != nil {
						logger.Error("fail to fetch additional logtail", zap.Error(err), zap.Any("table", t.table))
						risk += 1
						return
					}
					wraps = append(wraps, wrapLogtail{id: t.id, tail: tail})
				}

				select {
				case <-ctx.Done():
					logger.Error("fail to convey additional logtail", zap.Error(ctx.Err()))
				case s.pubChan <- publishment{from: from, to: to, wraps: wraps}:
					risk = 0
				}
			}

			collectFunc()
		}
	}
}

// Close closes api server.
func (s *LogtailServer) Close() error {
	s.cancelFunc()
	s.stopper.Stop()
	return s.rpc.Close()
}

// Start starts logtail publishment service.
func (s *LogtailServer) Start() error {
	logger := s.logger

	if err := s.stopper.RunNamedTask("session error handler", s.sessionErrorHandler); err != nil {
		logger.Error("fail to start session error handler", zap.Error(err))
		return err
	}

	if err := s.stopper.RunNamedTask("logtail sender", s.logtailSender); err != nil {
		logger.Error("fail to start logtail sender", zap.Error(err))
		return err
	}

	if err := s.stopper.RunNamedTask("logtail collector", s.collector); err != nil {
		logger.Error("fail to start logtail collector", zap.Error(err))
		return err
	}

	return s.rpc.Start()
}
