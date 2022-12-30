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
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	LogtailServiceRPCName = "logtail-push-rpc"

	KiB = 1024
)

// LogtailFetcher provides logtail for the specified table.
type LogtailFetcher interface {
	// FetchLogtail returns logtail for the specified table.
	FetchLogtail(
		ctx context.Context, table *api.TableID, have, want *timestamp.Timestamp,
	) (*logtail.TableLogtail, error)
}

// TableID is type for api.TableID
type TableID string

type ServerOption func(*LogtailServer)

// WithServerLogger sets logger
func WithServerLogger(logger *zap.Logger) ServerOption {
	return func(s *LogtailServer) {
		s.options.logger = logger
	}
}

// WithServerMaxMessageSize sets max rpc message size
func WithServerMaxMessageSize(maxMessageSize int) ServerOption {
	return func(s *LogtailServer) {
		s.options.maxMessageSize = maxMessageSize
	}
}

// WithServerPayloadCopyBufferSize sets payload copy buffer size
func WithServerPayloadCopyBufferSize(size int) ServerOption {
	return func(s *LogtailServer) {
		s.options.payloadCopyBufferSize = size
	}
}

// WithServerEnableChecksum enables checksum
func WithServerEnableChecksum(enable bool) ServerOption {
	return func(s *LogtailServer) {
		s.options.enableChecksum = enable
	}
}

// WithServerCollectInterval sets logtail collection interval.
func WithServerCollectInterval(interval time.Duration) ServerOption {
	return func(s *LogtailServer) {
		s.options.collectInterval = interval
	}
}

func WithServerSendTimeout(timeout time.Duration) ServerOption {
	return func(s *LogtailServer) {
		s.options.sendTimeout = timeout
	}
}

// tableLogtail describes a table's logtail.
type tableLogtail struct {
	id   TableID
	tail *logtail.TableLogtail
}

// publishment describes a batch of logtail.
type publishment struct {
	want  timestamp.Timestamp
	tails []tableLogtail
}

// sessionError describes error when writing via morpc stream.
type sessionError struct {
	session *Session
	err     error
}

// subscription describes new subscription.
type subscription struct {
	sendCtx context.Context
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

	options struct {
		logger                *zap.Logger
		maxMessageSize        int
		payloadCopyBufferSize int
		enableChecksum        bool
		collectInterval       time.Duration
		sendTimeout           time.Duration
	}

	ssmgr      *SessionManager
	subscribed *Waterliner

	errChan chan sessionError // errChan has no buffer in order to improve sensitivity.
	pubChan chan publishment
	subChan chan subscription

	fetcher LogtailFetcher
	clock   clock.Clock

	rpc morpc.RPCServer

	rootCtx    context.Context
	cancelFunc context.CancelFunc
	stopper    *stopper.Stopper
}

// NewLogtailServer initializes a server for logtail push model.
func NewLogtailServer(
	address string, fetcher LogtailFetcher, clock clock.Clock, opts ...ServerOption,
) (morpc.RPCServer, error) {
	s := &LogtailServer{
		ssmgr:      NewSessionManager(),
		subscribed: NewWaterliner(),
		errChan:    make(chan sessionError),
		pubChan:    make(chan publishment),
		subChan:    make(chan subscription, 10),
		fetcher:    fetcher,
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

	// default configuration
	s.options.maxMessageSize = 1024 * KiB
	s.options.payloadCopyBufferSize = 1024 * KiB
	s.options.logger = logutil.GetGlobalLogger().
		Named(LogtailServiceRPCName).
		With(zap.String("uuid", uuid.NewString()))
	s.options.enableChecksum = true
	s.options.collectInterval = 50 * time.Millisecond
	s.options.sendTimeout = 5 * time.Second

	for _, opt := range opts {
		opt(s)
	}

	codec := morpc.NewMessageCodec(s.acquireRequest,
		morpc.WithCodecPayloadCopyBufferSize(s.options.payloadCopyBufferSize),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(s.options.maxMessageSize),
	)

	rpc, err := morpc.NewRPCServer(LogtailServiceRPCName, address, codec,
		morpc.WithServerLogger(s.options.logger),
		morpc.WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(v interface{}) {
			m := v.(morpc.RPCMessage)
			s.ReleaseResponse(m.Message.(*LogtailResponse))
		})),
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
		LogtailServiceRPCName, stopper.WithLogger(s.options.logger),
	)

	return rpc, nil
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

// onMessage is the handler for morpc stream.
func (s *LogtailServer) onMessage(
	ctx context.Context, request morpc.Message, seq uint64, cs morpc.ClientSession,
) error {
	ctx, span := trace.Debug(ctx, "LogtailServer.onMessage")
	defer span.End()

	logger := s.options.logger

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

	if req := msg.GetSubscribeTable(); req != nil {
		logger.Debug("handle subscritpion", zap.Any("request", req))
		return s.onSubscription(ctx, cs, req)
	}

	if req := msg.GetUnsubscribeTable(); req != nil {
		logger.Debug("handle unsubscritpion", zap.Any("request", req))
		return s.onUnsubscription(ctx, cs, req)
	}

	return moerr.NewInvalidArg(ctx, "request", msg)
}

// onSubscription handls subscription.
func (s *LogtailServer) onSubscription(
	sendCtx context.Context, cs morpc.ClientSession, req *logtail.SubscribeRequest,
) error {
	session := s.ssmgr.GetSession(
		s.rootCtx, s.options.logger, s.options.sendTimeout, s, s, cs,
	)

	tableID := TableID(req.Table.String())
	duplicated := session.Register(tableID, *req.Table)
	if duplicated {
		return session.SendErrorResponse(sendCtx, *req.Table, moerr.ErrDuplicate, "duplicated subscription")
	}

	sub := subscription{
		sendCtx: sendCtx,
		tableID: tableID,
		req:     req,
		session: session,
	}

	select {
	case <-s.rootCtx.Done():
		s.options.logger.Error("fail to handle subscription", zap.Error(s.rootCtx.Err()))
		return s.rootCtx.Err()
	case <-sendCtx.Done():
		s.options.logger.Error("fail to handle subscription", zap.Error(sendCtx.Err()))
		return sendCtx.Err()
	case s.subChan <- sub:
	}

	return nil
}

// onUnsubscription sends response for unsubscription.
func (s *LogtailServer) onUnsubscription(
	sendCtx context.Context, cs morpc.ClientSession, req *logtail.UnsubscribeRequest,
) error {
	session := s.ssmgr.GetSession(
		s.rootCtx, s.options.logger, s.options.sendTimeout, s, s, cs,
	)

	tableID := TableID(req.Table.String())
	state := session.Unregister(tableID)
	if state == TableNotFound {
		return session.SendErrorResponse(sendCtx, *req.Table, moerr.ErrNotFound, "subscription not found")
	}

	if state == TableSubscribed {
		s.subscribed.Unregister(tableID)
	}

	return session.SendUnsubscriptionResponse(sendCtx, *req.Table)
}

// notifySessionError notifies session manager with session error.
func (s *LogtailServer) NotifySessionError(
	session *Session, err error,
) {
	select {
	case <-s.rootCtx.Done():
		s.options.logger.Error("fail to notify session error", zap.Error(s.rootCtx.Err()))
	case s.errChan <- sessionError{session: session, err: err}:
	}
}

// sessionErrorHandler handles morpc stream writing error.
func (s *LogtailServer) sessionErrorHandler(ctx context.Context) {
	logger := s.options.logger

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
				s.ssmgr.DeleteSession(e.session.cs)
				e.session.Drop()
				s.subscribed.Unregister(e.session.ListSubscribedTable()...)
			}
		}
	}
}

// logtailSender sends total or additional logtail.
func (s *LogtailServer) logtailSender(ctx context.Context) {
	logger := s.options.logger
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

			logger.Debug("start to handle subscription", zap.Any("table", sub.req.Table))

			// evaluate waterline for the table
			want, exist := s.subscribed.Waterline(sub.tableID)
			if !exist {
				want, _ = s.clock.Now()
			}

			// fetch logtail
			tail, err := s.fetcher.FetchLogtail(sub.sendCtx, sub.req.Table, nil, &want)
			if err != nil {
				logger.Error("fail to fetch logtail", zap.Error(err))

				if err := sub.session.SendErrorResponse(
					sub.sendCtx, *sub.req.Table, moerr.ErrInternal, "fail to fetch logtail",
				); err != nil {
					logger.Error("fail to send error response", zap.Error(err))
				}

				continue
			}

			// send subscription response
			if err := sub.session.SendSubscriptionResponse(sub.sendCtx, tail); err != nil {
				logger.Error("fail to send subscription response", zap.Error(err))
				continue
			}

			// mark table as subscribed
			sub.session.AdvanceState(sub.tableID)

			// register subscribed table
			s.subscribed.Register(sub.tableID, *sub.req.Table, want)

		case pub, ok := <-s.pubChan:
			if !ok {
				logger.Info("publishment channel closed")
				return
			}

			// publish all subscribed tables via session manager
			for _, session := range s.ssmgr.ListSession() {
				if err := session.Publish(ctx, pub.tails...); err != nil {
					logger.Error("fail to publish additional logtail", zap.Error(err))
					continue
				}
			}

			// update waterline for all subscribed tables
			s.subscribed.AdvanceWaterline(pub.want)
		}
	}
}

// collector collects logtail by interval.
func (s *LogtailServer) collector(ctx context.Context) {
	logger := s.options.logger

	ticker := time.NewTicker(s.options.collectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Error("stop logtail collector", zap.Error(ctx.Err()))
			return

		case <-ticker.C:
			// take a snapshot for all subscribed tables
			tables := s.subscribed.ListTable()

			// take current timestamp
			want, _ := s.clock.Now()

			tails := make([]tableLogtail, 0, len(tables))
			for _, t := range tables {
				tail, err := s.fetcher.FetchLogtail(ctx, &t.table, &t.waterline, &want)
				if err != nil {
					logger.Error("fail to fetch logtail", zap.Error(err), zap.Any("table", t.table))
					continue
				}
				tails = append(tails, tableLogtail{id: t.id, tail: tail})
			}

			s.pubChan <- publishment{want: want, tails: tails}
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
	logger := s.options.logger

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
