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
	"math"
	gotrace "runtime/trace"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

const (
	LogtailServiceRPCName = "logtail-push-rpc"

	// FIXME: make this configurable
	// duration to detect slow morpc stream
	RpcStreamPoisionTime = 1 * time.Second
)

type ServerOption func(*LogtailServer)

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
		requests  RequestPool
		responses ResponsePool
		segments  SegmentPool
	}
	maxChunkSize int

	rt     runtime.Runtime
	logger *log.MOLogger

	nowFn func() (timestamp.Timestamp, timestamp.Timestamp)

	cfg *options.LogtailServerCfg

	ssmgr      *SessionManager
	waterline  *Waterliner
	subscribed *TableStacker

	errChan chan sessionError // errChan has no buffer in order to improve sensitivity.
	subChan chan subscription

	logtail taelogtail.Logtailer

	rpc morpc.RPCServer

	rootCtx    context.Context
	cancelFunc context.CancelFunc
	stopper    *stopper.Stopper
}

// NewLogtailServer initializes a server for logtail push model.
func NewLogtailServer(
	address string, cfg *options.LogtailServerCfg, logtail taelogtail.Logtailer, rt runtime.Runtime, opts ...ServerOption,
) (*LogtailServer, error) {
	s := &LogtailServer{
		rt:         rt,
		logger:     rt.Logger(),
		cfg:        cfg,
		ssmgr:      NewSessionManager(),
		waterline:  NewWaterliner(logtail.Now),
		subscribed: NewTableStacker(),
		errChan:    make(chan sessionError),
		subChan:    make(chan subscription, 10),
		logtail:    logtail,
		nowFn:      logtail.Now,
	}

	for _, opt := range opts {
		opt(s)
	}

	s.logger = s.logger.Named(LogtailServiceRPCName).
		With(zap.String("server-id", uuid.NewString()))

	s.pool.requests = NewRequestPool()
	s.pool.responses = NewResponsePool()
	s.pool.segments = NewSegmentPool(int(s.cfg.RpcMaxMessageSize))
	s.maxChunkSize = s.pool.segments.LeastEffectiveCapacity()
	if s.maxChunkSize <= 0 {
		panic("rpc max message size isn't enough")
	}

	s.logger.Debug("max data chunk size for segment", zap.Int("value", s.maxChunkSize))

	codecOpts := []morpc.CodecOption{
		morpc.WithCodecMaxBodySize(int(s.cfg.RpcMaxMessageSize)),
	}
	if s.cfg.RpcEnableChecksum {
		codecOpts = append(codecOpts, morpc.WithCodecEnableChecksum())
	}
	codec := morpc.NewMessageCodec(s.pool.requests.Acquire, codecOpts...)

	rpc, err := morpc.NewRPCServer(LogtailServiceRPCName, address, codec,
		morpc.WithServerLogger(s.logger.RawLogger()),
		morpc.WithServerGoettyOptions(
			goetty.WithSessionReleaseMsgFunc(func(v interface{}) {
				msg := v.(morpc.RPCMessage)
				if !msg.InternalMessage() {
					s.pool.segments.Release(msg.Message.(*LogtailResponseSegment))
				}
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
		LogtailServiceRPCName, stopper.WithLogger(s.logger.RawLogger()),
	)

	return s, nil
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
	defer s.pool.requests.Release(msg)

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	stream := morpcStream{
		streamID: msg.RequestId,
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
		s.rootCtx, logger, s.cfg.ResponseSendTimeout, s.pool.responses, s, stream, s.streamPoisionTime(),
	)

	repeated := session.Register(tableID, *req.Table)
	if repeated {
		return nil
	}

	sub := subscription{
		timeout: ContextTimeout(sendCtx, s.cfg.ResponseSendTimeout),
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
	tableID := MarshalTableID(req.Table)
	session := s.ssmgr.GetSession(
		s.rootCtx, s.logger, s.cfg.ResponseSendTimeout, s.pool.responses, s, stream, s.streamPoisionTime(),
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
	return RpcStreamPoisionTime
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

// logtailSender sends total or incremental logtail.
func (s *LogtailServer) logtailSender(ctx context.Context) {
	logger := s.logger

	activate := false

	publishTicker := time.NewTicker(s.cfg.LogtailCollectInterval)
	defer publishTicker.Stop()

	risk := 0
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

			logger.Info("handle subscription asynchronously", zap.Any("table", sub.req.Table))

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
				var tail logtail.TableLogtail
				gotrace.WithRegion(ctx, "subscription-pull-logtail", func() {
					tail, subErr = s.logtail.TableLogtail(sendCtx, table, from, to)
				})
				if subErr != nil {
					logger.Error("fail to fetch table total logtail", zap.Error(subErr), zap.Any("table", table))
					if err := sub.session.SendErrorResponse(
						sendCtx, table, moerr.ErrInternal, "fail to fetch table total logtail",
					); err != nil {
						logger.Error("fail to send error response", zap.Error(err))
					}
					return
				}

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

				if !activate {
					activate = true
				}
			}

			subscriptionFunc(sub)

		case <-publishTicker.C:
			publishmentFunc := func() {
				// no subscription, no publishment
				if !activate {
					return
				}

				defer func() {
					if risk >= s.cfg.MaxLogtailFetchFailure {
						panic("fail to fetch incremental logtail many times")
					}
				}()

				from := s.waterline.Waterline()
				to, _ := s.nowFn()

				// fetch incremental logtail for tables
				var tails []logtail.TableLogtail
				var err error
				gotrace.WithRegion(ctx, "publishment-pull-logtail", func() {
					tails, err = s.logtail.RangeLogtail(ctx, from, to)
				})
				if err != nil {
					logger.Error("fail to fetch incremental logtail", zap.Error(err))
					risk += 1
					return
				}
				if len(tails) == 1 && tails[0].Table.TbId == math.MaxUint64 {
					panic("unexpected checkpoint within time range")
				}

				// format table ID beforehand
				wraps := make([]wrapLogtail, 0, len(tails))
				for _, tail := range tails {
					// skip empty logtail
					if tail.CkpLocation == "" && len(tail.Commands) == 0 {
						continue
					}
					wraps = append(wraps, wrapLogtail{id: MarshalTableID(tail.GetTable()), tail: tail})
				}

				// publish incremental logtail for all subscribed tables
				for _, session := range s.ssmgr.ListSession() {
					if err := session.Publish(ctx, from, to, wraps...); err != nil {
						logger.Error("fail to publish incremental logtail", zap.Error(err), zap.Uint64("stream-id", session.stream.streamID))
						continue
					}
				}

				// update waterline for all subscribed tables
				s.waterline.Advance(to)
			}

			publishmentFunc()
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

	if err := s.stopper.RunNamedTask("logtail sender", s.logtailSender); err != nil {
		s.logger.Error("fail to start logtail sender", zap.Error(err))
		return err
	}

	return s.rpc.Start()
}
