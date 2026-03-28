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
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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

	// activationTailChan carries phase-1 results for account activation
	// requests, consumed by logtailSender for serialized phase-2 + response.
	activationTailChan chan *catalogActivationPhase1
	// activationReqChan is a bounded queue feeding a fixed activation worker
	// pool, so bursts do not turn directly into unbounded parked goroutines.
	activationReqChan chan catalogActivation

	// pullWorkerPool is used to control the parallel of the pull workers.
	pullWorkerPool chan struct{}

	event *Notifier

	logtailer taelogtail.Logtailer

	rpc morpc.RPCServer

	rootCtx    context.Context
	cancelFunc context.CancelFunc
	stopper    *stopper.Stopper
}

// --- activation types and callback utilities ---

// catalogActivation represents an in-flight activation request.
type catalogActivation struct {
	timeout   time.Duration
	accountID uint32
	seq       uint64
	session   *Session
}

const lazyCatalogTableCount = 3

// catalogActivationPhase1 carries phase-1 pull results for all three catalog
// tables. It is sent from the pull goroutine to the logtailSender for
// serialized phase-2 completion.
type catalogActivationPhase1 struct {
	activation catalogActivation
	tails      [lazyCatalogTableCount]logtail.TableLogtail
	closeCBs   [lazyCatalogTableCount]func()
}

// lazyCatalogTableIDs lists the three catalog tables in a fixed order that
// aligns with the tails/closeCBs arrays in catalogActivationPhase1.
var lazyCatalogTableIDs = [lazyCatalogTableCount]api.TableID{
	{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_DATABASE_ID},
	{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
	{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID},
}

func (p *catalogActivationPhase1) closeAll() {
	for i := range p.closeCBs {
		if p.closeCBs[i] != nil {
			p.closeCBs[i]()
			p.closeCBs[i] = nil
		}
	}
}

func (p *catalogActivationPhase1) takeCloseCB(idx int) func() {
	cb := p.closeCBs[idx]
	p.closeCBs[idx] = nil
	return cb
}

func closeCallbacks(callbacks ...func()) {
	for _, cb := range callbacks {
		if cb != nil {
			cb()
		}
	}
}

func composeCloseCallback(callbacks ...func()) func() {
	var nonNil []func()
	for _, cb := range callbacks {
		if cb != nil {
			nonNil = append(nonNil, cb)
		}
	}
	switch len(nonNil) {
	case 0:
		return nil
	case 1:
		return nonNil[0]
	default:
		return func() {
			closeCallbacks(nonNil...)
		}
	}
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
		rt:                 rt,
		logger:             rt.Logger(),
		cfg:                cfg,
		ssmgr:              NewSessionManager(),
		waterline:          NewWaterliner(),
		errChan:            make(chan sessionError, 1),
		subReqChan:         make(chan subscription, 100),
		subTailChan:        make(chan *LogtailPhase, 300),
		activationTailChan: make(chan *catalogActivationPhase1, 64),
		activationReqChan:  make(chan catalogActivation, activationWorkerCount(cfg)),
		pullWorkerPool:     make(chan struct{}, cfg.PullWorkerPoolSize),
		logtailer:          logtailer,
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

	if req := msg.GetActivateAccountForCatalog(); req != nil {
		return s.onActivateAccountForCatalog(ctx, stream, req)
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
	if err := session.configureLazyCatalogSubscription(req); err != nil {
		session.Unregister(tableID)
		return err
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

func (s *LogtailServer) onActivateAccountForCatalog(
	ctx context.Context, stream morpcStream, req *logtail.ActivateAccountForCatalogRequest,
) error {
	logger := s.logger
	session := s.ssmgr.GetSession(
		s.rootCtx, logger, s.pool.responses, s, stream,
		s.cfg.ResponseSendTimeout,
		s.cfg.RPCStreamPoisonTime,
		s.cfg.LogtailCollectInterval,
	)

	accountID := req.GetAccountId()
	seq := req.GetSeq()

	if !session.beginLazyCatalogActivation(accountID, seq) {
		return moerr.NewNotSupported(ctx, "activate account for catalog on non-lazy session")
	}

	act := catalogActivation{
		timeout:   ContextTimeout(ctx, s.cfg.ResponseSendTimeout),
		accountID: accountID,
		seq:       seq,
		session:   session,
	}

	select {
	case <-s.rootCtx.Done():
		session.abortLazyCatalogActivation(accountID, seq)
		return moerr.AttachCause(s.rootCtx, s.rootCtx.Err())
	case <-ctx.Done():
		session.abortLazyCatalogActivation(accountID, seq)
		return moerr.AttachCause(ctx, ctx.Err())
	case s.activationReqChan <- act:
		return nil
	}
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

func activationWorkerCount(cfg *options.LogtailServerCfg) int {
	if cfg == nil || cfg.PullWorkerPoolSize <= 0 {
		return 1
	}
	return int(cfg.PullWorkerPoolSize)
}

func (s *LogtailServer) activationPullWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Error("stop activation pull worker", zap.Error(ctx.Err()))
			return
		case act, ok := <-s.activationReqChan:
			if !ok {
				s.logger.Info("activation request channel closed")
				return
			}
			s.pullActivationPhase1(ctx, act)
		}
	}
}

// pullActivationPhase1 concurrently pulls historical row-level delta for all
// three catalog tables and sends the combined result to activationTailChan.
func (s *LogtailServer) pullActivationPhase1(ctx context.Context, act catalogActivation) {
	s.pullWorkerPool <- struct{}{}
	defer func() { <-s.pullWorkerPool }()

	s.logger.Info("activation phase1 start",
		zap.Uint32("account-id", act.accountID),
		zap.Uint64("seq", act.seq),
	)

	waterline := s.waterline.Waterline()
	allowedAccounts := newLazyCatalogAllowedAccounts(act.accountID)

	var result catalogActivationPhase1
	result.activation = act

	enqueued := false
	defer func() {
		if !enqueued {
			result.closeAll()
			act.session.abortLazyCatalogActivation(act.accountID, act.seq)
		}
	}()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, table := range lazyCatalogTableIDs {
		wg.Add(1)
		go func(idx int, tbl api.TableID) {
			defer wg.Done()
			tail, closeCB, err := s.pullTableLogtail(
				ctx,
				tbl,
				timestamp.Timestamp{},
				waterline,
				allowedAccounts,
			)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				closeCallbacks(closeCB)
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			result.tails[idx] = tail
			result.closeCBs[idx] = closeCB
		}(i, table)
	}
	wg.Wait()

	if firstErr != nil {
		s.logger.Error("activation phase1 failed",
			zap.Uint32("account-id", act.accountID),
			zap.Uint64("seq", act.seq),
			zap.Error(firstErr),
		)
		return
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Error("context done during activation phase1 enqueue", zap.Error(ctx.Err()))
			return
		case s.activationTailChan <- &result:
			enqueued = true
			return
		default:
			s.logger.Warn("activation tail chan full, retrying")
			time.Sleep(time.Second)
		}
	}
}

func (s *LogtailServer) pullTableLogtail(
	ctx context.Context,
	table api.TableID,
	from, to timestamp.Timestamp,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (logtail.TableLogtail, func(), error) {
	tail, closeCB, err := s.logtailer.TableLogtail(ctx, table, from, to)
	if err != nil {
		return logtail.TableLogtail{}, closeCB, err
	}

	filtered, filterCloseCB, err := filterLazyCatalogPulledTail(tail, allowedAccounts)
	if err != nil {
		closeCallbacks(closeCB, filterCloseCB)
		return logtail.TableLogtail{}, nil, err
	}
	return filtered, composeCloseCallback(closeCB, filterCloseCB), nil
}

// logtailSender sends total or incremental logtail.
func (s *LogtailServer) logtailSender(ctx context.Context) {
	select {
	case <-s.rootCtx.Done():
		s.logger.Info("context done", zap.Error(s.rootCtx.Err()))
		return

	case e, ok := <-s.event.C:
		if !ok {
			s.logger.Info("publishment channel closed")
			return
		}
		s.waterline.Advance(e.to)
		s.logger.Info(
			"logtail.service.init.waterline",
			zap.String("ts", e.to.String()),
		)
	}

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

		case actPhase1, ok := <-s.activationTailChan:
			if !ok {
				s.logger.Info("activation channel closed")
				return
			}
			s.sendActivation(ctx, actPhase1)

		case e, ok := <-s.event.C:
			if !ok {
				s.logger.Info("publishment channel closed")
				return
			}
			s.publishEvent(ctx, e)
		}
	}
}

// sendActivation completes phase-2 for each catalog table, filters rows for
// the target account, builds and sends the ActivateAccountForCatalogResponse,
// and only then adds the account to activeAccounts.
func (s *LogtailServer) sendActivation(ctx context.Context, p1 *catalogActivationPhase1) {
	act := p1.activation
	sendCtx, cancel := context.WithTimeoutCause(ctx, act.timeout, moerr.CauseSendSubscription)
	defer cancel()

	targetTS := s.waterline.Waterline()
	allowedAccounts := newLazyCatalogAllowedAccounts(act.accountID)

	var responseTails []logtail.TableLogtail
	var allCloseCBs []func()
	sent := false

	defer func() {
		if !sent {
			p1.closeAll()
			closeCallbacks(allCloseCBs...)
			act.session.abortLazyCatalogActivation(act.accountID, act.seq)
		}
	}()

	for i, table := range lazyCatalogTableIDs {
		phase1Ts := timestamp.Timestamp{}
		if p1.tails[i].Ts != nil {
			phase1Ts = *p1.tails[i].Ts
		}

		phase2Tail, closeCB, err := s.pullTableLogtail(sendCtx, table, phase1Ts, targetTS, allowedAccounts)
		if err != nil {
			closeCallbacks(closeCB)
			s.logger.Error("activation phase2 failed",
				zap.Uint32("account-id", act.accountID),
				zap.Uint64("seq", act.seq),
				zap.Error(err),
			)
			return
		}
		merged, mergedCloseCB := newLogtailMerger(
			&LogtailPhase{tail: p1.tails[i], closeCB: p1.takeCloseCB(i)},
			&LogtailPhase{tail: phase2Tail, closeCB: closeCB},
		).Merge()
		allCloseCBs = append(allCloseCBs, mergedCloseCB)

		if !isEmptyLogtail(merged) {
			responseTails = append(responseTails, merged)
		}
	}

	// Transfer cleanup ownership to the response path.
	responseCB := composeCloseCallback(allCloseCBs...)
	allCloseCBs = nil

	resp := logtail.ActivateAccountForCatalogResponse{
		AccountId: act.accountID,
		Seq:       act.seq,
		TargetTs:  &targetTS,
		Tails:     responseTails,
	}
	if err := act.session.SendActivateAccountForCatalogResponse(sendCtx, resp, responseCB); err != nil {
		// SendResponse.Release already called responseCB for cleanup.
		s.logger.Error("fail to send activation response",
			zap.Uint32("account-id", act.accountID),
			zap.Uint64("seq", act.seq),
			zap.Error(err),
		)
		return
	}
	sent = true

	// Only after the response has successfully entered the session's FIFO
	// sendChan do we promote the account to active for steady-state push.
	if !act.session.completeLazyCatalogActivation(act.accountID, act.seq) {
		s.logger.Warn("activation seq superseded, account not promoted",
			zap.Uint32("account-id", act.accountID),
			zap.Uint64("seq", act.seq),
		)
	}

	s.logger.Info("activation complete",
		zap.Uint32("account-id", act.accountID),
		zap.Uint64("seq", act.seq),
		zap.String("target-ts", targetTS.String()),
	)
}

func (s *LogtailServer) getSubLogtailPhase(
	ctx context.Context, sub subscription, from, to timestamp.Timestamp,
) (*LogtailPhase, error) {
	sendCtx, cancel := context.WithTimeoutCause(ctx, sub.timeout, moerr.CauseGetSubLogtailPhase)
	defer cancel()

	var subErr error
	defer func() {
		if subErr != nil {
			sub.session.Unregister(sub.tableID)
		}
	}()

	table := *sub.req.Table
	allowedAccounts, _ := sub.session.lazyCatalogSubscribeAccountsForFilter(sub.req)

	var tail logtail.TableLogtail
	var closeCB func()
	moprobe.WithRegion(ctx, moprobe.SubscriptionPullLogTail, func() {
		tail, closeCB, subErr = s.pullTableLogtail(sendCtx, table, from, to, allowedAccounts)
		subErr = moerr.AttachCause(sendCtx, subErr)
	})
	if subErr != nil {
		// if error occurs, just send the error immediately.
		closeCallbacks(closeCB)
		s.logger.Error("fail to fetch table total logtail", zap.Error(subErr), zap.Any("table", table))

		subErrCode, ok := moerr.GetMoErrCode(subErr)
		if !ok {
			subErrCode = moerr.ErrInternal
		}
		subErrMsg := fmt.Sprintf("fail to fetch table total logtail:%s", subErr.Error())

		if err := sub.session.SendErrorResponse(
			sendCtx, table, subErrCode, subErrMsg,
		); err != nil {
			err = moerr.AttachCause(sendCtx, err)
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
	sendCtx, cancel := context.WithTimeoutCause(ctx, sub.timeout, moerr.CauseSendSubscription)
	defer cancel()
	tail, cb := newLogtailMerger(p1, p2).Merge()
	// send subscription response
	if err := sub.session.SendSubscriptionResponse(sendCtx, tail, cb); err != nil {
		err = moerr.AttachCause(sendCtx, err)
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
		if isEmptyLogtail(tail) {
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
		firstLazyCatalogIndex := slices.IndexFunc(wraps, func(w wrapLogtail) bool {
			return catalog.IsLazyCatalogTableID(w.tail.Table.TbId)
		})
		refcount.Add(int32(len(sessions)))
		for _, session := range sessions {
			publishWraps := wraps
			if firstLazyCatalogIndex >= 0 {
				// Event-level fast path: if this batch does not contain the three lazy
				// catalog tables, no session should even enter the lazy publish helper.
				var err error
				publishWraps, err = session.prepareLazyCatalogPublishWrapsFromIndex(wraps, firstLazyCatalogIndex)
				if err != nil {
					err = moerr.AttachCause(ctx, err)
					closeCB()
					s.NotifySessionError(session, err)
					s.logger.Error("fail to filter catalog incremental logtail", zap.Error(err),
						zap.Uint64("stream-id", session.stream.streamID), zap.String("remote", session.stream.remote),
					)
					continue
				}
			}
			if err := session.Publish(ctx, from, to, closeCB, publishWraps...); err != nil {
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

	for i := 0; i < activationWorkerCount(s.cfg); i++ {
		name := fmt.Sprintf("activation pull worker %d", i)
		if err := s.stopper.RunNamedTask(name, s.activationPullWorker); err != nil {
			s.logger.Error("fail to start activation pull worker", zap.Int("worker", i), zap.Error(err))
			return err
		}
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
