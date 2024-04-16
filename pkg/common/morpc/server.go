// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// WithServerLogger set rpc server logger
func WithServerLogger(logger *zap.Logger) ServerOption {
	return func(rs *server) {
		rs.logger = logger
	}
}

// WithServerSessionBufferSize set the buffer size of the write response chan.
// Default is 16.
func WithServerSessionBufferSize(size int) ServerOption {
	return func(s *server) {
		s.options.bufferSize = size
	}
}

// WithServerWriteFilter set write filter func. Input ready to send Messages, output
// is really need to be send Messages.
func WithServerWriteFilter(filter func(Message) bool) ServerOption {
	return func(s *server) {
		s.options.filter = filter
	}
}

// WithServerGoettyOptions set write filter func. Input ready to send Messages, output
// is really need to be send Messages.
func WithServerGoettyOptions(options ...goetty.Option) ServerOption {
	return func(s *server) {
		s.options.goettyOptions = options
	}
}

// WithServerBatchSendSize set the maximum number of messages to be sent together
// at each batch. Default is 8.
func WithServerBatchSendSize(size int) ServerOption {
	return func(s *server) {
		s.options.batchSendSize = size
	}
}

// WithServerDisableAutoCancelContext disable automatic cancel messaging for the context.
// The server will receive RPC messages from the client, each message comes with a Context,
// and morpc will call the handler to process it, and when the handler returns, the Context
// will be auto cancel the context. But in some scenarios, the handler is asynchronous,
// so morpc can't directly cancel the context after the handler returns, otherwise many strange
// problems will occur.
func WithServerDisableAutoCancelContext() ServerOption {
	return func(s *server) {
		s.options.disableAutoCancelContext = true
	}
}

type server struct {
	name        string
	metrics     *serverMetrics
	address     string
	logger      *zap.Logger
	codec       Codec
	application goetty.NetApplication
	stopper     *stopper.Stopper
	handler     func(ctx context.Context, request RPCMessage, sequence uint64, cs ClientSession) error
	sessions    *sync.Map // session-id => *clientSession
	options     struct {
		goettyOptions            []goetty.Option
		bufferSize               int
		batchSendSize            int
		filter                   func(Message) bool
		disableAutoCancelContext bool
	}
	pool struct {
		futures *sync.Pool
	}
}

// NewRPCServer create rpc server with options. After the rpc server starts, one link corresponds to two
// goroutines, one read and one write. All messages to be written are first written to a buffer chan and
// sent to the client by the write goroutine.
func NewRPCServer(
	name, address string,
	codec Codec,
	options ...ServerOption) (RPCServer, error) {
	s := &server{
		name:     name,
		metrics:  newServerMetrics(name),
		address:  address,
		codec:    codec,
		stopper:  stopper.NewStopper(name),
		sessions: &sync.Map{},
	}
	for _, opt := range options {
		opt(s)
	}
	s.adjust()

	s.options.goettyOptions = append(s.options.goettyOptions,
		goetty.WithSessionCodec(codec),
		goetty.WithSessionLogger(s.logger))

	app, err := goetty.NewApplication(
		s.address,
		s.onMessage,
		goetty.WithAppLogger(s.logger),
		goetty.WithAppSessionOptions(s.options.goettyOptions...),
	)
	if err != nil {
		s.logger.Error("create rpc server failed",
			zap.Error(err))
		return nil, err
	}
	s.application = app
	s.pool.futures = &sync.Pool{
		New: func() interface{} {
			return newFuture(s.releaseFuture)
		},
	}
	if err := s.stopper.RunTask(s.closeDisconnectedSession); err != nil {
		panic(err)
	}
	return s, nil
}

func (s *server) Start() error {
	err := s.application.Start()
	if err != nil {
		s.logger.Fatal("start rpc server failed",
			zap.Error(err))
		return err
	}
	return nil
}

func (s *server) Close() error {
	s.stopper.Stop()
	err := s.application.Stop()
	if err != nil {
		s.logger.Error("stop rpc server failed",
			zap.Error(err))
	}

	return err
}

func (s *server) RegisterRequestHandler(handler func(
	ctx context.Context,
	request RPCMessage,
	sequence uint64,
	cs ClientSession) error) {
	s.handler = handler
}

func (s *server) adjust() {
	s.logger = logutil.Adjust(s.logger).With(zap.String("name", s.name))
	if s.options.batchSendSize == 0 {
		s.options.batchSendSize = 8
	}
	if s.options.bufferSize == 0 {
		s.options.bufferSize = 16
	}
	if s.options.filter == nil {
		s.options.filter = func(messages Message) bool {
			return true
		}
	}
}

func (s *server) onMessage(rs goetty.IOSession, value any, sequence uint64) error {
	s.metrics.receiveCounter.Inc()

	cs, err := s.getSession(rs)
	if err != nil {
		return err
	}
	request := value.(RPCMessage)
	s.metrics.inputBytesCounter.Add(float64(request.Message.Size()))
	if ce := s.logger.Check(zap.DebugLevel, "received request"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Uint64("request-id", request.Message.GetID()),
			zap.String("request", request.Message.DebugString()))
	}

	// Can't be sure that the Context is properly consumed if disableAutoCancelContext is set to
	// true. So we use the pessimistic wait for the context to time out automatically be canceled
	// behavior here, which may cause some resources to be released more slowly.
	// FIXME: Use the CancelFunc pass to let the handler decide to cancel itself
	if !s.options.disableAutoCancelContext && request.Cancel != nil {
		defer request.Cancel()
	}
	// get requestID here to avoid data race, because the request maybe released in handler
	requestID := request.Message.GetID()

	if request.stream &&
		!cs.validateStreamRequest(requestID, request.streamSequence) {
		s.logger.Error("failed to handle stream request",
			zap.Uint32("last-sequence", cs.receivedStreamSequences[requestID]),
			zap.Uint32("current-sequence", request.streamSequence),
			zap.String("client", rs.RemoteAddress()))
		cs.cancelWrite()
		return moerr.NewStreamClosedNoCtx()
	}

	// handle internal message
	if request.internal {
		if m, ok := request.Message.(*flagOnlyMessage); ok {
			switch m.flag {
			case flagPing:
				sendAt := time.Now()
				n := len(cs.c)
				err := cs.WriteRPCMessage(RPCMessage{
					Ctx:      request.Ctx,
					internal: true,
					Message: &flagOnlyMessage{
						flag: flagPong,
						id:   m.id,
					},
				})
				if err != nil {
					failedAt := time.Now()
					s.logger.Error("handle ping failed",
						zap.Time("sendAt", sendAt),
						zap.Time("failedAt", failedAt),
						zap.Int("queue-size", n),
						zap.Error(err))
				}
				return nil
			default:
				panic(fmt.Sprintf("invalid internal message, flag %d", m.flag))
			}
		}
	}

	if err := s.handler(request.Ctx, request, sequence, cs); err != nil {
		s.logger.Error("handle request failed",
			zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Error(err))
		cs.cancelWrite()
		return err
	}

	if ce := s.logger.Check(zap.DebugLevel, "handle request completed"); ce != nil {
		ce.Write(zap.Uint64("sequence", sequence),
			zap.String("client", rs.RemoteAddress()),
			zap.Uint64("request-id", requestID))
	}
	return nil
}

func (s *server) startWriteLoop(cs *clientSession) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		defer s.closeClientSession(cs)

		responses := make([]*Future, 0, s.options.batchSendSize)
		needClose := make([]*Future, 0, s.options.batchSendSize)
		fetch := func() {
			defer func() {
				cs.metrics.sendingQueueSizeGauge.Set(float64(len(cs.c)))
			}()

			for i := 0; i < len(responses); i++ {
				responses[i] = nil
			}
			for i := 0; i < len(needClose); i++ {
				needClose[i] = nil
			}
			responses = responses[:0]
			needClose = needClose[:0]

			for i := 0; i < s.options.batchSendSize; i++ {
				if len(responses) == 0 {
					select {
					case <-ctx.Done():
						responses = nil
						return
					case <-cs.ctx.Done():
						responses = nil
						return
					case resp, ok := <-cs.c:
						if ok {
							responses = append(responses, resp)
						}
					}
				} else {
					select {
					case <-ctx.Done():
						return
					case <-cs.ctx.Done():
						return
					case resp, ok := <-cs.c:
						if ok {
							responses = append(responses, resp)
						}
					default:
						return
					}
				}
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-cs.ctx.Done():
				return
			default:
				fetch()

				if len(responses) > 0 {
					s.metrics.sendingBatchSizeGauge.Set(float64(len(responses)))

					start := time.Now()

					var fields []zap.Field
					ce := s.logger.Check(zap.DebugLevel, "write responses")
					if ce != nil {
						fields = append(fields, zap.String("client", cs.conn.RemoteAddress()))
					}

					written := responses[:0]
					timeout := time.Duration(0)
					for _, f := range responses {
						s.metrics.writeLatencyDurationHistogram.Observe(start.Sub(f.send.createAt).Seconds())
						if f.oneWay {
							needClose = append(needClose, f)
						}

						if !s.options.filter(f.send.Message) {
							f.messageSent(messageSkipped)
							continue
						}

						if f.send.Timeout() {
							f.messageSent(f.send.Ctx.Err())
							continue
						}

						v, err := f.send.GetTimeoutFromContext()
						if err != nil {
							f.messageSent(err)
							continue
						}

						timeout += v
						// Record the information of some responses in advance, because after flush,
						// these responses will be released, thus avoiding causing data race.
						if ce != nil {
							fields = append(fields, zap.Uint64("request-id",
								f.send.Message.GetID()))
							fields = append(fields, zap.String("response",
								f.send.Message.DebugString()))
						}
						if err := cs.conn.Write(f.send, goetty.WriteOptions{}); err != nil {
							s.logger.Error("write response failed",
								zap.Uint64("request-id", f.send.Message.GetID()),
								zap.Error(err))
							f.messageSent(err)
							return
						}
						written = append(written, f)
					}

					if len(written) > 0 {
						s.metrics.outputBytesCounter.Add(float64(cs.conn.OutBuf().Readable()))
						err := cs.conn.Flush(timeout)
						if err != nil {
							if ce != nil {
								fields = append(fields, zap.Error(err))
							}
							for _, f := range responses {
								if s.options.filter(f.send.Message) {
									id := f.getSendMessageID()
									s.logger.Error("write response failed",
										zap.Uint64("request-id", id),
										zap.Error(err))
									f.messageSent(err)
								}
							}
						}
						if ce != nil {
							ce.Write(fields...)
						}
						if err != nil {
							return
						}
					}

					for _, f := range written {
						f.messageSent(nil)
					}
					for _, f := range needClose {
						f.Close()
					}

					s.metrics.writeDurationHistogram.Observe(time.Since(start).Seconds())
				}
			}
		}
	})
}

func (s *server) closeClientSession(cs *clientSession) {
	s.sessions.Delete(cs.conn.ID())
	s.metrics.sessionSizeGauge.Set(float64(s.getSessionCount()))
	if err := cs.Close(); err != nil {
		s.logger.Error("close client session failed",
			zap.Error(err))
	}
}

func (s *server) getSession(rs goetty.IOSession) (*clientSession, error) {
	if v, ok := s.sessions.Load(rs.ID()); ok {
		return v.(*clientSession), nil
	}

	cs := newClientSession(s.metrics, rs, s.codec, s.newFuture)
	v, loaded := s.sessions.LoadOrStore(rs.ID(), cs)
	if loaded {
		close(cs.c)
		return v.(*clientSession), nil
	}

	s.metrics.sessionSizeGauge.Set(float64(s.getSessionCount()))
	rs.Ref()
	if err := s.startWriteLoop(cs); err != nil {
		s.closeClientSession(cs)
		return nil, err
	}
	return cs, nil
}

func (s *server) releaseFuture(f *Future) {
	f.reset()
	s.pool.futures.Put(f)
}

func (s *server) newFuture() *Future {
	return s.pool.futures.Get().(*Future)
}

func (s *server) closeDisconnectedSession(ctx context.Context) {
	// TODO(fagongzi): modify goetty to support connection event
	timer := time.NewTicker(time.Second * 10)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.sessions.Range(func(key, value any) bool {
				id := key.(uint64)
				rs, err := s.application.GetSession(id)
				if err == nil && rs == nil {
					s.closeClientSession(value.(*clientSession))
				}
				return true
			})
		}
	}
}

func (s *server) getSessionCount() int {
	n := 0
	s.sessions.Range(func(key, value any) bool {
		n++
		return true
	})
	return n
}

type clientSession struct {
	metrics       *serverMetrics
	codec         Codec
	conn          goetty.IOSession
	c             chan *Future
	newFutureFunc func() *Future
	// streaming id -> last received sequence, no concurrent, access in io goroutine
	receivedStreamSequences map[uint64]uint32
	// streaming id -> last sent sequence, multi-stream access in multi-goroutines if
	// the tcp connection is shared. But no concurrent in one stream.
	sentStreamSequences   sync.Map
	cancel                context.CancelFunc
	ctx                   context.Context
	checkTimeoutCacheOnce sync.Once
	closedC               chan struct{}
	mu                    struct {
		sync.RWMutex
		closed bool
		caches map[uint64]cacheWithContext
	}
}

func newClientSession(
	metrics *serverMetrics,
	conn goetty.IOSession,
	codec Codec,
	newFutureFunc func() *Future) *clientSession {
	ctx, cancel := context.WithCancel(context.Background())
	cs := &clientSession{
		metrics:                 metrics,
		closedC:                 make(chan struct{}),
		codec:                   codec,
		c:                       make(chan *Future, 1024),
		receivedStreamSequences: make(map[uint64]uint32),
		conn:                    conn,
		ctx:                     ctx,
		cancel:                  cancel,
		newFutureFunc:           newFutureFunc,
	}
	cs.mu.caches = make(map[uint64]cacheWithContext)
	return cs
}

func (cs *clientSession) RemoteAddress() string {
	return cs.conn.RemoteAddress()
}

func (cs *clientSession) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.mu.closed {
		return nil
	}
	close(cs.closedC)
	cs.cleanSend()
	close(cs.c)
	cs.mu.closed = true
	for _, c := range cs.mu.caches {
		c.cache.Close()
	}
	cs.mu.caches = nil
	cs.cancelWrite()
	return cs.conn.Close()
}

func (cs *clientSession) cleanSend() {
	for {
		select {
		case f, ok := <-cs.c:
			if !ok {
				return
			}
			f.messageSent(backendClosed)
		default:
			return
		}
	}
}

func (cs *clientSession) WriteRPCMessage(msg RPCMessage) error {
	f, err := cs.send(msg)
	if err != nil {
		return err
	}
	defer f.Close()

	// stream only wait send completed
	return f.waitSendCompleted()
}

func (cs *clientSession) Write(
	ctx context.Context,
	response Message) error {
	if ctx == nil {
		panic("Write nil context")
	}
	return cs.WriteRPCMessage(RPCMessage{
		Ctx:     ctx,
		Message: response,
	})
}

func (cs *clientSession) AsyncWrite(response Message) error {
	_, err := cs.send(RPCMessage{
		Ctx:     context.Background(),
		Message: response,
		oneWay:  true,
	})
	return err
}

func (cs *clientSession) send(msg RPCMessage) (*Future, error) {
	cs.metrics.sendCounter.Inc()

	response := msg.Message
	if err := cs.codec.Valid(response); err != nil {
		return nil, err
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.mu.closed {
		return nil, moerr.NewClientClosedNoCtx()
	}

	id := response.GetID()
	if v, ok := cs.sentStreamSequences.Load(id); ok {
		seq := v.(uint32) + 1
		cs.sentStreamSequences.Store(id, seq)
		msg.stream = true
		msg.streamSequence = seq
	}

	f := cs.newFutureFunc()
	f.init(msg)
	if !f.oneWay {
		f.ref()
	}
	cs.c <- f
	cs.metrics.sendingQueueSizeGauge.Set(float64(len(cs.c)))
	return f, nil
}

func (cs *clientSession) startCheckCacheTimeout() {
	cs.checkTimeoutCacheOnce.Do(cs.checkCacheTimeout)
}

func (cs *clientSession) checkCacheTimeout() {
	go func() {
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-cs.closedC:
				return
			case <-timer.C:
				cs.mu.Lock()
				for k, c := range cs.mu.caches {
					if c.closeIfTimeout() {
						delete(cs.mu.caches, k)
					}
				}
				cs.mu.Unlock()
				timer.Reset(time.Second)
			}
		}
	}()
}

func (cs *clientSession) cancelWrite() {
	cs.cancel()
}

func (cs *clientSession) validateStreamRequest(
	id uint64,
	sequence uint32) bool {
	expectSequence := cs.receivedStreamSequences[id] + 1
	if sequence != expectSequence {
		return false
	}
	cs.receivedStreamSequences[id] = sequence
	if sequence == 1 {
		cs.sentStreamSequences.Store(id, uint32(0))
	}
	return true
}

func (cs *clientSession) CreateCache(
	ctx context.Context,
	cacheID uint64) (MessageCache, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.mu.closed {
		return nil, moerr.NewClientClosedNoCtx()
	}

	v, ok := cs.mu.caches[cacheID]
	if !ok {
		v = cacheWithContext{ctx: ctx, cache: newCache()}
		cs.mu.caches[cacheID] = v
		cs.startCheckCacheTimeout()
	}
	return v.cache, nil
}

func (cs *clientSession) DeleteCache(cacheID uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.mu.closed {
		return
	}
	if c, ok := cs.mu.caches[cacheID]; ok {
		c.cache.Close()
		delete(cs.mu.caches, cacheID)
	}
}

func (cs *clientSession) GetCache(cacheID uint64) (MessageCache, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.mu.closed {
		return nil, moerr.NewClientClosedNoCtx()
	}

	if c, ok := cs.mu.caches[cacheID]; ok {
		return c.cache, nil
	}
	return nil, nil
}

type cacheWithContext struct {
	ctx   context.Context
	cache MessageCache
}

func (c cacheWithContext) closeIfTimeout() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}
