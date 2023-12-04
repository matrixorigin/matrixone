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
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"go.uber.org/zap"
)

var (
	stateRunning = int32(0)
	stateStopped = int32(1)

	backendClosed  = moerr.NewBackendClosedNoCtx()
	messageSkipped = moerr.NewInvalidStateNoCtx("request is skipped")
)

// WithBackendLogger set the backend logger
func WithBackendLogger(logger *zap.Logger) BackendOption {
	return func(rb *remoteBackend) {
		rb.logger = logger
	}
}

// WithBackendBufferSize set the buffer size of the wait send chan.
// Default is 1024.
func WithBackendBufferSize(size int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.bufferSize = size
	}
}

// WithBackendBusyBufferSize if len(writeC) >= size, backend is busy.
// Default is 3/4 buffer size.
func WithBackendBusyBufferSize(size int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.busySize = size
	}
}

// WithBackendFilter set send filter func. Input ready to send futures, output
// is really need to be send futures.
func WithBackendFilter(filter func(Message, string) bool) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.filter = filter
	}
}

// WithBackendBatchSendSize set the maximum number of messages to be sent together
// at each batch. Default is 8.
func WithBackendBatchSendSize(size int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.batchSendSize = size
	}
}

// WithBackendConnectTimeout set the timeout for connect to remote. Default 10s.
func WithBackendConnectTimeout(timeout time.Duration) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.connectTimeout = timeout
	}
}

// WithBackendHasPayloadResponse has payload response means read a response that hold
// a slice of data in the read buffer to avoid data copy.
func WithBackendHasPayloadResponse() BackendOption {
	return func(rb *remoteBackend) {
		rb.options.hasPayloadResponse = true
	}
}

// WithBackendStreamBufferSize set buffer size for stream receive message chan
func WithBackendStreamBufferSize(value int) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.streamBufferSize = value
	}
}

// WithBackendGoettyOptions set goetty connection options. e.g. set read/write buffer
// size, adjust net.Conn attribute etc.
func WithBackendGoettyOptions(options ...goetty.Option) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.goettyOptions = options
	}
}

// WithBackendReadTimeout set read timeout for read loop.
func WithBackendReadTimeout(value time.Duration) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.readTimeout = value
	}
}

// WithBackendMetrics setup backend metrics
func WithBackendMetrics(metrics *metrics) BackendOption {
	return func(rb *remoteBackend) {
		rb.metrics = metrics
	}
}

// WithBackendFreeOrphansResponse setup free orphans response func
func WithBackendFreeOrphansResponse(value func(Message)) BackendOption {
	return func(rb *remoteBackend) {
		rb.options.freeResponse = value
	}
}

type remoteBackend struct {
	remote       string
	metrics      *metrics
	logger       *zap.Logger
	codec        Codec
	conn         goetty.IOSession
	writeC       chan *Future
	stopWriteC   chan struct{}
	resetConnC   chan struct{}
	stopper      *stopper.Stopper
	readStopper  *stopper.Stopper
	closeOnce    sync.Once
	ctx          context.Context
	cancel       context.CancelFunc
	cancelOnce   sync.Once
	pingTimer    *time.Timer
	lastPingTime time.Time

	options struct {
		hasPayloadResponse bool
		goettyOptions      []goetty.Option
		connectTimeout     time.Duration
		bufferSize         int
		busySize           int
		batchSendSize      int
		streamBufferSize   int
		filter             func(msg Message, backendAddr string) bool
		readTimeout        time.Duration
		freeResponse       func(Message)
	}

	stateMu struct {
		sync.RWMutex
		state          int32
		readLoopActive bool
		locked         bool
	}

	mu struct {
		sync.RWMutex
		futures       map[uint64]*Future
		activeStreams map[uint64]*stream
	}

	atomic struct {
		id             uint64
		lastActiveTime atomic.Value //time.Time
	}

	pool struct {
		streams *sync.Pool
		futures *sync.Pool
	}
}

// NewRemoteBackend create a goetty connection based backend. This backend will start 2
// goroutine, one for read and one for write. If there is a network error in the underlying
// goetty connection, it will automatically retry until the Future times out.
func NewRemoteBackend(
	remote string,
	codec Codec,
	options ...BackendOption) (Backend, error) {
	rb := &remoteBackend{
		stopper:     stopper.NewStopper(fmt.Sprintf("backend-write-%s", remote)),
		readStopper: stopper.NewStopper(fmt.Sprintf("backend-read-%s", remote)),
		remote:      remote,
		codec:       codec,
		resetConnC:  make(chan struct{}, 1),
		stopWriteC:  make(chan struct{}),
	}

	for _, opt := range options {
		opt(rb)
	}
	rb.adjust()
	rb.metrics.createCounter.Inc()

	rb.ctx, rb.cancel = context.WithCancel(context.Background())
	rb.pool.futures = &sync.Pool{
		New: func() interface{} {
			return newFuture(rb.releaseFuture)
		},
	}
	rb.pool.streams = &sync.Pool{
		New: func() any {
			return newStream(
				rb,
				make(chan Message, rb.options.streamBufferSize),
				rb.newFuture,
				rb.doSend,
				rb.removeActiveStream,
				rb.active)
		},
	}
	rb.writeC = make(chan *Future, rb.options.bufferSize)
	rb.mu.futures = make(map[uint64]*Future, rb.options.bufferSize)
	rb.mu.activeStreams = make(map[uint64]*stream, rb.options.bufferSize)
	if rb.options.hasPayloadResponse {
		rb.options.goettyOptions = append(rb.options.goettyOptions,
			goetty.WithSessionDisableAutoResetInBuffer())
	}
	rb.conn = goetty.NewIOSession(rb.options.goettyOptions...)

	if err := rb.resetConn(); err != nil {
		rb.logger.Error("connect to remote failed")
		return nil, err
	}
	rb.activeReadLoop(false)

	if err := rb.stopper.RunTask(rb.writeLoop); err != nil {
		return nil, err
	}

	rb.active()
	return rb, nil
}

func (rb *remoteBackend) adjust() {
	if rb.options.bufferSize == 0 {
		rb.options.bufferSize = 1024
	}
	if rb.options.busySize == 0 {
		rb.options.busySize = rb.options.bufferSize * 3 / 4
		if rb.options.busySize == 0 {
			rb.options.busySize = 1
		}
	}
	if rb.options.batchSendSize == 0 {
		rb.options.batchSendSize = 8
	}
	if rb.options.connectTimeout == 0 {
		rb.options.connectTimeout = time.Second * 5
	}
	if rb.options.streamBufferSize == 0 {
		rb.options.streamBufferSize = 16
	}
	if rb.options.filter == nil {
		rb.options.filter = func(Message, string) bool {
			return true
		}
	}

	rb.logger = logutil.Adjust(rb.logger).With(zap.String("remote", rb.remote),
		zap.String("backend-id", uuid.NewString()))
	rb.options.goettyOptions = append(rb.options.goettyOptions,
		goetty.WithSessionCodec(rb.codec),
		goetty.WithSessionLogger(rb.logger))
}

func (rb *remoteBackend) Send(ctx context.Context, request Message) (*Future, error) {
	if ctx == nil {
		panic("remoteBackend Send nil context")
	}
	return rb.send(ctx, request, false)
}

func (rb *remoteBackend) SendInternal(ctx context.Context, request Message) (*Future, error) {
	if ctx == nil {
		panic("remoteBackend SendInternal nil context")
	}
	return rb.send(ctx, request, true)
}

func (rb *remoteBackend) send(ctx context.Context, request Message, internal bool) (*Future, error) {
	f := rb.getFuture(ctx, request, internal)
	if err := rb.doSend(f); err != nil {
		f.Close()
		return nil, err
	}
	rb.active()
	return f, nil
}

func (rb *remoteBackend) getFuture(ctx context.Context, request Message, internal bool) *Future {
	request.SetID(rb.nextID())
	f := rb.newFuture()
	f.init(RPCMessage{Ctx: ctx, Message: request, internal: internal})
	rb.addFuture(f)
	return f
}

func (rb *remoteBackend) NewStream(unlockAfterClose bool) (Stream, error) {
	rb.stateMu.RLock()
	defer rb.stateMu.RUnlock()

	if rb.stateMu.state == stateStopped {
		return nil, backendClosed
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	st := rb.acquireStream()
	st.init(rb.nextID(), unlockAfterClose)
	rb.mu.activeStreams[st.ID()] = st
	rb.active()
	return st, nil
}

func (rb *remoteBackend) doSend(f *Future) error {
	rb.metrics.sendCounter.Inc()

	if err := rb.codec.Valid(f.send.Message); err != nil {
		return err
	}

	for {
		rb.stateMu.RLock()
		if rb.stateMu.state == stateStopped {
			rb.stateMu.RUnlock()
			return backendClosed
		}

		// The close method need acquire the write lock, so we cannot block at here.
		// The write loop may reset the backend's network link and may not be able to
		// process writeC for a long time, causing the writeC buffer to reach its limit.
		select {
		case rb.writeC <- f:
			rb.metrics.sendingQueueSizeGauge.Set(float64(len(rb.writeC)))
			rb.stateMu.RUnlock()
			return nil
		case <-f.send.Ctx.Done():
			rb.stateMu.RUnlock()
			return f.send.Ctx.Err()
		default:
			rb.stateMu.RUnlock()
		}
	}
}

func (rb *remoteBackend) Close() {
	rb.metrics.closeCounter.Inc()
	rb.cancelOnce.Do(func() {
		rb.cancel()
	})
	rb.stateMu.Lock()
	if rb.stateMu.state == stateStopped {
		rb.stateMu.Unlock()
		return
	}
	rb.stateMu.state = stateStopped
	rb.stopWriteLoop()
	rb.stateMu.Unlock()

	rb.stopper.Stop()
	rb.doClose()
	rb.inactive()
}

func (rb *remoteBackend) Busy() bool {
	return len(rb.writeC) >= rb.options.busySize
}

func (rb *remoteBackend) LastActiveTime() time.Time {
	return rb.atomic.lastActiveTime.Load().(time.Time)
}

func (rb *remoteBackend) Lock() {
	rb.stateMu.Lock()
	defer rb.stateMu.Unlock()
	if rb.stateMu.locked {
		panic("backend is already locked")
	}
	rb.stateMu.locked = true
}

func (rb *remoteBackend) Unlock() {
	rb.stateMu.Lock()
	defer rb.stateMu.Unlock()
	if !rb.stateMu.locked {
		panic("backend is not locked")
	}
	rb.stateMu.locked = false
}

func (rb *remoteBackend) Locked() bool {
	rb.stateMu.RLock()
	defer rb.stateMu.RUnlock()
	return rb.stateMu.locked
}

func (rb *remoteBackend) active() {
	now := time.Now()
	rb.atomic.lastActiveTime.Store(now)
}

func (rb *remoteBackend) inactive() {
	rb.atomic.lastActiveTime.Store(time.Time{})
}

func (rb *remoteBackend) writeLoop(ctx context.Context) {
	rb.logger.Debug("write loop started")
	defer func() {
		rb.pingTimer.Stop()
		rb.closeConn(false)
		rb.readStopper.Stop()
		rb.closeConn(true)
		rb.logger.Debug("write loop stopped")
	}()

	defer func() {
		rb.makeAllWritesDoneWithClosed()
		close(rb.writeC)
	}()

	// fatal if panic
	defer func() {
		if err := recover(); err != nil {
			rb.logger.Fatal("write loop failed",
				zap.Any("err", err))
		}
	}()

	rb.pingTimer = time.NewTimer(rb.getPingTimeout())
	messages := make([]*Future, 0, rb.options.batchSendSize)
	stopped := false
	lastScheduleTime := time.Now()
	for {
		messages, stopped = rb.fetch(messages, rb.options.batchSendSize)
		interval := time.Since(lastScheduleTime)
		if rb.options.readTimeout > 0 && interval > time.Second*5 {
			getLogger().Warn("system is busy, write loop schedule interval is too large",
				zap.Duration("interval", interval),
				zap.Time("last-ping-trigger-time", rb.lastPingTime),
				zap.Duration("ping-interval", rb.getPingTimeout()))
		}
		if len(messages) > 0 {
			rb.metrics.sendingBatchSizeGauge.Set(float64(len(messages)))
			start := time.Now()

			writeTimeout := time.Duration(0)
			written := messages[:0]
			for _, f := range messages {
				rb.metrics.writeLatencyDurationHistogram.Observe(start.Sub(f.send.createAt).Seconds())

				id := f.getSendMessageID()
				if stopped {
					f.messageSent(backendClosed)
					continue
				}

				if v := rb.doWrite(id, f); v > 0 {
					writeTimeout += v
					written = append(written, f)
				}
			}

			if len(written) > 0 {
				if err := rb.conn.Flush(writeTimeout); err != nil {
					for _, f := range written {
						id := f.getSendMessageID()
						rb.logger.Error("write request failed",
							zap.Uint64("request-id", id),
							zap.Error(err))
						f.messageSent(err)
					}
				} else {
					for _, f := range written {
						f.messageSent(nil)
					}
				}
			}

			rb.metrics.writeDurationHistogram.Observe(time.Since(start).Seconds())
		}
		if stopped {
			return
		}
		lastScheduleTime = time.Now()
	}
}

func (rb *remoteBackend) doWrite(id uint64, f *Future) time.Duration {
	if !rb.options.filter(f.send.Message, rb.remote) {
		f.messageSent(messageSkipped)
		return 0
	}
	// already timeout in future, and future will get a ctx timeout
	if f.send.Timeout() {
		f.messageSent(f.send.Ctx.Err())
		return 0
	}

	v, err := f.send.GetTimeoutFromContext()
	if err != nil {
		f.messageSent(err)
		return 0
	}

	// For PayloadMessage, the internal Codec will write the Payload directly to the underlying socket
	// instead of copying it to the buffer, so the write deadline of the underlying conn needs to be reset
	// here, otherwise an old deadline will be out causing io/timeout.
	conn := rb.conn.RawConn()
	if _, ok := f.send.Message.(PayloadMessage); ok && conn != nil {
		conn.SetWriteDeadline(time.Now().Add(v))
	}
	if ce := rb.logger.Check(zap.DebugLevel, "write request"); ce != nil {
		ce.Write(zap.Uint64("request-id", id),
			zap.String("request", f.send.Message.DebugString()))
	}
	if err := rb.conn.Write(f.send, goetty.WriteOptions{}); err != nil {
		rb.logger.Error("write request failed",
			zap.Uint64("request-id", id), zap.Error(err))
		f.messageSent(err)
		return 0
	}
	return v
}

func (rb *remoteBackend) readLoop(ctx context.Context) {
	rb.logger.Debug("read loop started")
	defer rb.logger.Error("read loop stopped")

	wg := &sync.WaitGroup{}
	var cb func()
	if rb.options.hasPayloadResponse {
		cb = wg.Done
	}

	// fatal if panic
	defer func() {
		if err := recover(); err != nil {
			rb.logger.Fatal("read loop failed",
				zap.Any("err", err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			rb.clean()
			return
		default:
			msg, err := rb.conn.Read(goetty.ReadOptions{Timeout: rb.options.readTimeout})
			if err != nil {
				rb.logger.Error("read from backend failed", zap.Error(err))
				rb.inactiveReadLoop()
				rb.cancelActiveStreams()
				rb.scheduleResetConn()
				return
			}
			rb.metrics.receiveCounter.Inc()

			rb.active()

			if rb.options.hasPayloadResponse {
				wg.Add(1)
			}
			resp := msg.(RPCMessage).Message
			rb.requestDone(ctx, resp.GetID(), msg.(RPCMessage), nil, cb)
			if rb.options.hasPayloadResponse {
				wg.Wait()
			}
		}
	}
}

func (rb *remoteBackend) fetch(messages []*Future, maxFetchCount int) ([]*Future, bool) {
	defer func() {
		rb.metrics.sendingQueueSizeGauge.Set(float64(len(rb.writeC)))
	}()

	n := len(messages)
	for i := 0; i < n; i++ {
		messages[i] = nil
	}
	messages = messages[:0]

	doHeartbeat := func() {
		rb.lastPingTime = time.Now()
		f := rb.getFuture(context.TODO(), &flagOnlyMessage{flag: flagPing}, true)
		// no need wait response, close immediately
		f.Close()
		messages = append(messages, f)
		rb.pingTimer.Reset(rb.getPingTimeout())
	}
	handleHeartbeat := func() {
		select {
		case <-rb.pingTimer.C:
			doHeartbeat()
		default:
		}
	}

	select {
	case <-rb.pingTimer.C:
		doHeartbeat()
	case f := <-rb.writeC:
		handleHeartbeat()
		messages = append(messages, f)
	case <-rb.resetConnC:
		// If the connect needs to be reset, then all futures in the waiting response state will never
		// get the response and need to be notified of an error immediately.
		rb.makeAllWaitingFutureFailed()
		rb.handleResetConn()
	case <-rb.stopWriteC:
		return rb.fetchN(messages, math.MaxInt), true
	}

	return rb.fetchN(messages, maxFetchCount), false
}

func (rb *remoteBackend) fetchN(messages []*Future, max int) []*Future {
	if len(messages) >= max {
		return messages
	}
	n := max - len(messages)
	for i := 0; i < n; i++ {
		select {
		case f := <-rb.writeC:
			messages = append(messages, f)
		default:
			return messages
		}
	}
	return messages
}

func (rb *remoteBackend) makeAllWritesDoneWithClosed() {
	for {
		select {
		case m := <-rb.writeC:
			m.messageSent(backendClosed)
		default:
			return
		}
	}
}

func (rb *remoteBackend) makeAllWaitingFutureFailed() {
	var ids []uint64
	var waitings []*Future
	func() {
		rb.mu.Lock()
		defer rb.mu.Unlock()
		ids = make([]uint64, 0, len(rb.mu.futures))
		waitings = make([]*Future, 0, len(rb.mu.futures))
		for id, f := range rb.mu.futures {
			if f.waiting.Load() {
				waitings = append(waitings, f)
				ids = append(ids, id)
			}
		}
	}()

	for i, f := range waitings {
		f.error(ids[i], backendClosed, nil)
	}
}

func (rb *remoteBackend) handleResetConn() {
	if err := rb.resetConn(); err != nil {
		rb.logger.Error("fail to reset backend connection", zap.Error(err))
		rb.inactive()
	}
}

func (rb *remoteBackend) doClose() {
	rb.closeOnce.Do(func() {
		close(rb.resetConnC)
		rb.closeConn(false)
		// TODO: re create when reconnect
		rb.conn = nil
	})
}

func (rb *remoteBackend) clean() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for id := range rb.mu.futures {
		delete(rb.mu.futures, id)
	}
}

func (rb *remoteBackend) acquireStream() *stream {
	return rb.pool.streams.Get().(*stream)
}

func (rb *remoteBackend) cancelActiveStreams() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for _, st := range rb.mu.activeStreams {
		st.done(context.TODO(), RPCMessage{}, true)
	}
}

func (rb *remoteBackend) removeActiveStream(s *stream) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	delete(rb.mu.activeStreams, s.id)
	delete(rb.mu.futures, s.id)
	if s.unlockAfterClose {
		rb.Unlock()
	}
	if len(s.c) > 0 {
		panic("BUG: stream channel is not empty")
	}
	rb.pool.streams.Put(s)
}

func (rb *remoteBackend) stopWriteLoop() {
	close(rb.stopWriteC)
}

func (rb *remoteBackend) requestDone(
	ctx context.Context,
	id uint64,
	msg RPCMessage,
	err error,
	cb func()) {
	start := time.Now()
	defer func() {
		rb.metrics.doneDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	response := msg.Message
	if msg.Cancel != nil {
		defer msg.Cancel()
	}
	if ce := rb.logger.Check(zap.DebugLevel, "read response"); ce != nil {
		debugStr := ""
		if response != nil {
			debugStr = response.DebugString()
		}
		ce.Write(zap.Uint64("request-id", id),
			zap.String("response", debugStr))
	}

	rb.mu.Lock()
	if f, ok := rb.mu.futures[id]; ok {
		delete(rb.mu.futures, id)
		rb.mu.Unlock()
		if err == nil {
			f.done(response, cb)
		} else {
			errutil.ReportError(ctx, err)
			f.error(id, err, cb)
		}
	} else if st, ok := rb.mu.activeStreams[id]; ok {
		rb.mu.Unlock()
		if response != nil {
			st.done(ctx, msg, false)
		}
	} else {
		// future has been removed, e.g. it has timed out.
		rb.mu.Unlock()
		if cb != nil {
			cb()
		}

		if !msg.internal &&
			response != nil &&
			rb.options.freeResponse != nil {
			rb.options.freeResponse(response)
		}
	}
}

func (rb *remoteBackend) addFuture(f *Future) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	f.ref()
	rb.mu.futures[f.getSendMessageID()] = f
}

func (rb *remoteBackend) releaseFuture(f *Future) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	delete(rb.mu.futures, f.getSendMessageID())
	f.reset()
	rb.pool.futures.Put(f)
}

func (rb *remoteBackend) running() bool {
	rb.stateMu.RLock()
	defer rb.stateMu.RUnlock()
	return rb.runningLocked()
}

func (rb *remoteBackend) resetConn() error {
	start := time.Now()
	defer func() {
		rb.metrics.connectDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	wait := time.Second
	sleep := time.Millisecond * 200
	for {
		if !rb.running() {
			return backendClosed
		}
		select {
		case <-rb.ctx.Done():
			return backendClosed
		default:
		}

		rb.logger.Debug("start connect to remote")
		rb.closeConn(false)
		rb.metrics.connectCounter.Inc()
		err := rb.conn.Connect(rb.remote, rb.options.connectTimeout)
		if err == nil {
			rb.logger.Debug("connect to remote succeed")
			rb.activeReadLoop(false)
			return nil
		}

		rb.metrics.connectFailedCounter.Inc()

		// only retry on temp net error
		canRetry := false
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			canRetry = true
		}
		rb.logger.Error("init remote connection failed, retry later",
			zap.Bool("can-retry", canRetry),
			zap.Error(err))

		if !canRetry {
			return err
		}
		duration := time.Duration(0)
		for {
			time.Sleep(sleep)
			duration += sleep
			if time.Since(start) > rb.options.connectTimeout {
				return moerr.NewBackendCannotConnectNoCtx()
			}
			select {
			case <-rb.ctx.Done():
				return backendClosed
			default:
			}
			if duration >= wait {
				break
			}
		}
		wait += wait / 2

		// reconnect failed, notify all future failed
		rb.notifyAllWaitWritesFailed(moerr.NewBackendCannotConnectNoCtx())
	}
}

func (rb *remoteBackend) notifyAllWaitWritesFailed(err error) {
	for {
		select {
		case f := <-rb.writeC:
			f.messageSent(err)
		default:
			return
		}
	}
}

func (rb *remoteBackend) activeReadLoop(locked bool) {
	if !locked {
		rb.stateMu.Lock()
		defer rb.stateMu.Unlock()
	}

	if rb.stateMu.readLoopActive {
		return
	}

	if err := rb.readStopper.RunTask(rb.readLoop); err != nil {
		rb.logger.Error("active read loop failed", zap.Error(err))
		return
	}
	rb.stateMu.readLoopActive = true
}

func (rb *remoteBackend) inactiveReadLoop() {
	rb.stateMu.Lock()
	defer rb.stateMu.Unlock()

	rb.stateMu.readLoopActive = false
}

func (rb *remoteBackend) runningLocked() bool {
	return rb.stateMu.state == stateRunning
}

func (rb *remoteBackend) scheduleResetConn() {
	rb.stateMu.RLock()
	defer rb.stateMu.RUnlock()

	if !rb.runningLocked() {
		return
	}

	select {
	case rb.resetConnC <- struct{}{}:
		rb.logger.Debug("schedule reset remote connection")
	default:
	}
}

func (rb *remoteBackend) closeConn(close bool) {
	fn := rb.conn.Disconnect
	if close {
		fn = rb.conn.Close
	}

	if err := fn(); err != nil {
		rb.logger.Error("close remote conn failed", zap.Error(err))
	}
}

func (rb *remoteBackend) newFuture() *Future {
	return rb.pool.futures.Get().(*Future)
}

func (rb *remoteBackend) nextID() uint64 {
	return atomic.AddUint64(&rb.atomic.id, 1)
}

func (rb *remoteBackend) getPingTimeout() time.Duration {
	if rb.options.readTimeout > 0 {
		return rb.options.readTimeout / 5
	}
	return time.Duration(math.MaxInt64)
}

type goettyBasedBackendFactory struct {
	codec   Codec
	options []BackendOption
}

func NewGoettyBasedBackendFactory(codec Codec, options ...BackendOption) BackendFactory {
	return &goettyBasedBackendFactory{
		codec:   codec,
		options: options,
	}
}

func (bf *goettyBasedBackendFactory) Create(
	remote string,
	extraOptions ...BackendOption) (Backend, error) {
	opts := append(bf.options, extraOptions...)
	return NewRemoteBackend(remote, bf.codec, opts...)
}

type stream struct {
	rb               *remoteBackend
	c                chan Message
	sendFunc         func(*Future) error
	activeFunc       func()
	unregisterFunc   func(*stream)
	newFutureFunc    func() *Future
	unlockAfterClose bool
	ctx              context.Context
	cancel           context.CancelFunc

	// reset fields
	id                   uint64
	sequence             uint32
	lastReceivedSequence uint32
	mu                   struct {
		sync.RWMutex
		closed bool
	}
}

func newStream(
	rb *remoteBackend,
	c chan Message,
	acquireFutureFunc func() *Future,
	sendFunc func(*Future) error,
	unregisterFunc func(*stream),
	activeFunc func()) *stream {
	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{
		rb:             rb,
		c:              c,
		ctx:            ctx,
		cancel:         cancel,
		sendFunc:       sendFunc,
		unregisterFunc: unregisterFunc,
		activeFunc:     activeFunc,
		newFutureFunc:  acquireFutureFunc,
	}
	s.setFinalizer()
	return s
}

func (s *stream) init(id uint64, unlockAfterClose bool) {
	s.id = id
	s.unlockAfterClose = unlockAfterClose
	s.sequence = 0
	s.lastReceivedSequence = 0
	s.mu.closed = false
	for {
		select {
		case <-s.c:
		default:
			return
		}
	}
}

func (s *stream) setFinalizer() {
	runtime.SetFinalizer(s, func(s *stream) {
		s.destroy()
	})
}

func (s *stream) destroy() {
	close(s.c)
	s.cancel()
}

func (s *stream) Send(ctx context.Context, request Message) error {
	if s.id != request.GetID() {
		panic("request.id != stream.id")
	}
	if _, ok := ctx.Deadline(); !ok {
		panic("deadline not set in context")
	}
	s.activeFunc()

	f := s.newFutureFunc()
	f.ref()
	defer f.Close()

	s.mu.RLock()
	if s.mu.closed {
		s.mu.RUnlock()
		return moerr.NewStreamClosedNoCtx()
	}

	err := s.doSendLocked(ctx, f, request)
	// unlock before future.close to avoid deadlock with future.Close
	// 1. current goroutine:        stream.RLock
	// 2. backend read goroutine:   cancelActiveStream -> backend.Lock
	// 3. backend read goroutine:   cancelActiveStream -> stream.Lock : deadlock here
	// 4. current goroutine:        f.Close -> backend.Lock           : deadlock here
	s.mu.RUnlock()

	if err != nil {
		return err
	}
	// stream only wait send completed
	return f.waitSendCompleted()
}

func (s *stream) doSendLocked(
	ctx context.Context,
	f *Future,
	request Message) error {
	s.sequence++
	f.init(RPCMessage{
		Ctx:            ctx,
		Message:        request,
		stream:         true,
		streamSequence: s.sequence,
	})

	return s.sendFunc(f)
}

func (s *stream) Receive() (chan Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return nil, moerr.NewStreamClosedNoCtx()
	}
	return s.c, nil
}

func (s *stream) Close(closeConn bool) error {
	if closeConn {
		s.rb.Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.closed {
		return nil
	}

	s.cleanCLocked()
	s.mu.closed = true
	s.unregisterFunc(s)
	return nil
}

func (s *stream) ID() uint64 {
	return s.id
}

func (s *stream) done(
	ctx context.Context,
	message RPCMessage,
	clean bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.mu.closed {
		return
	}

	if clean {
		s.cleanCLocked()
	}
	response := message.Message
	if message.Cancel != nil {
		defer message.Cancel()
	}
	if response != nil && !message.stream {
		panic("BUG")
	}
	if response != nil &&
		message.streamSequence != s.lastReceivedSequence+1 {
		response = nil
	}

	s.lastReceivedSequence = message.streamSequence
	moprobe.WithRegion(ctx, moprobe.RPCStreamReceive, func() {
		select {
		case s.c <- response:
		case <-ctx.Done():
		}
	})
}

func (s *stream) cleanCLocked() {
	for {
		select {
		case <-s.c:
		default:
			return
		}
	}
}
