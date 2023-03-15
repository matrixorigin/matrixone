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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// WithBackendFilter set send fiter func. Input ready to send futures, output
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

type remoteBackend struct {
	remote      string
	logger      *zap.Logger
	codec       Codec
	conn        goetty.IOSession
	writeC      chan *Future
	stopWriteC  chan struct{}
	resetConnC  chan struct{}
	stopper     *stopper.Stopper
	readStopper *stopper.Stopper
	closeOnce   sync.Once
	ctx         context.Context
	cancel      context.CancelFunc
	cancelOnce  sync.Once

	options struct {
		hasPayloadResponse bool
		goettyOptions      []goetty.Option
		connectTimeout     time.Duration
		bufferSize         int
		busySize           int
		batchSendSize      int
		streamBufferSize   int
		filter             func(msg Message, backendAddr string) bool
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
// goroutiune, one for read and one for write. If there is a network error in the underlying
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
		resetConnC:  make(chan struct{}),
		stopWriteC:  make(chan struct{}),
	}

	for _, opt := range options {
		opt(rb)
	}
	rb.adjust()
	rb.ctx, rb.cancel = context.WithCancel(context.Background())
	rb.pool.futures = &sync.Pool{
		New: func() interface{} {
			return newFuture(rb.releaseFuture)
		},
	}
	rb.pool.streams = &sync.Pool{
		New: func() any {
			return newStream(make(chan Message, rb.options.streamBufferSize),
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
		rb.logger.Error("connect to remote failed", zap.Error(err))
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
		rb.options.connectTimeout = time.Second * 10
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
	return rb.send(ctx, request, false)
}

func (rb *remoteBackend) SendInternal(ctx context.Context, request Message) (*Future, error) {
	return rb.send(ctx, request, true)
}

func (rb *remoteBackend) send(ctx context.Context, request Message, internal bool) (*Future, error) {
	rb.active()
	request.SetID(rb.nextID())

	f := rb.newFuture()
	f.init(RPCMessage{Ctx: ctx, Message: request, internal: internal})
	rb.addFuture(f)

	if err := rb.doSend(f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (rb *remoteBackend) NewStream(unlockAfterClose bool) (Stream, error) {
	rb.active()
	rb.stateMu.RLock()
	defer rb.stateMu.RUnlock()

	if rb.stateMu.state == stateStopped {
		return nil, moerr.NewBackendClosedNoCtx()
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	st := rb.acquireStream()
	st.init(rb.nextID(), unlockAfterClose)
	rb.mu.activeStreams[st.ID()] = st
	return st, nil
}

func (rb *remoteBackend) doSend(f *Future) error {
	if err := rb.codec.Valid(f.send.Message); err != nil {
		return err
	}

	for {
		rb.stateMu.RLock()
		if rb.stateMu.state == stateStopped {
			rb.stateMu.RUnlock()
			return moerr.NewBackendClosedNoCtx()
		}

		// The close method need acquire the write lock, so we cannot block at here.
		// The write loop may reset the backend's network link and may not be able to
		// process writeC for a long time, causing the writeC buffer to reach its limit.
		select {
		case rb.writeC <- f:
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
	rb.logger.Info("write loop started")
	defer func() {
		rb.closeConn(false)
		rb.readStopper.Stop()
		rb.closeConn(true)
		rb.logger.Info("write loop stopped")
	}()

	defer func() {
		rb.makeAllWritesDoneWithClosed(ctx)
		close(rb.writeC)
	}()

	messages := make([]*Future, 0, rb.options.batchSendSize)
	stopped := false
	for {
		messages, stopped = rb.fetch(ctx, messages, rb.options.batchSendSize)
		if len(messages) > 0 {
			written := 0
			writeTimeout := time.Duration(0)
			for _, f := range messages {
				id := f.getSendMessageID()
				if stopped {
					f.messageSended(backendClosed)
					continue
				}

				if v := rb.doWrite(ctx, id, f); v > 0 {
					writeTimeout += v
					written++
				}
			}

			if written > 0 {
				if err := rb.conn.Flush(writeTimeout); err != nil {
					for _, f := range messages {
						if rb.options.filter(f.send.Message, rb.remote) {
							id := f.getSendMessageID()
							rb.logger.Error("write request failed",
								zap.Uint64("request-id", id),
								zap.Error(err))
							f.messageSended(err)
						}
					}
				}
			}

			for _, m := range messages {
				m.messageSended(nil)
			}
		}
		if stopped {
			return
		}
	}
}

func (rb *remoteBackend) doWrite(ctx context.Context, id uint64, f *Future) time.Duration {
	if !rb.options.filter(f.send.Message, rb.remote) {
		f.messageSended(messageSkipped)
		return 0
	}
	// already timeout in future, and future will get a ctx timeout
	if f.send.Timeout() {
		f.messageSended(f.send.Ctx.Err())
		return 0
	}

	v, err := f.send.GetTimeoutFromContext()
	if err != nil {
		f.messageSended(err)
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
			zap.Uint64("request-id", id),
			zap.Error(err))
		f.messageSended(err)
		return 0
	}
	return v
}

func (rb *remoteBackend) readLoop(ctx context.Context) {
	rb.logger.Info("read loop started")
	defer rb.logger.Error("read loop stopped")

	wg := &sync.WaitGroup{}
	var cb func()
	if rb.options.hasPayloadResponse {
		cb = wg.Done
	}

	for {
		select {
		case <-ctx.Done():
			rb.clean()
			return
		default:
			msg, err := rb.conn.Read(goetty.ReadOptions{})
			if err != nil {
				rb.logger.Error("read from backend failed",
					zap.Error(err))
				rb.inactiveReadLoop()
				rb.cancelActiveStreams()
				rb.scheduleResetConn()
				return
			}

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

func (rb *remoteBackend) fetch(
	ctx context.Context,
	messages []*Future,
	maxFetchCount int) ([]*Future, bool) {
	n := len(messages)
	for i := 0; i < n; i++ {
		messages[i] = nil
	}
	messages = messages[:0]
	select {
	case f := <-rb.writeC:
		messages = append(messages, f)
		n := maxFetchCount - 1
	OUTER:
		for i := 0; i < n; i++ {
			select {
			case f := <-rb.writeC:
				messages = append(messages, f)
			default:
				break OUTER
			}
		}
	case <-rb.resetConnC:
		rb.handleResetConn()
	case <-rb.stopWriteC:
		for {
			select {
			case f := <-rb.writeC:
				messages = append(messages, f)
			default:
				return messages, true
			}
		}
	}
	return messages, false
}

func (rb *remoteBackend) makeAllWritesDoneWithClosed(ctx context.Context) {
	for {
		select {
		case m := <-rb.writeC:
			m.messageSended(backendClosed)
		default:
			return
		}
	}
}

func (rb *remoteBackend) handleResetConn() {
	if err := rb.resetConn(); err != nil {
		rb.logger.Error("fail to reset backend connection",
			zap.Error(err))
		rb.inactive()
	}
}

func (rb *remoteBackend) doClose() {
	rb.closeOnce.Do(func() {
		close(rb.resetConnC)
		rb.closeConn(false)
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
		st.done(RPCMessage{})
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
	rb.pool.streams.Put(s)
}

func (rb *remoteBackend) stopWriteLoop() {
	close(rb.stopWriteC)
}

func (rb *remoteBackend) requestDone(ctx context.Context, id uint64, msg RPCMessage, err error, cb func()) {
	response := msg.Message
	if ce := rb.logger.Check(zap.DebugLevel, "read response"); ce != nil {
		debugStr := ""
		if response != nil {
			debugStr = response.DebugString()
		}
		ce.Write(zap.Uint64("request-id", id),
			zap.String("response", debugStr),
			zap.Error(err))
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
			st.done(msg)
		}
	} else {
		// future has been removed, e.g. it has timed out.
		rb.mu.Unlock()
		if cb != nil {
			cb()
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
	wait := time.Second
	sleep := time.Millisecond * 200
	for {
		if !rb.running() {
			return moerr.NewBackendClosedNoCtx()
		}
		select {
		case <-rb.ctx.Done():
			return moerr.NewBackendClosedNoCtx()
		default:
		}

		rb.logger.Info("start connect to remote")
		rb.closeConn(false)
		err := rb.conn.Connect(rb.remote, rb.options.connectTimeout)
		if err == nil {
			rb.logger.Info("connect to remote succeed")
			rb.activeReadLoop(false)
			return nil
		}
		rb.logger.Error("init remote connection failed, retry later",
			zap.Error(err))

		duration := time.Duration(0)
		for {
			time.Sleep(sleep)
			duration += sleep
			if time.Since(start) > rb.options.connectTimeout {
				return moerr.NewBackendCannotConnectNoCtx()
			}
			select {
			case <-rb.ctx.Done():
				return moerr.NewBackendClosedNoCtx()
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
			f.messageSended(err)
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
		rb.logger.Error("active read loop failed",
			zap.Error(err))
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
	case <-time.After(time.Second * 10):
		rb.logger.Fatal("BUG: schedule reset remote connection timeout")
	}
}

func (rb *remoteBackend) closeConn(close bool) {
	fn := rb.conn.Disconnect
	if close {
		fn = rb.conn.Close
	}

	if err := fn(); err != nil {
		rb.logger.Error("close remote conn failed",
			zap.Error(err))
	}
}

func (rb *remoteBackend) newFuture() *Future {
	return rb.pool.futures.Get().(*Future)
}

func (rb *remoteBackend) nextID() uint64 {
	return atomic.AddUint64(&rb.atomic.id, 1)
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

func (bf *goettyBasedBackendFactory) Create(remote string) (Backend, error) {
	return NewRemoteBackend(remote, bf.codec, bf.options...)
}

type stream struct {
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
	c chan Message,
	acquireFutureFunc func() *Future,
	sendFunc func(*Future) error,
	unregisterFunc func(*stream),
	activeFunc func()) *stream {
	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{
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
	s.sequence = 0
	s.unlockAfterClose = unlockAfterClose
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
	// 1. current goroutine:        stream.Rlock
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

func (s *stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.closed {
		return nil
	}

	// the stream is reuseable, so use nil to notify stream is closed
	s.c <- nil
	s.mu.closed = true
	s.unregisterFunc(s)
	return nil
}

func (s *stream) ID() uint64 {
	return s.id
}

func (s *stream) done(message RPCMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.closed {
		return
	}

	response := message.Message
	if response != nil && !message.stream {
		panic("BUG")
	}
	if response != nil &&
		message.streamSequence != s.lastReceivedSequence+1 {
		response = nil
	}

	s.lastReceivedSequence = message.streamSequence
	s.c <- response
}
