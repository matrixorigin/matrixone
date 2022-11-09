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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var (
	stateRunning = int32(0)
	stateStopped = int32(1)
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

// WithBackendConnectWhenCreate connection the goetty connection while create the
// backend.
func WithBackendConnectWhenCreate() BackendOption {
	return func(rb *remoteBackend) {
		rb.options.connect = true
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
	remote     string
	logger     *zap.Logger
	codec      Codec
	conn       goetty.IOSession
	writeC     chan backendSendMessage
	resetConnC chan struct{}
	stopper    *stopper.Stopper
	closeOnce  sync.Once

	options struct {
		connect            bool
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
		stopper:    stopper.NewStopper(fmt.Sprintf("backend-%s", remote)),
		remote:     remote,
		codec:      codec,
		resetConnC: make(chan struct{}),
	}

	for _, opt := range options {
		opt(rb)
	}
	rb.adjust()

	rb.pool.futures = &sync.Pool{
		New: func() interface{} {
			return newFuture(rb.releaseFuture)
		},
	}
	rb.pool.streams = &sync.Pool{
		New: func() any {
			return newStream(make(chan Message, rb.options.streamBufferSize),
				rb.doSend, rb.removeActiveStream, rb.active)
		},
	}
	rb.writeC = make(chan backendSendMessage, rb.options.bufferSize)
	rb.mu.futures = make(map[uint64]*Future, rb.options.bufferSize)
	rb.mu.activeStreams = make(map[uint64]*stream, rb.options.bufferSize)
	if rb.options.hasPayloadResponse {
		rb.options.goettyOptions = append(rb.options.goettyOptions,
			goetty.WithSessionDisableAutoResetInBuffer())
	}
	rb.conn = goetty.NewIOSession(rb.options.goettyOptions...)

	if rb.options.connect {
		if err := rb.resetConn(); err != nil {
			rb.logger.Error("connect to remote failed", zap.Error(err))
			return nil, err
		}
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

	rb.logger = logutil.Adjust(rb.logger).With(zap.String("remote", rb.remote))
	rb.options.goettyOptions = append(rb.options.goettyOptions,
		goetty.WithSessionCodec(rb.codec),
		goetty.WithSessionLogger(rb.logger))
}

func (rb *remoteBackend) Send(ctx context.Context, request Message) (*Future, error) {
	rb.active()
	request.SetID(rb.nextID())

	f := rb.newFuture()
	f.init(request.GetID(), ctx)
	rb.addFuture(f)
	if err := rb.doSend(backendSendMessage{message: RPCMessage{Ctx: ctx, Message: request}, completed: f.unRef}); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (rb *remoteBackend) NewStream() (Stream, error) {
	rb.active()
	rb.stateMu.RLock()
	defer rb.stateMu.RUnlock()

	if rb.stateMu.state == stateStopped {
		return nil, moerr.NewBackendClosed()
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	st := rb.acquireStream()
	st.init(rb.nextID())
	rb.mu.activeStreams[st.ID()] = st
	return st, nil
}

func (rb *remoteBackend) doSend(m backendSendMessage) error {
	for {
		rb.stateMu.RLock()
		if rb.stateMu.state == stateStopped {
			rb.stateMu.RUnlock()
			return moerr.NewBackendClosed()
		}

		// The close method need acquire the write lock, so we cannot block at here.
		// The write loop may reset the backend's network link and may not be able to
		// process writeC for a long time, causing the writeC buffer to reach its limit.
		select {
		case rb.writeC <- m:
			rb.stateMu.RUnlock()
			return nil
		case <-m.message.Ctx.Done():
			rb.stateMu.RUnlock()
			return m.message.Ctx.Err()
		default:
			rb.stateMu.RUnlock()
		}
	}
}

func (rb *remoteBackend) Close() {
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
		rb.closeConn(true)
		rb.logger.Info("write loop stopped")
	}()

	resetConnTimes := uint64(0)
	retry := false
	retryAt := uint64(0)
	futures := make([]backendSendMessage, 0, rb.options.batchSendSize)

	resetRetry := func() {
		retry = false
		retryAt = 0
	}

	handleResetConn := func() {
		if err := rb.resetConn(); err != nil {
			rb.logger.Error("fail to reset backend connection",
				zap.Error(err))
			rb.inactive()
		}
		resetConnTimes++
	}

	fetch := func() {
		for i := 0; i < len(futures); i++ {
			futures[i] = backendSendMessage{}
		}
		futures = futures[:0]

		for i := 0; i < rb.options.batchSendSize; i++ {
			if len(futures) == 0 {
				select {
				case f, ok := <-rb.writeC:
					if !ok {
						return
					}
					futures = append(futures, f)
				case _, ok := <-rb.resetConnC:
					if !ok {
						return
					}
					handleResetConn()
				}
			} else {
				select {
				case f, ok := <-rb.writeC:
					if !ok {
						return
					}
					futures = append(futures, f)
				case _, ok := <-rb.resetConnC:
					if !ok {
						return
					}
					handleResetConn()
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
		default:
			if !retry {
				fetch()
			} else {
				select {
				case _, ok := <-rb.resetConnC:
					if !ok {
						return
					}
					handleResetConn()
				default:
				}
			}

			if len(futures) > 0 {
				if retry && !rb.conn.Connected() {
					if retryAt < resetConnTimes {
						for _, f := range futures {
							f.completed()
						}
						resetRetry()
					}
					continue
				}

				resetRetry()
				written := 0
				writeTimeout := time.Duration(0)
				for _, f := range futures {
					if rb.options.filter(f.message.Message, rb.remote) && !f.message.Timeout() {
						v, err := f.message.GetTimeoutFromContext()
						if err != nil {
							continue
						}

						writeTimeout += v

						// For PayloadMessage, the internal Codec will write the Payload directly to the underlying socket
						// instead of copying it to the buffer, so the write deadline of the underlying conn needs to be reset
						// here, otherwise an old deadline will be out causing io/timeout.
						conn := rb.conn.RawConn()
						if _, ok := f.message.Message.(PayloadMessage); ok && conn != nil {
							conn.SetWriteDeadline(time.Now().Add(v))
						}
						if ce := rb.logger.Check(zap.DebugLevel, "write request"); ce != nil {
							ce.Write(zap.Uint64("request-id", f.message.Message.GetID()),
								zap.String("request", f.message.Message.DebugString()))
						}
						if err := rb.conn.Write(f.message, goetty.WriteOptions{}); err != nil {
							rb.logger.Error("write request failed",
								zap.Uint64("request-id", f.message.Message.GetID()),
								zap.Error(err))
							retry = true
							written = 0
							break
						}
						written++
					}
				}

				if written > 0 {
					if err := rb.conn.Flush(writeTimeout); err != nil {
						for _, f := range futures {
							if rb.options.filter(f.message.Message, rb.remote) {
								rb.logger.Error("write request failed",
									zap.Uint64("request-id", f.message.Message.GetID()),
									zap.Error(err))
							}
						}
						retry = true
					}
				}

				if !retry {
					for _, f := range futures {
						if f.completed != nil {
							f.completed()
						}
					}
				} else {
					retryAt = resetConnTimes
				}
			}
		}
	}
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
			rb.requestDone(msg.(RPCMessage).Message, cb)
			if rb.options.hasPayloadResponse {
				wg.Wait()
			}
		}
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
		st.done(nil)
	}
}

func (rb *remoteBackend) removeActiveStream(s *stream) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	delete(rb.mu.activeStreams, s.id)
	delete(rb.mu.futures, s.id)
	rb.pool.streams.Put(s)
}

func (rb *remoteBackend) stopWriteLoop() {
	rb.closeConn(false)
	close(rb.writeC)
}

func (rb *remoteBackend) requestDone(response Message, cb func()) {
	id := response.GetID()

	if ce := rb.logger.Check(zap.DebugLevel, "read response"); ce != nil {
		ce.Write(zap.Uint64("request-id", id),
			zap.String("response", response.DebugString()))
	}

	rb.mu.Lock()
	if f, ok := rb.mu.futures[id]; ok {
		delete(rb.mu.futures, id)
		rb.mu.Unlock()
		f.done(response, cb)
	} else if st, ok := rb.mu.activeStreams[id]; ok {
		rb.mu.Unlock()
		st.done(response)
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
	rb.mu.futures[f.id] = f
}

func (rb *remoteBackend) releaseFuture(f *Future) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	delete(rb.mu.futures, f.id)
	f.reset()
	rb.pool.futures.Put(f)
}

func (rb *remoteBackend) resetConn() error {
	rb.stateMu.Lock()
	defer rb.stateMu.Unlock()

	start := time.Now()
	wait := time.Second
	sleep := time.Millisecond * 200
	for {
		if !rb.runningLocked() {
			return moerr.NewBackendClosed()
		}

		rb.logger.Info("start connect to remote")
		rb.closeConn(false)
		err := rb.conn.Connect(rb.remote, rb.options.connectTimeout)
		if err == nil {
			rb.logger.Info("connect to remote succeed")
			rb.activeReadLoop(true)
			return nil
		}
		rb.logger.Error("init remote connection failed, retry later",
			zap.Error(err))

		duration := time.Duration(0)
		for {
			time.Sleep(sleep)
			duration += sleep
			if time.Since(start) > rb.options.connectTimeout {
				return moerr.NewBackendClosed()
			}
			if duration >= wait {
				break
			}
		}
		wait += wait / 2
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

	if err := rb.stopper.RunTask(rb.readLoop); err != nil {
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
	c              chan Message
	sendFunc       func(m backendSendMessage) error
	activeFunc     func()
	unregisterFunc func(*stream)
	ctx            context.Context
	cancel         context.CancelFunc

	// reset fields
	id uint64
	mu struct {
		sync.RWMutex
		closed bool
	}
}

func newStream(c chan Message,
	sendFunc func(m backendSendMessage) error,
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
	}
	s.setFinalizer()
	return s
}

func (s *stream) init(id uint64) {
	s.id = id
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return moerr.NewStreamClosed()
	}

	return s.sendFunc(backendSendMessage{message: RPCMessage{Ctx: ctx, Message: request}})
}

func (s *stream) Receive() (chan Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return nil, moerr.NewStreamClosed()
	}
	return s.c, nil
}

func (s *stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.closed {
		return nil
	}

	s.c <- nil
	s.mu.closed = true
	s.unregisterFunc(s)
	return nil
}

func (s *stream) ID() uint64 {
	return s.id
}

func (s *stream) done(message Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.closed {
		return
	}

	s.c <- message
}

type backendSendMessage struct {
	message   RPCMessage
	completed func()
}
