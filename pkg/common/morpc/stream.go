// Copyright 2021 - 2024 Matrix Origin
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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.uber.org/zap"
)

// StreamOptions stream options
type StreamOptions struct {
	feature int
}

func DefaultStreamOptions() StreamOptions {
	return StreamOptions{
		feature: 0,
	}
}

func (opts StreamOptions) WithEnableControl() StreamOptions {
	opts.feature |= featureEnableControl
	return opts
}

func (opts StreamOptions) WithExclusive() StreamOptions {
	opts.feature |= featureExclusive
	return opts
}

func (opts StreamOptions) isExclusive() bool {
	return opts.feature&featureExclusive != 0
}

func (opts StreamOptions) controlEnabled() bool {
	return opts.feature&featureEnableControl != 0
}

type stream struct {
	rb             *remoteBackend
	c              chan Message
	sendFunc       func(*Future) error
	activeFunc     func()
	unregisterFunc func(*stream)
	newFutureFunc  func() *Future
	opts           StreamOptions
	ctx            context.Context
	cancel         context.CancelFunc

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
	activeFunc func(),
) *stream {
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

func (s *stream) init(
	id uint64,
	opts StreamOptions,
) {
	s.id = id
	s.opts = opts
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

func (s *stream) Send(
	ctx context.Context,
	request Message,
	opts WriteOptions,
) error {
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
		s.rb.logger.Warn("stream is closed on send", zap.Uint64("stream-id", s.id))
		return moerr.NewStreamClosedNoCtx()
	}

	err := s.doSendLocked(ctx, f, request, opts)
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

func (s *stream) Resume(ctx context.Context) error {
	if !s.opts.controlEnabled() {
		return nil
	}
	return s.Send(ctx, newResumeStream(s.id), InternalWrite)
}

func (s *stream) doSendLocked(
	ctx context.Context,
	f *Future,
	request Message,
	opts WriteOptions,
) error {
	s.sequence++
	f.init(RPCMessage{
		Ctx:     ctx,
		Message: request,
		opts:    opts.Stream(s.sequence),
	})

	return s.sendFunc(f)
}

func (s *stream) Receive() (chan Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		s.rb.logger.Warn("stream is closed on receive", zap.Uint64("stream-id", s.id))
		return nil, moerr.NewStreamClosedNoCtx()
	}
	return s.c, nil
}

func (s *stream) Close() error {
	if s.opts.isExclusive() {
		s.rb.logger.Info("stream call closed on client", zap.Uint64("stream-id", s.id))
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
	if response != nil && !message.opts.stream {
		panic("BUG")
	}
	if response != nil &&
		message.opts.streamSequence != s.lastReceivedSequence+1 {
		response = nil
	}

	s.lastReceivedSequence = message.opts.streamSequence
	select {
	case s.c <- response:
	case <-ctx.Done():
	}
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

type streamPeer struct {
	opts     StreamOptions
	c        chan struct{}
	deny     atomic.Bool
	sequence struct {
		recv uint32
		sent uint32
	}
}

func newStreamPeer() *streamPeer {
	return &streamPeer{}
}

func (s *streamPeer) init(opts StreamOptions) {
	s.opts = opts
	if opts.controlEnabled() {
		s.c = make(chan struct{}, 1)
		s.pause()
	}
}

func (s *streamPeer) pause() {
	select {
	case <-s.c:
		getLogger().Fatal("BUG: pause failed")
	default:
	}
	s.deny.Store(true)
}

func (s *streamPeer) resume() {
	select {
	case s.c <- struct{}{}:
	default:
		getLogger().Fatal("BUG: resume failed")
	}
}

func (s *streamPeer) wait(msg RPCMessage) error {
	if !s.opts.controlEnabled() {
		return nil
	}

	if !msg.opts.chunk || msg.opts.lastChunk {
		defer s.pause()
	} else {
		defer s.resume()
	}

	if !s.deny.Load() {
		return nil
	}
	select {
	case <-s.c:
	case <-msg.Ctx.Done():
		return msg.Ctx.Err()
	}
	return nil
}
