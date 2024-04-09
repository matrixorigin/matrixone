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
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"go.uber.org/zap"
)

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
) error {
	return s.send(ctx, request, false)
}

func (s *stream) Resume(ctx context.Context) error {
	if !s.opts.controlEnabled() {
		return nil
	}
	return s.send(ctx, newResumeStream(), true)
}

func (s *stream) send(
	ctx context.Context,
	request Message,
	internal bool,
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

	err := s.doSendLocked(ctx, f, request, internal)
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
	request Message,
	internal bool,
) error {
	s.sequence++
	f.init(RPCMessage{
		Ctx:            ctx,
		Message:        request,
		stream:         true,
		streamSequence: s.sequence,
		internal:       internal,
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

func (s *stream) Close(closeConn bool) error {
	if closeConn {
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

type streamPeer struct {
	opts     StreamOptions
	ctl      sync.WaitGroup
	allow    atomic.Bool
	sequence struct {
		recv uint32
		sent uint32
	}
}

func (s *streamPeer) init(opts StreamOptions) {
	s.opts = opts
	if !opts.controlEnabled() {
		s.allow.Store(true)
	}
}
