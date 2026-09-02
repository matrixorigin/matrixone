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
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func newFuture(releaseFunc func(f *Future)) *Future {
	f := &Future{
		c:           make(chan Message, 1),
		errC:        make(chan error, 1),
		writtenC:    make(chan error, 1),
		releaseFunc: releaseFunc,
	}
	f.setFinalizer()
	return f
}

// Future is used to obtain response data synchronously.
type Future struct {
	id   uint64
	send RPCMessage
	c    chan Message
	errC chan error
	// used to check error for sending message
	writtenC chan error
	waiting  atomic.Bool
	// writtenAt is the backend-relative tick of a successfully flushed ordinary
	// unary request. Zero plus waiting=false means write admission/in progress;
	// zero plus waiting=true means terminal send failure.
	writtenAt atomic.Int64
	// requestMetricObserved makes terminal request accounting exactly once even
	// when timeout, transport failure, response delivery, and Close race.
	requestMetricObserved atomic.Bool
	requestMetrics        *metrics
	releaseFunc           func(*Future)
	sendRelease           func(Message)
	// responseRelease remains owned by the Future until Get receives the
	// response. If the caller abandons an already-delivered response and closes
	// the Future, clear returns it to its application pool.
	responseRelease func(Message)
	oneWay          bool
	mu              struct {
		sync.Mutex
		notified bool
		closed   bool
		released bool
		ref      int
		cb       func()
	}
}

func (f *Future) init(send RPCMessage) {
	if _, ok := send.Ctx.Deadline(); !ok && !send.oneWay && !send.internal {
		panic("context deadline not set")
	}
	f.waiting.Store(false)
	f.writtenAt.Store(0)
	f.requestMetricObserved.Store(false)
	f.requestMetrics = nil
	f.send = send
	f.send.createAt = time.Now()
	f.id = send.Message.GetID()
	f.oneWay = send.oneWay
	f.mu.Lock()
	f.mu.closed = false
	f.mu.notified = false
	f.mu.released = false
	f.mu.Unlock()
}

// enableRequestMetrics starts lifecycle accounting for a client-side unary
// request. Internal heartbeat, stream, and server-side write Futures never call
// this method, so they remain message-level traffic only.
func (f *Future) enableRequestMetrics(m *metrics) {
	if m == nil || f.oneWay || f.send.internal || f.send.stream {
		return
	}
	f.requestMetrics = m
	m.requestStarted()
}

func (f *Future) observeRequest(outcome requestOutcome) {
	m := f.requestMetrics
	if m == nil || !f.requestMetricObserved.CompareAndSwap(false, true) {
		return
	}
	m.requestCompleted(f.send.createAt, outcome)
}

func (f *Future) observeRequestError(err error, fallback requestOutcome) {
	if f.requestMetrics == nil || f.requestMetricObserved.Load() {
		return
	}
	// A deadline/cancellation that happened before a later transport callback is
	// the terminal condition visible to the caller and must win classification.
	if f.send.Ctx != nil {
		if ctxErr := f.send.Ctx.Err(); ctxErr != nil {
			f.observeRequest(requestOutcomeForError(ctxErr, fallback))
			return
		}
	}
	f.observeRequest(requestOutcomeForError(err, fallback))
}

func (f *Future) observeRequestClose() {
	if f.requestMetrics == nil || f.requestMetricObserved.Load() {
		return
	}
	if f.send.Ctx != nil {
		if err := f.send.Ctx.Err(); err != nil {
			f.observeRequest(requestOutcomeForError(err, requestOutcomeAbandoned))
			return
		}
	}
	f.observeRequest(requestOutcomeAbandoned)
}

// Get get the response data synchronously, blocking until `context.Done` or the response is received.
// This method cannot be called more than once. After calling `Get`, `Close` must be called to close
// `Future`.
func (f *Future) Get() (Message, error) {
	// we have to wait until the message is written, otherwise it will result in the message still
	// waiting in the send queue after the Get returns, causing concurrent reading and writing on the
	// request.
	if err := f.waitSendCompleted(); err != nil {
		f.observeRequestError(err, requestOutcomeSendError)
		return nil, err
	}
	select {
	case <-f.send.Ctx.Done():
		f.observeRequestError(f.send.Ctx.Err(), requestOutcomeCanceled)
		return nil, f.send.Ctx.Err()
	case resp := <-f.c:
		f.observeRequest(requestOutcomeSuccess)
		return resp, nil
	case err := <-f.errC:
		f.observeRequestError(err, requestOutcomeBackendError)
		return nil, err
	}
}

// Close closes the future. It must be called exactly once; the Future must not
// be accessed again because Close may return it to an internal object pool.
func (f *Future) Close() {
	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return
	}
	f.observeRequestClose()
	f.mu.closed = true
	cb := f.mu.cb
	f.mu.cb = nil
	release := f.takeReleaseLocked()
	f.mu.Unlock()
	if release != nil {
		release(f)
	}
	if cb != nil {
		cb()
	}
}

func (f *Future) waitSendCompleted() error {
	if f.oneWay {
		panic("one way cannot call waitSendCompleted")
	}
	if f.sendRelease == nil && !f.send.internal {
		return <-f.writtenC
	}
	select {
	case err := <-f.writtenC:
		return err
	case <-f.send.Ctx.Done():
		return f.send.Ctx.Err()
	}
}

func (f *Future) messageSent(err error) {
	if !f.oneWay && f.waiting.CompareAndSwap(false, true) {
		if err != nil {
			f.observeRequestError(err, requestOutcomeSendError)
		}
		if f.sendRelease != nil {
			f.sendRelease(f.send.Message)
		}
		f.writtenC <- err
		f.unRef()
	}
}

func (f *Future) setSendRelease(release func(Message)) {
	f.sendRelease = release
}

func (f *Future) setResponseRelease(release func(Message)) {
	f.responseRelease = release
}

func (f *Future) clearSendRelease() {
	f.sendRelease = nil
}

func (f *Future) takeReleaseLocked() func(*Future) {
	if f.mu.closed && f.mu.ref == 0 && !f.mu.released && f.releaseFunc != nil {
		f.mu.released = true
		f.clear()
		return f.releaseFunc
	}
	return nil
}

func (f *Future) clear() {
	for {
		select {
		case response := <-f.c:
			if f.responseRelease != nil {
				f.responseRelease(response)
			}
		case <-f.errC:
		case <-f.writtenC:
		default:
			return
		}
	}
}

func (f *Future) getSendMessageID() uint64 {
	return f.id
}

// isUserUnary reports whether this Future carries an ordinary user unary
// request: the only traffic class whose response owns a per-request read
// window. The writeLoop flush stamp and pendingRequestReadWindow must use this
// same predicate; a Future counted by the scan but never stamped would read as
// pending forever, and one stamped but not scanned would lose its window.
// (The probe-mode trackLiveness predicate in doWrite intentionally differs:
// it also tracks one-way user traffic.)
func (f *Future) isUserUnary() bool {
	return !f.send.internal && !f.send.stream && !f.oneWay
}

func (f *Future) done(response Message, cb func()) bool {
	f.mu.Lock()
	if f.mu.notified || f.mu.closed || f.timeout() ||
		response.GetID() != f.getSendMessageID() {
		f.mu.Unlock()
		if cb != nil {
			cb()
		}
		return false
	}
	f.mu.cb = cb
	f.observeRequest(requestOutcomeSuccess)
	f.c <- response
	f.mu.notified = true
	f.mu.Unlock()
	return true
}

func (f *Future) error(id uint64, err error, cb func()) bool {
	f.mu.Lock()
	if f.mu.notified || f.mu.closed || f.timeout() ||
		id != f.getSendMessageID() {
		f.mu.Unlock()
		if cb != nil {
			cb()
		}
		return false
	}
	f.mu.cb = cb
	f.observeRequestError(err, requestOutcomeBackendError)
	f.errC <- err
	f.mu.notified = true
	f.mu.Unlock()
	return true
}

func (f *Future) ref() {
	if !f.tryRef() {
		panic("ref released MORPC Future")
	}
}

func (f *Future) tryRef() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.released {
		return false
	}
	f.mu.ref++
	return true
}

func (f *Future) unRef() {
	f.mu.Lock()
	f.mu.ref--
	if f.mu.ref < 0 {
		f.mu.Unlock()
		panic("BUG")
	}
	release := f.takeReleaseLocked()
	f.mu.Unlock()
	if release != nil {
		release(f)
	}
}

func (f *Future) reset() {
	select {
	case <-f.c:
	default:
	}
	f.send = RPCMessage{}
	f.writtenAt.Store(0)
	f.sendRelease = nil
	f.responseRelease = nil
	f.requestMetrics = nil
	f.mu.cb = nil
	f.id = 0
}

func (f *Future) timeout() bool {
	select {
	case <-f.send.Ctx.Done():
		return true
	default:
		return false
	}
}

func (f *Future) setFinalizer() {
	// when we need to reuse, we need to keep chan from being closed to avoid
	// repeated creation. When Future is released by sync.Pool and is GC'd, we
	// need to close chan to avoid resource leaks.
	runtime.SetFinalizer(f, func(f *Future) {
		close(f.c)
		close(f.errC)
		close(f.writtenC)
	})
}
