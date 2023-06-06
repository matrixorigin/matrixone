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
	writtenC    chan error
	sent        atomic.Bool
	releaseFunc func(*Future)
	mu          struct {
		sync.Mutex
		closed bool
		ref    int
		cb     func()
	}
}

func (f *Future) init(send RPCMessage) {
	if _, ok := send.Ctx.Deadline(); !ok {
		panic("context deadline not set")
	}
	f.sent.Store(false)
	f.send = send
	f.id = send.Message.GetID()
	f.mu.Lock()
	f.mu.closed = false
	f.mu.Unlock()
}

// Get get the response data synchronously, blocking until `context.Done` or the response is received.
// This method cannot be called more than once. After calling `Get`, `Close` must be called to close
// `Future`.
func (f *Future) Get() (Message, error) {
	// we have to wait until the message is written, otherwise it will result in the message still
	// waiting in the send queue after the Get returns, causing concurrent reading and writing on the
	// request.
	if err := f.waitSendCompleted(); err != nil {
		return nil, err
	}
	select {
	case <-f.send.Ctx.Done():
		return nil, f.send.Ctx.Err()
	case resp := <-f.c:
		return resp, nil
	case err := <-f.errC:
		return nil, err
	}
}

// Close closes the future.
func (f *Future) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.closed = true
	if f.mu.cb != nil {
		f.mu.cb()
	}
	f.maybeReleaseLocked()
}

func (f *Future) waitSendCompleted() error {
	return <-f.writtenC
}

func (f *Future) messageSent(err error) {
	if f.sent.CompareAndSwap(false, true) {
		f.writtenC <- err
		f.unRef()
	}
}

func (f *Future) maybeReleaseLocked() {
	if f.mu.closed && f.mu.ref == 0 && f.releaseFunc != nil {
		f.releaseFunc(f)
	}
}

func (f *Future) getSendMessageID() uint64 {
	return f.id
}

func (f *Future) done(response Message, cb func()) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.mu.closed && !f.timeout() {
		if response.GetID() != f.getSendMessageID() {
			return
		}
		f.mu.cb = cb
		f.c <- response
	} else if cb != nil {
		cb()
	}
}

func (f *Future) error(id uint64, err error, cb func()) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.mu.closed && !f.timeout() {
		if id != f.getSendMessageID() {
			return
		}
		f.mu.cb = cb
		f.errC <- err
	} else if cb != nil {
		cb()
	}
}

func (f *Future) ref() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.ref++
}

func (f *Future) unRef() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.ref--
	if f.mu.ref < 0 {
		panic("BUG")
	}
	f.maybeReleaseLocked()
}

func (f *Future) reset() {
	select {
	case <-f.c:
	default:
	}
	f.send = RPCMessage{}
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
