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
	"bytes"
	"context"
	"runtime"
	"sync"
	"time"
)

var (
	futurePool = sync.Pool{
		New: func() interface{} {
			f := &Future{
				c: make(chan struct{}, 1),
			}
			f.setFinalizer()
			return f
		},
	}
)

func acquireFuture(ctx context.Context, request Message, opts SendOptions) *Future {
	if _, ok := ctx.Deadline(); !ok {
		panic("context deadline not set")
	}

	f := futurePool.Get().(*Future)
	f.mu.closed = false
	f.ctx = ctx
	f.request = request
	f.opts = opts
	return f
}

func releaseFuture(f *Future) {
	f.reset()
	futurePool.Put(f)
}

// Future is used to obtain response data synchronously.
type Future struct {
	response        Message
	request         Message
	opts            SendOptions
	err             error
	ctx             context.Context
	ctxDoneCallback func(Message)
	c               chan struct{}

	mu struct {
		sync.Mutex
		closed bool
	}
}

// Get get the response data synchronously, blocking until `context.Done` or the response is received.
// This method cannot be called more than once. After calling `Get`, `Close` must be called to close
// `Future`.
func (f *Future) Get() (Message, error) {
	select {
	case <-f.ctx.Done():
		if f.ctxDoneCallback != nil {
			f.ctxDoneCallback(f.request)
		}
		return nil, f.ctx.Err()
	case <-f.c:
		return f.response, f.err
	}
}

// Close close the future.
func (f *Future) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.closed = true
	releaseFuture(f)
}

func (f *Future) done(response Message, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.mu.closed {
		if response != nil &&
			(f.request == nil || !bytes.Equal(response.ID(), f.request.ID())) {
			return
		}

		f.response = response
		f.err = err
		select {
		case f.c <- struct{}{}:
		default:
			panic("BUG")
		}
	}
}

func (f *Future) reset() {
	f.request = nil
	f.response = nil
	f.err = nil
	f.ctx = nil
	select {
	case <-f.c:
	default:
	}
}

func (f *Future) timeoutDuration() time.Duration {
	timeoutAt, _ := f.ctx.Deadline()
	return time.Until(timeoutAt)
}

func (f *Future) timeout() bool {
	select {
	case <-f.ctx.Done():
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
	})
}

func (f *Future) setContextDoneCallback(ctxDoneCallback func(Message)) {
	f.ctxDoneCallback = ctxDoneCallback
}
