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
)

func newFuture(releaseFunc func(f *Future)) *Future {
	f := &Future{
		c:           make(chan struct{}, 1),
		releaseFunc: releaseFunc,
	}
	f.setFinalizer()
	return f
}

// Future is used to obtain response data synchronously.
type Future struct {
	ctx         context.Context
	opts        SendOptions
	response    Message
	request     Message
	c           chan struct{}
	releaseFunc func(*Future)

	mu struct {
		sync.Mutex
		closed bool
		ref    int
	}
}

func (f *Future) init(ctx context.Context, request Message, opts SendOptions) *Future {
	if _, ok := ctx.Deadline(); !ok {
		panic("context deadline not set")
	}

	f.ctx = ctx
	f.request = request
	f.opts = opts

	f.mu.Lock()
	f.mu.closed = false
	f.mu.Unlock()
	return f
}

// Get get the response data synchronously, blocking until `context.Done` or the response is received.
// This method cannot be called more than once. After calling `Get`, `Close` must be called to close
// `Future`.
func (f *Future) Get() (Message, error) {
	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case <-f.c:
		return f.response, nil
	}
}

// Close close the future.
func (f *Future) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.mu.closed = true
	f.maybeReleaseLocked()
}

func (f *Future) maybeReleaseLocked() {
	if f.mu.closed && f.mu.ref == 0 && f.releaseFunc != nil {
		f.releaseFunc(f)
	}
}

func (f *Future) done(response Message) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.mu.closed {
		if response != nil &&
			(f.request == nil || !bytes.Equal(response.ID(), f.request.ID())) {
			return
		}

		f.response = response
		select {
		case f.c <- struct{}{}:
		default:
			panic("BUG")
		}
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
	f.request = nil
	f.response = nil
	f.ctx = nil
	f.opts = SendOptions{}
	select {
	case <-f.c:
	default:
	}
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
