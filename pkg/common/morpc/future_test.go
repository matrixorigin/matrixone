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
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewFutureWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must panic")
		}
	}()
	f := newFuture(nil)
	f.init(0, context.Background())
}

func TestCloseChanAfterGC(t *testing.T) {
	f := newFuture(nil)
	c := f.c
	c <- &testMessage{}
	f = nil
	debug.FreeOSMemory()
	for {
		select {
		case _, ok := <-c:
			if !ok {
				return
			}
		case <-time.After(time.Second * 5):
			assert.Fail(t, "failed")
		}
	}
}

func TestNewFuture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f := newFuture(nil)
	f.init(1, ctx)
	defer f.Close()

	assert.NotNil(t, f)
	assert.False(t, f.mu.closed, false)
	assert.NotNil(t, f.c)
	assert.Equal(t, 0, len(f.c))
	assert.Equal(t, uint64(1), f.id)
	assert.Equal(t, ctx, f.ctx)
}

func TestReleaseFuture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage(1)
	f := newFuture(func(f *Future) { f.reset() })
	f.init(1, ctx)
	f.c <- req
	f.Close()
	assert.True(t, f.mu.closed)
	assert.Equal(t, 0, len(f.c))
	assert.Equal(t, uint64(0), f.id)
	assert.Nil(t, f.ctx)
}

func TestGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage(1)
	f := newFuture(func(f *Future) { f.reset() })
	f.init(1, ctx)
	defer f.Close()

	f.done(req, nil)
	resp, err := f.Get()
	assert.Nil(t, err)
	assert.Equal(t, req, resp)
}

func TestGetWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()

	f := newFuture(func(f *Future) { f.reset() })
	f.init(1, ctx)
	defer f.Close()

	resp, err := f.Get()
	assert.NotNil(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, ctx.Err(), err)
}

func TestGetWithInvalidResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f := newFuture(func(f *Future) { f.reset() })
	f.init(1, ctx)
	defer f.Close()

	f.done(newTestMessage(2), nil)
	assert.Equal(t, 0, len(f.c))
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	f := newFuture(func(f *Future) { f.reset() })
	f.init(1, ctx)
	defer f.Close()

	assert.False(t, f.timeout())
	cancel()
	assert.True(t, f.timeout())
}
