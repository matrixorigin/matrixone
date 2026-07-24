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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFutureWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must panic")
		}
	}()
	f := newFuture(nil)
	f.init(RPCMessage{Ctx: context.Background()})
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
	f.init(newTestRPCMessage(ctx, 1))
	defer f.Close()

	assert.NotNil(t, f)
	assert.False(t, f.mu.closed, false)
	assert.NotNil(t, f.c)
	assert.Equal(t, 0, len(f.c))
	assert.Equal(t, uint64(1), f.getSendMessageID())
	assert.Equal(t, ctx, f.send.Ctx)
}

func TestReleaseFuture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage(1)
	f := newFuture(func(f *Future) { f.reset() })
	f.init(newTestRPCMessage(ctx, 1))
	f.c <- req
	f.Close()
	assert.True(t, f.mu.closed)
	assert.Equal(t, 0, len(f.c))
	assert.Equal(t, RPCMessage{}, f.send)
	assert.Nil(t, f.send.Ctx)
}

func TestGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := newTestMessage(1)
	f := newFuture(func(f *Future) { f.reset() })
	f.ref()
	f.init(newTestRPCMessage(ctx, 1))
	defer f.Close()

	f.messageSent(nil)
	f.done(req, nil)
	resp, err := f.Get()
	assert.Nil(t, err)
	assert.Equal(t, req, resp)
}

func TestGetWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()

	f := newFuture(func(f *Future) { f.reset() })
	f.ref()
	f.init(newTestRPCMessage(ctx, 1))
	defer f.Close()

	f.messageSent(nil)
	resp, err := f.Get()
	assert.NotNil(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, ctx.Err(), err)
}

func TestGetWithError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f := newFuture(func(f *Future) { f.reset() })
	f.ref()
	f.init(newTestRPCMessage(ctx, 1))
	defer f.Close()

	errResp := moerr.NewBackendClosed(context.TODO())
	f.error(1, errResp, nil)

	f.messageSent(nil)
	resp, err := f.Get()
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, errResp, err)
}

func TestGetOwnedRequestReturnsBeforeWriteCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	requestReleased := make(chan Message, 1)
	futureReleased := make(chan struct{}, 1)

	f := newFuture(func(*Future) { futureReleased <- struct{}{} })
	f.ref()
	f.init(newTestRPCMessage(ctx, 1))
	f.setSendRelease(func(message Message) {
		requestReleased <- message
	})

	cancel()
	resp, err := f.Get()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, resp)
	f.Close()

	select {
	case <-requestReleased:
		t.Fatal("request released before the writer completed")
	default:
	}
	select {
	case <-futureReleased:
		t.Fatal("future released while the writer still owned it")
	default:
	}

	f.messageSent(context.Canceled)
	require.Equal(t, uint64(1), (<-requestReleased).GetID())
	select {
	case <-futureReleased:
	case <-time.After(time.Second):
		t.Fatal("future was not released after the writer completed")
	}
}

func TestGetInternalReturnsBeforeWriteCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	futureReleased := make(chan struct{}, 1)

	f := newFuture(func(*Future) { futureReleased <- struct{}{} })
	f.ref()
	f.init(RPCMessage{
		Ctx:      ctx,
		internal: true,
		Message:  &flagOnlyMessage{flag: flagPing},
	})

	cancel()
	resp, err := f.Get()
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, resp)
	f.Close()

	select {
	case <-futureReleased:
		t.Fatal("internal future released while the writer still owned it")
	default:
	}

	f.messageSent(context.Canceled)
	select {
	case <-futureReleased:
	case <-time.After(time.Second):
		t.Fatal("internal future was not released after writer completion")
	}
}

func TestGetWithInvalidResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f := newFuture(func(f *Future) { f.reset() })
	f.init(newTestRPCMessage(ctx, 1))
	defer f.Close()

	f.done(newTestMessage(2), nil)
	assert.Equal(t, 0, len(f.c))
}

func TestFutureTerminalCallbackRunsExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var callbacks atomic.Int32

	f := newFuture(nil)
	f.init(newTestRPCMessage(ctx, 1))
	require.True(t, f.done(newTestMessage(1), func() {
		callbacks.Add(1)
	}))
	f.Close()
	require.Equal(t, int32(1), callbacks.Load())

	late := newFuture(nil)
	late.init(newTestRPCMessage(ctx, 2))
	require.True(t, late.error(2, moerr.NewBackendClosedNoCtx(), nil))
	require.False(t, late.done(newTestMessage(2), func() {
		callbacks.Add(1)
	}))
	require.Equal(t, int32(2), callbacks.Load(),
		"a rejected late response must release its payload callback")
}

func TestFutureCloseReleasesAbandonedResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var callbacks, responses atomic.Int32

	f := newFuture(func(*Future) {})
	f.setResponseRelease(func(Message) {
		responses.Add(1)
	})
	f.init(newTestRPCMessage(ctx, 1))
	require.True(t, f.done(newTestMessage(1), func() {
		callbacks.Add(1)
	}))

	// Simulate a context/response select race or a caller that closes without
	// consuming the response. Future still owns the queued response.
	f.Close()
	require.Equal(t, int32(1), callbacks.Load())
	require.Equal(t, int32(1), responses.Load())
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	f := newFuture(func(f *Future) { f.reset() })
	f.init(newTestRPCMessage(ctx, 1))
	defer f.Close()

	assert.False(t, f.timeout())
	cancel()
	assert.True(t, f.timeout())
}

func TestEarliestDeadline(t *testing.T) {
	now := time.Now()
	early := now.Add(time.Second)
	late := now.Add(3 * time.Second)
	assert.Equal(t, late, earliestDeadline(time.Time{}, late))
	assert.Equal(t, early, earliestDeadline(late, early))
	assert.Equal(t, early, earliestDeadline(early, late))
	assert.Equal(t, early, earliestDeadline(early, time.Time{}))
	assert.Equal(t, time.Second, remainingDeadlineTimeout(early, now))
	assert.Equal(t, time.Nanosecond, remainingDeadlineTimeout(now, early))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	timeout, err := (RPCMessage{Ctx: ctx, internal: true}).GetTimeoutFromContext()
	require.NoError(t, err)
	require.Positive(t, timeout)
	require.LessOrEqual(t, timeout, time.Second)
}

func newTestRPCMessage(ctx context.Context, id uint64) RPCMessage {
	return RPCMessage{Ctx: ctx, Message: newTestMessage(id)}
}
