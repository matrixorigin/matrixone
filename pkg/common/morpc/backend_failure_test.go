// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package morpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type admissionBarrierContext struct {
	context.Context
	calls   atomic.Int32
	waiting chan struct{}
}

func (c *admissionBarrierContext) Done() <-chan struct{} {
	// doSend checks Done once while attempting admission, then waitWrite
	// checks it again. The second call proves the full-queue branch was taken.
	if c.calls.Add(1) == 2 {
		close(c.waiting)
	}
	return c.Context.Done()
}

type blockedFailureSession struct {
	*testIOSession
	failFlush bool
	entered   chan struct{}
	release   chan struct{}
	closeOnce sync.Once
}

func (s *blockedFailureSession) fail() error {
	close(s.entered)
	<-s.release
	return assert.AnError
}

func (s *blockedFailureSession) Write(any, goetty.WriteOptions) error {
	if !s.failFlush {
		return s.fail()
	}
	return nil
}

func (s *blockedFailureSession) Flush(time.Duration) error { return s.fail() }

func (s *blockedFailureSession) Close() error {
	s.closeOnce.Do(func() { _ = s.testIOSession.Close() })
	return nil
}

func TestBackendFailureWakesBlockedStreamAdmission(t *testing.T) {
	for _, failFlush := range []bool{false, true} {
		name := "write"
		if failFlush {
			name = "flush"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn := &blockedFailureSession{testIOSession: newTestIOSession(nil, nil), failFlush: failFlush, entered: make(chan struct{}), release: make(chan struct{})}
			rb := &remoteBackend{
				conn: conn, codec: newTestCodec(), metrics: newMetrics(t.Name()),
				stopper: stopper.NewStopper(t.Name()), readStopper: stopper.NewStopper(t.Name()),
				writeC: make(chan *Future, 1), waitWriteC: make(chan struct{}, 1), stopWriteC: make(chan struct{}),
				resetConnC: make(chan error, 1), closeDone: make(chan struct{}),
			}
			rb.ctx, rb.cancel = context.WithCancel(context.Background())
			rb.adjust()
			rb.stateMu.state = stateRunning
			rb.options.batchSendSize = 1
			s := newStream(rb, make(chan Message, 1), func() *Future { return newFuture(nil) }, rb.doSend, rb.removeActiveStream, func() {})
			s.init(10, false)
			rb.mu.activeStreams = map[uint64]*stream{10: s}
			rb.mu.futures = make(map[uint64]*Future)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(conn.release) }) }
			defer func() { release(); cancel(); rb.Close(); _ = s.Close(false) }()
			queue := func(id uint64) *Future {
				f := newFuture(nil)
				f.init(RPCMessage{Ctx: ctx, Message: newTestMessage(id)})
				f.ref()
				rb.writeC <- f
				rb.changeQueueDepth(1)
				return f
			}
			first := queue(1)
			defer first.Close()
			done := make(chan struct{})
			require.NoError(t, rb.stopper.RunTask(func(c context.Context) { rb.writeLoop(c); close(done) }))
			select {
			case <-conn.entered:
			case <-ctx.Done():
				t.Fatal("writer did not reach failure barrier")
			}
			queued := queue(2)
			defer queued.Close()
			// Consume the earlier fetch notification before admitting the sender.
			select {
			case <-rb.waitWriteC:
			default:
			}
			admissionCtx := &admissionBarrierContext{Context: ctx, waiting: make(chan struct{})}
			sent := make(chan error, 1)
			go func() { sent <- s.Send(admissionCtx, newTestMessage(10)) }()
			select {
			case <-admissionCtx.waiting:
			case <-ctx.Done():
				t.Fatal("sender did not reach full queue")
			}
			release()
			select {
			case err := <-sent:
				require.ErrorIs(t, err, backendClosed)
			case <-ctx.Done():
				t.Fatal("failure did not release admission")
			}
			select {
			case message := <-s.c:
				require.Nil(t, message)
			case <-ctx.Done():
				t.Fatal("failure did not terminate stream")
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("failure did not finish writer")
			}
			require.Error(t, first.waitSendCompleted())
			require.ErrorIs(t, queued.waitSendCompleted(), backendClosed)
			require.NoError(t, ctx.Err(), "caller cancellation must not release cleanup")
			// Failure owns the first stop broadcast; subsequent Close must join
			// teardown without double-closing the signal or joining itself.
			rb.Close()
			rb.Close()
		})
	}
}
