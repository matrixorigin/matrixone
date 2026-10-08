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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// A slow unary response is live work even when business activity is old. Use
// the real socket and heartbeat; age only the idle timestamp, not the request.
func TestIdleGCPreservesPendingUnary(t *testing.T) {
	for _, scenario := range []string{"no_gc", "response", "cancel", "deadline", "draining", "cleanup_full", "other_pending"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)
			// Keep the Unix socket path below the platform length limit.
			dir, err := os.MkdirTemp("", "morpc-idle-")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
			path := filepath.Join(dir, "rpc.sock")
			addr := "unix://" + path
			type receivedRequest struct {
				conn   goetty.IOSession
				id     uint64
				cancel context.CancelFunc
			}
			received := make(chan receivedRequest, 1)
			var writeMu sync.Mutex
			app := newTestAppWithAddr(t, addr, path, func(conn goetty.IOSession, value interface{}, _ uint64) error {
				msg := value.(RPCMessage)
				if msg.internal {
					if msg.Cancel != nil {
						defer msg.Cancel()
					}
					writeMu.Lock()
					defer writeMu.Unlock()
					return conn.Write(RPCMessage{Ctx: ctx, internal: true,
						Message: &flagOnlyMessage{flag: flagPong, id: msg.Message.GetID()}}, goetty.WriteOptions{Flush: true})
				}
				received <- receivedRequest{conn, msg.Message.GetID(), msg.Cancel}
				return nil
			})
			require.NoError(t, app.Start())
			t.Cleanup(func() { require.NoError(t, app.Stop()) })
			cli, err := NewClient("idle-unary", NewGoettyBasedBackendFactory(
				newTestCodec(), WithBackendReadTimeout(time.Minute)),
				WithClientMaxBackendMaxIdleDuration(time.Minute),
				WithClientLogger(logutil.GetPanicLoggerWithLevel(zap.FatalLevel)))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cli.Close()) })
			c := cli.(*client)
			requestCtx, cancelRequest := context.WithCancel(ctx)
			t.Cleanup(cancelRequest)
			if scenario == "deadline" {
				expiring := newManuallyExpiringContext(time.Now().Add(time.Hour))
				requestCtx, cancelRequest = expiring, expiring.expire
				t.Cleanup(cancelRequest)
			}
			f, err := cli.Send(requestCtx, addr, newTestMessage(1))
			require.NoError(t, err)
			t.Cleanup(func() {
				if f != nil {
					f.Close()
				}
			})
			// Creation uses the manager. After admission, drive idle GC directly.
			c.gcManager.unregister(c)
			var request receivedRequest
			select {
			case request = <-received:
			case <-ctx.Done():
				t.Fatal("server did not receive request")
			}
			if request.cancel != nil {
				t.Cleanup(request.cancel)
			}
			c.mu.Lock()
			backends := append([]Backend(nil), c.mu.backends[addr]...)
			c.mu.Unlock()
			require.Len(t, backends, 1)
			rb := backends[0].(*remoteBackend)
			require.False(t, rb.Locked())
			aged := time.Now().Add(-2 * time.Minute)
			rb.atomic.lastActiveTime.Store(aged)
			ping := rb.getFuture(ctx, &flagOnlyMessage{flag: flagPing}, true)
			err = rb.doSend(ping)
			if err == nil {
				_, err = ping.Get()
			} else {
				ping.messageSent(err)
			}
			ping.Close()
			require.NoError(t, err, "healthy peer must answer heartbeat")
			require.Equal(t, aged, rb.LastActiveTime(), "heartbeat is not business activity")
			var other *Future
			var otherRequest receivedRequest
			if scenario == "other_pending" {
				other, err = cli.Send(requestCtx, addr, newTestMessage(2))
				require.NoError(t, err)
				t.Cleanup(other.Close)
				select {
				case otherRequest = <-received:
				case <-ctx.Done():
					t.Fatal("server did not receive second request")
				}
				if otherRequest.cancel != nil {
					t.Cleanup(otherRequest.cancel)
				}
			}
			if scenario == "draining" {
				rb.atomic.draining.Store(true)
			}
			if scenario != "no_gc" {
				require.Zero(t, c.closeIdleBackends(), "an accepted unary request is not an idle connection")
			}
			require.NoError(t, requestCtx.Err())
			if scenario == "cancel" || scenario == "deadline" {
				cancelRequest()
				require.Equal(t, 1, c.closeIdleBackends(), "an expired request must not pin the backend until Future.Close")
				_, err = f.Get()
				require.ErrorIs(t, err, requestCtx.Err())
			} else {
				writeMu.Lock()
				err = request.conn.Write(RPCMessage{Ctx: ctx, Message: newTestMessage(request.id)}, goetty.WriteOptions{Flush: true})
				writeMu.Unlock()
				require.NoError(t, err)
				response, err := f.Get()
				require.NoError(t, err)
				require.Equal(t, request.id, response.GetID())
			}
			f.Close()
			f = nil
			if scenario != "cancel" && scenario != "deadline" {
				if scenario != "draining" {
					rb.atomic.lastActiveTime.Store(aged)
				}
				if other != nil {
					require.Zero(t, c.closeIdleBackends(), "completing one request must preserve another live request")
					writeMu.Lock()
					err = otherRequest.conn.Write(RPCMessage{Ctx: ctx, Message: newTestMessage(otherRequest.id)}, goetty.WriteOptions{Flush: true})
					writeMu.Unlock()
					require.NoError(t, err)
					response, err := other.Get()
					require.NoError(t, err)
					require.Equal(t, otherRequest.id, response.GetID())
					// Response removal, before Future.Close, ends occupancy.
					rb.atomic.lastActiveTime.Store(aged)
				}
				if scenario == "cleanup_full" {
					// A saturated cleanup queue must retain the sealed backend in
					// pool accounting until a later GC can admit its cleanup.
					for i := 0; i < cap(c.backendCleanupSlots); i++ {
						c.backendCleanupSlots <- struct{}{}
					}
					closed := c.closeIdleBackends()
					for i := 0; i < cap(c.backendCleanupSlots); i++ {
						<-c.backendCleanupSlots
					}
					require.Zero(t, closed)
					require.False(t, rb.running())
					c.mu.Lock()
					remaining := len(c.mu.backends[addr])
					c.mu.Unlock()
					require.Equal(t, 1, remaining)
				}
				require.Equal(t, 1, c.closeIdleBackends(), "completed work must not pin an idle connection")
			}
			select {
			case <-rb.closeDone:
			case <-ctx.Done():
				t.Fatal("idle backend cleanup did not finish")
			}
		})
	}
}

func TestIdleGCAdmissionOrder(t *testing.T) {
	for _, scenario := range []string{"request_first", "gc_first", "heartbeat", "fresh_activity", "locked_stream"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			rb := &remoteBackend{
				codec: newTestCodec(), metrics: newMetrics("idle-admission"),
				writeC: make(chan *Future, 1),
			}
			rb.mu.futures = make(map[uint64]*Future)
			rb.pool.futures = &sync.Pool{New: func() interface{} { return newFuture(rb.releaseFuture) }}
			rb.atomic.lastActiveTime.Store(time.Now().Add(-2 * time.Minute))
			switch scenario {
			case "fresh_activity":
				rb.active()
				require.False(t, rb.markIdle(time.Minute), "recheck activity after the pool's old timestamp snapshot")
				return
			case "locked_stream":
				rb.Lock()
				require.False(t, rb.markIdle(time.Minute))
				rb.Unlock()
				require.True(t, rb.markIdle(time.Minute))
				return
			case "gc_first":
				require.True(t, rb.markIdle(time.Minute))
			}
			internal := scenario == "heartbeat"
			f := rb.getFuture(ctx, newTestMessage(1), internal)
			if scenario == "request_first" {
				require.False(t, rb.markIdle(time.Minute), "registration protects work before queue admission")
			} else if internal {
				require.True(t, rb.markIdle(time.Minute), "heartbeat alone must not prevent idle GC")
			}
			err := rb.doSend(f)
			if scenario == "request_first" {
				require.NoError(t, err)
				require.Same(t, f, <-rb.writeC)
				rb.changeQueueDepth(-1)
				f.messageSent(nil)
				require.False(t, rb.markIdle(time.Minute), "a written unary still owns its response")
			} else {
				require.ErrorIs(t, err, backendClosed, "a handle selected before GC cannot admit new work after sealing")
				require.Empty(t, rb.writeC)
				_, streamErr := rb.NewStream(false)
				require.ErrorIs(t, streamErr, backendClosed)
				f.messageSent(err)
			}
			f.Close()
			require.True(t, rb.markIdle(time.Minute))
			require.False(t, rb.running())
			rb.active()
			require.True(t, rb.LastActiveTime().IsZero(), "late activity cannot reactivate a sealed backend")
		})
	}
}
