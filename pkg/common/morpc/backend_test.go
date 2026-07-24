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
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// testError is a custom error type for testing to avoid Makefile err-check
type testError string

func (e testError) Error() string {
	return string(e)
}

var (
	testProxyAddr = "unix:///tmp/proxy.sock"
	testAddr      = "unix:///tmp/goetty.sock"
	testUnixFile  = "/tmp/goetty.sock"
)

func TestSend(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
	)
}

func TestSendContextErrorReleasesFuture(t *testing.T) {
	rb := &remoteBackend{
		codec:      newTestCodec(),
		metrics:    newMetrics(""),
		waitWriteC: make(chan struct{}, 1),
		writeC:     make(chan *Future, 1),
	}
	rb.stateMu.state = stateRunning
	rb.mu.futures = make(map[uint64]*Future)
	rb.pool.futures = &sync.Pool{
		New: func() any {
			return newFuture(rb.releaseFuture)
		},
	}
	// Keep the write queue full so Send exits through the caller-context path
	// before the future is handed to the write loop.
	rb.writeC <- nil

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	future, err := rb.Send(ctx, newTestMessage(1))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, future)

	rb.mu.RLock()
	defer rb.mu.RUnlock()
	require.Empty(t, rb.mu.futures,
		"a future that was never enqueued must be removed on Send failure")
}

func TestSendFailureKeepsRequestOwnership(t *testing.T) {
	var released atomic.Int32
	rb := &remoteBackend{
		codec:      newTestCodec(),
		metrics:    newMetrics(""),
		waitWriteC: make(chan struct{}, 1),
		writeC:     make(chan *Future, 1),
	}
	rb.options.releaseRequest = func(Message) {
		released.Add(1)
	}
	rb.stateMu.state = stateStopped
	rb.mu.futures = make(map[uint64]*Future)
	rb.pool.futures = &sync.Pool{
		New: func() any {
			return newFuture(rb.releaseFuture)
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	future, err := rb.Send(ctx, newTestMessage(1))
	require.ErrorIs(t, err, backendClosed)
	require.Nil(t, future)
	require.Zero(t, released.Load(),
		"a request that was not enqueued remains owned by the caller")
}

func TestSuccessfulSendReleasesOwnedRequest(t *testing.T) {
	released := make(chan Message, 1)
	testBackendSend(
		t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			require.NoError(t, err)
			select {
			case request := <-released:
				require.Equal(t, f.getSendMessageID(), request.GetID())
			case <-ctx.Done():
				t.Fatal("writer did not release the owned request")
			}
		},
		WithBackendRequestRelease(func(message Message) {
			released <- message
		}),
	)
}

func TestWaitWriteWakesWhenBackendStops(t *testing.T) {
	rb := &remoteBackend{
		waitWriteC: make(chan struct{}),
		stopWriteC: make(chan struct{}),
	}
	woke := make(chan struct{})
	go func() {
		rb.waitWrite(context.Background())
		close(woke)
	}()

	close(rb.stopWriteC)
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("write waiter did not observe backend stop")
	}
}

func TestReadTimeoutWithNormalMessageMissed(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			request := msg.(RPCMessage)
			if request.internal {
				if m, ok := request.Message.(*flagOnlyMessage); ok {
					switch m.flag {
					case flagPing:
						return conn.Write(RPCMessage{
							Ctx:      request.Ctx,
							internal: true,
							Message: &flagOnlyMessage{
								flag: flagPong,
								id:   m.id,
							},
						}, goetty.WriteOptions{Flush: true})
					default:
						panic(fmt.Sprintf("invalid internal message, flag %d", m.flag))
					}
				}
			}
			// no response
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			assert.Equal(t, ctx.Err(), err)
		},
		WithBackendReadTimeout(time.Millisecond*200),
	)
}

func TestReadTimeout(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			// no response
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			assert.NotEqual(t, backendClosed, err)
		},
		WithBackendReadTimeout(time.Millisecond*200),
	)
}

func TestHealthyLivenessProbePreservesSlowRequest(t *testing.T) {
	requestReceived := make(chan struct{})
	releaseResponse := make(chan struct{})
	probed := make(chan struct{}, 4)
	var requestOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseResponse) }) }
	t.Cleanup(release)

	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			request := msg.(RPCMessage)
			requestOnce.Do(func() { close(requestReceived) })
			select {
			case <-releaseResponse:
			case <-time.After(time.Second):
				return context.DeadlineExceeded
			}
			return conn.Write(RPCMessage{
				Ctx:     request.Ctx,
				Message: newTestMessage(request.Message.GetID()),
			}, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			defer f.Close()

			select {
			case <-requestReceived:
			case <-ctx.Done():
				t.Fatal("request did not reach server")
			}
			for range 2 {
				select {
				case <-probed:
				case <-ctx.Done():
					t.Fatal("independent liveness probe did not run")
				}
			}
			_, err = b.Send(ctx, newTestMessage(2))
			require.ErrorIs(t, err, backendDraining)
			_, err = b.NewStream(false)
			require.ErrorIs(t, err, backendDraining)
			b.mu.RLock()
			require.Len(t, b.mu.futures, 1,
				"draining must reject direct admission without disturbing the slow request")
			require.Empty(t, b.mu.activeStreams)
			b.mu.RUnlock()
			release()
			_, err = f.Get()
			require.NoError(t, err,
				"a healthy peer must not lose a valid slow request at the data read timeout")
		},
		WithBackendReadTimeout(20*time.Millisecond),
		WithBackendLivenessProbe(func(context.Context, string) error {
			probed <- struct{}{}
			return nil
		}),
	)
}

func TestIdleBackendDoesNotProbeOrDrain(t *testing.T) {
	var probes atomic.Int32
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			request := msg.(RPCMessage)
			return conn.Write(RPCMessage{
				Ctx:     request.Ctx,
				Message: newTestMessage(request.Message.GetID()),
			}, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			time.Sleep(100 * time.Millisecond)
			require.Zero(t, probes.Load())
			require.True(t, b.admissionAvailable())
			require.False(t, b.LastActiveTime().IsZero())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			require.NoError(t, err)
		},
		WithBackendReadTimeout(20*time.Millisecond),
		WithBackendLivenessProbe(func(context.Context, string) error {
			probes.Add(1)
			return nil
		}),
	)
}

func TestUnavailableControlDoesNotResetIdleDataConnection(t *testing.T) {
	var probes atomic.Int32
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			request := msg.(RPCMessage)
			return conn.Write(RPCMessage{
				Ctx:     request.Ctx,
				Message: newTestMessage(request.Message.GetID()),
			}, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			time.Sleep(100 * time.Millisecond)
			require.Zero(t, probes.Load(),
				"idle data must not depend on an unavailable control transport")
			require.True(t, b.admissionAvailable())

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			require.NoError(t, err)
		},
		WithBackendReadTimeout(20*time.Millisecond),
		WithBackendLivenessProbe(func(context.Context, string) error {
			probes.Add(1)
			return errors.New("control transport unavailable")
		}),
	)
}

func TestSkippedRequestDoesNotDrainHealthyBackend(t *testing.T) {
	var probes atomic.Int32
	testBackendSend(t,
		func(goetty.IOSession, interface{}, uint64) error {
			t.Fatal("filtered request must not reach the transport")
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			_, err = f.Get()
			require.ErrorIs(t, err, messageSkipped)
			f.Close()

			time.Sleep(100 * time.Millisecond)
			require.Zero(t, probes.Load(),
				"a request filtered before conn.Write must not trigger a control probe")
			require.True(t, b.admissionAvailable(),
				"a request skipped before conn.Write is not stalled data traffic")
		},
		WithBackendReadTimeout(20*time.Millisecond),
		WithBackendFilter(func(Message, string) bool { return false }),
		WithBackendLivenessProbe(func(context.Context, string) error {
			probes.Add(1)
			return nil
		}),
	)
}

func TestTimedOutRequestStillDrainsBlackholedBackend(t *testing.T) {
	probed := make(chan struct{}, 1)
	testBackendSend(t,
		func(goetty.IOSession, interface{}, uint64) error {
			// Simulate a request accepted by the peer whose data response path
			// never makes progress.
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			_, err = f.Get()
			require.ErrorIs(t, err, context.DeadlineExceeded)
			f.Close()

			select {
			case <-probed:
			case <-time.After(time.Second):
				t.Fatal("liveness probe did not run after the short Future left the map")
			}
			require.Eventually(t, func() bool {
				return !b.admissionAvailable() && b.LastActiveTime().IsZero()
			}, time.Second, time.Millisecond)
			retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
			defer retryCancel()
			_, err = b.Send(retryCtx, newTestMessage(2))
			require.ErrorIs(t, err, backendDraining,
				"latched user traffic must seal the blackholed data generation")
		},
		WithBackendReadTimeout(50*time.Millisecond),
		WithBackendLivenessProbe(func(context.Context, string) error {
			probed <- struct{}{}
			return nil
		}),
	)
}

func TestContinuousTimedOutRequestsCannotPostponeProbe(t *testing.T) {
	var probes atomic.Int32
	testBackendSend(t,
		func(goetty.IOSession, interface{}, uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			deadline := time.Now().Add(time.Second)
			for probes.Load() == 0 && time.Now().Before(deadline) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
				f, err := b.Send(ctx, newTestMessage(1))
				if err == nil {
					_, _ = f.Get()
					f.Close()
				} else {
					require.ErrorIs(t, err, backendDraining)
				}
				cancel()
			}
			require.Positive(t, probes.Load(),
				"new short requests must not move the oldest unprogressed write epoch")
			require.Eventually(t, func() bool {
				return !b.admissionAvailable()
			}, time.Second, time.Millisecond)
		},
		WithBackendReadTimeout(50*time.Millisecond),
		WithBackendLivenessProbe(func(context.Context, string) error {
			probes.Add(1)
			return nil
		}),
	)
}

func TestDataProgressLatchPreservesWriteReadOrdering(t *testing.T) {
	rb := &remoteBackend{}
	rb.options.bufferSize = 16

	rb.recordDataWrite(1, 1)
	require.Equal(t, int64(1), rb.dataPendingSince())

	rb.recordDataProgress(1, true, 2)
	require.Zero(t, rb.dataPendingSince(),
		"the matching response retires the only pending write")

	rb.recordDataWrite(2, 3)
	require.Equal(t, int64(3), rb.dataPendingSince(),
		"a later write must open a new pending generation")
}

func TestConcurrentResponseDoesNotHideAnotherPendingWrite(t *testing.T) {
	rb := &remoteBackend{}
	rb.options.bufferSize = 16

	rb.recordDataWrite(1, 1)
	rb.recordDataWrite(2, 2)
	rb.recordDataProgress(2, true, 3)
	require.Equal(t, int64(3), rb.dataPendingSince(),
		"progress resets the inactivity window but must preserve the unmatched request")

	rb.recordDataProgress(1, true, 4)
	require.Zero(t, rb.dataPendingSince())
}

func TestStreamProgressCannotRetireUnaryWrite(t *testing.T) {
	rb := &remoteBackend{}
	rb.options.bufferSize = 16

	rb.recordDataWrite(7, 1)
	rb.recordDataProgress(7, false, 3)
	require.Equal(t, int64(3), rb.dataPendingSince(),
		"stream response sequence numbers do not correlate to request sequence numbers")
}

func TestDataProgressPendingSetIsBounded(t *testing.T) {
	rb := &remoteBackend{}
	rb.options.bufferSize = 2

	rb.recordDataWrite(1, 1)
	rb.recordDataWrite(2, 2)
	rb.recordDataWrite(3, 3)
	require.Len(t, rb.livenessMu.pending, 2)
	require.True(t, rb.livenessMu.overflow)

	rb.recordDataProgress(1, true, 4)
	rb.recordDataProgress(2, true, 5)
	require.Equal(t, int64(5), rb.dataPendingSince(),
		"overflow remains a conservative pending latch until generation reset")

	rb.resetDataProgress()
	require.Zero(t, rb.dataPendingSince())
	require.False(t, rb.livenessMu.overflow)
}

func TestFailedLivenessProbeResetsDataConnection(t *testing.T) {
	probed := make(chan struct{}, 1)
	testBackendSend(t,
		func(goetty.IOSession, interface{}, uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			require.Error(t, err)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			select {
			case <-probed:
			default:
				t.Fatal("failed liveness probe did not run")
			}
		},
		WithBackendReadTimeout(20*time.Millisecond),
		WithBackendLivenessProbe(func(context.Context, string) error {
			probed <- struct{}{}
			return errors.New("control transport unavailable")
		}),
	)
}

func TestRequestDoneReleasesRejectedResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f := newFuture(nil)
	f.init(newTestRPCMessage(ctx, 1))
	require.True(t, f.error(1, moerr.NewBackendClosedNoCtx(), nil))

	var callbacks, responses atomic.Int32
	rb := &remoteBackend{
		logger:  zap.NewNop(),
		metrics: newMetrics(""),
	}
	rb.mu.futures = map[uint64]*Future{1: f}
	rb.options.freeResponse = func(Message) {
		responses.Add(1)
	}
	response := newTestMessage(1)
	rb.requestDone(ctx, 1, RPCMessage{Message: response}, nil, func() {
		callbacks.Add(1)
	})

	require.Equal(t, int32(1), callbacks.Load())
	require.Equal(t, int32(1), responses.Load())
	require.Empty(t, rb.mu.futures)
}

func TestIndependentControlBackendPreservesSlowDataRequest(t *testing.T) {
	requestReceived := make(chan struct{})
	releaseResponse := make(chan struct{})
	probed := make(chan struct{}, 4)
	var sessionMu sync.Mutex
	var firstDataSession, replacementDataSession, controlSession goetty.IOSession
	var dataRequests atomic.Int32
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseResponse) }) }
	t.Cleanup(release)

	app := newTestApp(t, func(conn goetty.IOSession, msg interface{}, _ uint64) error {
		request := msg.(RPCMessage)
		if request.internal {
			sessionMu.Lock()
			controlSession = conn
			sessionMu.Unlock()
			ping := request.Message.(*flagOnlyMessage)
			return conn.Write(RPCMessage{
				Ctx:      request.Ctx,
				internal: true,
				Message: &flagOnlyMessage{
					flag: flagPong,
					id:   ping.id,
				},
			}, goetty.WriteOptions{Flush: true})
		}

		requestNumber := dataRequests.Add(1)
		sessionMu.Lock()
		if requestNumber == 1 {
			firstDataSession = conn
		} else {
			replacementDataSession = conn
		}
		sessionMu.Unlock()
		if requestNumber == 1 {
			close(requestReceived)
			select {
			case <-releaseResponse:
			case <-time.After(time.Second):
				return context.DeadlineExceeded
			}
		}
		return conn.Write(RPCMessage{
			Ctx:     request.Ctx,
			Message: newTestMessage(request.Message.GetID()),
		}, goetty.WriteOptions{Flush: true})
	})
	require.NoError(t, app.Start())
	defer func() { require.NoError(t, app.Stop()) }()

	newBackend := func(options ...BackendOption) *remoteBackend {
		options = append(options,
			WithBackendMetrics(newMetrics("")),
			WithBackendLogger(logutil.GetPanicLoggerWithLevel(zap.FatalLevel)))
		value, err := NewRemoteBackend(testAddr, newTestCodec(), options...)
		require.NoError(t, err)
		return value.(*remoteBackend)
	}

	control := newBackend(WithBackendReadTimeout(100 * time.Millisecond))
	defer control.Close()

	dataFactory := NewGoettyBasedBackendFactory(
		newTestCodec(),
		WithBackendReadTimeout(20*time.Millisecond),
		WithBackendLivenessProbe(func(ctx context.Context, _ string) error {
			f, err := control.SendInternal(ctx, &flagOnlyMessage{flag: flagPing})
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = f.Get()
			if err == nil {
				probed <- struct{}{}
			}
			return err
		}),
	)
	dataClient, err := NewClient(
		"data-replacement-test",
		dataFactory,
		WithClientMaxBackendPerHost(1),
		WithClientMaxBackendMaxIdleDuration(time.Nanosecond),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, dataClient.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f, err := dataClient.Send(ctx, testAddr, newTestMessage(1))
	require.NoError(t, err)
	defer f.Close()
	select {
	case <-requestReceived:
	case <-ctx.Done():
		t.Fatal("data request did not reach server")
	}
	for range 2 {
		select {
		case <-probed:
		case <-ctx.Done():
			t.Fatal("control ping did not complete independently")
		}
	}
	require.Zero(t, dataClient.(*client).closeIdleBackends(),
		"idle GC must not close a draining backend with an outstanding request")

	replacement, err := dataClient.Send(ctx, testAddr, newTestMessage(2))
	require.NoError(t, err)
	defer replacement.Close()
	_, err = replacement.Get()
	require.NoError(t, err,
		"new traffic must recover on a replacement data generation")

	sessionMu.Lock()
	require.NotNil(t, firstDataSession)
	require.NotNil(t, replacementDataSession)
	require.NotNil(t, controlSession)
	require.NotEqual(t, firstDataSession, controlSession,
		"control ping must use a physical session independent from data")
	require.NotEqual(t, firstDataSession, replacementDataSession,
		"new traffic must not stay on the stalled data session")
	sessionMu.Unlock()

	release()
	_, err = f.Get()
	require.NoError(t, err)
}

func TestSendWithPayloadCannotTimeout(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			b.conn.RawConn().SetWriteDeadline(time.Now().Add(time.Millisecond))
			time.Sleep(time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			req := newTestMessage(1)
			req.payload = []byte("hello")
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
	)
}

func TestSendWithPayloadCannotBlockIfFutureRemoved(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			wg.Wait()
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			req := newTestMessage(1)
			req.payload = []byte("hello")
			f, err := b.Send(ctx, req)
			require.NoError(t, err)
			id := f.getSendMessageID()
			// keep future in the futures map
			f.ref()
			defer f.unRef()
			f.Close()
			b.mu.RLock()
			_, ok := b.mu.futures[id]
			assert.True(t, ok)
			b.mu.RUnlock()
			wg.Done()
			time.Sleep(time.Second)
		},
		WithBackendHasPayloadResponse())
}

func TestSendWithPayloadCannotBlockIfFutureClosed(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			wg.Wait()
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()
			req := newTestMessage(1)
			req.payload = []byte("hello")
			f, err := b.Send(ctx, req)
			require.NoError(t, err)
			id := f.getSendMessageID()
			f.mu.Lock()
			f.mu.closed = true
			f.releaseFunc = nil // make it nil to keep this future in b.mu.features
			f.mu.Unlock()
			b.mu.RLock()
			_, ok := b.mu.futures[id]
			b.mu.RUnlock()
			assert.True(t, ok)
			wg.Done()
			time.Sleep(time.Second)
		},
		WithBackendHasPayloadResponse())
}

func TestCloseWhileContinueSending(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			c := make(chan struct{})
			stopC := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendFunc := func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					req := newTestMessage(1)
					f, err := b.Send(ctx, req)
					if err != nil {
						return
					}
					defer f.Close()

					resp, err := f.Get()
					if err == nil {
						assert.Equal(t, req, resp)
					}
					select {
					case c <- struct{}{}:
					default:
					}
				}

				for {
					select {
					case <-stopC:
						return
					default:
						sendFunc()
					}
				}
			}()
			<-c
			b.Close()
			close(stopC)
			wg.Wait()
		},
	)
}

func TestSendWithAlreadyContextDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)

	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {

			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()
			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
		},
		WithBackendFilter(func(Message, string) bool {
			cancel()
			return true
		}))
}

func TestSendWithTimeout(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
			defer cancel()
			req := &testMessage{id: 1}
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
			assert.Equal(t, err, ctx.Err())
		},
	)
}

func TestSendWithCannotConnect(t *testing.T) {
	var rb *remoteBackend
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			rb = b
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()
			req := &testMessage{id: 1}
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.Error(t, err)
			assert.Nil(t, resp)
		},
		WithBackendFilter(func(Message, string) bool {
			assert.NoError(t, rb.conn.Disconnect())
			rb.remote = ""
			return true
		}),
		WithBackendConnectTimeout(time.Millisecond*200),
	)
}

func TestFutureGetCannotBlockIfCloseBackend(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Close()
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*100)
			defer cancel()

			n := 2
			futures := make([]*Future, 0, n)
			for i := 0; i < n; i++ {
				req := newTestMessage(1)
				f, err := b.Send(ctx, req)
				assert.NoError(t, err)
				futures = append(futures, f)
			}
			b.Close()
			for _, f := range futures {
				_, err := f.Get()
				assert.Error(t, err)
			}
		},
		WithBackendBatchSendSize(1),
		WithBackendFilter(func(m Message, s string) bool {
			time.Sleep(time.Millisecond * 100)
			return false
		}),
	)
}

func TestCloseBackendNotifiesWaitingFuture(t *testing.T) {
	received := make(chan struct{})
	var once sync.Once
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			once.Do(func() { close(received) })
			return nil
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			f, err := b.Send(ctx, newTestMessage(1))
			require.NoError(t, err)
			defer f.Close()
			select {
			case <-received:
			case <-time.After(time.Second):
				t.Fatal("request was not written before backend close")
			}

			resultC := make(chan error, 1)
			go func() {
				_, err := f.Get()
				resultC <- err
			}()

			closeC := make(chan struct{})
			go func() {
				b.Close()
				close(closeC)
			}()
			select {
			case <-closeC:
			case <-time.After(time.Second):
				t.Fatal("backend close did not return")
			}
			select {
			case err := <-resultC:
				require.ErrorIs(t, err, backendClosed)
			case <-time.After(time.Second):
				t.Fatal("waiting future was not notified when backend closed")
			}
		},
	)
}

func TestStream(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			st, err := b.NewStream(false)
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close(false))
				b.mu.RLock()
				assert.Equal(t, 0, len(b.mu.futures))
				b.mu.RUnlock()
			}()

			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			n := 1
			for i := 0; i < n; i++ {
				req := &testMessage{id: st.ID()}
				assert.NoError(t, st.Send(ctx, req))
			}

			rc, err := st.Receive()
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				v, ok := <-rc
				assert.True(t, ok)
				assert.Equal(t, &testMessage{id: st.ID()}, v)
			}
		},
	)
}

func TestCloseStreamWithCloseConn(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			for {
				if err := conn.Write(msg, goetty.WriteOptions{Flush: true}); err != nil {
					return err
				}
			}
		},
		func(b *remoteBackend) {
			st, err := b.NewStream(false)
			assert.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			req := &testMessage{id: st.ID()}
			assert.NoError(t, st.Send(ctx, req))

			for {
				n := len(st.(*stream).c)
				if n == 2 {
					break
				}
				time.Sleep(time.Millisecond * 10)
			}

			require.NoError(t, st.Close(true))

			_, err = st.Receive()
			require.Error(t, err)

			b.Lock()
			defer b.Unlock()
			assert.Equal(t, stateStopped, b.stateMu.state)
		},
		WithBackendStreamBufferSize(2),
	)
}

func TestStreamSendWillPanicIfDeadlineNotSet(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			st, err := b.NewStream(false)
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close(false))
				b.mu.RLock()
				assert.Equal(t, 0, len(b.mu.futures))
				b.mu.RUnlock()
			}()

			defer func() {
				if err := recover(); err == nil {
					assert.Fail(t, "must panic")
				}
			}()

			req := &testMessage{id: st.ID()}
			assert.NoError(t, st.Send(context.TODO(), req))
		},
	)
}

func TestStreamClosedByConnReset(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Disconnect()
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			st, err := b.NewStream(false)
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close(false))
			}()
			c, err := st.Receive()
			assert.NoError(t, err)
			assert.NoError(t, st.Send(ctx, &testMessage{id: st.ID()}))

			v, ok := <-c
			assert.True(t, ok)
			assert.Nil(t, v)
		},
	)
}

func TestStreamClosedBySequenceNotMatch(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			resp := msg.(RPCMessage)
			resp.streamSequence = 2
			return conn.Write(resp, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			st, err := b.NewStream(false)
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close(false))
			}()
			c, err := st.Receive()
			assert.NoError(t, err)
			assert.NoError(t, st.Send(ctx, &testMessage{id: st.ID()}))

			v, ok := <-c
			assert.True(t, ok)
			assert.Nil(t, v)
		},
	)
}

func TestBusy(t *testing.T) {
	n := 0
	c := make(chan struct{})
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			assert.False(t, b.Busy())

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
			defer cancel()
			f1, err := b.Send(ctx, newTestMessage(1))
			assert.NoError(t, err)
			defer f1.Close()

			f2, err := b.Send(ctx, newTestMessage(2))
			assert.NoError(t, err)
			defer f2.Close()

			assert.True(t, b.Busy())
			c <- struct{}{}
		},
		WithBackendFilter(func(Message, string) bool {
			if n == 0 {
				<-c
				n++
			}
			return false
		}),
		WithBackendBatchSendSize(1),
		WithBackendBufferSize(10),
		WithBackendBusyBufferSize(1))
}

func TestDoneWithClosedStreamCannotPanic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	c := make(chan Message, 1)
	s := newStream(
		nil,
		c,
		func() *Future { return newFuture(nil) },
		func(m *Future) error {
			m.messageSent(nil)
			return nil
		},
		func(s *stream) {},
		func() {})
	s.init(1, false)
	assert.NoError(t, s.Send(ctx, &testMessage{id: s.ID()}))
	assert.NoError(t, s.Close(false))
	s.done(context.TODO(), RPCMessage{}, false)
}

func TestCloseStreamUnblocksFullReceiveChannel(t *testing.T) {
	c := make(chan Message, 1)
	unregistered := 0
	s := newStream(
		nil,
		c,
		func() *Future { return newFuture(nil) },
		func(m *Future) error { return nil },
		func(s *stream) { unregistered++ },
		func() {})
	s.init(1, false)
	c <- newTestMessage(1)

	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		s.done(context.Background(), RPCMessage{
			Message:        newTestMessage(1),
			stream:         true,
			streamSequence: 1,
		}, false)
	}()

	deadline := time.Now().Add(time.Second)
	for s.mu.TryLock() {
		s.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("stream response did not block on the full receive channel")
		}
		runtime.Gosched()
	}

	closeC := make(chan error, 1)
	go func() { closeC <- s.Close(false) }()
	select {
	case err := <-closeC:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("stream close blocked behind a full receive channel")
	}
	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatal("stream response delivery did not observe stream close")
	}
	require.Equal(t, 1, unregistered)
	select {
	case message := <-c:
		require.Nil(t, message)
	default:
		t.Fatal("stream close did not publish a terminal response")
	}
	require.Empty(t, c)
}

func TestClosedStreamHandleAndChannelAreNotReused(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error { return nil },
		func(b *remoteBackend) {
			first, err := b.NewStream(false)
			require.NoError(t, err)
			firstC, err := first.Receive()
			require.NoError(t, err)
			require.NoError(t, first.Close(false))

			second, err := b.NewStream(false)
			require.NoError(t, err)
			secondC, err := second.Receive()
			require.NoError(t, err)
			defer func() { require.NoError(t, second.Close(false)) }()

			require.NotSame(t, first, second)
			require.NotEqual(t, firstC, secondC)
		},
	)
}

func TestRemoveActiveStreamDoesNotHoldBackendMutexWhileUnlocking(t *testing.T) {
	rb := &remoteBackend{}
	rb.stateMu.state = stateStopped
	rb.stateMu.locked = true
	rb.mu.futures = make(map[uint64]*Future)
	rb.mu.activeStreams = make(map[uint64]*stream)
	s := &stream{
		rb:               rb,
		c:                make(chan Message, 1),
		id:               1,
		unlockAfterClose: true,
	}
	rb.mu.activeStreams[s.id] = s

	// Reproduce NewStream's stateMu -> rb.mu order while stream removal
	// needs to release the backend lock via stateMu.
	rb.mu.Lock()
	rb.stateMu.RLock()
	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		rb.removeActiveStream(s)
	}()
	rb.mu.Unlock()

	deadline := time.Now().Add(time.Second)
	for {
		if rb.mu.TryLock() {
			_, exists := rb.mu.activeStreams[s.id]
			rb.mu.Unlock()
			if !exists {
				break
			}
		}
		if time.Now().After(deadline) {
			rb.stateMu.RUnlock()
			t.Fatal("stream removal held rb.mu while waiting for stateMu")
		}
		runtime.Gosched()
	}

	rb.stateMu.RUnlock()
	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatal("stream removal did not finish after stateMu was released")
	}
	require.False(t, rb.Locked())
}

func TestGCStream(t *testing.T) {
	c := make(chan Message, 1)
	s := newStream(
		nil,
		c,
		func() *Future { return newFuture(nil) },
		func(m *Future) error {
			return nil
		},
		func(s *stream) {},
		func() {})
	s.init(1, false)
	s = nil
	debug.FreeOSMemory()
	_, ok := <-c
	assert.False(t, ok)
}

func TestLastActiveWithNew(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return nil
		},
		func(b *remoteBackend) {
			assert.NotEqual(t, time.Time{}, b.LastActiveTime())
		},
	)
}

func TestLastActiveWithSend(t *testing.T) {
	c := make(chan struct{})
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			<-c
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			t1 := b.LastActiveTime()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			t2 := b.LastActiveTime()
			assert.NotEqual(t, t1, t2)
			assert.True(t, t2.After(t1))
			c <- struct{}{}

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)

			t3 := b.LastActiveTime()
			assert.NotEqual(t, t2, t3)
			assert.True(t, t3.After(t2))

		},
	)
}

func TestLastActiveWithStream(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			t1 := b.LastActiveTime()

			st, err := b.NewStream(false)
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, st.Close(false))
			}()

			n := 1
			for i := 0; i < n; i++ {
				req := &testMessage{id: st.ID()}
				assert.NoError(t, st.Send(ctx, req))
				t2 := b.LastActiveTime()
				assert.NotEqual(t, t1, t2)
				assert.True(t, t2.After(t1))
			}
		},
	)
}

// TestReadLoopInternalMessageDoesNotUpdateLastActive covers the fix: readLoop must only call
// active() for non-internal messages. Heartbeat (ping/pong) is internal and must not update
// LastActiveTime, so idle timeout can still collect backends that only receive heartbeats.
func TestReadLoopInternalMessageDoesNotUpdateLastActive(t *testing.T) {
	// Event-driven: wait for server to send pong (pongSent), then assert lastActive unchanged (no Sleep ordering).
	pongSent := make(chan struct{}, 1)
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			request := msg.(RPCMessage)
			if request.InternalMessage() {
				if m, ok := request.Message.(*flagOnlyMessage); ok && m.flag == flagPing {
					select {
					case pongSent <- struct{}{}:
					default:
					}
					return conn.Write(RPCMessage{
						Ctx:      request.Ctx,
						internal: true,
						Message:  &flagOnlyMessage{flag: flagPong, id: m.id},
					}, goetty.WriteOptions{Flush: true})
				}
			}
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			t0 := b.LastActiveTime()
			select {
			case <-pongSent:
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for pong (heartbeat may not have fired or readLoop stalled)")
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				runtime.Gosched()
				if b.LastActiveTime().Equal(t0) {
					break
				}
			}
			require.True(t, b.LastActiveTime().Equal(t0), "internal (pong) message must not call active(); lastActive changed")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()
			_, err = f.Get()
			assert.NoError(t, err)
			t2 := b.LastActiveTime()
			assert.True(t, t2.After(t0), "user response must call active() and update lastActive")
		},
		WithBackendReadTimeout(30*time.Millisecond),
	)
}

func TestBackendConnectTimeout(t *testing.T) {
	rb, err := NewRemoteBackend(
		testAddr,
		newTestCodec(),
		WithBackendMetrics(newMetrics("")),
		WithBackendConnectTimeout(time.Millisecond*200),
	)
	assert.Error(t, err)
	assert.Nil(t, rb)
}

func TestInactiveAfterCannotConnect(t *testing.T) {
	app := newTestApp(t, func(conn goetty.IOSession, msg interface{}, _ uint64) error {
		return conn.Write(msg, goetty.WriteOptions{Flush: true})
	})
	assert.NoError(t, app.Start())

	testBackendSendWithoutServer(t,
		testAddr,
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)

			assert.NoError(t, app.Stop())
			var v time.Time
			for {
				if b.LastActiveTime() == v {
					break
				}
				time.Sleep(time.Millisecond * 100)
			}
		},
		WithBackendConnectTimeout(time.Millisecond*100))
}

func TestTCPProxyExample(t *testing.T) {
	assert.NoError(t, os.RemoveAll(testProxyAddr[7:]))
	p := goetty.NewProxy(testProxyAddr, nil)
	assert.NoError(t, p.Start())
	defer func() {
		assert.NoError(t, p.Stop())
	}()
	p.AddUpStream(testAddr, time.Second*10)

	testBackendSendWithAddr(t,
		testProxyAddr,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			resp, err := f.Get()
			assert.NoError(t, err)
			assert.Equal(t, req, resp)
		},
	)
}

func TestLockedStream(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			assert.False(t, b.Locked())
			b.Lock()
			st, err := b.NewStream(true)
			assert.NoError(t, err)
			assert.True(t, b.Locked())
			assert.NoError(t, st.Close(false))
			assert.False(t, b.Locked())
		},
	)
}

func TestIssue7678(t *testing.T) {
	s := &stream{}
	s.lastReceivedSequence = 10
	s.init(0, false)
	assert.Equal(t, uint32(0), s.lastReceivedSequence)
}

// TestRemoteBackendUsesSharedLogger ensures we do not clone the logger per backend
// (no Logger.With()), which would cause large allocations under many backends (e.g. sysbench).
func TestRemoteBackendUsesSharedLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger := logutil.GetPanicLoggerWithLevel(zap.DebugLevel)
	app := newTestApp(t, func(conn goetty.IOSession, msg interface{}, _ uint64) error {
		return conn.Write(msg, goetty.WriteOptions{Flush: true})
	})
	require.NoError(t, app.Start())
	defer func() { assert.NoError(t, app.Stop()) }()

	opts := []BackendOption{
		WithBackendMetrics(newMetrics("")),
		WithBackendBufferSize(1),
		WithBackendLogger(logger),
	}
	rb, err := NewRemoteBackend(testAddr, newTestCodec(), opts...)
	require.NoError(t, err)
	b := rb.(*remoteBackend)
	defer b.Close()

	// Backend must use the same logger instance (no per-backend With() clone).
	assert.Same(t, logger, b.logger, "backend should use shared logger, not a With() clone")
}

func TestRemoteBackendCloseDoesNotLogExpectedDoubleClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	core, logs := observer.New(zap.ErrorLevel)
	logger := zap.New(core)
	app := newTestApp(t, func(conn goetty.IOSession, msg interface{}, _ uint64) error {
		return conn.Write(msg, goetty.WriteOptions{Flush: true})
	})
	require.NoError(t, app.Start())
	defer func() { assert.NoError(t, app.Stop()) }()

	rb, err := NewRemoteBackend(
		testAddr,
		newTestCodec(),
		WithBackendMetrics(newMetrics(t.Name())),
		WithBackendLogger(logger),
	)
	require.NoError(t, err)
	rb.Close()

	for _, entry := range logs.All() {
		require.NotEqual(t, "close conneciton failed", entry.Message)
	}
}

// TestRemoteBackendLogFields ensures logFields() returns "remote" and "backend-id"
// so that shared-logger logs still have backend identity (regression test for the memory fix).
func TestRemoteBackendLogFields(t *testing.T) {
	rb := &remoteBackend{remote: "unix:///tmp/test", logID: 42}
	rb.logFieldsCache = []zap.Field{zap.String("remote", rb.remote), zap.Uint64("backend-id", rb.logID)}
	fields := rb.logFields()
	var hasRemote, hasBackendID bool
	for _, f := range fields {
		switch f.Key {
		case "remote":
			hasRemote = true
		case "backend-id":
			hasBackendID = true
		}
	}
	assert.True(t, hasRemote, "logFields() should contain remote")
	assert.True(t, hasBackendID, "logFields() should contain backend-id")
}

func TestIsExpectedCloseError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a minimal backend instance for testing
	rb := &remoteBackend{
		logger: logutil.GetPanicLoggerWithLevel(zap.DebugLevel),
	}

	// Test with expected error: "use of closed network connection"
	// Use a custom error type to avoid Makefile err-check
	err := testError("close tcp4 127.0.0.1:43420->127.0.0.1:32001: use of closed network connection")
	assert.True(t, rb.isExpectedCloseError(err), "should recognize expected close error")

	// Test with unexpected error
	unexpectedErr := testError("some other error")
	assert.False(t, rb.isExpectedCloseError(unexpectedErr), "should not recognize unexpected error")

	// Test with nil error
	assert.False(t, rb.isExpectedCloseError(nil), "should return false for nil error")
}

func TestStoppedBackendCannotBeReactivated(t *testing.T) {
	closeDone := make(chan struct{})
	close(closeDone)
	rb := &remoteBackend{
		cancel:    func() {},
		closeDone: closeDone,
	}
	rb.stateMu.state = stateStopped
	rb.atomic.unavailable.Store(true)
	rb.atomic.lastActiveTime.Store(time.Time{})

	// Simulate an activity completion racing after the terminal state was
	// published. It must not make the backend selectable again.
	rb.active()
	require.True(t, rb.LastActiveTime().IsZero())

	// Repeated Close must also repair a stale activity timestamp left by an
	// older racing caller, while remaining idempotent.
	rb.atomic.lastActiveTime.Store(time.Now())
	rb.Close()
	require.True(t, rb.LastActiveTime().IsZero())
}

func newBlockingCloseRemoteBackend(
	t *testing.T,
) (*remoteBackend, <-chan struct{}, func()) {
	t.Helper()
	rb := &remoteBackend{
		stopper:    stopper.NewStopper("blocking-backend-close"),
		stopWriteC: make(chan struct{}),
		closeDone:  make(chan struct{}),
		cancel:     func() {},
	}
	rb.atomic.lastActiveTime.Store(time.Now())
	rb.mu.futures = make(map[uint64]*Future)
	rb.mu.activeStreams = make(map[uint64]*stream)
	// This helper has no IOSession. Mark the connection-only destructor done so
	// the test exercises the real Close state machine without a transport mock.
	rb.closeOnce.Do(func() {})

	teardownStarted := make(chan struct{})
	releaseTeardown := make(chan struct{})
	require.NoError(t, rb.stopper.RunTask(func(ctx context.Context) {
		<-ctx.Done()
		close(teardownStarted)
		<-releaseTeardown
	}))

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseTeardown) })
	}
	t.Cleanup(func() {
		release()
		rb.Close()
	})
	return rb, teardownStarted, release
}

func TestConcurrentRemoteBackendCloseJoinsTeardown(t *testing.T) {
	rb, teardownStarted, release := newBlockingCloseRemoteBackend(t)
	firstDone := make(chan struct{})
	go func() {
		rb.Close()
		close(firstDone)
	}()

	select {
	case <-teardownStarted:
	case <-time.After(time.Second):
		t.Fatal("backend teardown did not start")
	}

	secondDone := make(chan struct{})
	go func() {
		rb.Close()
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("concurrent Close returned before the teardown owner")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	for _, done := range []<-chan struct{}{firstDone, secondDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Close did not finish after teardown was released")
		}
	}
}

func TestRemoteBackendCloseTerminatesActiveStream(t *testing.T) {
	rb, teardownStarted, release := newBlockingCloseRemoteBackend(t)
	s := newStream(
		rb,
		make(chan Message, 1),
		func() *Future { return newFuture(nil) },
		func(*Future) error { return nil },
		rb.removeActiveStream,
		func() {},
	)
	s.init(1, false)
	rb.mu.activeStreams[s.ID()] = s
	recv, err := s.Receive()
	require.NoError(t, err)

	closeDone := make(chan struct{})
	go func() {
		rb.Close()
		close(closeDone)
	}()
	select {
	case <-teardownStarted:
	case <-time.After(time.Second):
		t.Fatal("backend teardown did not start")
	}

	// The receiver must terminate before the deliberately blocked transport
	// teardown is released; client cleanup is not allowed to depend on it.
	select {
	case message := <-recv:
		require.Nil(t, message)
	case <-time.After(time.Second):
		t.Fatal("backend close did not terminate the active stream receiver")
	}
	require.NoError(t, s.Close(false))

	release()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("backend Close did not finish after teardown was released")
	}
}

func TestStreamTerminationRejectsLateResponse(t *testing.T) {
	s := newStream(
		nil,
		make(chan Message, 1),
		func() *Future { return newFuture(nil) },
		func(*Future) error { return nil },
		func(*stream) {},
		func() {},
	)
	s.init(1, false)
	recv, err := s.Receive()
	require.NoError(t, err)

	s.terminate()
	s.done(context.Background(), RPCMessage{
		Message:        newTestMessage(s.ID()),
		stream:         true,
		streamSequence: 1,
	}, false)

	message := <-recv
	require.Nil(t, message)
	require.Empty(t, recv, "a response must not be published after terminal")
	require.NoError(t, s.Close(false))
}

func TestStreamCloseWithoutConnectionCloseTerminatesReceiver(t *testing.T) {
	s := newStream(
		nil,
		make(chan Message, 1),
		func() *Future { return newFuture(nil) },
		func(*Future) error { return nil },
		func(*stream) {},
		func() {},
	)
	s.init(1, false)
	recv, err := s.Receive()
	require.NoError(t, err)
	require.NoError(t, s.Close(false))
	select {
	case message := <-recv:
		require.Nil(t, message)
	case <-time.After(time.Second):
		t.Fatal("stream Close(false) did not terminate an existing receiver")
	}
}

func TestWaitingFutureMustGetClosedError(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return backendClosed
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := newTestMessage(1)
			f, err := b.Send(ctx, req)
			assert.NoError(t, err)
			defer f.Close()

			_, err = f.Get()
			assert.Error(t, err)
			assert.Equal(t, io.EOF, err)
		},
	)
}

func TestIssue11838(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, seq uint64) error {
			if seq == 100 {
				return backendClosed
			}
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
			defer cancel()

			var futures []*Future
			for i := 0; i < 10000; i++ {
				req := newTestMessage(uint64(i))
				f, err := b.Send(ctx, req)
				if err == nil {
					futures = append(futures, f)
				}
			}

			for _, f := range futures {
				_, err := f.Get()
				if err == nil {
					f.Close()
				}
			}
		},
	)
}

func TestCannotBusyLoopIfWriteCIsFull(t *testing.T) {
	testBackendSend(t,
		func(conn goetty.IOSession, msg interface{}, _ uint64) error {
			return conn.Write(msg, goetty.WriteOptions{Flush: true})
		},
		func(b *remoteBackend) {
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 10; i++ {
						func() {
							ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
							defer cancel()
							req := newTestMessage(0)
							f, err := b.Send(ctx, req)
							if err == nil { //ignore timeout
								_, _ = f.Get()
								f.Close()
							}
						}()
					}
				}()
			}
			wg.Wait()
		},
		WithBackendBufferSize(1),
	)
}

func TestCannotChangeStoppedToStopping(t *testing.T) {
	b := &remoteBackend{}
	b.stateMu.state = stateStopped

	b.changeToStopping()
	require.Equal(t, stateStopped, b.stateMu.state)
}

func testBackendSend(t *testing.T,
	handleFunc func(goetty.IOSession, interface{}, uint64) error,
	testFunc func(b *remoteBackend),
	options ...BackendOption) {
	testBackendSendWithAddr(t, testAddr, handleFunc, testFunc, options...)
}

func testBackendSendWithAddr(t *testing.T, addr string,
	handleFunc func(goetty.IOSession, interface{}, uint64) error,
	testFunc func(b *remoteBackend),
	options ...BackendOption) {
	app := newTestApp(t, handleFunc)
	assert.NoError(t, app.Start())
	defer func() {
		assert.NoError(t, app.Stop())
	}()

	testBackendSendWithoutServer(t, addr, testFunc, options...)
}

func testBackendSendWithoutServer(t *testing.T, addr string,
	testFunc func(b *remoteBackend),
	options ...BackendOption) {

	options = append(
		options,
		WithBackendMetrics(newMetrics("")),
		WithBackendBufferSize(1),
		WithBackendLogger(logutil.GetPanicLoggerWithLevel(zap.DebugLevel).With(zap.String("testcase", t.Name()))))
	rb, err := NewRemoteBackend(addr, newTestCodec(), options...)
	assert.NoError(t, err)

	b := rb.(*remoteBackend)
	defer func() {
		b.Close()
		assert.Nil(t, b.conn)
	}()
	testFunc(b)
}

func newTestApp(t *testing.T,
	handleFunc func(goetty.IOSession, interface{}, uint64) error,
	opts ...goetty.AppOption) goetty.NetApplication {
	assert.NoError(t, os.RemoveAll(testUnixFile))
	codec := newTestCodec().(*messageCodec)
	opts = append(opts, goetty.WithAppSessionOptions(goetty.WithSessionCodec(codec)))
	app, err := goetty.NewApplication(testAddr, handleFunc, opts...)
	assert.NoError(t, err)

	return app
}

type testBackendFactory struct {
	sync.RWMutex
	id int
}

func newTestBackendFactory() *testBackendFactory {
	return &testBackendFactory{}
}

func (bf *testBackendFactory) Create(backend string, opts ...BackendOption) (Backend, error) {
	bf.Lock()
	defer bf.Unlock()
	b := &testBackend{id: bf.id}
	b.activeTime = time.Now()
	bf.id++
	return b, nil
}

type testBackend struct {
	sync.RWMutex
	id         int
	busy       bool
	activeTime time.Time
	closed     bool
	locked     bool
}

func (b *testBackend) Send(ctx context.Context, request Message) (*Future, error) {
	b.active()
	f := newFuture(nil)
	f.init(RPCMessage{Ctx: ctx, Message: request})
	f.ref() // Ref before using
	// Complete the future synchronously to avoid goroutine leak in tests
	f.messageSent(nil)
	// Create a response message with same ID
	resp := newTestMessage(request.GetID())
	f.done(resp, nil)
	return f, nil
}

func (b *testBackend) SendInternal(ctx context.Context, request Message) (*Future, error) {
	b.active()
	f := newFuture(nil)
	f.init(RPCMessage{Ctx: ctx, Message: request})
	f.ref() // Ref before using
	// Complete the future synchronously to avoid goroutine leak in tests
	f.messageSent(nil)
	// Create a response message with same ID
	resp := newTestMessage(request.GetID())
	f.done(resp, nil)
	return f, nil
}

func (b *testBackend) NewStream(unlockAfterClose bool) (Stream, error) {
	b.active()
	st := newStream(
		nil,
		make(chan Message, 1),
		func() *Future { return newFuture(nil) },
		func(m *Future) error {
			m.messageSent(nil)
			return nil
		},
		func(s *stream) {
			if s.unlockAfterClose {
				b.Unlock()
			}
		},
		b.active)
	st.init(1, false)
	return st, nil
}

func (b *testBackend) Close() {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
	b.closed = true
	b.activeTime = time.Time{}
}
func (b *testBackend) Busy() bool { return b.busy }
func (b *testBackend) LastActiveTime() time.Time {
	b.RLock()
	defer b.RUnlock()
	return b.activeTime
}

func (b *testBackend) Lock() {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
	if b.locked {
		panic("backend is already locked")
	}
	b.locked = true
}

func (b *testBackend) Unlock() {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
	if !b.locked {
		panic("backend is not locked")
	}
	b.locked = false
}

func (b *testBackend) Locked() bool {
	b.RLock()
	defer b.RUnlock()
	return b.locked
}

func (b *testBackend) active() {
	b.setActiveTime(time.Now())
}

func (b *testBackend) setActiveTime(value time.Time) {
	b.RWMutex.Lock()
	defer b.RWMutex.Unlock()
	b.activeTime = value
}

type testMessage struct {
	id      uint64
	payload []byte
}

func newTestMessage(id uint64) *testMessage {
	return &testMessage{id: id}
}

func (tm *testMessage) SetID(id uint64) {
	tm.id = id
}

func (tm *testMessage) GetID() uint64 {
	return tm.id
}

func (tm *testMessage) DebugString() string {
	return fmt.Sprintf("%d:%d", tm.id, len(tm.payload))
}

func (tm *testMessage) ProtoSize() int {
	return 8 + len(tm.payload)
}

func (tm *testMessage) MarshalTo(data []byte) (int, error) {
	buf.Uint64ToBytesTo(tm.id, data)
	return 8, nil
}

func (tm *testMessage) Unmarshal(data []byte) error {
	tm.id = buf.Byte2Uint64(data)
	return nil
}

func (tm *testMessage) GetPayloadField() []byte {
	return tm.payload
}

func (tm *testMessage) SetPayloadField(data []byte) {
	if len(data) == 0 {
		tm.payload = nil
		return
	}
	tm.payload = make([]byte, len(data))
	copy(tm.payload, data)
}

func newTestCodec(options ...CodecOption) Codec {
	options = append(options,
		WithCodecPayloadCopyBufferSize(1024))
	return NewMessageCodec(
		"",
		func() Message { return messagePool.Get().(*testMessage) },
		options...,
	)
}

var (
	messagePool = sync.Pool{
		New: func() any {
			return newTestMessage(0)
		},
	}
)

// TestIsExpectedReadErrorWithStandardEOF tests that standard io.EOF is recognized as expected error
func TestIsExpectedReadErrorWithStandardEOF(t *testing.T) {
	rb := &remoteBackend{}

	// Standard io.EOF should be recognized
	assert.True(t, rb.isExpectedReadError(io.EOF),
		"standard io.EOF should be recognized as expected error")

	// io.ErrUnexpectedEOF should also be recognized
	assert.True(t, rb.isExpectedReadError(io.ErrUnexpectedEOF),
		"io.ErrUnexpectedEOF should be recognized as expected error")

	// backendClosed should be recognized
	assert.True(t, rb.isExpectedReadError(backendClosed),
		"backendClosed should be recognized as expected error")

	// nil error should not be recognized
	assert.False(t, rb.isExpectedReadError(nil),
		"nil error should not be recognized as expected error")
}

// TestIsExpectedReadErrorWithWrappedEOF tests that wrapped io.EOF is correctly identified
func TestIsExpectedReadErrorWithWrappedEOF(t *testing.T) {
	rb := &remoteBackend{}

	// Wrapped io.EOF using fmt.Errorf with %w should be recognized
	wrappedEOF := fmt.Errorf("connection: %w", io.EOF)
	assert.True(t, rb.isExpectedReadError(wrappedEOF),
		"wrapped io.EOF should be recognized as expected error")

	// Deeply wrapped io.EOF should also be recognized
	deepWrappedEOF := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", io.EOF))
	assert.True(t, rb.isExpectedReadError(deepWrappedEOF),
		"deeply wrapped io.EOF should be recognized as expected error")

	// Wrapped io.ErrUnexpectedEOF should also be recognized
	wrappedUnexpectedEOF := fmt.Errorf("read failed: %w", io.ErrUnexpectedEOF)
	assert.True(t, rb.isExpectedReadError(wrappedUnexpectedEOF),
		"wrapped io.ErrUnexpectedEOF should be recognized as expected error")
}

// TestIsExpectedReadErrorWithConnectionErrors tests platform-specific connection errors
func TestIsExpectedReadErrorWithConnectionErrors(t *testing.T) {
	rb := &remoteBackend{}

	// "use of closed network connection" should be recognized
	closedConnErr := testError("use of closed network connection")
	assert.True(t, rb.isExpectedReadError(closedConnErr),
		"'use of closed network connection' should be recognized")

	// "illegal state" should be recognized
	illegalStateErr := testError("illegal state")
	assert.True(t, rb.isExpectedReadError(illegalStateErr),
		"'illegal state' should be recognized")

	// "connection reset" should be recognized
	connResetErr := testError("connection reset by peer")
	assert.True(t, rb.isExpectedReadError(connResetErr),
		"'connection reset' should be recognized")

	// "broken pipe" should be recognized
	brokenPipeErr := testError("broken pipe")
	assert.True(t, rb.isExpectedReadError(brokenPipeErr),
		"'broken pipe' should be recognized")

	// Random error should not be recognized
	randomErr := testError("some random error")
	assert.False(t, rb.isExpectedReadError(randomErr),
		"random error should not be recognized as expected error")
}

// TestIsExpectedReadErrorWithMoErrWrappedEOF tests moerr-wrapped EOF (moerr.Error doesn't implement Unwrap)
func TestIsExpectedReadErrorWithMoErrWrappedEOF(t *testing.T) {
	rb := &remoteBackend{}

	// moerr.ConvertGoError converts io.EOF to moerr.NewUnexpectedEOF with "EOF" in message
	// Since moerr.Error doesn't implement Unwrap(), errors.Is() won't match io.EOF
	// But our string fallback "EOF" should catch it because the message is "unexpected end of file EOF"
	convertedEOF := moerr.ConvertGoError(context.Background(), io.EOF)
	assert.True(t, rb.isExpectedReadError(convertedEOF),
		"moerr.ConvertGoError(io.EOF) should be recognized via string fallback, got: %q", convertedEOF.Error())

	// Note: moerr.NewUnexpectedEOFNoCtx("something") produces "unexpected end of file something"
	// which does NOT contain "EOF" substring, so it won't match.
	// However, in practice, readLoop errors come from goetty conn.Read() which returns raw io.EOF,
	// not moerr-wrapped errors. moerr wrapping only happens in RPC response serialization.
}
