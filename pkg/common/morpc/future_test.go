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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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

func TestFutureRequestLifecycleMetricsTerminalOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		outcome requestOutcome
		run     func(*testing.T, *Future, context.CancelFunc)
	}{
		{
			name:    "success",
			outcome: requestOutcomeSuccess,
			run: func(t *testing.T, f *Future, _ context.CancelFunc) {
				f.messageSent(nil)
				require.True(t, f.done(newTestMessage(f.id), nil))
				_, err := f.Get()
				require.NoError(t, err)
				f.Close()
			},
		},
		{
			name:    "timeout",
			outcome: requestOutcomeTimeout,
			run: func(t *testing.T, f *Future, _ context.CancelFunc) {
				f.messageSent(nil)
				_, err := f.Get()
				require.ErrorIs(t, err, context.DeadlineExceeded)
				f.Close()
			},
		},
		{
			name:    "canceled",
			outcome: requestOutcomeCanceled,
			run: func(t *testing.T, f *Future, cancel context.CancelFunc) {
				cancel()
				f.messageSent(nil)
				_, err := f.Get()
				require.ErrorIs(t, err, context.Canceled)
				f.Close()
			},
		},
		{
			name:    "send-error",
			outcome: requestOutcomeSendError,
			run: func(t *testing.T, f *Future, _ context.CancelFunc) {
				sendErr := errors.New("send failed")
				f.messageSent(sendErr)
				_, err := f.Get()
				require.ErrorIs(t, err, sendErr)
				f.Close()
			},
		},
		{
			name:    "backend-error",
			outcome: requestOutcomeBackendError,
			run: func(t *testing.T, f *Future, _ context.CancelFunc) {
				backendErr := errors.New("backend failed")
				f.messageSent(nil)
				require.True(t, f.error(f.id, backendErr, nil))
				_, err := f.Get()
				require.ErrorIs(t, err, backendErr)
				f.Close()
			},
		},
		{
			name:    "abandoned",
			outcome: requestOutcomeAbandoned,
			run: func(_ *testing.T, f *Future, _ context.CancelFunc) {
				f.Close()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMetrics(t.Name())
			startedBefore := testutil.ToFloat64(m.requestStartedCounter)
			completedBefore := requestCompletedCount(m)
			outcomeBefore := testutil.ToFloat64(m.requestCompletedCounters[tt.outcome])
			histogramCountBefore, _ := observerHistogram(t, m.requestDurationHistogram)

			var ctx context.Context
			var cancel context.CancelFunc
			if tt.outcome == requestOutcomeTimeout {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), time.Hour)
			}
			defer cancel()

			f := newFuture(nil)
			f.init(newTestRPCMessage(ctx, 1))
			f.ref()
			f.enableRequestMetrics(m)
			tt.run(t, f, cancel)

			require.Equal(t, startedBefore+1, testutil.ToFloat64(m.requestStartedCounter))
			require.Equal(t, completedBefore+1, requestCompletedCount(m))
			require.Equal(t, outcomeBefore+1, testutil.ToFloat64(m.requestCompletedCounters[tt.outcome]))
			histogramCount, histogramSum := observerHistogram(t, m.requestDurationHistogram)
			require.Equal(t, histogramCountBefore+1, histogramCount)
			require.GreaterOrEqual(t, histogramSum, float64(0))
		})
	}
}

func TestFutureRequestLifecycleMetricsFirstTerminalOutcomeWins(t *testing.T) {
	m := newMetrics(t.Name())
	completedBefore := requestCompletedCount(m)
	sendErrorBefore := testutil.ToFloat64(m.requestCompletedCounters[requestOutcomeSendError])
	histogramBefore, _ := observerHistogram(t, m.requestDurationHistogram)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	f := newFuture(nil)
	f.init(newTestRPCMessage(ctx, 1))
	f.ref()
	f.enableRequestMetrics(m)
	f.messageSent(errors.New("first send failure"))
	cancel()
	require.False(t, f.done(newTestMessage(1), nil))
	require.False(t, f.error(1, errors.New("late backend failure"), nil))
	f.Close()

	require.Equal(t, completedBefore+1, requestCompletedCount(m))
	require.Equal(t, sendErrorBefore+1, testutil.ToFloat64(m.requestCompletedCounters[requestOutcomeSendError]))
	count, _ := observerHistogram(t, m.requestDurationHistogram)
	require.Equal(t, histogramBefore+1, count)
}

func TestFutureRequestLifecycleContextWinsLaterTransportFailure(t *testing.T) {
	tests := []struct {
		name    string
		context func() (context.Context, context.CancelFunc)
		outcome requestOutcome
	}{
		{
			name: "deadline",
			context: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			outcome: requestOutcomeTimeout,
		},
		{
			name: "cancel",
			context: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				cancel()
				return ctx, cancel
			},
			outcome: requestOutcomeCanceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMetrics(t.Name())
			outcomeBefore := testutil.ToFloat64(m.requestCompletedCounters[tt.outcome])
			ctx, cancel := tt.context()
			defer cancel()
			f := newFuture(nil)
			f.init(newTestRPCMessage(ctx, 1))
			f.ref()
			f.enableRequestMetrics(m)

			f.messageSent(errors.New("late socket failure"))
			f.Close()
			require.Equal(t, outcomeBefore+1,
				testutil.ToFloat64(m.requestCompletedCounters[tt.outcome]))
		})
	}
}

func TestFutureRequestLifecycleMetricsConcurrentTerminals(t *testing.T) {
	m := newMetrics(t.Name())
	startedBefore := testutil.ToFloat64(m.requestStartedCounter)
	completedBefore := requestCompletedCount(m)
	const futures = 256

	for i := range futures {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		f := newFuture(nil)
		f.init(newTestRPCMessage(ctx, uint64(i+1)))
		f.ref()
		f.enableRequestMetrics(m)

		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			f.done(newTestMessage(f.id), nil)
		}()
		go func() {
			defer wg.Done()
			f.error(f.id, errors.New("backend failed"), nil)
		}()
		go func() {
			defer wg.Done()
			f.messageSent(errors.New("send failed"))
		}()
		go func() {
			defer wg.Done()
			f.Close()
		}()
		wg.Wait()
		cancel()
	}

	require.Equal(t, startedBefore+futures, testutil.ToFloat64(m.requestStartedCounter))
	require.Equal(t, completedBefore+futures, requestCompletedCount(m))
}

func TestFutureRequestLifecycleMetricsPoolReuseHasNoGenerationLeak(t *testing.T) {
	m := newMetrics(t.Name())
	startedBefore := testutil.ToFloat64(m.requestStartedCounter)
	completedBefore := requestCompletedCount(m)
	sendErrorBefore := testutil.ToFloat64(m.requestCompletedCounters[requestOutcomeSendError])
	successBefore := testutil.ToFloat64(m.requestCompletedCounters[requestOutcomeSuccess])
	histogramBefore, _ := observerHistogram(t, m.requestDurationHistogram)
	released := make(chan *Future, 1)
	f := newFuture(func(f *Future) {
		f.reset()
		released <- f
	})

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Hour)
	f.init(newTestRPCMessage(ctx1, 1))
	f.ref()
	f.enableRequestMetrics(m)
	f.messageSent(errors.New("send failed"))
	f.Close()
	f = <-released
	cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Hour)
	defer cancel2()
	f.init(newTestRPCMessage(ctx2, 2))
	f.ref()
	f.enableRequestMetrics(m)
	f.messageSent(nil)
	require.True(t, f.done(newTestMessage(2), nil))
	_, err := f.Get()
	require.NoError(t, err)
	f.Close()
	<-released

	require.Equal(t, startedBefore+2, testutil.ToFloat64(m.requestStartedCounter))
	require.Equal(t, completedBefore+2, requestCompletedCount(m))
	require.Equal(t, sendErrorBefore+1, testutil.ToFloat64(m.requestCompletedCounters[requestOutcomeSendError]))
	require.Equal(t, successBefore+1, testutil.ToFloat64(m.requestCompletedCounters[requestOutcomeSuccess]))
	count, _ := observerHistogram(t, m.requestDurationHistogram)
	require.Equal(t, histogramBefore+2, count)
}

func TestFutureRequestLifecycleMetricsExcludeNonUnaryTraffic(t *testing.T) {
	m := newMetrics(t.Name())
	startedBefore := testutil.ToFloat64(m.requestStartedCounter)
	completedBefore := requestCompletedCount(m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	messages := []RPCMessage{
		{Ctx: context.Background(), Message: newTestMessage(1), internal: true},
		{Ctx: context.Background(), Message: newTestMessage(2), oneWay: true},
		{Ctx: ctx, Message: newTestMessage(3), stream: true},
	}
	for _, message := range messages {
		f := newFuture(nil)
		f.init(message)
		f.enableRequestMetrics(m)
		f.Close()
	}

	require.Equal(t, startedBefore, testutil.ToFloat64(m.requestStartedCounter))
	require.Equal(t, completedBefore, requestCompletedCount(m))
}

func TestFutureReleaseCommitsBeforeCallbackWithoutHoldingFutureLock(t *testing.T) {
	callbackStarted := make(chan struct{})
	allowCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	f := newFuture(func(f *Future) {
		if !f.mu.TryLock() {
			panic("Future release callback ran while Future lock was held")
		}
		f.mu.Unlock()
		close(callbackStarted)
		<-allowCallback
		close(callbackDone)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	f.init(newTestRPCMessage(ctx, 1))
	f.ref()
	f.Close()

	go f.messageSent(nil)
	<-callbackStarted
	require.False(t, f.tryRef(),
		"terminal delivery must not pin a Future whose return to the pool is committed")
	close(allowCallback)
	<-callbackDone
}

func requestCompletedCount(m *metrics) float64 {
	var total float64
	for _, counter := range m.requestCompletedCounters {
		total += testutil.ToFloat64(counter)
	}
	return total
}

func observerHistogram(t *testing.T, observer prometheus.Observer) (uint64, float64) {
	t.Helper()
	metric, ok := observer.(prometheus.Metric)
	require.True(t, ok)
	value := &dto.Metric{}
	require.NoError(t, metric.Write(value))
	require.NotNil(t, value.Histogram)
	return value.Histogram.GetSampleCount(), value.Histogram.GetSampleSum()
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
