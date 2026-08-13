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
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCreateServerWithOptions(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		assert.Equal(t, 100, rs.options.batchSendSize)
		assert.Equal(t, 200, rs.options.bufferSize)
	}, WithServerBatchSendSize(100),
		WithServerSessionBufferSize(200))
}

type connectionJoiningApplication struct {
	stopAndWaitCalled chan struct{}
	releaseHandlers   chan struct{}
}

func (a *connectionJoiningApplication) Start() error {
	return nil
}

func (a *connectionJoiningApplication) Stop() error {
	return nil
}

func (a *connectionJoiningApplication) StopAndWait() error {
	close(a.stopAndWaitCalled)
	<-a.releaseHandlers
	return nil
}

func (a *connectionJoiningApplication) GetSession(uint64) (goetty.IOSession, error) {
	return nil, nil
}

func TestServerCloseWaitsForAcceptedConnections(t *testing.T) {
	application := &connectionJoiningApplication{
		stopAndWaitCalled: make(chan struct{}),
		releaseHandlers:   make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseConnection := func() {
		releaseOnce.Do(func() {
			close(application.releaseHandlers)
		})
	}
	t.Cleanup(releaseConnection)
	s := &server{
		logger:      logutil.GetPanicLoggerWithLevel(zap.FatalLevel),
		application: application,
		stopper:     stopper.NewStopper("test-server-close-connections"),
	}
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.Close()
	}()

	select {
	case <-application.stopAndWaitCalled:
	case <-time.After(time.Second):
		t.Fatal("application StopAndWait was not called")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("server Close returned before the accepted connection closed: %v", err)
	default:
	}

	releaseConnection()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server Close did not return after the accepted connection closed")
	}
}

type recordingSessionAware struct {
	created chan struct{}
	closed  chan struct{}
}

func (a *recordingSessionAware) Created(goetty.IOSession) {
	select {
	case a.created <- struct{}{}:
	default:
	}
}

func (a *recordingSessionAware) Closed(goetty.IOSession) {
	select {
	case a.closed <- struct{}{}:
	default:
	}
}

func TestServerPreservesCallerSessionAware(t *testing.T) {
	aware := &recordingSessionAware{
		created: make(chan struct{}, 1),
		closed:  make(chan struct{}, 1),
	}
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client := newTestClient(t)
		t.Cleanup(func() {
			require.NoError(t, client.Close())
		})
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		future, err := client.Send(ctx, testAddr, newTestMessage(1))
		require.NoError(t, err)
		defer future.Close()
		_, err = future.Get()
		require.NoError(t, err)
		select {
		case <-aware.created:
		case <-ctx.Done():
			t.Fatal("caller IOSessionAware did not receive Created")
		}

		require.NoError(t, rs.Close())
		select {
		case <-aware.closed:
		case <-ctx.Done():
			t.Fatal("caller IOSessionAware did not receive Closed")
		}
	}, WithServerGoettyOptions(goetty.WithSessionAware(aware)))
}

func TestHandleServer(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
		defer cancel()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req)
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, req, resp)
	})
}

func TestHandleServerWithPayloadMessage(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		req := &testMessage{id: 1, payload: []byte("payload")}
		f, err := c.Send(ctx, testAddr, req)
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, req, resp)
	})
}

func TestHandleServerWriteWithClosedSession(t *testing.T) {
	wc := make(chan struct{}, 1)
	defer close(wc)

	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		c := newTestClient(t)
		handlerDone := make(chan error, 1)
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			assert.NoError(t, c.Close())
			// The peer may still accept a write briefly after the client closes;
			// transport buffering does not guarantee an immediate write error.
			err := cs.Write(ctx, request.Message)
			handlerDone <- err
			return err
		})

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req)
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.ErrorIs(t, err, backendClosed)
		assert.Nil(t, resp)
		select {
		case <-handlerDone:
		case <-ctx.Done():
			t.Fatal("server handler did not finish after client close")
		}
	})
}

func TestHandleServerWriteWithClosedClientSession(t *testing.T) {
	wc := make(chan struct{}, 1)
	defer close(wc)

	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		c := newTestClient(t)
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			assert.NoError(t, cs.Close())
			return cs.Write(ctx, request.Message)
		})

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req)
		assert.NoError(t, err)

		defer f.Close()
		_, err = f.Get()
		assert.Error(t, err)
		assert.Equal(t, io.EOF, err)
	})
}

func TestClientSessionWriteReturnsWhenSendQueueFullAndContextExpires(t *testing.T) {
	released := 0
	cs := newClientSession(
		newServerMetrics("test"),
		nil,
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		func(Message) { released++ },
	)
	cs.c = make(chan *Future, 1)
	queued := newFuture(nil)
	enqueueClientSessionFutureForTest(cs, queued)
	defer func() {
		<-cs.c
		cs.changeQueueDepth(-1)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- cs.Write(ctx, newTestMessage(1))
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, released)
	case <-time.After(time.Second):
		t.Fatal("write blocked after context deadline")
	}
}

func TestClientSessionBoundedWriteExpiresBehindBlockedAsyncWrite(t *testing.T) {
	cs := newClientSession(
		newServerMetrics(t.Name()),
		nil,
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		nil,
	)
	cs.c = make(chan *Future, 1)
	queued := newFuture(nil)
	enqueueClientSessionFutureForTest(cs, queued)

	asyncDone := make(chan error, 1)
	boundedDone := make(chan error, 1)
	boundedStarted := false
	boundedReturned := false
	go func() {
		asyncDone <- cs.AsyncWrite(newTestMessage(1))
	}()
	defer func() {
		first := <-cs.c
		cs.changeQueueDepth(-1)
		first.Close()
		select {
		case <-asyncDone:
		case <-time.After(time.Second):
			t.Error("unbounded writer did not finish after queue capacity was released")
			return
		}
		if boundedStarted && !boundedReturned {
			select {
			case <-boundedDone:
			case <-time.After(time.Second):
				t.Error("bounded writer remained blocked during test cleanup")
				return
			}
		}
		cs.cleanSend()
	}()
	require.Eventually(t, func() bool {
		if cs.mu.TryLock() {
			cs.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond,
		"unbounded writer did not reach the full queue")

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	boundedStarted = true
	go func() {
		boundedDone <- cs.Write(ctx, newTestMessage(2))
	}()
	cancel()

	select {
	case err := <-boundedDone:
		boundedReturned = true
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("bounded Write did not return after its context was canceled")
	}
}

func TestServerSessionMetricFollowsMapOwnership(t *testing.T) {
	m := newServerMetrics(t.Name())
	s := &server{metrics: m, sessions: &sync.Map{}}
	first := &clientSession{}
	replacement := &clientSession{}

	actual, loaded := s.loadOrStoreClientSession(1, first)
	require.False(t, loaded)
	require.Same(t, first, actual)
	require.Equal(t, float64(1), testutil.ToFloat64(m.sessionSizeGauge))

	actual, loaded = s.loadOrStoreClientSession(1, replacement)
	require.True(t, loaded)
	require.Same(t, first, actual)
	require.Equal(t, float64(1), testutil.ToFloat64(m.sessionSizeGauge))
	require.False(t, s.deleteClientSession(1, replacement),
		"a stale generation must not delete or decrement the live session")
	require.Equal(t, float64(1), testutil.ToFloat64(m.sessionSizeGauge))

	require.True(t, s.deleteClientSession(1, first))
	require.False(t, s.deleteClientSession(1, first),
		"repeated cleanup must not decrement twice")
	require.Equal(t, float64(0), testutil.ToFloat64(m.sessionSizeGauge))

	actual, loaded = s.loadOrStoreClientSession(1, replacement)
	require.False(t, loaded)
	require.Same(t, replacement, actual)
	require.False(t, s.deleteClientSession(1, first),
		"cleanup from an old generation must not remove its replacement")
	require.Equal(t, float64(1), testutil.ToFloat64(m.sessionSizeGauge))
	require.True(t, s.deleteClientSession(1, replacement))
	require.Equal(t, float64(0), testutil.ToFloat64(m.sessionSizeGauge))
}

func TestServerSessionMetricConcurrentOwnership(t *testing.T) {
	m := newServerMetrics(t.Name())
	s := &server{metrics: m, sessions: &sync.Map{}}
	const competitors = 32

	for generation := range 64 {
		id := uint64(generation + 1)
		start := make(chan struct{})
		actuals := make(chan *clientSession, competitors)
		var createWG sync.WaitGroup
		for range competitors {
			createWG.Add(1)
			go func() {
				defer createWG.Done()
				candidate := &clientSession{}
				<-start
				actual, _ := s.loadOrStoreClientSession(id, candidate)
				actuals <- actual
			}()
		}
		close(start)
		createWG.Wait()
		close(actuals)

		var owner *clientSession
		for actual := range actuals {
			if owner == nil {
				owner = actual
			}
			require.Same(t, owner, actual)
		}
		require.Equal(t, float64(1), testutil.ToFloat64(m.sessionSizeGauge))

		var deleted atomic.Int32
		var deleteWG sync.WaitGroup
		for range competitors {
			deleteWG.Add(1)
			go func() {
				defer deleteWG.Done()
				if s.deleteClientSession(id, owner) {
					deleted.Add(1)
				}
			}()
		}
		deleteWG.Wait()
		require.Equal(t, int32(1), deleted.Load())
		require.Equal(t, float64(0), testutil.ToFloat64(m.sessionSizeGauge))
	}
}

func TestClientSessionCleanSendReleasesQueuedMessages(t *testing.T) {
	released := 0
	futureReleased := 0
	cs := newClientSession(
		newServerMetrics("test"),
		nil,
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		func(Message) { released++ },
	)

	f := newFuture(func(*Future) { futureReleased++ })
	f.init(RPCMessage{
		Ctx:     context.Background(),
		Message: newTestMessage(1),
		oneWay:  true,
	})
	enqueueClientSessionFutureForTest(cs, f)

	cs.cleanSend()
	require.Equal(t, 1, released)
	require.Equal(t, 1, futureReleased)
}

func TestServerQueueMetricAggregatesAcrossSessionsAndCloseDrain(t *testing.T) {
	m := newServerMetrics(t.Name())
	newSession := func() *clientSession {
		return newClientSession(
			m,
			nil,
			newTestCodec(),
			func() *Future { return newFuture(nil) },
			nil,
		)
	}
	cs1 := newSession()
	cs2 := newSession()

	require.NoError(t, cs1.AsyncWrite(newTestMessage(1)))
	require.NoError(t, cs1.AsyncWrite(newTestMessage(2)))
	require.NoError(t, cs2.AsyncWrite(newTestMessage(3)))
	require.Equal(t, float64(3), testutil.ToFloat64(m.sendingQueueSizeGauge))

	cs1.cleanSend()
	require.Equal(t, float64(1), testutil.ToFloat64(m.sendingQueueSizeGauge))
	cs2.cleanSend()
	require.Equal(t, float64(0), testutil.ToFloat64(m.sendingQueueSizeGauge))
}

func TestServerQueueMetricDoesNotDeadlockFullQueue(t *testing.T) {
	m := newServerMetrics(t.Name())
	cs := newClientSession(
		m,
		nil,
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		nil,
	)
	cs.c = make(chan *Future, 1)
	initial := newFuture(nil)
	enqueueClientSessionFutureForTest(cs, initial)

	firstSent := make(chan struct{})
	producerDone := make(chan error, 1)
	go func() {
		if err := cs.AsyncWrite(newTestMessage(1)); err != nil {
			producerDone <- err
			return
		}
		close(firstSent)
		producerDone <- cs.AsyncWrite(newTestMessage(2))
	}()

	initialReceived := make(chan struct{})
	allowAccounting := make(chan struct{})
	var allowAccountingOnce sync.Once
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		f := <-cs.c
		close(initialReceived)
		<-allowAccounting
		cs.changeQueueDepth(-1)
		f.Close()
		for range 2 {
			f = <-cs.c
			cs.changeQueueDepth(-1)
			f.Close()
		}
	}()

	producerReturned := false
	consumerReturned := false
	defer func() {
		allowAccountingOnce.Do(func() { close(allowAccounting) })
		if !producerReturned {
			select {
			case <-producerDone:
			case <-time.After(time.Second):
				t.Error("producer remained blocked during test cleanup")
			}
		}
		if !consumerReturned {
			select {
			case <-consumerDone:
			case <-time.After(time.Second):
				t.Error("consumer remained blocked during test cleanup")
			}
		}
	}()

	select {
	case <-initialReceived:
	case <-time.After(time.Second):
		t.Fatal("consumer did not receive the initial Future")
	}
	select {
	case <-firstSent:
	case <-time.After(time.Second):
		t.Fatal("producer did not send the first Future")
	}
	require.Eventually(t, func() bool {
		if cs.mu.TryLock() {
			cs.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond,
		"second producer did not reach the full queue")
	allowAccountingOnce.Do(func() { close(allowAccounting) })

	select {
	case err := <-producerDone:
		producerReturned = true
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("producer and consumer deadlocked through queue metric accounting")
	}
	select {
	case <-consumerDone:
		consumerReturned = true
	case <-time.After(time.Second):
		t.Fatal("consumer did not drain the queue")
	}
	require.Equal(t, float64(0), testutil.ToFloat64(m.sendingQueueSizeGauge))
}

func TestServerQueueMetricConcurrentProducersAndConsumer(t *testing.T) {
	m := newServerMetrics(t.Name())
	cs := newClientSession(
		m,
		nil,
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		nil,
	)
	cs.c = make(chan *Future, 32)
	const producers = 8
	const perProducer = 128
	const total = producers * perProducer

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for range total {
			f := <-cs.c
			cs.changeQueueDepth(-1)
			f.Close()
		}
	}()

	errorsC := make(chan error, total)
	var producersWG sync.WaitGroup
	for producer := range producers {
		producersWG.Add(1)
		go func(producer int) {
			defer producersWG.Done()
			for offset := range perProducer {
				errorsC <- cs.AsyncWrite(
					newTestMessage(uint64(producer*perProducer + offset + 1)))
			}
		}(producer)
	}
	producersWG.Wait()
	close(errorsC)
	for err := range errorsC {
		require.NoError(t, err)
	}
	select {
	case <-consumerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server queue consumer did not drain all admitted Futures")
	}

	require.Equal(t, float64(0), testutil.ToFloat64(m.sendingQueueSizeGauge))
	cs.queueMetricMu.Lock()
	require.Zero(t, cs.queueMetricMu.depth)
	require.Zero(t, cs.queueMetricMu.waiters)
	cs.queueMetricMu.Unlock()
}

func TestStartWriteLoopClosesOneWayFuturesOnWriteFailures(t *testing.T) {
	run := func(t *testing.T, conn *testIOSession) {
		var futureReleased atomic.Int32
		s := &server{
			name:     "test",
			metrics:  newServerMetrics("test"),
			logger:   logutil.GetPanicLoggerWithLevel(zap.FatalLevel),
			stopper:  stopper.NewStopper("test"),
			sessions: &sync.Map{},
		}
		s.adjust()
		s.options.batchSendSize = 1

		cs := newClientSession(
			s.metrics,
			conn,
			newTestCodec(),
			func() *Future { return newFuture(nil) },
			nil,
		)

		f := newFuture(func(*Future) { futureReleased.Add(1) })
		f.init(RPCMessage{
			Ctx:     context.Background(),
			Message: newTestMessage(1),
			oneWay:  true,
		})
		enqueueClientSessionFutureForTest(cs, f)

		require.NoError(t, s.startWriteLoop(cs))
		require.Eventually(t, func() bool {
			return futureReleased.Load() == 1
		}, time.Second, time.Millisecond*10)
		s.stopper.Stop()
	}

	t.Run("write error", func(t *testing.T) {
		run(t, newTestIOSession(goetty.ErrIllegalState, nil))
	})
	t.Run("flush error", func(t *testing.T) {
		run(t, newTestIOSession(nil, io.ErrClosedPipe))
	})
}

func TestStartWriteLoopCompletesBatchOnWriteFailure(t *testing.T) {
	var released atomic.Int32
	s := &server{
		name:     "test",
		metrics:  newServerMetrics("test"),
		logger:   logutil.GetPanicLoggerWithLevel(zap.FatalLevel),
		stopper:  stopper.NewStopper("test"),
		sessions: &sync.Map{},
	}
	s.adjust()
	s.options.batchSendSize = 3
	defer s.stopper.Stop()

	cs := newClientSession(
		s.metrics,
		newTestIOSessionWithWriteErrorAt(2, goetty.ErrIllegalState, nil),
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		func(Message) { released.Add(1) },
	)

	newSyncFuture := func(id uint64) *Future {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		t.Cleanup(cancel)
		f := newFuture(nil)
		f.init(RPCMessage{
			Ctx:     ctx,
			Message: newTestMessage(id),
		})
		f.ref()
		t.Cleanup(f.Close)
		return f
	}
	f1 := newSyncFuture(1)
	f2 := newSyncFuture(2)
	f3 := newSyncFuture(3)
	enqueueClientSessionFutureForTest(cs, f1)
	enqueueClientSessionFutureForTest(cs, f2)
	enqueueClientSessionFutureForTest(cs, f3)

	require.NoError(t, s.startWriteLoop(cs))
	for _, f := range []*Future{f1, f2, f3} {
		select {
		case err := <-f.writtenC:
			require.ErrorIs(t, err, goetty.ErrIllegalState)
		case <-time.After(time.Second):
			t.Fatalf("future %d was not completed after batch write failure", f.getSendMessageID())
		}
	}
	require.Equal(t, int32(2), released.Load())
}

func TestStartWriteLoopFlushFailureOnlyCompletesWrittenFutures(t *testing.T) {
	s := &server{
		name:     "test",
		metrics:  newServerMetrics("test"),
		logger:   logutil.GetPanicLoggerWithLevel(zap.FatalLevel),
		stopper:  stopper.NewStopper("test"),
		sessions: &sync.Map{},
	}
	s.adjust()
	s.options.batchSendSize = 2
	defer s.stopper.Stop()

	var filterMu sync.Mutex
	filterCalls := make(map[uint64]int)
	lateAccess := false
	s.options.filter = func(message Message) bool {
		filterMu.Lock()
		defer filterMu.Unlock()
		if message == nil {
			lateAccess = true
			return false
		}
		id := message.GetID()
		filterCalls[id]++
		return id != 1
	}

	conn := newTestIOSession(nil, io.ErrClosedPipe)
	cs := newClientSession(
		s.metrics,
		conn,
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		nil,
	)
	released := make(chan uint64, 2)
	newResponse := func(id uint64) *Future {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		t.Cleanup(cancel)
		f := newFuture(func(f *Future) {
			f.reset()
			released <- id
		})
		f.init(RPCMessage{Ctx: ctx, Message: newTestMessage(id)})
		f.ref()
		f.Close()
		return f
	}
	enqueueClientSessionFutureForTest(cs, newResponse(1))
	enqueueClientSessionFutureForTest(cs, newResponse(2))

	require.NoError(t, s.startWriteLoop(cs))
	for range 2 {
		select {
		case <-released:
		case <-time.After(time.Second):
			t.Fatal("future was not released after batch flush failure")
		}
	}
	s.stopper.Stop()

	filterMu.Lock()
	defer filterMu.Unlock()
	require.False(t, lateAccess, "flush failure accessed a future after it was released")
	require.Equal(t, map[uint64]int{1: 1, 2: 1}, filterCalls)
}

func TestStartWriteLoopUsesEarliestBatchDeadline(t *testing.T) {
	s := &server{
		name:     "test",
		metrics:  newServerMetrics("test"),
		logger:   logutil.GetPanicLoggerWithLevel(zap.FatalLevel),
		stopper:  stopper.NewStopper("test"),
		sessions: &sync.Map{},
	}
	s.adjust()
	s.options.batchSendSize = 2
	defer s.stopper.Stop()

	conn := newTestIOSession(nil, nil)
	cs := newClientSession(
		s.metrics,
		conn,
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		nil,
	)

	newResponse := func(id uint64, timeout time.Duration) *Future {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		t.Cleanup(cancel)
		f := newFuture(nil)
		f.init(RPCMessage{Ctx: ctx, Message: newTestMessage(id)})
		f.ref()
		t.Cleanup(f.Close)
		return f
	}
	enqueueClientSessionFutureForTest(cs, newResponse(1, 3*time.Second))
	enqueueClientSessionFutureForTest(cs, newResponse(2, time.Second))

	require.NoError(t, s.startWriteLoop(cs))
	select {
	case timeout := <-conn.flushC:
		require.Positive(t, timeout)
		require.LessOrEqual(t, timeout, time.Second)
	case <-time.After(time.Second):
		t.Fatal("server writer did not flush the queued batch")
	}
}

func TestStreamServer(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		wg := sync.WaitGroup{}
		wg.Add(1)
		n := 10
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					assert.NoError(t, cs.Write(ctx, request.Message))
				}
			}()
			return nil
		})

		st, err := c.NewStream(context.Background(), testAddr, false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(ctx, req))

		rc, err := st.Receive()
		assert.NoError(t, err)
		for i := 0; i < n; i++ {
			assert.Equal(t, req, <-rc)
		}

		wg.Wait()
	})
}

type testIOSession struct {
	out        *buf.ByteBuf
	writeErr   error
	writeErrAt int32
	writeCount atomic.Int32
	flushErr   error
	flushC     chan time.Duration
}

func newTestIOSession(writeErr, flushErr error) *testIOSession {
	writeErrAt := int32(0)
	if writeErr != nil {
		writeErrAt = 1
	}
	return newTestIOSessionWithWriteErrorAt(writeErrAt, writeErr, flushErr)
}

// enqueueClientSessionFutureForTest mirrors production admission followed by
// accounting; a racing receiver waits for this accounting before decrementing.
func enqueueClientSessionFutureForTest(cs *clientSession, f *Future) {
	cs.c <- f
	cs.changeQueueDepth(1)
}

func newTestIOSessionWithWriteErrorAt(writeErrAt int32, writeErr, flushErr error) *testIOSession {
	return &testIOSession{
		out:        buf.NewByteBuf(1),
		writeErr:   writeErr,
		writeErrAt: writeErrAt,
		flushErr:   flushErr,
		flushC:     make(chan time.Duration, 1),
	}
}

func (s *testIOSession) ID() uint64                           { return 1 }
func (s *testIOSession) Connect(string, time.Duration) error  { return nil }
func (s *testIOSession) Connected() bool                      { return true }
func (s *testIOSession) Disconnect() error                    { return nil }
func (s *testIOSession) Close() error                         { s.out.Close(); return nil }
func (s *testIOSession) Ref()                                 {}
func (s *testIOSession) Read(goetty.ReadOptions) (any, error) { return nil, io.EOF }
func (s *testIOSession) Write(any, goetty.WriteOptions) error {
	if s.writeErr != nil && s.writeCount.Add(1) == s.writeErrAt {
		return s.writeErr
	}
	return nil
}
func (s *testIOSession) Flush(timeout time.Duration) error {
	select {
	case s.flushC <- timeout:
	default:
	}
	return s.flushErr
}
func (s *testIOSession) RemoteAddress() string { return "" }
func (s *testIOSession) RawConn() net.Conn     { return nil }
func (s *testIOSession) UseConn(net.Conn)      {}
func (s *testIOSession) OutBuf() *buf.ByteBuf  { return s.out }

func TestStreamServerWithCache(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(ctx context.Context, msg RPCMessage, seq uint64, cs ClientSession) error {
			request := msg.Message
			if seq == 1 {
				cache, err := cs.CreateCache(ctx, request.GetID())
				if err != nil {
					return err
				}
				m := newTestMessage(request.GetID())
				return cache.Add(m)
			} else {
				cache, err := cs.GetCache(request.GetID())
				if err != nil {
					return err
				}
				m, _, err := cache.Pop()
				if err != nil {
					return err
				}
				if err := cs.Write(ctx, m); err != nil {
					return err
				}
				if err := cs.Write(ctx, request); err != nil {
					return err
				}
			}
			return nil
		})

		st, err := c.NewStream(context.Background(), testAddr, false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		req1 := newTestMessage(st.ID())
		req1.payload = []byte{1}
		assert.NoError(t, st.Send(ctx, req1))

		req2 := newTestMessage(st.ID())
		req2.payload = []byte{2}
		assert.NoError(t, st.Send(ctx, req2))

		cc, err := st.Receive()
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			select {
			case <-ctx.Done():
				assert.Fail(t, "message failed")
			case <-cc:
			}
		}
	})
}

func TestFinishStreamFlushesAckAndRetiresSequenceState(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var requestCount atomic.Int32
		retired := make(chan *clientSession, 1)
		rs.RegisterRequestHandler(func(ctx context.Context, msg RPCMessage, _ uint64, session ClientSession) error {
			cs := session.(*clientSession)
			if requestCount.Add(1) == 1 {
				return cs.Write(ctx, newTestMessage(msg.Message.GetID()))
			}
			token, ok := StreamTerminalTokenFromContext(ctx)
			if !ok {
				return moerr.NewInternalErrorNoCtx("missing terminal token")
			}
			if err := cs.FinishStream(ctx, token, newTestMessage(msg.Message.GetID())); err != nil {
				return err
			}
			retired <- cs
			return nil
		})

		client := newTestClient(t)
		defer func() { require.NoError(t, client.Close()) }()
		stream, err := client.NewStream(ctx, testAddr, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, stream.Close(false)) }()
		responses, err := stream.Receive()
		require.NoError(t, err)

		require.NoError(t, stream.Send(ctx, newTestMessage(stream.ID())))
		select {
		case <-responses:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		require.NoError(t, stream.Send(ctx, newTestMessage(stream.ID())))
		select {
		case <-responses:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}

		select {
		case cs := <-retired:
			cs.streamStateMu.Lock()
			_, receivedExists := cs.receivedStreamSequences[stream.ID()]
			cs.streamStateMu.Unlock()
			sentExists := cs.sentStreams.contains(stream.ID())
			require.False(t, receivedExists)
			require.False(t, sentExists)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	})
}

func TestCanceledStreamResponseDoesNotCreateSequenceGap(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		canceledWriteResult := make(chan error, 1)

		rs.RegisterRequestHandler(func(_ context.Context, msg RPCMessage, _ uint64, session ClientSession) error {
			canceledCtx, cancelWrite := context.WithTimeout(context.Background(), time.Second)
			cancelWrite()
			canceledWriteResult <- session.Write(canceledCtx, newTestMessage(msg.Message.GetID()))
			return session.Write(ctx, newTestMessage(msg.Message.GetID()))
		})

		client := newTestClient(t)
		defer func() { require.NoError(t, client.Close()) }()
		stream, err := client.NewStream(ctx, testAddr, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, stream.Close(false)) }()
		responses, err := stream.Receive()
		require.NoError(t, err)

		require.NoError(t, stream.Send(ctx, newTestMessage(stream.ID())))
		select {
		case response := <-responses:
			require.NotNil(t, response)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		require.Error(t, <-canceledWriteResult)
	})
}

func TestAssignStreamSequenceProgressesWhileCloseWaitsOnFullQueue(t *testing.T) {
	cs := newClientSession(
		newServerMetrics("test"),
		newTestIOSession(nil, nil),
		newTestCodec(),
		func() *Future { return newFuture(nil) },
		nil,
	)
	cs.c = make(chan *Future, 1)
	require.True(t, cs.sentStreams.start(11))

	queued := newFuture(nil)
	queued.init(RPCMessage{
		Ctx:     context.Background(),
		Message: newTestMessage(10),
		oneWay:  true,
	})
	enqueueClientSessionFutureForTest(cs, queued)

	senderDone := make(chan error, 1)
	go func() {
		senderDone <- cs.AsyncWrite(newTestMessage(12))
	}()
	senderBlocked := assert.Eventually(t, func() bool {
		if cs.mu.TryLock() {
			cs.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- cs.Close()
	}()
	closeWaiting := assert.Eventually(t, func() bool {
		if cs.mu.TryRLock() {
			cs.mu.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)

	msg := RPCMessage{Message: newTestMessage(11)}
	assigned := make(chan bool, 1)
	go func() {
		assigned <- cs.assignStreamSequence(&msg)
	}()
	var assignProgressed bool
	select {
	case assignProgressed = <-assigned:
	case <-time.After(time.Second):
	}

	if f, ok := <-cs.c; ok && f != nil {
		cs.changeQueueDepth(-1)
		f.Close()
	}

	var senderErr, closeErr error
	select {
	case senderErr = <-senderDone:
	case <-time.After(time.Second):
		t.Fatal("blocked sender did not finish")
	}
	select {
	case closeErr = <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("session close did not finish")
	}

	require.True(t, senderBlocked)
	require.True(t, closeWaiting)
	require.True(t, assignProgressed)
	require.True(t, msg.stream)
	require.Equal(t, uint32(1), msg.streamSequence)
	require.NoError(t, senderErr)
	require.NoError(t, closeErr)

	late := RPCMessage{Message: newTestMessage(11)}
	require.False(t, cs.assignStreamSequence(&late))
	require.False(t, late.stream)
	require.False(t, cs.sentStreams.contains(11))
}

func TestFinishStreamPoisonsSessionWithPendingCache(t *testing.T) {
	cs := newClientSession(nil, newTestIOSession(nil, nil), nil, func() *Future { return &Future{} }, nil)
	cs.receivedStreamSequences[11] = 2
	require.True(t, cs.sentStreams.start(11))
	_, stream, open := cs.sentStreams.next(11)
	require.True(t, open)
	require.True(t, stream)
	_, err := cs.CreateCache(context.Background(), 11)
	require.NoError(t, err)
	token := StreamTerminalToken{owner: cs, streamID: 11, sequence: 2}
	err = cs.FinishStream(context.Background(), token, newTestMessage(11))
	require.Error(t, err)
	cs.mu.RLock()
	require.True(t, cs.mu.closed)
	cs.mu.RUnlock()
}

func TestFinishStreamRacesWithSessionClose(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done := make(chan struct{})
		rs.RegisterRequestHandler(func(ctx context.Context, msg RPCMessage, _ uint64, session ClientSession) error {
			cs := session.(*clientSession)
			token, ok := StreamTerminalTokenFromContext(ctx)
			if !ok {
				return moerr.NewInternalErrorNoCtx("missing terminal token")
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_ = cs.FinishStream(ctx, token, newTestMessage(msg.Message.GetID()))
			}()
			go func() {
				defer wg.Done()
				<-start
				_ = cs.Close()
			}()
			close(start)
			wg.Wait()
			close(done)
			return nil
		})

		client := newTestClient(t)
		defer func() { require.NoError(t, client.Close()) }()
		stream, err := client.NewStream(ctx, testAddr, false)
		require.NoError(t, err)
		require.NoError(t, stream.Send(ctx, newTestMessage(stream.ID())))
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	})
}

func TestServerTimeoutCacheWillRemoved(t *testing.T) {
	type cacheObservation struct {
		cache   MessageCache
		session ClientSession
	}

	scanDone := make(chan struct{}, 1)
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		cacheCreated := make(chan cacheObservation, 1)
		rs.RegisterRequestHandler(func(ctx context.Context, msg RPCMessage, seq uint64, cs ClientSession) error {
			request := msg.Message
			cache, err := cs.CreateCache(ctx, request.GetID())
			if err != nil {
				return err
			}
			if err := cache.Add(request); err != nil {
				return err
			}
			select {
			case cacheCreated <- cacheObservation{
				cache:   cache,
				session: cs,
			}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		st, err := c.NewStream(context.Background(), testAddr, false)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		// Stream.Send requires request.GetID() == stream.ID(); stream id is assigned by backend at NewStream().
		require.NoError(t, st.Send(ctx, newTestMessage(st.ID())))
		var observation cacheObservation
		select {
		case observation = <-cacheCreated:
		case <-ctx.Done():
			t.Fatal("server handler did not create the message cache")
		}
	waitForRetirement:
		for {
			select {
			case <-scanDone:
				cached, err := observation.session.GetCache(st.ID())
				require.NoError(t, err)
				if cached == nil {
					break waitForRetirement
				}
			case <-ctx.Done():
				t.Fatal("message cache was not retired by timeout scans")
			}
		}
		_, err = observation.cache.Len()
		require.Error(t, err, "expired cache must be closed before removal")
	}, WithServerMessageCacheScanHookForTesting(func() {
		select {
		case scanDone <- struct{}{}:
		default:
		}
	}))
}

type cacheRetirementObserver struct {
	session           *clientSession
	cacheID           uint64
	registeredAtClose chan bool
}

func (c *cacheRetirementObserver) Add(Message) error           { return nil }
func (c *cacheRetirementObserver) Len() (int, error)           { return 0, nil }
func (c *cacheRetirementObserver) Pop() (Message, bool, error) { return nil, false, nil }

func (c *cacheRetirementObserver) Close() {
	_, registered := c.session.mu.caches[c.cacheID]
	c.registeredAtClose <- registered
}

func TestMessageCacheRetirementLinearizesBeforeRemoval(t *testing.T) {
	newSessionWithCache := func(t *testing.T, ctx context.Context) (*clientSession, *cacheRetirementObserver) {
		t.Helper()
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		t.Cleanup(func() { require.NoError(t, cs.Close()) })
		cache := &cacheRetirementObserver{
			session:           cs,
			cacheID:           1,
			registeredAtClose: make(chan bool, 1),
		}
		cs.mu.caches[1] = cacheWithContext{ctx: ctx, cache: cache}
		return cs, cache
	}
	assertLinearized := func(t *testing.T, cache *cacheRetirementObserver) {
		t.Helper()
		select {
		case registered := <-cache.registeredAtClose:
			require.True(t, registered, "cache was removed before MessageCache.Close ran")
		case <-time.After(time.Second):
			t.Fatal("cache retirement did not close the cache")
		}
	}

	t.Run("explicit delete", func(t *testing.T) {
		cs, cache := newSessionWithCache(t, context.Background())
		cs.DeleteCache(1)
		assertLinearized(t, cache)
		cached, err := cs.GetCache(1)
		require.NoError(t, err)
		require.Nil(t, cached)
		require.NoError(t, cs.Close())
	})

	t.Run("session close", func(t *testing.T) {
		cs, cache := newSessionWithCache(t, context.Background())
		require.NoError(t, cs.Close())
		assertLinearized(t, cache)
	})

	t.Run("request timeout", func(t *testing.T) {
		ctx, expire := context.WithCancel(context.Background())
		cs, cache := newSessionWithCache(t, ctx)
		scanDone := make(chan struct{}, 1)
		cs.messageCacheScanHook = func() { scanDone <- struct{}{} }
		cs.startCheckCacheTimeout()
		expire()
		select {
		case <-scanDone:
		case <-time.After(2500 * time.Millisecond):
			t.Fatal("message cache timeout scan did not complete")
		}
		assertLinearized(t, cache)
		cached, err := cs.GetCache(1)
		require.NoError(t, err)
		require.Nil(t, cached)
		require.NoError(t, cs.Close())
	})
}

func TestCancelableMessageCacheLifecycle(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		var canceled atomic.Int32
		cache, err := cs.CreateCacheWithCancel(
			context.Background(),
			1,
			func() { canceled.Add(1) },
		)
		require.NoError(t, err)
		require.NoError(t, cache.Add(newTestMessage(1)))
		cs.DeleteCache(1)
		require.Equal(t, int32(1), canceled.Load())
		_, err = cache.Len()
		require.Error(t, err)
		require.NoError(t, cs.Close())
		require.Equal(t, int32(1), canceled.Load())
	})

	t.Run("session close", func(t *testing.T) {
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		var canceled atomic.Int32
		cache, err := cs.CreateCacheWithCancel(
			context.Background(),
			1,
			func() { canceled.Add(1) },
		)
		require.NoError(t, err)
		require.NoError(t, cs.Close())
		require.Equal(t, int32(1), canceled.Load())
		_, err = cache.Len()
		require.Error(t, err)
	})

	t.Run("all fragments", func(t *testing.T) {
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		var canceled atomic.Int32
		for range 2 {
			_, err := cs.CreateCacheWithCancel(
				context.Background(),
				1,
				func() { canceled.Add(1) },
			)
			require.NoError(t, err)
		}
		cs.DeleteCache(1)
		require.Equal(t, int32(2), canceled.Load())
		require.NoError(t, cs.Close())
	})

	t.Run("request timeout", func(t *testing.T) {
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		ctx, expire := context.WithCancel(context.Background())
		scanDone := make(chan struct{}, 1)
		cs.messageCacheScanHook = func() { scanDone <- struct{}{} }
		var canceled atomic.Int32
		cache, err := cs.CreateCacheWithCancel(
			ctx,
			1,
			func() { canceled.Add(1) },
		)
		require.NoError(t, err)
		expire()
		select {
		case <-scanDone:
		case <-time.After(2500 * time.Millisecond):
			t.Fatal("message cache timeout scan did not complete")
		}
		require.Equal(t, int32(1), canceled.Load())
		_, err = cache.Len()
		require.Error(t, err)
		require.NoError(t, cs.Close())
		require.Equal(t, int32(1), canceled.Load())
	})
}

func TestCancelableMessageCacheCallbacksCanReenterSession(t *testing.T) {
	assertReentrantCancel := func(
		t *testing.T,
		cs *clientSession,
		trigger func(),
	) {
		t.Helper()
		callbackDone := make(chan struct{})
		cacheClosed := make(chan error, 1)
		var cache MessageCache
		cache, err := cs.CreateCacheWithCancel(
			context.Background(),
			1,
			func() {
				_, err := cache.Len()
				cacheClosed <- err
				_, _ = cs.GetCache(1)
				close(callbackDone)
			},
		)
		require.NoError(t, err)
		triggerDone := make(chan struct{})
		go func() {
			trigger()
			close(triggerDone)
		}()
		select {
		case <-callbackDone:
		case <-time.After(time.Second):
			t.Fatal("cache cancel callback deadlocked while re-entering the session")
		}
		require.Error(t, <-cacheClosed, "cache must be closed before cancellation callbacks run")
		select {
		case <-triggerDone:
		case <-time.After(time.Second):
			t.Fatal("cache cleanup did not return after the callback completed")
		}
	}

	t.Run("delete", func(t *testing.T) {
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		assertReentrantCancel(t, cs, func() { cs.DeleteCache(1) })
		require.NoError(t, cs.Close())
	})

	t.Run("session close", func(t *testing.T) {
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		assertReentrantCancel(t, cs, func() { _ = cs.Close() })
		require.NoError(t, cs.Close())
	})

	t.Run("request timeout", func(t *testing.T) {
		cs := newClientSession(nil, newTestIOSession(nil, nil), nil, nil, nil)
		ctx, expire := context.WithCancel(context.Background())
		callbackDone := make(chan struct{})
		cacheClosed := make(chan error, 1)
		var cache MessageCache
		cache, err := cs.CreateCacheWithCancel(ctx, 1, func() {
			_, err := cache.Len()
			cacheClosed <- err
			_, _ = cs.GetCache(1)
			close(callbackDone)
		})
		require.NoError(t, err)
		expire()
		select {
		case <-callbackDone:
		case <-time.After(2500 * time.Millisecond):
			t.Fatal("timeout cancel callback deadlocked while re-entering the session")
		}
		require.Error(t, <-cacheClosed, "cache must be closed before cancellation callbacks run")
		require.NoError(t, cs.Close())
	})
}

func TestStreamServerWithSequenceNotMatch(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		v, err := c.NewStream(context.Background(), testAddr, false)
		require.NoError(t, err)
		st := v.(*stream)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		rc, err := st.Receive()
		require.NoError(t, err)
		require.NotNil(t, rc)

		st.sequence = 2
		req := newTestMessage(st.ID())
		require.NoError(t, st.Send(ctx, req))

		select {
		case resp := <-rc:
			assert.Nil(t, resp)
		case <-ctx.Done():
			t.Fatal("stream receiver was not terminated after sequence mismatch")
		}
	})
}

func TestStreamReadCannotBlockWrite(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		st, err := c.NewStream(context.Background(), testAddr, false)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close(false))
		}()

		ch, err := st.Receive()
		require.NoError(t, err)

		cc := make(chan struct{})
		n := 1000
		go func() {
			defer close(cc)
			i := 0
			for {
				<-ch
				i++
				if i == n {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		for i := 0; i < n; i++ {
			require.NoError(t, st.Send(ctx, newTestMessage(st.ID())))
		}
		<-cc
	})
}

func TestCannotGetClosedBackend(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t, WithClientMaxBackendPerHost(2))
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		st, err := c.NewStream(context.Background(), testAddr, true)
		require.NoError(t, err)
		require.NoError(t, st.Close(true))

		require.NoError(t, c.Ping(ctx, testAddr))
	})
}

func TestCloseStreamWithCloseConnNotifiesReceiver(t *testing.T) {
	testRPCServer(t, func(_ *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		st, err := c.NewStream(context.Background(), testAddr, true)
		require.NoError(t, err)
		recv, err := st.Receive()
		require.NoError(t, err)

		// Do not start the receiver before Close. This deterministically covers
		// the race where the backend's first nil notification is still buffered
		// and stream.Close used to drain it without publishing another one.
		require.NoError(t, st.Close(true))
		select {
		case message := <-recv:
			require.Nil(t, message)
		case <-time.After(time.Second):
			t.Fatal("stream receiver was not notified after closing the connection")
		}
	})
}

func TestCloseStreamUnregistersWithoutStreamLock(t *testing.T) {
	c := make(chan Message, 1)
	s := newStream(
		nil,
		c,
		func() *Future { return newFuture(nil) },
		func(*Future) error { return nil },
		func(st *stream) {
			// Backend cancellation enters stream from rb.mu. Requiring the stream
			// lock here proves Close does not keep the inverse s.mu -> rb.mu order
			// across unregister.
			st.mu.RLock()
			st.mu.RUnlock()
		},
		func() {},
	)
	s.init(1, false)

	done := make(chan error, 1)
	go func() { done <- s.Close(false) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("stream close held the stream lock while unregistering")
	}
}

func TestPingError(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t, WithClientMaxBackendPerHost(2))
		defer func() {
			assert.NoError(t, c.Close())
		}()
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(context.Background(), request.Message)
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		cancel()
		require.Error(t, c.Ping(ctx, testAddr))
	})
}

func BenchmarkSend(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	testRPCServer(b, func(rs *server) {
		c := newTestClient(b,
			WithClientMaxBackendPerHost(1),
			WithClientInitBackends([]string{testAddr}, []int{1}))
		defer func() {
			assert.NoError(b, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		req := newTestMessage(1)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, err := c.Send(ctx, testAddr, req)
			if err == nil {
				_, err := f.Get()
				if err != nil {
					assert.Equal(b, ctx.Err(), err)
				}
				f.Close()
			}
		}
	}, WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(i interface{}) {
		msg := i.(RPCMessage)
		if !msg.InternalMessage() {
			messagePool.Put(msg.Message)
		}
	})))
}

func testRPCServer(t assert.TestingT, testFunc func(*server), options ...ServerOption) {
	assert.NoError(t, os.RemoveAll(testUnixFile))

	options = append(options,
		WithServerLogger(logutil.GetPanicLoggerWithLevel(zap.InfoLevel)))
	s, err := NewRPCServer("test", testAddr, newTestCodec(), options...)
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()

	testFunc(s.(*server))
}

func newTestClient(t assert.TestingT, options ...ClientOption) RPCClient {
	bf := NewGoettyBasedBackendFactory(newTestCodec())
	// Add auto-create by default for tests
	defaultOptions := []ClientOption{WithClientEnableAutoCreateBackend()}
	defaultOptions = append(defaultOptions, options...)
	c, err := NewClient(
		"",
		bf,
		defaultOptions...)
	assert.NoError(t, err)
	return c
}

func TestPing(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		assert.NoError(t, c.Ping(ctx, testAddr))
	})
}
