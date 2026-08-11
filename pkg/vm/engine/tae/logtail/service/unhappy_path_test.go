// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
	"go.uber.org/ratelimit"
)

type recordingSessionNotifier struct {
	notified chan error
}

func (n *recordingSessionNotifier) NotifySessionError(_ *Session, err error) {
	select {
	case n.notified <- err:
	default:
	}
}

type cancelOrderSession struct {
	*captureSession
	sessionCanceled   <-chan struct{}
	closeBeforeCancel atomic.Bool
}

func (s *cancelOrderSession) Close() error {
	select {
	case <-s.sessionCanceled:
	default:
		s.closeBeforeCancel.Store(true)
	}
	return s.captureSession.Close()
}

type controlledWriteSession struct {
	*captureSession
	writeOnce        sync.Once
	closeOnce        sync.Once
	writeEntered     chan struct{}
	closed           chan struct{}
	allowWriteReturn chan struct{}
}

func newControlledWriteSession() *controlledWriteSession {
	return &controlledWriteSession{
		captureSession:   newCaptureSession(),
		writeEntered:     make(chan struct{}),
		closed:           make(chan struct{}),
		allowWriteReturn: make(chan struct{}),
	}
}

func (s *controlledWriteSession) Close() error {
	err := s.captureSession.Close()
	s.closeOnce.Do(func() { close(s.closed) })
	return err
}

func (s *controlledWriteSession) Write(context.Context, morpc.Message) error {
	s.writeOnce.Do(func() { close(s.writeEntered) })
	<-s.allowWriteReturn
	return context.Canceled
}

func TestSessionStopsWhenTransportDisconnects(t *testing.T) {
	transport := newCaptureSession()
	notifier := &recordingSessionNotifier{notified: make(chan error, 1)}
	session := NewSession(
		context.Background(), mockMOLogger(), NewLogtailResponsePool(),
		notifier, newCaptureStream(transport),
		time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	transport.cancel()
	select {
	case err := <-notifier.notified:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("session sender did not observe transport cancellation")
	}
	require.ErrorIs(t, session.sessionCtx.Err(), context.Canceled)
}

func TestPostCleanCancelsBeforeClosingTransport(t *testing.T) {
	transport := &cancelOrderSession{captureSession: newCaptureSession()}
	responses := NewLogtailResponsePool()
	session := NewSession(
		context.Background(), mockMOLogger(), responses,
		&recordingSessionNotifier{notified: make(chan error, 1)},
		mockMorpcStream(transport, 1, 1024),
		time.Second, time.Second, time.Hour, time.Hour,
	)
	transport.sessionCanceled = session.sessionCtx.Done()

	session.PostClean()

	require.False(t, transport.closeBeforeCancel.Load(),
		"cleanup must cancel response admission before entering transport Close")
	require.ErrorIs(t, session.sessionCtx.Err(), context.Canceled)
	require.ErrorIs(t, transport.ctx.Err(), context.Canceled)
	session.enqueueMu.RLock()
	admissionClosed := session.enqueueClosed
	session.enqueueMu.RUnlock()
	require.True(t, admissionClosed)

	var released atomic.Int32
	response := responses.Acquire()
	response.closeCB = func() { released.Add(1) }
	require.ErrorIs(t,
		session.SendResponse(context.Background(), response),
		context.Canceled,
	)
	require.Equal(t, int32(1), released.Load())
	require.Empty(t, session.sendChan)
}

func TestPostCleanDoesNotHoldAdmissionBarrierWhileJoiningSender(t *testing.T) {
	transport := newControlledWriteSession()
	notifier := &recordingSessionNotifier{notified: make(chan error, 1)}
	responses := NewLogtailResponsePool()
	session := NewSession(
		context.Background(), mockMOLogger(), responses, notifier,
		mockMorpcStream(transport, 1, 1024),
		time.Second, time.Second, time.Hour, time.Hour,
	)

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	_, generation := session.RegisterWithGeneration(id, table)
	require.NoError(t, session.SendUpdateResponse(
		context.Background(),
		mockTimestamp(1, 0),
		mockTimestamp(2, 0),
		nil,
	))
	select {
	case <-transport.writeEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("session sender did not enter the controlled transport write")
	}

	var allowWriteReturnOnce sync.Once
	allowWriteReturn := func() {
		allowWriteReturnOnce.Do(func() { close(transport.allowWriteReturn) })
	}
	t.Cleanup(allowWriteReturn)

	// Hold a reader so PostClean can be observed queued as the admission-barrier
	// writer before subscription completion tries to enter as a later reader.
	session.enqueueMu.RLock()
	admissionReadLocked := true
	defer func() {
		if admissionReadLocked {
			session.enqueueMu.RUnlock()
		}
	}()

	postCleanDone := make(chan struct{})
	go func() {
		session.PostClean()
		close(postCleanDone)
	}()
	select {
	case <-transport.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("PostClean did not close the transport")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if !session.enqueueMu.TryRLock() {
			break
		}
		session.enqueueMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("PostClean did not reach the response-admission barrier")
		}
		runtime.Gosched()
	}

	type completionResult struct {
		completed bool
		err       error
	}
	var released atomic.Int32
	completionDone := make(chan completionResult, 1)
	go func() {
		completed, err := session.CompleteSubscription(
			context.Background(),
			id,
			generation,
			mockLogtail(table, mockTimestamp(3, 0)),
			func() { released.Add(1) },
		)
		completionDone <- completionResult{completed: completed, err: err}
	}()

	deadline = time.Now().Add(10 * time.Second)
	for {
		if !session.mu.TryLock() {
			break
		}
		session.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("subscription completion did not acquire Session.mu")
		}
		runtime.Gosched()
	}

	// Model a sender-side state read that must finish before wg can reach zero.
	// If PostClean keeps enqueueMu held while joining, this worker and
	// CompleteSubscription recreate the production lock cycle.
	session.wg.Add(1)
	stateReadStarted := make(chan struct{})
	go func() {
		close(stateReadStarted)
		session.mu.Lock()
		session.mu.Unlock()
		session.wg.Done()
	}()
	<-stateReadStarted

	allowWriteReturn()
	session.enqueueMu.RUnlock()
	admissionReadLocked = false

	select {
	case result := <-completionDone:
		require.True(t, result.completed)
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("subscription completion deadlocked with cleanup")
	}
	select {
	case <-postCleanDone:
	case <-time.After(10 * time.Second):
		t.Fatal("PostClean deadlocked while joining the sender")
	}

	require.Equal(t, int32(1), released.Load())
	require.Zero(t, session.Active())
	require.Equal(t, TableNotFound, session.Unregister(id))
}

func TestSessionSeparatesProgressAndTransportIntervals(t *testing.T) {
	server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
	transport := newCaptureSession()
	table := mockTable(1, 1, 1)
	require.NoError(t, server.onSubscription(
		context.Background(), newCaptureStream(transport),
		&logtail.SubscribeRequest{Table: &table},
	))

	sessions := server.ssmgr.ListSession()
	require.Len(t, sessions, 1)
	require.Equal(t, server.cfg.LogtailCollectInterval, sessions[0].progressInterval)
	require.Equal(t, server.transportProbeInterval, sessions[0].transportProbeInterval)
	require.NotEqual(t, sessions[0].progressInterval, sessions[0].transportProbeInterval,
		"idle connection probing must not slow logtail progress heartbeats")
}

func newTransportProbeTestSession(
	t *testing.T,
	sentThrough timestamp.Timestamp,
	exactFrom timestamp.Timestamp,
) (*Session, *captureSession) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	transport := newCaptureSession()
	t.Cleanup(transport.cancel)
	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	return &Session{
		sessionCtx:  ctx,
		cancelFunc:  cancel,
		logger:      mockMOLogger(),
		sendTimeout: time.Second,
		responses:   NewLogtailResponsePool(),
		stream:      mockMorpcStream(transport, 1, 1<<20),
		sendChan:    make(chan message, 1),
		tables:      map[TableID]tableSubscription{id: {state: TableSubscribed}},
		exactFrom:   exactFrom,
		sentThrough: sentThrough,
	}, transport
}

func receiveCapturedLogtailResponse(
	t *testing.T,
	transport *captureSession,
) *LogtailResponse {
	t.Helper()
	select {
	case value := <-transport.writes:
		segment, ok := value.(*LogtailResponseSegment)
		require.True(t, ok)
		require.Equal(t, int32(1), segment.MaxSequence,
			"probe ordering tests require a single transport segment")
		response := &LogtailResponse{}
		require.NoError(t, response.Unmarshal(segment.Payload))
		return response
	case <-time.After(10 * time.Second):
		t.Fatal("logtail response was not written")
		return nil
	}
}

type failingWriteSession struct {
	*captureSession
	err error
}

func (s *failingWriteSession) Write(context.Context, morpc.Message) error {
	return s.err
}

func TestTransportProbeCannotOvertakeQueuedUpdate(t *testing.T) {
	from := timestamp.Timestamp{PhysicalTime: 10}
	to := timestamp.Timestamp{PhysicalTime: 20}
	session, transport := newTransportProbeTestSession(t, from, to)

	table := mockTable(1, 1, 1)
	var released atomic.Int32
	response := session.responses.Acquire()
	response.Response = newUpdateResponse(from, to, mockLogtail(table, to))
	response.closeCB = func() { released.Add(1) }
	session.sendChan <- message{
		createAt: time.Now(),
		timeout:  time.Second,
		response: response,
	}

	result, err := session.sendProbeOrPending(0)
	require.NoError(t, err)
	require.Equal(t, probeWroteQueuedResponse, result)
	require.Equal(t, int32(1), released.Load())

	result, err = session.sendProbeOrPending(1)
	require.NoError(t, err)
	require.Equal(t, probeWroteHeartbeat, result)

	update := receiveCapturedLogtailResponse(t, transport).GetUpdateResponse()
	require.NotNil(t, update)
	require.NotNil(t, update.From)
	require.NotNil(t, update.To)
	require.Equal(t, from, *update.From)
	require.Equal(t, to, *update.To)
	require.Len(t, update.LogtailList, 1)

	heartbeat := receiveCapturedLogtailResponse(t, transport).GetUpdateResponse()
	require.NotNil(t, heartbeat)
	require.NotNil(t, heartbeat.From)
	require.NotNil(t, heartbeat.To)
	require.Equal(t, to, *heartbeat.From)
	require.Equal(t, to, *heartbeat.To)
	require.Empty(t, heartbeat.LogtailList)
}

func TestTransportProbeReportsOnlySentProgress(t *testing.T) {
	sent := timestamp.Timestamp{PhysicalTime: 10}
	enqueued := timestamp.Timestamp{PhysicalTime: 20}
	session, transport := newTransportProbeTestSession(t, sent, enqueued)

	result, err := session.sendProbeOrPending(0)
	require.NoError(t, err)
	require.Equal(t, probeWroteHeartbeat, result)

	heartbeat := receiveCapturedLogtailResponse(t, transport).GetUpdateResponse()
	require.NotNil(t, heartbeat)
	require.NotNil(t, heartbeat.From)
	require.NotNil(t, heartbeat.To)
	require.Equal(t, sent, *heartbeat.From)
	require.Equal(t, sent, *heartbeat.To)
	require.Empty(t, heartbeat.LogtailList)
}

func TestTransportProbePreservesSubscriptionProgress(t *testing.T) {
	from := timestamp.Timestamp{PhysicalTime: 10}
	subscribedAt := timestamp.Timestamp{PhysicalTime: 20}
	session, transport := newTransportProbeTestSession(t, from, from)

	response := session.responses.Acquire()
	response.Response = newSubscritpionResponse(
		mockLogtail(mockTable(1, 1, 1), subscribedAt),
	)
	session.sendChan <- message{
		createAt: time.Now(),
		timeout:  time.Second,
		response: response,
	}

	result, err := session.sendProbeOrPending(0)
	require.NoError(t, err)
	require.Equal(t, probeWroteQueuedResponse, result)
	require.Equal(t, subscribedAt, session.sentThrough)

	result, err = session.sendProbeOrPending(1)
	require.NoError(t, err)
	require.Equal(t, probeWroteHeartbeat, result)

	subscribe := receiveCapturedLogtailResponse(t, transport).GetSubscribeResponse()
	require.NotNil(t, subscribe)
	require.NotNil(t, subscribe.Logtail.Ts)
	require.Equal(t, subscribedAt, *subscribe.Logtail.Ts)

	heartbeat := receiveCapturedLogtailResponse(t, transport).GetUpdateResponse()
	require.NotNil(t, heartbeat)
	require.NotNil(t, heartbeat.From)
	require.NotNil(t, heartbeat.To)
	require.Equal(t, subscribedAt, *heartbeat.From)
	require.Equal(t, subscribedAt, *heartbeat.To)
}

func TestFailedQueuedUpdateDoesNotAdvanceSentProgress(t *testing.T) {
	from := timestamp.Timestamp{PhysicalTime: 10}
	to := timestamp.Timestamp{PhysicalTime: 20}
	session, transport := newTransportProbeTestSession(t, from, to)

	var released atomic.Int32
	response := session.responses.Acquire()
	response.Response = newUpdateResponse(from, to, mockLogtail(mockTable(1, 1, 1), to))
	response.closeCB = func() { released.Add(1) }
	session.sendChan <- message{
		createAt: time.Now(),
		timeout:  time.Second,
		response: response,
	}
	session.stream.cs = &failingWriteSession{
		captureSession: transport,
		err:            context.Canceled,
	}

	result, err := session.sendProbeOrPending(0)
	require.Error(t, err)
	require.Equal(t, probeWroteQueuedResponse, result)
	require.Equal(t, from, session.sentThrough)
	require.Equal(t, int32(1), released.Load())
}

func TestCancelledSubscriptionPhaseCannotPublishStaleResponse(t *testing.T) {
	server := newUnitLogtailServer(t, &controlledLogtailer{})
	transport := newCaptureSession()
	session := server.ssmgr.GetSession(
		server.rootCtx, server.logger, server.pool.responses, server,
		newCaptureStream(transport), time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	table := mockTable(1, 1, 1)
	tableID := MarshalTableID(&table)
	repeated, generation := session.RegisterWithGeneration(tableID, table)
	require.False(t, repeated)
	require.Equal(t, TableOnSubscription, session.Unregister(tableID))

	sub := subscription{
		timeout:    time.Second,
		tableID:    tableID,
		generation: generation,
		req:        &logtail.SubscribeRequest{Table: &table},
		session:    session,
	}
	var released atomic.Int32
	phase := func(ts int64) *LogtailPhase {
		return &LogtailPhase{
			tail:    mockLogtail(table, timestamp.Timestamp{PhysicalTime: ts}),
			closeCB: func() { released.Add(1) },
			sub:     sub,
		}
	}

	server.sendSubscription(server.rootCtx, phase(1), phase(2))

	require.Zero(t, session.Active(), "cancelled subscription must not become active")
	require.Equal(t, int32(2), released.Load(), "stale phase resources must be released")
}

func TestStaleSubscriptionGenerationCannotCompleteNewAttempt(t *testing.T) {
	transport := newCaptureSession()
	session := NewSession(
		context.Background(), mockMOLogger(), NewLogtailResponsePool(),
		&recordingSessionNotifier{notified: make(chan error, 1)}, newCaptureStream(transport),
		time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	_, first := session.RegisterWithGeneration(id, table)
	require.Equal(t, TableOnSubscription, session.Unregister(id))
	_, second := session.RegisterWithGeneration(id, table)
	require.NotEqual(t, first, second)

	var staleReleased atomic.Int32
	completed, err := session.CompleteSubscription(
		context.Background(), id, first, mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1}),
		func() { staleReleased.Add(1) },
	)
	require.NoError(t, err)
	require.False(t, completed)
	require.Equal(t, int32(1), staleReleased.Load())
	require.Equal(t, TableOnSubscription, session.Tables()[id])

	completed, err = session.CompleteSubscription(
		context.Background(), id, second, mockLogtail(table, timestamp.Timestamp{PhysicalTime: 2}), nil,
	)
	require.NoError(t, err)
	require.True(t, completed)
	require.Equal(t, TableSubscribed, session.Tables()[id])
	require.Equal(t, 1, session.Active())
}

func TestStaleSubscriptionFailureCannotCancelNewAttempt(t *testing.T) {
	server := newUnitLogtailServer(t, &controlledLogtailer{
		tableFn: func(context.Context, api.TableID, timestamp.Timestamp, timestamp.Timestamp) (logtail.TableLogtail, func(), error) {
			return logtail.TableLogtail{}, nil, context.DeadlineExceeded
		},
	})
	transport := newCaptureSession()
	session := server.ssmgr.GetSession(
		server.rootCtx, server.logger, server.pool.responses, server,
		newCaptureStream(transport), time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	_, first := session.RegisterWithGeneration(id, table)
	require.Equal(t, TableOnSubscription, session.Unregister(id))
	_, second := session.RegisterWithGeneration(id, table)
	require.NotEqual(t, first, second)

	_, err := server.getSubLogtailPhase(server.rootCtx, subscription{
		timeout:    time.Second,
		tableID:    id,
		generation: first,
		req:        &logtail.SubscribeRequest{Table: &table},
		session:    session,
	}, timestamp.Timestamp{}, timestamp.Timestamp{PhysicalTime: 1})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, TableOnSubscription, session.Tables()[id],
		"a late failure from the old attempt must preserve the new attempt")
	require.Equal(t, TableOnSubscription, session.unregisterGeneration(id, second))
	require.Equal(t, TableNotFound, session.Unregister(id))
}

func TestCancelledSubscriptionRequestDoesNotRemainRegistered(t *testing.T) {
	server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
	for len(server.subReqChan) < cap(server.subReqChan) {
		server.subReqChan <- subscription{}
	}

	transport := newCaptureSession()
	stream := newCaptureStream(transport)
	table := mockTable(1, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, server.onSubscription(
		ctx, stream, &logtail.SubscribeRequest{Table: &table},
	), context.Canceled)

	session := server.ssmgr.GetSession(
		server.rootCtx, server.logger, server.pool.responses, server,
		stream, time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)
	require.Equal(t, TableNotFound, session.Unregister(MarshalTableID(&table)))
}

func TestConcurrentSubscriptionGenerationLifecycle(t *testing.T) {
	transport := newCaptureSession()
	session := NewSession(
		context.Background(), mockMOLogger(), NewLogtailResponsePool(),
		&recordingSessionNotifier{notified: make(chan error, 1)}, newCaptureStream(transport),
		time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)
	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)

	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if repeated, _ := session.RegisterWithGeneration(id, table); !repeated {
				session.Unregister(id)
			}
		}()
	}
	wg.Wait()
	state := session.Unregister(id)
	require.Contains(t, []TableState{TableNotFound, TableOnSubscription}, state)
	require.Zero(t, session.Active())
}

func TestCancellingPendingSubscriptionDoesNotDecrementActiveCount(t *testing.T) {
	transport := newCaptureSession()
	session := NewSession(
		context.Background(), mockMOLogger(), NewLogtailResponsePool(),
		&recordingSessionNotifier{notified: make(chan error, 1)}, newCaptureStream(transport),
		time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	require.False(t, session.Register(id, table))
	state := session.Unregister(id)
	require.Equal(t, TableOnSubscription, state)
	require.NoError(t, session.CompleteUnsubscription(context.Background(), table, state))
	require.Zero(t, session.Active())
}

func TestFailedUnsubscriptionResponseStillRemovesActiveOwnership(t *testing.T) {
	transport := newCaptureSession()
	session := NewSession(
		context.Background(), mockMOLogger(), NewLogtailResponsePool(),
		&recordingSessionNotifier{notified: make(chan error, 1)}, newCaptureStream(transport),
		time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	_, generation := session.RegisterWithGeneration(id, table)
	completed, err := session.CompleteSubscription(
		context.Background(), id, generation,
		mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1}), nil,
	)
	require.NoError(t, err)
	require.True(t, completed)
	require.Equal(t, 1, session.Active())

	state := session.Unregister(id)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, session.CompleteUnsubscription(ctx, table, state), context.Canceled)
	require.Zero(t, session.Active(),
		"response failure must not retain a logically removed subscription")
}

func TestPublishEventDoesNotWaitForSlowSessionQueue(t *testing.T) {
	server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
	transport := newCaptureSession()
	ctx, cancel := context.WithCancel(server.rootCtx)
	t.Cleanup(cancel)
	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	responses := NewLogtailResponsePool()
	slow := &Session{
		sessionCtx:       ctx,
		cancelFunc:       cancel,
		logger:           server.logger,
		sendTimeout:      time.Second,
		responses:        responses,
		notifier:         server,
		stream:           newCaptureStream(transport),
		poisonTime:       time.Hour,
		sendChan:         make(chan message, 1),
		active:           1,
		tables:           map[TableID]tableSubscription{id: {state: TableSubscribed, generation: 1}},
		progressInterval: time.Hour,
		progressTimer:    time.NewTimer(time.Hour),
	}
	t.Cleanup(func() { slow.progressTimer.Stop() })
	// A full queue represents a session whose sender cannot make progress.
	slow.sendChan <- message{response: responses.Acquire()}
	server.ssmgr.clients[slow.stream] = slow
	// A pending wakeup represents a reaper that is already busy cleaning a
	// different failed session. Error reporting must still not feed that delay
	// back into the global publisher.
	server.errChan <- struct{}{}

	done := make(chan struct{})
	go func() {
		server.publishEvent(server.rootCtx, event{
			to:       timestamp.Timestamp{PhysicalTime: 1},
			logtails: []logtail.TableLogtail{mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1})},
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		// This is only a slow-CI deadlock guard. The operation itself has no
		// scheduler-dependent ordering: its input queue is already full.
		cancel()
		<-done
		t.Fatal("one slow session blocked the global logtail publisher")
	}
	server.pendingSessionErrors.Lock()
	_, cleanupRetained := server.pendingSessionErrors.items[slow]
	server.pendingSessionErrors.Unlock()
	require.True(t, cleanupRetained,
		"non-blocking notification must retain cleanup responsibility")
}

func TestCompleteSubscriptionDoesNotWaitForFullSessionQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := newCaptureSession()
	responses := NewLogtailResponsePool()
	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	session := &Session{
		sessionCtx:       ctx,
		cancelFunc:       cancel,
		logger:           mockMOLogger(),
		sendTimeout:      time.Hour,
		responses:        responses,
		notifier:         &recordingSessionNotifier{notified: make(chan error, 1)},
		stream:           mockMorpcStream(transport, 1, 1024),
		poisonTime:       time.Hour,
		sendChan:         make(chan message, 1),
		tables:           map[TableID]tableSubscription{id: {state: TableOnSubscription, generation: 1}},
		progressInterval: time.Hour,
		progressTimer:    time.NewTimer(time.Hour),
	}
	t.Cleanup(session.PostClean)

	// Keep the queue full without a sender goroutine. The completion path must
	// reject this session synchronously instead of waiting for poisonTime.
	session.sendChan <- message{response: responses.Acquire()}
	var released atomic.Int32
	type result struct {
		completed bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		completed, err := session.CompleteSubscription(
			context.Background(), id, 1,
			mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1}),
			func() { released.Add(1) },
		)
		done <- result{completed: completed, err: err}
	}()

	select {
	case result := <-done:
		require.True(t, result.completed)
		require.True(t, moerr.IsMoErrCode(result.err, moerr.ErrStreamClosed))
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("subscription completion waited for a congested session queue")
	}
	require.Equal(t, int32(1), released.Load())
	require.Zero(t, session.Active())
	require.Equal(t, TableNotFound, session.Unregister(id),
		"failed response hand-off must roll back the subscription generation")
	require.ErrorIs(t, transport.ctx.Err(), context.Canceled,
		"a congested session must be closed so it can rebuild from a snapshot")
}

func TestCongestedSubscriptionDoesNotBlockHealthyIncrementalProgress(t *testing.T) {
	phaseTwoEntered := make(chan struct{})
	var phaseTwoOnce sync.Once
	var subscriptionReleased atomic.Int32
	logtailer := &controlledLogtailer{
		tableFn: func(
			_ context.Context, table api.TableID, _, to timestamp.Timestamp,
		) (logtail.TableLogtail, func(), error) {
			phaseTwoOnce.Do(func() { close(phaseTwoEntered) })
			return mockLogtail(table, to), func() { subscriptionReleased.Add(1) }, nil
		},
	}
	server := newUnitLogtailServer(t, logtailer)
	responses := server.pool.responses

	newSession := func(
		streamID uint64, table api.TableID, state TableState, active int32,
	) (*Session, *captureSession) {
		ctx, cancel := context.WithCancel(server.rootCtx)
		transport := newCaptureSession()
		id := MarshalTableID(&table)
		return &Session{
			sessionCtx:       ctx,
			cancelFunc:       cancel,
			logger:           server.logger,
			sendTimeout:      time.Hour,
			responses:        responses,
			notifier:         server,
			stream:           mockMorpcStream(transport, streamID, 1024),
			poisonTime:       time.Hour,
			sendChan:         make(chan message, 1),
			active:           active,
			tables:           map[TableID]tableSubscription{id: {state: state, generation: 1}},
			progressInterval: time.Hour,
			progressTimer:    time.NewTimer(time.Hour),
		}, transport
	}

	slowTable := mockTable(1, 1, 1)
	slow, slowTransport := newSession(1, slowTable, TableOnSubscription, 0)
	// No sender consumes this placeholder, so subscription completion observes
	// a deterministically full queue.
	slow.sendChan <- message{response: responses.Acquire()}
	healthyTable := mockTable(1, 2, 1)
	healthy, _ := newSession(2, healthyTable, TableSubscribed, 1)
	server.ssmgr.Lock()
	server.ssmgr.clients[slow.stream] = slow
	server.ssmgr.clients[healthy.stream] = healthy
	server.ssmgr.Unlock()

	slowID := MarshalTableID(&slowTable)
	server.subTailChan <- &LogtailPhase{
		tail: mockLogtail(slowTable, timestamp.Timestamp{PhysicalTime: 1}),
		closeCB: func() {
			subscriptionReleased.Add(1)
		},
		sub: subscription{
			timeout:    time.Hour,
			tableID:    slowID,
			generation: 1,
			req:        &logtail.SubscribeRequest{Table: &slowTable},
			session:    slow,
		},
	}

	select {
	case <-phaseTwoEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("global sender did not enter subscription phase two")
	}

	var incrementalReleased atomic.Int32
	released := make(chan struct{})
	require.NoError(t, logtailer.notify(
		timestamp.Timestamp{PhysicalTime: 1},
		timestamp.Timestamp{PhysicalTime: 2},
		func() {
			if incrementalReleased.Add(1) == 1 {
				close(released)
			}
		},
		mockLogtail(healthyTable, timestamp.Timestamp{PhysicalTime: 2}),
	))

	select {
	case msg := <-healthy.sendChan:
		update := msg.response.GetUpdateResponse()
		require.NotNil(t, update)
		require.Len(t, update.LogtailList, 1)
		responses.Release(msg.response)
	case <-time.After(10 * time.Second):
		slow.cancelFunc()
		t.Fatal("congested subscription blocked healthy incremental progress")
	}
	select {
	case <-released:
	case <-time.After(10 * time.Second):
		t.Fatal("healthy incremental response ownership was not released")
	}

	require.Equal(t, int32(2), subscriptionReleased.Load(),
		"both subscription phases must be released exactly once")
	require.Equal(t, int32(1), incrementalReleased.Load())
	require.Zero(t, slow.Active())
	require.Equal(t, TableNotFound, slow.Unregister(slowID))
	require.ErrorIs(t, slowTransport.ctx.Err(), context.Canceled)
}

func TestCongestedSubscriptionErrorDoesNotBlockHealthyIncrementalProgress(t *testing.T) {
	phaseTwoEntered := make(chan struct{})
	returnPhaseTwoError := make(chan struct{})
	var returnPhaseTwoOnce sync.Once
	returnPhaseTwo := func() { returnPhaseTwoOnce.Do(func() { close(returnPhaseTwoError) }) }
	var phaseTwoReleased atomic.Int32
	logtailer := &controlledLogtailer{
		tableFn: func(
			_ context.Context, _ api.TableID, _, _ timestamp.Timestamp,
		) (logtail.TableLogtail, func(), error) {
			close(phaseTwoEntered)
			<-returnPhaseTwoError
			return logtail.TableLogtail{}, func() { phaseTwoReleased.Add(1) }, context.DeadlineExceeded
		},
	}
	server := newUnitLogtailServer(t, logtailer)
	t.Cleanup(returnPhaseTwo)
	responses := server.pool.responses

	newSession := func(
		streamID uint64, table api.TableID, state TableState, active int32,
	) (*Session, *captureSession) {
		ctx, cancel := context.WithCancel(server.rootCtx)
		transport := newCaptureSession()
		id := MarshalTableID(&table)
		return &Session{
			sessionCtx:       ctx,
			cancelFunc:       cancel,
			logger:           server.logger,
			sendTimeout:      time.Hour,
			responses:        responses,
			notifier:         server,
			stream:           mockMorpcStream(transport, streamID, 1024),
			poisonTime:       time.Hour,
			sendChan:         make(chan message, 1),
			active:           active,
			tables:           map[TableID]tableSubscription{id: {state: state, generation: 1}},
			progressInterval: time.Hour,
			progressTimer:    time.NewTimer(time.Hour),
		}, transport
	}

	slowTable := mockTable(1, 1, 1)
	slow, slowTransport := newSession(1, slowTable, TableOnSubscription, 0)
	slow.sendChan <- message{response: responses.Acquire()}
	healthyTable := mockTable(1, 2, 1)
	healthy, _ := newSession(2, healthyTable, TableSubscribed, 1)
	server.ssmgr.Lock()
	server.ssmgr.clients[slow.stream] = slow
	server.ssmgr.clients[healthy.stream] = healthy
	server.ssmgr.Unlock()

	var phaseOneReleased atomic.Int32
	slowID := MarshalTableID(&slowTable)
	server.subTailChan <- &LogtailPhase{
		tail:    mockLogtail(slowTable, timestamp.Timestamp{PhysicalTime: 1}),
		closeCB: func() { phaseOneReleased.Add(1) },
		sub: subscription{
			timeout:    time.Hour,
			tableID:    slowID,
			generation: 1,
			req:        &logtail.SubscribeRequest{Table: &slowTable},
			session:    slow,
		},
	}

	select {
	case <-phaseTwoEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("global sender did not enter subscription phase two")
	}

	var incrementalReleased atomic.Int32
	released := make(chan struct{})
	require.NoError(t, logtailer.notify(
		timestamp.Timestamp{PhysicalTime: 1},
		timestamp.Timestamp{PhysicalTime: 2},
		func() {
			if incrementalReleased.Add(1) == 1 {
				close(released)
			}
		},
		mockLogtail(healthyTable, timestamp.Timestamp{PhysicalTime: 2}),
	))
	returnPhaseTwo()

	select {
	case msg := <-healthy.sendChan:
		update := msg.response.GetUpdateResponse()
		require.NotNil(t, update)
		require.Len(t, update.LogtailList, 1)
		responses.Release(msg.response)
	case <-time.After(10 * time.Second):
		slow.cancelFunc()
		t.Fatal("congested subscription error blocked healthy incremental progress")
	}
	select {
	case <-released:
	case <-time.After(10 * time.Second):
		t.Fatal("healthy incremental response ownership was not released")
	}

	require.Equal(t, int32(1), phaseOneReleased.Load())
	require.Equal(t, int32(1), phaseTwoReleased.Load())
	require.Equal(t, int32(1), incrementalReleased.Load())
	require.Zero(t, slow.Active())
	require.Equal(t, TableNotFound, slow.Unregister(slowID))
	require.ErrorIs(t, slowTransport.ctx.Err(), context.Canceled)
}

func TestServerRejectsSessionAdmissionAfterShutdownStarts(t *testing.T) {
	tests := []struct {
		name string
		call func(*LogtailServer, morpcStream, api.TableID) error
	}{
		{
			name: "subscribe",
			call: func(server *LogtailServer, stream morpcStream, table api.TableID) error {
				return server.onSubscription(
					context.Background(), stream, &logtail.SubscribeRequest{Table: &table})
			},
		},
		{
			name: "unsubscribe",
			call: func(server *LogtailServer, stream morpcStream, table api.TableID) error {
				return server.onUnsubscription(
					context.Background(), stream, &logtail.UnsubscribeRequest{Table: &table})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
			server.cancelFunc()
			transport := newCaptureSession()
			err := test.call(server, newCaptureStream(transport), mockTable(1, 1, 1))
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, server.ssmgr.ListSession(),
				"shutdown must close the check-then-insert admission race")
		})
	}
}

func TestPublishEventReleasesBatchExactlyOnceAcrossSessions(t *testing.T) {
	server := newUnitLogtailServer(t, &controlledLogtailer{})
	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	for streamID := uint64(1); streamID <= 2; streamID++ {
		transport := newCaptureSession()
		session := server.ssmgr.GetSession(
			server.rootCtx, server.logger, server.pool.responses, server,
			mockMorpcStream(transport, streamID, 1024), time.Second, time.Second, time.Hour, time.Hour,
		)
		_, generation := session.RegisterWithGeneration(id, table)
		completed, err := session.CompleteSubscription(
			context.Background(), id, generation, mockLogtail(table, timestamp.Timestamp{}), nil,
		)
		require.NoError(t, err)
		require.True(t, completed)
	}

	var released atomic.Int32
	releasedCh := make(chan struct{})
	server.publishEvent(server.rootCtx, event{
		to: timestamp.Timestamp{PhysicalTime: 1},
		closeCB: func() {
			if released.Add(1) == 1 {
				close(releasedCh)
			}
		},
		logtails: []logtail.TableLogtail{mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1})},
	})
	select {
	case <-releasedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("not all session responses released the shared batch")
	}
	require.Equal(t, int32(1), released.Load())
}

func TestLogtailServerCloseReleasesQueuedEvent(t *testing.T) {
	server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
	var released atomic.Int32
	require.NoError(t, server.NotifyLogtail(
		timestamp.Timestamp{}, timestamp.Timestamp{PhysicalTime: 1},
		func() { released.Add(1) },
	))
	require.NoError(t, server.Close())
	require.Equal(t, int32(1), released.Load())
}

func TestLogtailServerCloseReleasesQueuedPhaseOneTail(t *testing.T) {
	server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
	var released atomic.Int32
	server.subTailChan <- &LogtailPhase{closeCB: func() { released.Add(1) }}

	require.NoError(t, server.Close())
	require.Equal(t, int32(1), released.Load())
}

func TestLogtailServerCloseReleasesQueuedSessionResponse(t *testing.T) {
	server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
	transport := newCaptureSession()
	responses := NewLogtailResponsePool()
	session := NewSession(
		server.rootCtx, server.logger, responses, server,
		newCaptureStream(transport), time.Second, time.Second, time.Hour, time.Hour,
	)
	server.ssmgr.clients[session.stream] = session

	var released atomic.Int32
	response := responses.Acquire()
	response.closeCB = func() { released.Add(1) }
	session.sendChan <- message{response: response}

	require.NoError(t, server.Close())
	require.Equal(t, int32(1), released.Load())
}

func TestLogtailServerCloseDrainsQueuedSubscription(t *testing.T) {
	server := newUnitLogtailServerWithStart(t, &controlledLogtailer{}, false)
	transport := newCaptureSession()
	table := mockTable(1, 1, 1)
	require.NoError(t, server.onSubscription(
		context.Background(), newCaptureStream(transport),
		&logtail.SubscribeRequest{Table: &table},
	))
	require.Len(t, server.subReqChan, 1)

	require.NoError(t, server.Close())
	require.Empty(t, server.subReqChan)
}

func TestPhaseTwoFailureReleasesPhaseOneTail(t *testing.T) {
	server := newUnitLogtailServer(t, &controlledLogtailer{
		tableFn: func(context.Context, api.TableID, timestamp.Timestamp, timestamp.Timestamp) (logtail.TableLogtail, func(), error) {
			return logtail.TableLogtail{}, nil, context.DeadlineExceeded
		},
	})
	transport := newCaptureSession()
	session := server.ssmgr.GetSession(
		server.rootCtx, server.logger, server.pool.responses, server,
		newCaptureStream(transport), time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	_, generation := session.RegisterWithGeneration(id, table)
	var released atomic.Int32
	releasedCh := make(chan struct{})
	server.subTailChan <- &LogtailPhase{
		tail: mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1}),
		closeCB: func() {
			released.Add(1)
			close(releasedCh)
		},
		sub: subscription{
			timeout:    time.Second,
			tableID:    id,
			generation: generation,
			req:        &logtail.SubscribeRequest{Table: &table},
			session:    session,
		},
	}

	select {
	case <-releasedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("phase-one ownership was not released after phase-two failure")
	}
	require.Equal(t, int32(1), released.Load())
	require.Equal(t, TableNotFound, session.Unregister(id))
}

func TestNotifierDrainIsIdempotent(t *testing.T) {
	notifier := NewNotifier(context.Background(), 2)
	var released atomic.Int32
	require.NoError(t, notifier.NotifyLogtail(
		timestamp.Timestamp{}, timestamp.Timestamp{PhysicalTime: 1},
		func() { released.Add(1) },
	))
	notifier.Drain()
	notifier.Drain()
	require.Equal(t, int32(1), released.Load())
	require.ErrorIs(t, notifier.NotifyLogtail(
		timestamp.Timestamp{}, timestamp.Timestamp{PhysicalTime: 2},
		func() { released.Add(1) },
	), context.Canceled)
	require.Equal(t, int32(1), released.Load(),
		"an event accepted after the final drain would leak its callback")
}

func TestSubscriptionWaitsForOrderedBootstrapEvent(t *testing.T) {
	phaseOneCalled := make(chan struct{})
	var phaseOneOnce sync.Once
	logtailer := &controlledLogtailer{
		now: timestamp.Timestamp{PhysicalTime: 100},
		tableFn: func(
			_ context.Context, table api.TableID, _, to timestamp.Timestamp,
		) (logtail.TableLogtail, func(), error) {
			phaseOneOnce.Do(func() { close(phaseOneCalled) })
			return mockLogtail(table, to), nil, nil
		},
	}
	server := newUnitLogtailServerWithStart(t, logtailer, false)
	require.NoError(t, server.Start())
	transport := newCaptureSession()
	table := mockTable(1, 1, 1)
	require.NoError(t, server.onSubscription(
		context.Background(), newCaptureStream(transport),
		&logtail.SubscribeRequest{Table: &table},
	))

	<-phaseOneCalled
	sessions := server.ssmgr.ListSession()
	require.Len(t, sessions, 1)
	require.Zero(t, sessions[0].Active(),
		"a wall-clock timestamp must not let a snapshot overtake unpublished events")
	require.True(t, server.waterline.Waterline().IsEmpty())

	require.NoError(t, logtailer.notify(
		timestamp.Timestamp{PhysicalTime: 10},
		timestamp.Timestamp{PhysicalTime: 10},
		nil,
	))
	select {
	case <-transport.writes:
	case <-time.After(10 * time.Second):
		t.Fatal("subscription response was not written after the bootstrap event")
	}
	require.Equal(t, 1, sessions[0].Active())
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 10}, server.waterline.Waterline())
}

func TestBootstrapWaterlineDoesNotSkipAcceptedEvents(t *testing.T) {
	logtailer := &controlledLogtailer{now: timestamp.Timestamp{PhysicalTime: 100}}
	server := newUnitLogtailServerWithStart(t, logtailer, false)
	require.NoError(t, server.Start())
	bootstrapped := make(chan struct{})
	require.NoError(t, logtailer.notify(
		timestamp.Timestamp{PhysicalTime: 10},
		timestamp.Timestamp{PhysicalTime: 10},
		func() { close(bootstrapped) },
	))
	select {
	case <-bootstrapped:
	case <-time.After(10 * time.Second):
		t.Fatal("logtail sender did not consume the bootstrap event")
	}
	require.True(t, server.waterline.Waterline().Equal(timestamp.Timestamp{PhysicalTime: 10}))

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	transport := newCaptureSession()
	ctx, cancel := context.WithCancel(server.rootCtx)
	responses := NewLogtailResponsePool()
	session := &Session{
		sessionCtx:    ctx,
		cancelFunc:    cancel,
		logger:        server.logger,
		sendTimeout:   time.Second,
		responses:     responses,
		notifier:      server,
		stream:        newCaptureStream(transport),
		poisonTime:    time.Second,
		sendChan:      make(chan message, 1),
		active:        1,
		tables:        map[TableID]tableSubscription{id: {state: TableSubscribed, generation: 1}},
		progressTimer: time.NewTimer(time.Hour),
	}
	server.ssmgr.clients[session.stream] = session

	var released atomic.Int32
	require.NoError(t, logtailer.notify(
		timestamp.Timestamp{PhysicalTime: 10},
		timestamp.Timestamp{PhysicalTime: 10},
		func() { released.Add(1) },
		mockLogtail(table, timestamp.Timestamp{PhysicalTime: 10}),
	))
	select {
	case msg := <-session.sendChan:
		update := msg.response.GetUpdateResponse()
		require.NotNil(t, update)
		require.Len(t, update.LogtailList, 1,
			"the first real transaction may share the bootstrap timestamp")
		responses.Release(msg.response)
	case <-time.After(10 * time.Second):
		t.Fatal("accepted event at the bootstrap timestamp was not published")
	}
	require.Equal(t, int32(1), released.Load())
	require.True(t, server.waterline.Waterline().Equal(timestamp.Timestamp{PhysicalTime: 10}))
}

func TestClientSubscribeHonorsCallerCancellationWhenRequestQueueIsFull(t *testing.T) {
	clientCtx, stopClient := context.WithCancel(context.Background())
	defer stopClient()
	client := &LogtailClient{
		ctx:      clientCtx,
		requestC: make(chan *LogtailRequest, 1),
		limiter:  ratelimit.New(10_000),
		broken:   make(chan struct{}),
	}
	client.requestC <- &LogtailRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- client.Subscribe(ctx, mockTable(1, 1, 1))
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		stopClient() // release the deliberately full request queue before failing.
		<-done
		t.Fatal("Subscribe ignored the caller cancellation while the request queue was full")
	}
}

// TestPullWorkerPoolBoundsAndCancelExits is a regression test for the pull
// worker pool: it verifies that (1) the number of concurrently active phase-1
// goroutines never exceeds PullWorkerPoolSize even when far more subscriptions
// are submitted, and (2) closing the server causes all blocked
// workers to exit promptly.
func TestPullWorkerPoolBoundsAndCancelExits(t *testing.T) {
	const poolSize = 3
	const numSubs = 20

	var active atomic.Int32
	entered := make(chan struct{}, numSubs)
	exited := make(chan struct{}, numSubs)

	logtailer := &controlledLogtailer{
		tableFn: func(ctx context.Context, table api.TableID, from, to timestamp.Timestamp) (logtail.TableLogtail, func(), error) {
			active.Add(1)
			entered <- struct{}{}
			defer func() {
				active.Add(-1)
				exited <- struct{}{}
			}()
			<-ctx.Done()
			return logtail.TableLogtail{}, nil, ctx.Err()
		},
	}

	cfg := options.NewDefaultLogtailServerCfg()
	cfg.RpcMaxMessageSize = 1024
	cfg.PullWorkerPoolSize = int64(poolSize)
	cfg.RPCStreamPoisonTime = 20 * time.Millisecond
	cfg.ResponseSendTimeout = time.Second

	server, err := NewLogtailServer(
		"", cfg, logtailer, mockRuntime(),
		func(string, string, *LogtailServer, ...morpc.ServerOption) (morpc.RPCServer, error) {
			return &testRPCServer{}, nil
		},
	)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	closed := false
	closeServer := func() {
		if closed {
			return
		}
		closed = true
		require.NoError(t, server.Close())
	}
	t.Cleanup(closeServer)

	// Create a session to own the subscriptions.
	transport := newCaptureSession()
	stream := newCaptureStream(transport)
	session := server.ssmgr.GetSession(
		server.rootCtx, server.logger, server.pool.responses, server,
		stream, time.Second, time.Second, time.Hour, time.Hour,
	)
	t.Cleanup(session.PostClean)

	// Submit far more subscriptions than the pool can hold.
	for i := 0; i < numSubs; i++ {
		table := mockTable(1, 1, uint64(i+1))
		tableID := MarshalTableID(&table)
		_, gen := session.RegisterWithGeneration(tableID, table)
		server.subReqChan <- subscription{
			timeout:    time.Minute,
			tableID:    tableID,
			generation: gen,
			req:        &logtail.SubscribeRequest{Table: &table},
			session:    session,
		}
	}

	// Phase 1: observe each admitted worker directly. Once all tokens are held,
	// a fourth worker cannot reach TableLogtail without violating the bound.
	for i := 0; i < poolSize; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("pull worker pool did not reach its configured capacity")
		}
	}
	require.Equal(t, int32(poolSize), active.Load())
	require.Equal(t, poolSize, len(server.pullWorkerPool),
		"all pool tokens should be acquired when saturated")
	require.Empty(t, entered, "a worker entered without acquiring a pool token")

	// Phase 2: Close cancels both the root context and the stopper task
	// contexts used by phase-1 workers.
	closeServer()
	require.Zero(t, active.Load())
	require.Len(t, exited, poolSize,
		"Close must join every admitted phase-one worker")
	require.Empty(t, entered,
		"cancellation admitted a queued subscription into a new worker")
}
