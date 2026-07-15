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
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	server := newUnitLogtailServer(t, &controlledLogtailer{})
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
