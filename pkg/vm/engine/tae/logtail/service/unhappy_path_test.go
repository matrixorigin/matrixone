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
		time.Second, time.Second, time.Hour,
	)
	t.Cleanup(session.PostClean)

	transport.cancel()
	require.Eventually(t, func() bool {
		return session.sessionCtx.Err() != nil
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, <-notifier.notified, context.Canceled)
}

func TestCancelledSubscriptionPhaseCannotPublishStaleResponse(t *testing.T) {
	server := newUnitLogtailServer(t, &controlledLogtailer{})
	transport := newCaptureSession()
	session := server.ssmgr.GetSession(
		server.rootCtx, server.logger, server.pool.responses, server,
		newCaptureStream(transport), time.Second, time.Second, time.Hour,
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
		time.Second, time.Second, time.Hour,
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

func TestConcurrentSubscriptionGenerationLifecycle(t *testing.T) {
	transport := newCaptureSession()
	session := NewSession(
		context.Background(), mockMOLogger(), NewLogtailResponsePool(),
		&recordingSessionNotifier{notified: make(chan error, 1)}, newCaptureStream(transport),
		time.Second, time.Second, time.Hour,
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
		time.Second, time.Second, time.Hour,
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

func TestPublishEventDoesNotWaitForSlowSessionQueue(t *testing.T) {
	server := newUnitLogtailServer(t, &controlledLogtailer{})
	transport := newCaptureSession()
	ctx, cancel := context.WithCancel(server.rootCtx)
	t.Cleanup(cancel)
	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	responses := NewLogtailResponsePool()
	slow := &Session{
		sessionCtx:        ctx,
		cancelFunc:        cancel,
		logger:            server.logger,
		sendTimeout:       time.Second,
		responses:         responses,
		notifier:          server,
		stream:            newCaptureStream(transport),
		poisonTime:        time.Second,
		sendChan:          make(chan message, 1),
		active:            1,
		tables:            map[TableID]tableSubscription{id: {state: TableSubscribed, generation: 1}},
		heartbeatInterval: time.Hour,
		heartbeatTimer:    time.NewTimer(time.Hour),
	}
	t.Cleanup(func() { slow.heartbeatTimer.Stop() })
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
	case <-time.After(500 * time.Millisecond):
		// The old implementation waits for the per-session poison timeout.
		// Release the goroutine before failing so the test leaves no residue.
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
			mockMorpcStream(transport, streamID, 1024), time.Second, time.Second, time.Hour,
		)
		_, generation := session.RegisterWithGeneration(id, table)
		completed, err := session.CompleteSubscription(
			context.Background(), id, generation, mockLogtail(table, timestamp.Timestamp{}), nil,
		)
		require.NoError(t, err)
		require.True(t, completed)
	}

	var released atomic.Int32
	server.publishEvent(server.rootCtx, event{
		to:       timestamp.Timestamp{PhysicalTime: 1},
		closeCB:  func() { released.Add(1) },
		logtails: []logtail.TableLogtail{mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1})},
	})
	require.Eventually(t, func() bool { return released.Load() == 1 }, time.Second, time.Millisecond)
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
		newCaptureStream(transport), time.Second, time.Second, time.Hour,
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
		newCaptureStream(transport), time.Second, time.Second, time.Hour,
	)
	t.Cleanup(session.PostClean)

	table := mockTable(1, 1, 1)
	id := MarshalTableID(&table)
	_, generation := session.RegisterWithGeneration(id, table)
	var released atomic.Int32
	server.subTailChan <- &LogtailPhase{
		tail:    mockLogtail(table, timestamp.Timestamp{PhysicalTime: 1}),
		closeCB: func() { released.Add(1) },
		sub: subscription{
			timeout:    time.Second,
			tableID:    id,
			generation: generation,
			req:        &logtail.SubscribeRequest{Table: &table},
			session:    session,
		},
	}

	require.Eventually(t, func() bool { return released.Load() == 1 }, time.Second, time.Millisecond)
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
}

func TestSubscriptionCompletesBeforeFirstIncrementalEvent(t *testing.T) {
	logtailer := &controlledLogtailer{now: timestamp.Timestamp{PhysicalTime: 1}}
	server := newUnitLogtailServer(t, logtailer)
	transport := newCaptureSession()
	table := mockTable(1, 1, 1)
	require.NoError(t, server.onSubscription(
		context.Background(), newCaptureStream(transport),
		&logtail.SubscribeRequest{Table: &table},
	))

	var session *Session
	require.Eventually(t, func() bool {
		sessions := server.ssmgr.ListSession()
		if len(sessions) != 1 {
			return false
		}
		session = sessions[0]
		return session.Active() == 1
	}, time.Second, time.Millisecond)
}

func TestBootstrapWaterlineDropsStaleEventAndReleasesBatch(t *testing.T) {
	server := newUnitLogtailServer(t, &controlledLogtailer{
		now: timestamp.Timestamp{PhysicalTime: 10},
	})
	var released atomic.Int32
	server.publishEvent(server.rootCtx, event{
		to:      timestamp.Timestamp{PhysicalTime: 9},
		closeCB: func() { released.Add(1) },
	})
	require.Equal(t, int32(1), released.Load())
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 10}, server.waterline.Waterline())
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
	case <-time.After(time.Second):
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

	gate := make(chan struct{}) // blocks phase-1 until closed
	var active atomic.Int32

	logtailer := &controlledLogtailer{
		tableFn: func(ctx context.Context, table api.TableID, from, to timestamp.Timestamp) (logtail.TableLogtail, func(), error) {
			active.Add(1)
			defer active.Add(-1)
			select {
			case <-gate:
			case <-ctx.Done():
				return logtail.TableLogtail{}, nil, ctx.Err()
			}
			return mockLogtail(table, to), nil, nil
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
		stream, time.Second, time.Second, time.Hour,
	)
	t.Cleanup(session.PostClean)

	// Submit far more subscriptions than the pool can hold.
	for i := 0; i < numSubs; i++ {
		table := mockTable(1, 1, uint64(i+1))
		tableID := MarshalTableID(&table)
		_, gen := session.RegisterWithGeneration(tableID, table)
		server.subReqChan <- subscription{
			timeout:    5 * time.Second,
			tableID:    tableID,
			generation: gen,
			req:        &logtail.SubscribeRequest{Table: &table},
			session:    session,
		}
	}

	// Phase 1: pool saturates at exactly poolSize.
	require.Eventually(t, func() bool {
		return active.Load() == int32(poolSize)
	}, 5*time.Second, 10*time.Millisecond,
		"pool should saturate at %d workers", poolSize)

	// Confirm no spike above poolSize across multiple samples.
	for i := 0; i < 20; i++ {
		require.LessOrEqual(t, active.Load(), int32(poolSize),
			"active workers exceeded pool size at sample %d", i)
		time.Sleep(5 * time.Millisecond)
	}

	// All pool tokens should be held.
	require.Equal(t, poolSize, len(server.pullWorkerPool),
		"all pool tokens should be acquired when saturated")

	// Phase 2: Close cancels both the root context and the stopper task
	// contexts used by phase-1 workers.
	closeServer()

	require.Eventually(t, func() bool {
		return active.Load() == 0
	}, 5*time.Second, 10*time.Millisecond,
		"all phase-1 workers should exit after cancel")
}
