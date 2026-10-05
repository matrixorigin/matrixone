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

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFinishStreamCloseWithQueuedAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var releases, futureReleases atomic.Int32
	cs := newClientSession(newServerMetrics("fin-close"), newTestIOSession(nil, nil), newTestCodec(), func() *Future { return newFuture(func(*Future) { futureReleases.Add(1) }) }, func(Message) { releases.Add(1) })
	t.Cleanup(func() { cs.cleanSend(); require.NoError(t, cs.Close()) })
	require.True(t, cs.validateStreamRequest(11, 1))
	finished := make(chan error, 1)
	go func() {
		finished <- cs.FinishStream(ctx, StreamTerminalToken{owner: cs, streamID: 11, sequence: 1}, newTestMessage(11))
	}()
	// Admission is the barrier, not the timeout. Return the Future to the queue
	// so Close (rather than a live writer) must complete it.
	var queued *Future
	select {
	case queued = <-cs.c:
		cs.changeQueueDepth(-1)
	case <-ctx.Done():
		t.Fatal("FIN was not admitted")
	}
	enqueueClientSessionFutureForTest(cs, queued)
	closed := make(chan error, 1)
	go func() { closed <- cs.Close() }()
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-ctx.Done():
		// Release the probe's blocked Future so a baseline failure does not leak.
		cs.cleanSend()
		<-finished
		<-closed
		t.Fatal("FIN and session Close deadlocked after writer exit")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("session Close did not complete")
	}
	require.Equal(t, int32(1), releases.Load())
	require.Equal(t, int32(1), futureReleases.Load())
	require.Equal(t, float64(0), testutil.ToFloat64(cs.metrics.receivedStreamStateGauge))
	require.Equal(t, float64(0), testutil.ToFloat64(cs.metrics.sentStreamStateGauge))
	require.Equal(t, float64(0), testutil.ToFloat64(cs.metrics.sendingQueueSizeGauge))
	require.False(t, cs.validateStreamRequest(11, 1), "closed session cannot resurrect a retired stream")
}

func TestFinishStreamWriterExitDrainsAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocked, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	var releases atomic.Int32
	cs := newClientSession(newServerMetrics("fin-writer-exit"), newTestIOSession(backendClosed, nil), newTestCodec(), func() *Future { return newFuture(nil) }, func(Message) { releases.Add(1) })
	require.True(t, cs.validateStreamRequest(11, 1))
	s := &server{metrics: cs.metrics, logger: logutil.GetPanicLoggerWithLevel(zap.FatalLevel), stopper: stopper.NewStopper("fin-writer-exit"), sessions: &sync.Map{}}
	s.options.batchSendSize = 1
	s.options.filter = func(Message) bool { close(blocked); <-resume; return true }
	require.NoError(t, s.startWriteLoop(cs))
	t.Cleanup(func() { unblock(); cs.cleanSend(); s.stopper.Stop() })
	require.NoError(t, cs.AsyncWrite(newTestMessage(10)))
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("writer did not reach filter")
	}
	finished := make(chan error, 1)
	go func() {
		finished <- cs.FinishStream(ctx, StreamTerminalToken{owner: cs, streamID: 11, sequence: 1}, newTestMessage(11))
	}()
	select {
	case queued := <-cs.c:
		cs.changeQueueDepth(-1)
		enqueueClientSessionFutureForTest(cs, queued)
	case <-ctx.Done():
		t.Fatal("FIN was not queued behind blocked writer")
	}
	unblock()
	stopped := make(chan struct{})
	go func() { s.stopper.Stop(); close(stopped) }()
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-ctx.Done():
		cs.cleanSend()
		<-finished
		<-stopped
		t.Fatal("writer exit and FIN deadlocked")
	}
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("writer Stopper did not terminate")
	}
	// The first fake Write fails before encoding with a non-illegal-state
	// error; its goetty owner is separate. MORPC releases the queued FIN once.
	require.Equal(t, int32(1), releases.Load())
	require.Equal(t, float64(0), testutil.ToFloat64(cs.metrics.receivedStreamStateGauge))
}

func TestFinishStreamClaimExcludesLaterRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var releases atomic.Int32
	cs := newClientSession(newServerMetrics("fin-claim"), newTestIOSession(nil, nil), newTestCodec(), func() *Future { return newFuture(nil) }, func(Message) { releases.Add(1) })
	t.Cleanup(func() { cs.cleanSend(); require.NoError(t, cs.Close()) })
	require.True(t, cs.validateStreamRequest(11, 1))
	finished := make(chan error, 1)
	go func() {
		finished <- cs.FinishStream(ctx, StreamTerminalToken{owner: cs, streamID: 11, sequence: 1}, newTestMessage(11))
	}()
	select {
	case queued := <-cs.c:
		cs.changeQueueDepth(-1)
		enqueueClientSessionFutureForTest(cs, queued)
	case <-ctx.Done():
		t.Fatal("FIN was not admitted")
	}
	require.False(t, cs.validateStreamRequest(11, 2))
	require.True(t, cs.validateStreamRequest(12, 1), "unrelated stream must progress")
	require.Error(t, cs.FinishStream(ctx, StreamTerminalToken{owner: cs, streamID: 11, sequence: 1}, newTestMessage(11)))
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("duplicate FIN did not poison and drain session")
	}
	require.Equal(t, int32(2), releases.Load())
}

func TestFinishStreamRejectsInvalidAuthority(t *testing.T) {
	for _, name := range []string{"absent", "stale", "wrong-owner", "wrong-response", "nil-response", "pending-cache"} {
		t.Run(name, func(t *testing.T) {
			var releases atomic.Int32
			cs := newClientSession(newServerMetrics("fin-authority"), newTestIOSession(nil, nil), newTestCodec(), func() *Future { return newFuture(nil) }, func(Message) { releases.Add(1) })
			t.Cleanup(func() { require.NoError(t, cs.Close()) })
			token := StreamTerminalToken{owner: cs, streamID: 11, sequence: 1}
			var response Message = newTestMessage(11)
			if name != "absent" {
				require.True(t, cs.validateStreamRequest(11, 1))
			}
			switch name {
			case "stale":
				token.sequence = 2
			case "wrong-owner":
				token.owner = &clientSession{}
			case "wrong-response":
				response.SetID(12)
			case "nil-response":
				response = nil
			case "pending-cache":
				_, err := cs.CreateCache(context.Background(), 11)
				require.NoError(t, err)
			}
			require.Error(t, cs.FinishStream(context.Background(), token, response))
			want := int32(1)
			if response == nil {
				want = 0
			}
			require.Equal(t, want, releases.Load())
			require.Empty(t, cs.receivedStreamSequences)
			require.Equal(t, float64(0), testutil.ToFloat64(cs.metrics.receivedStreamStateGauge))
		})
	}
}

func TestFinishStreamCanceledAdmissionDrainsFullQueue(t *testing.T) {
	var releases, futureReleases atomic.Int32
	cs := newClientSession(newServerMetrics("fin-canceled-admission"), newTestIOSession(nil, nil), newTestCodec(), func() *Future {
		return newFuture(func(*Future) { futureReleases.Add(1) })
	}, func(Message) { releases.Add(1) })
	t.Cleanup(func() { cs.cleanSend(); require.NoError(t, cs.Close()) })
	cs.c = make(chan *Future, 1)
	require.True(t, cs.validateStreamRequest(11, 1))
	require.NoError(t, cs.AsyncWrite(newTestMessage(10)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cancel()
	err := cs.FinishStream(ctx, StreamTerminalToken{owner: cs, streamID: 11, sequence: 1}, newTestMessage(11))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(2), releases.Load(), "rejected FIN and queued response each have one release owner")
	require.Equal(t, int32(2), futureReleases.Load())
	require.Empty(t, cs.receivedStreamSequences)
	require.Equal(t, float64(0), testutil.ToFloat64(cs.metrics.sendingQueueSizeGauge))
}
