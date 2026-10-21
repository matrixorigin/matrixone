// Copyright 2026 Matrix Origin
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

package process

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TestPipelineEdgePublishEndIsIdempotent verifies that calling PublishEnd
// multiple times is safe and that Done() is closed exactly once.
func TestPipelineEdgePublishEndIsIdempotent(t *testing.T) {
	edge := NewPipelineEdge(1, 1)

	// First PublishEnd should succeed and close Done.
	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("first PublishEnd failed")
	}

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after first PublishEnd")
	}

	// Second PublishEnd should be idempotent (no double-close).
	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("second PublishEnd should be a no-op but returned true")
	}
}

// TestPipelineEdgePublishErrorPropagatesErr verifies that PublishError
// stores the error and Err() returns it.
func TestPipelineEdgePublishErrorPropagatesErr(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	testErr := moerr.NewInternalErrorNoCtx("test error")

	if !publishTerminalForTest(edge, NewErrorSignal(testErr)) {
		t.Fatal("PublishError failed")
	}

	if edge.Err() == nil {
		t.Fatal("Err() returned nil after PublishError")
	}

	// Done should be closed after PublishError.
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after PublishError")
	}
}

// TestPipelineEdgeSendDataCancelsWithContext verifies that SendData
// returns false when the context is cancelled.
func TestPipelineEdgeSendDataCancelsWithContext(t *testing.T) {
	edge := NewPipelineEdge(1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if edge.SendData(ctx, nil, 0) {
		t.Fatal("SendData succeeded with cancelled context")
	}
}

func TestPipelineEdgeSendDataNilContextDoesNotPanic(t *testing.T) {
	edge := NewPipelineEdge(1, 1)

	if !edge.SendDataDirect(nil, nil, nil) {
		t.Fatal("SendDataDirect with nil context should use a background cleanup context")
	}
}

func TestPipelineEdgeSharedEndRequiresAllSenders(t *testing.T) {
	edge := NewPipelineEdge(3, 2)

	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("first PublishEnd failed")
	}
	select {
	case <-edge.Done():
		t.Fatal("Done closed after only one of two expected End signals")
	default:
	}

	if !edge.SendDataDirect(context.Background(), nil, nil) {
		t.Fatal("data send was rejected before all shared senders ended")
	}

	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("second PublishEnd failed")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after all expected End signals")
	}
}

// TestPipelineEdgeSendDataFailedEdge verifies that SendData
// returns false when the edge has been aborted.
func TestPipelineEdgeSendDataFailedEdge(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	publishTerminalForTest(edge, NewErrorSignal(moerr.NewInternalErrorNoCtx("abort")))

	if edge.SendData(context.Background(), nil, 0) {
		t.Fatal("SendData succeeded on aborted edge")
	}
}

func TestPipelineEdgeSendDataRejectedAfterNormalEnd(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("PublishEnd failed")
	}

	if edge.SendDataDirect(context.Background(), nil, nil) {
		t.Fatal("SendDataDirect succeeded after normal End closed the edge")
	}
}

func TestPipelineEdgeBlockedDataSendUnblocksOnError(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	edge.Ch2 <- NewPipelineSignalToDirectly(nil, nil, nil)

	done := make(chan bool, 1)
	go func() {
		done <- edge.SendDataDirect(context.Background(), nil, nil)
	}()

	select {
	case ok := <-done:
		t.Fatalf("blocked data send returned before terminal state: %v", ok)
	case <-time.After(10 * time.Millisecond):
	}

	publishTerminalForTest(edge, NewErrorSignal(moerr.NewInternalErrorNoCtx("abort")))

	select {
	case ok := <-done:
		if ok {
			t.Fatal("blocked data send succeeded after Abort")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked data send did not unblock after Abort")
	}
}

// TestNewPipelineEventTypeIsTerminal verifies the IsTerminal method.
func TestNewPipelineEventTypeIsTerminal(t *testing.T) {
	if EventData.IsTerminal() {
		t.Fatal("EventData should not be terminal")
	}
	if !EventEnd.IsTerminal() {
		t.Fatal("EventEnd should be terminal")
	}
	if !EventError.IsTerminal() {
		t.Fatal("EventError should be terminal")
	}
}

// TestNewSignalConstructors verifies that typed signal constructors
// set the correct EventType.
func TestNewSignalConstructors(t *testing.T) {
	end := NewEndSignal()
	if end.EventType != EventEnd {
		t.Fatalf("NewEndSignal EventType: got %s, want EventEnd", end.EventType)
	}
	if !end.EventType.IsTerminal() {
		t.Fatal("NewEndSignal should be terminal")
	}

	testErr := moerr.NewInternalErrorNoCtx("test")
	errSignal := NewErrorSignal(testErr)
	if errSignal.EventType != EventError {
		t.Fatalf("NewErrorSignal EventType: got %s, want EventError", errSignal.EventType)
	}
	if errSignal.TerminalErr() != testErr {
		t.Fatal("NewErrorSignal TerminalErr mismatch")
	}

}

func TestFailureTerminalWithoutCauseUsesSentinel(t *testing.T) {
	errSignal := NewErrorSignal(nil)
	if errSignal.EventType != EventError {
		t.Fatalf("NewErrorSignal(nil) EventType: got %s, want EventError", errSignal.EventType)
	}
	if !moerr.IsMoErrCode(errSignal.TerminalErr(), moerr.ErrInternal) {
		t.Fatal("NewErrorSignal(nil) did not attach an internal error cause")
	}
	if errSignal.TerminalErr() != ErrPipelineTerminalWithoutCause {
		t.Fatal("NewErrorSignal(nil) did not use ErrPipelineTerminalWithoutCause")
	}

}

func TestBuildCleanupSignalFailedNilErrorUsesSentinel(t *testing.T) {
	signal := BuildCleanupSignal(true, nil)
	if signal.EventType != EventError {
		t.Fatalf("BuildCleanupSignal(true, nil) EventType: got %s, want EventError", signal.EventType)
	}
	if signal.TerminalErr() != ErrPipelineTerminalWithoutCause {
		t.Fatal("BuildCleanupSignal(true, nil) did not attach ErrPipelineTerminalWithoutCause")
	}
}

// TestPipelineEventTypeString verifies String representation.
func TestPipelineEventTypeString(t *testing.T) {
	if EventData.String() != "Data" {
		t.Fatalf("EventData.String() = %s, want Data", EventData.String())
	}
	if EventEnd.String() != "End" {
		t.Fatalf("EventEnd.String() = %s, want End", EventEnd.String())
	}
	if EventError.String() != "Error" {
		t.Fatalf("EventError.String() = %s, want Error", EventError.String())
	}
}

// TestPipelineEdgeConcurrentEnd verifies that many goroutines calling
// PublishEnd concurrently produce exactly one close of Done and no panics.
func TestPipelineEdgeConcurrentEnd(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	const goroutines = 50

	var wg sync.WaitGroup
	results := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- publishTerminalForTest(edge, NewEndSignal())
		}()
	}
	wg.Wait()
	close(results)

	delivered := 0
	noop := 0
	for r := range results {
		if r {
			delivered++
		} else {
			noop++
		}
	}

	// Exactly one goroutine should report delivery.
	if delivered != goroutines || len(edge.Ch2) != 1 {
		t.Fatalf("expected durable publications and exactly one queued End; got %d publications, %d failures", delivered, noop)
	}

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after concurrent PublishEnd")
	}
}

// TestPipelineEdgeConcurrentError verifies concurrent Abort safety.
func TestPipelineEdgeConcurrentError(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	const goroutines = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publishTerminalForTest(edge, NewErrorSignal(moerr.NewInternalErrorNoCtx("abort")))
		}()
	}
	wg.Wait()

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after concurrent Abort")
	}
}

// TestPipelineEdgeMixedConcurrentTerminal verifies that PublishEnd and Abort
// called concurrently from different goroutines don't cause races.
func TestPipelineEdgeMixedConcurrentTerminal(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	const goroutines = 50

	var wg sync.WaitGroup

	// Half call PublishEnd, half call Abort.
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publishTerminalForTest(edge, NewEndSignal())
		}()
	}
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publishTerminalForTest(edge, NewErrorSignal(moerr.NewInternalErrorNoCtx("abort")))
		}()
	}
	wg.Wait()

	// Done must be closed.
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after mixed concurrent terminal calls")
	}

}

// TestPipelineEdgeFatalPublicationNeverWaitsForCapacity verifies that the
// production terminal helper closes Done for fatal terminal signals even if the
// signal cannot be delivered because the channel is full.
func TestPipelineEdgeFatalPublicationNeverWaitsForCapacity(t *testing.T) {
	reg := NewPipelineEdge(1, 0)
	reg.Ch2 <- NewEndSignal() // fill it

	start := time.Now()
	if !publishTerminalForTest(reg, NewErrorSignal(moerr.NewInternalErrorNoCtx("fatal"))) {
		t.Fatal("terminal publication failed on a full channel")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("fatal send waited behind the full data channel: %s", elapsed)
	}

	select {
	case <-reg.Done():
	default:
		t.Fatal("PipelineEdge Done was not closed")
	}
}

func TestPipelineEdgeTerminalSignalSnapshotDefaultsToEnd(t *testing.T) {
	var nilEdge *PipelineEdge
	if signal := nilEdge.terminalSignalSnapshot(); signal.EventType != EventEnd {
		t.Fatalf("nil edge snapshot returned %s, want End", signal.EventType)
	}

	edge := NewPipelineEdge(1, 1)
	if signal := edge.terminalSignalSnapshot(); signal.EventType != EventEnd {
		t.Fatalf("non-terminal edge snapshot returned %s, want End", signal.EventType)
	}
}

func TestPipelineEdgeResetForReuseClearsTerminalState(t *testing.T) {
	edge := NewPipelineEdge(1, 0)
	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("initial PublishEnd failed")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed before reset")
	}

	edge.ResetForReuse(3, 3)
	if cap(edge.Ch2) != 3 {
		t.Fatalf("channel cap after reset = %d, want 3", cap(edge.Ch2))
	}
	if edge.NilBatchCnt != 3 {
		t.Fatalf("NilBatchCnt after reset = %d, want 3", edge.NilBatchCnt)
	}
	select {
	case <-edge.Done():
		t.Fatal("Done stayed closed after reset")
	default:
	}
	if edge.Err() != nil {
		t.Fatal("Err was not cleared after reset")
	}

	testErr := moerr.NewInternalErrorNoCtx("after reset")
	if !publishTerminalForTest(edge, NewErrorSignal(testErr)) {
		t.Fatal("PublishError after reset failed")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after reset terminal")
	}
	if edge.Err() != testErr {
		t.Fatal("Err after reset terminal mismatch")
	}
}

func TestPipelineEdgeSetNilBatchCntForReusePreservesChannel(t *testing.T) {
	edge := NewPipelineEdge(2, 1)
	originalCh := edge.Ch2
	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("initial PublishEnd failed")
	}

	edge.SetNilBatchCntForReuse(4)
	if edge.Ch2 != originalCh {
		t.Fatal("SetNilBatchCntForReuse should not recreate Ch2")
	}
	if edge.NilBatchCnt != 4 {
		t.Fatalf("NilBatchCnt after update = %d, want 4", edge.NilBatchCnt)
	}
	select {
	case <-edge.Ch2:
		t.Fatal("SetNilBatchCntForReuse did not drain stale buffered signal")
	default:
	}
	select {
	case <-edge.Done():
		t.Fatal("Done stayed closed after SetNilBatchCntForReuse")
	default:
	}

	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("PublishEnd after SetNilBatchCntForReuse failed")
	}
	select {
	case signal := <-edge.Ch2:
		if signal.EventType != EventEnd {
			t.Fatalf("signal after reuse = %s, want End", signal.EventType)
		}
	default:
		t.Fatal("PublishEnd after reuse did not enqueue terminal signal")
	}
}

// A prepared statement reuses its cached pipeline across executions: after one
// execution delivered End and closed the edge, the next execution must be able
// to send data and End again once the terminal state is cleared.
func TestPipelineEdgeResetTerminalStateForReuse(t *testing.T) {
	edge := NewPipelineEdge(2, 1)
	originalCh := edge.Ch2
	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("initial PublishEnd failed")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed before reset")
	}
	if edge.SendDataDirect(context.Background(), &batch.Batch{}, nil) {
		t.Fatal("done edge should reject data before reset")
	}

	edge.ResetTerminalStateForReuse()

	if edge.Ch2 != originalCh {
		t.Fatal("ResetTerminalStateForReuse should not recreate Ch2")
	}
	if edge.NilBatchCnt != 1 {
		t.Fatalf("NilBatchCnt after reset = %d, want 1", edge.NilBatchCnt)
	}
	select {
	case <-edge.Ch2:
		t.Fatal("ResetTerminalStateForReuse did not drain stale buffered signal")
	default:
	}
	select {
	case <-edge.Done():
		t.Fatal("Done stayed closed after reset")
	default:
	}

	if !edge.SendDataDirect(context.Background(), &batch.Batch{}, nil) {
		t.Fatal("SendDataDirect after reset failed")
	}
	if !publishTerminalForTest(edge, NewEndSignal()) {
		t.Fatal("PublishEnd after reset failed")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after reset End")
	}

	var nilEdge *PipelineEdge
	nilEdge.ResetTerminalStateForReuse()
}

// Draining a stale PipelineSignal must release its held resources (direct
// batch mpool memory, spool references) or the reuse path would leak.
func TestPipelineEdgeDrainReleasesSignalResources(t *testing.T) {
	for _, replaceChannel := range []bool{false, true} {
		mp := mpool.MustNewZeroNoFixed()
		t.Cleanup(func() { mpool.DeleteMPool(mp) })
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		bat.SetRowCount(1)
		edge := NewPipelineEdge(1, 1)
		require.True(t, edge.SendDataDirect(context.Background(), bat, mp))
		require.Positive(t, mp.CurrNB())
		if replaceChannel {
			edge.ResetForReuse(1, 1)
		} else {
			edge.ResetTerminalStateForReuse()
		}
		require.Zero(t, mp.CurrNB())
		require.True(t, edge.SendDataDirect(context.Background(), batch.EmptyBatch, mp))
		edge.DrainForCleanup()
	}
}

func TestPipelineEdgePublishTerminal(t *testing.T) {
	first := moerr.NewInternalErrorNoCtx("first failure")
	second := moerr.NewInternalErrorNoCtx("later failure")
	for _, fatal := range []PipelineSignal{NewErrorSignal(first)} {
		t.Run(fatal.EventType.String(), func(t *testing.T) {
			for _, full := range []bool{false, true} {
				edge := NewPipelineEdge(1, 3)
				end, ok := edge.PublishTerminal(NewEndSignal())
				require.True(t, ok)
				require.Equal(t, EventEnd, end.EventType)
				if !full {
					<-edge.Ch2
				}
				select {
				case <-edge.Done():
					t.Fatal("one End must not complete a three-sender edge")
				default:
				}
				for _, requested := range []PipelineSignal{fatal, NewEndSignal(), NewErrorSignal(second)} {
					effective, ok := edge.PublishTerminal(requested)
					require.True(t, ok)
					require.Equal(t, fatal.EventType, effective.EventType)
					require.Same(t, first, effective.TerminalErr())
				}
				require.Equal(t, 1, edge.endRecorded)
				require.Equal(t, 2, edge.fatalRemaining)
				<-edge.Done()
				receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{edge})
				got, err := receiver.GetNextBatch(nil)
				require.Nil(t, got)
				require.ErrorIs(t, err, first)
			}
		})
	}
	t.Run("graceful and reuse", func(t *testing.T) {
		edge := NewPipelineEdge(1, 1)
		for _, requested := range []PipelineSignal{NewEndSignal(), NewEndSignal(), NewErrorSignal(first)} {
			effective, ok := edge.PublishTerminal(requested)
			require.True(t, ok)
			require.Equal(t, EventEnd, effective.EventType)
			require.NoError(t, effective.TerminalErr())
		}
		require.Equal(t, 1, edge.endRecorded)
		edge.ResetForReuse(1, 1)
		effective, ok := edge.PublishTerminal(NewErrorSignal(second))
		require.True(t, ok)
		require.Same(t, second, effective.TerminalErr())
	})
	t.Run("unavailable", func(t *testing.T) {
		for _, edge := range []*PipelineEdge{nil, {}} {
			_, ok := edge.PublishTerminal(NewEndSignal())
			require.False(t, ok)
		}
		edge := NewPipelineEdge(1, 1)
		_, ok := edge.PublishTerminal(NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil))
		require.False(t, ok)
		require.Zero(t, edge.endRecorded)
	})
}
