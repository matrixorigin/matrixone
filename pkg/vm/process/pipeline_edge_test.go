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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// TestPipelineEdgeSendEndIsIdempotent verifies that calling SendEnd
// multiple times is safe and that Done() is closed exactly once.
func TestPipelineEdgeSendEndIsIdempotent(t *testing.T) {
	edge := NewPipelineEdge(1, 1)

	// First SendEnd should succeed and close Done.
	if !edge.SendEnd() {
		t.Fatal("first SendEnd failed")
	}

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after first SendEnd")
	}

	// Second SendEnd should be idempotent (no double-close).
	if edge.SendEnd() {
		t.Fatal("second SendEnd should be a no-op but returned true")
	}
}

// TestPipelineEdgeSendErrorPropagatesErr verifies that SendError
// stores the error and Err() returns it.
func TestPipelineEdgeSendErrorPropagatesErr(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	testErr := moerr.NewInternalErrorNoCtx("test error")

	if !edge.SendError(testErr) {
		t.Fatal("SendError failed")
	}

	if edge.Err() == nil {
		t.Fatal("Err() returned nil after SendError")
	}

	// Done should be closed after SendError.
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after SendError")
	}
}

// TestPipelineEdgeAbortClosesAborted verifies that Abort closes
// both Done and Aborted channels.
func TestPipelineEdgeAbortClosesAborted(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	testErr := moerr.NewInternalErrorNoCtx("abort")

	if !edge.Abort(testErr) {
		t.Fatal("Abort failed")
	}

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after Abort")
	}

	select {
	case <-edge.Aborted():
	default:
		t.Fatal("Aborted was not closed after Abort")
	}

	if edge.Err() == nil {
		t.Fatal("Err() returned nil after Abort")
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

// TestPipelineEdgeTrySendEndOnFullChannel verifies that TrySendEnd
// succeeds when the channel has buffer space.
func TestPipelineEdgeTrySendEndOnFullChannel(t *testing.T) {
	edge := NewPipelineEdge(1, 1)

	if !edge.TrySendEnd() {
		t.Fatal("TrySendEnd failed on empty channel")
	}

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after TrySendEnd")
	}
}

func TestPipelineEdgeSharedEndRequiresAllSenders(t *testing.T) {
	edge := NewPipelineEdge(3, 2)

	if !edge.SendEnd() {
		t.Fatal("first SendEnd failed")
	}
	select {
	case <-edge.Done():
		t.Fatal("Done closed after only one of two expected End signals")
	default:
	}

	if !edge.SendDataDirect(context.Background(), nil, nil) {
		t.Fatal("data send was rejected before all shared senders ended")
	}

	if !edge.SendEnd() {
		t.Fatal("second SendEnd failed")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after all expected End signals")
	}
}

func TestPipelineEdgeSharedEndDeliversOneTerminalPerSender(t *testing.T) {
	edge := NewPipelineEdge(2, 2)

	if !edge.SendEnd() {
		t.Fatal("first SendEnd failed")
	}
	if !edge.SendEnd() {
		t.Fatal("second SendEnd failed")
	}
	if edge.SendEnd() {
		t.Fatal("third SendEnd should be rejected after expected terminal count")
	}

	for i := 0; i < 2; i++ {
		select {
		case signal := <-edge.Ch2:
			if signal.EventType != EventEnd {
				t.Fatalf("signal %d = %s, want End", i, signal.EventType)
			}
		default:
			t.Fatalf("missing End signal %d", i)
		}
	}
}

func TestPipelineEdgeSharedFatalConsumesRemainingSenderCount(t *testing.T) {
	edge := NewPipelineEdge(2, 2)
	testErr := moerr.NewInternalErrorNoCtx("shared fatal")

	if !edge.SendError(testErr) {
		t.Fatal("SendError failed")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after shared fatal")
	}
	if edge.Err() != testErr {
		t.Fatal("Err did not preserve the first fatal error")
	}
	if edge.SendError(moerr.NewInternalErrorNoCtx("second fatal")) {
		t.Fatal("second fatal should be a no-op after all fatal signals are delivered")
	}

	for i := 0; i < 2; i++ {
		select {
		case signal := <-edge.Ch2:
			if signal.EventType != EventError {
				t.Fatalf("signal %d = %s, want Error", i, signal.EventType)
			}
			if signal.TerminalErr() != testErr {
				t.Fatalf("signal %d did not preserve the first fatal error", i)
			}
		default:
			t.Fatalf("missing fatal signal %d", i)
		}
	}
}

func TestPipelineEdgeSharedFatalRetryAfterPartialDelivery(t *testing.T) {
	edge := NewPipelineEdge(1, 2)
	testErr := moerr.NewInternalErrorNoCtx("shared fatal")

	if edge.TrySendError(testErr) {
		t.Fatal("TrySendError should report false when only part of the shared fatal count was delivered")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after partial fatal delivery")
	}

	signal := <-edge.Ch2
	if signal.EventType != EventError {
		t.Fatalf("first signal = %s, want Error", signal.EventType)
	}
	if signal.TerminalErr() != testErr {
		t.Fatal("first signal did not preserve fatal error")
	}

	if !SendPipelineSignalWithTimeout(edge, NewAbortSignal(moerr.NewInternalErrorNoCtx("second fatal")), time.Second) {
		t.Fatal("fatal retry did not deliver the remaining shared fatal signal")
	}
	signal = <-edge.Ch2
	if signal.EventType != EventError {
		t.Fatalf("retry signal = %s, want original Error", signal.EventType)
	}
	if signal.TerminalErr() != testErr {
		t.Fatal("retry signal did not preserve the first fatal error")
	}
}

// TestPipelineEdgeAsWaitRegister verifies that WaitRegister is the same edge
// object, not a second state holder.
func TestPipelineEdgeAsWaitRegister(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	reg := edge.AsWaitRegister()

	if reg == nil {
		t.Fatal("AsWaitRegister returned nil")
	}
	if reg != edge {
		t.Fatal("AsWaitRegister must return the same edge object")
	}

	signal := NewEndSignal()
	if !SendPipelineSignalWithTimeout(reg, signal, time.Second) {
		t.Fatal("failed to send terminal signal through WaitRegister name")
	}

	select {
	case <-edge.Done():
	default:
		t.Fatal("terminal send through WaitRegister name did not close edge Done")
	}

	// Receive through edge channel.
	select {
	case received := <-edge.Ch2:
		if received.EventType != EventEnd {
			t.Fatalf("expected EventEnd, got %s", received.EventType)
		}
	default:
		t.Fatal("no signal received through edge channel")
	}
}

// TestPipelineEdgeNewFromNilReg creates an edge from a nil WaitRegister.
func TestPipelineEdgeNewFromNilReg(t *testing.T) {
	edge := NewPipelineEdgeFromReg(nil)

	if edge == nil {
		t.Fatal("NewPipelineEdgeFromReg returned nil")
	}
	if edge.Ch2 == nil {
		t.Fatal("Ch2 is nil for edge created from nil reg")
	}

	// Done should NOT be closed yet for a new edge.
	select {
	case <-edge.Done():
		t.Fatal("Done should not be closed for a new edge")
	default:
	}
}

// TestPipelineEdgeSendDataAbortedEdge verifies that SendData
// returns false when the edge has been aborted.
func TestPipelineEdgeSendDataAbortedEdge(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	edge.Abort(moerr.NewInternalErrorNoCtx("abort"))

	if edge.SendData(context.Background(), nil, 0) {
		t.Fatal("SendData succeeded on aborted edge")
	}
}

func TestPipelineEdgeSendDataRejectedAfterNormalEnd(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	if !edge.SendEnd() {
		t.Fatal("SendEnd failed")
	}

	if edge.SendDataDirect(context.Background(), nil, nil) {
		t.Fatal("SendDataDirect succeeded after normal End closed the edge")
	}
}

func TestPipelineEdgeBlockedDataSendUnblocksOnAbort(t *testing.T) {
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

	edge.Abort(moerr.NewInternalErrorNoCtx("abort"))

	select {
	case ok := <-done:
		if ok {
			t.Fatal("blocked data send succeeded after Abort")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked data send did not unblock after Abort")
	}
}

// TestPipelineEdgeNilEdgeSafety verifies that methods on nil edges
// do not panic.
func TestPipelineEdgeNilEdgeSafety(t *testing.T) {
	var edge *PipelineEdge

	// These should not panic.
	edge.SendEnd()
	edge.SendError(nil)
	edge.Abort(nil)
	edge.TrySendEnd()
	edge.TrySendError(nil)
	edge.TryAbort(nil)
	edge.SendData(context.Background(), nil, 0)
	edge.SendDataDirect(context.Background(), nil, nil)

	if edge.AsWaitRegister() != nil {
		t.Fatal("nil edge AsWaitRegister should return nil")
	}

	// Done() on nil edge returns a closed channel.
	select {
	case <-edge.Done():
	default:
		t.Fatal("nil edge Done should return a closed channel")
	}

	// Err() on nil edge returns nil.
	if edge.Err() != nil {
		t.Fatal("nil edge Err should return nil")
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
	if !EventAbort.IsTerminal() {
		t.Fatal("EventAbort should be terminal")
	}
}

// TestNewSignalConstructors verifies that typed signal constructors
// set the correct EventType.
func TestNewSignalConstructors(t *testing.T) {
	end := NewEndSignal()
	if end.EventType != EventEnd {
		t.Fatalf("NewEndSignal EventType: got %s, want EventEnd", end.EventType)
	}
	if !end.IsTerminal() {
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

	abortSignal := NewAbortSignal(testErr)
	if abortSignal.EventType != EventAbort {
		t.Fatalf("NewAbortSignal EventType: got %s, want EventAbort", abortSignal.EventType)
	}
	if abortSignal.TerminalErr() != testErr {
		t.Fatal("NewAbortSignal TerminalErr mismatch")
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

	abortSignal := NewAbortSignal(nil)
	if abortSignal.EventType != EventAbort {
		t.Fatalf("NewAbortSignal(nil) EventType: got %s, want EventAbort", abortSignal.EventType)
	}
	if abortSignal.TerminalErr() != ErrPipelineTerminalWithoutCause {
		t.Fatal("NewAbortSignal(nil) did not use ErrPipelineTerminalWithoutCause")
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

// TestSendPipelineSignalTimeout verifies the timeout behavior.
func TestSendPipelineSignalTimeout(t *testing.T) {
	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}

	// Fill the channel.
	reg.Ch2 <- NewEndSignal()

	// Try to send with timeout - should fail since channel is full.
	if SendPipelineSignalWithTimeout(reg, NewEndSignal(), 10*time.Millisecond) {
		t.Fatal("SendPipelineSignalWithTimeout succeeded on full channel")
	}

	// Nil register should return false.
	if SendPipelineSignalWithTimeout(nil, NewEndSignal(), time.Second) {
		t.Fatal("SendPipelineSignalWithTimeout succeeded on nil register")
	}
}

// TestTrySendPipelineSignal verifies non-blocking send.
func TestTrySendPipelineSignal(t *testing.T) {
	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}

	if !TrySendPipelineSignal(reg, NewEndSignal()) {
		t.Fatal("TrySendPipelineSignal failed on empty channel")
	}

	if TrySendPipelineSignal(reg, NewEndSignal()) {
		t.Fatal("TrySendPipelineSignal succeeded on full channel")
	}
}

// TestSendPipelineSignalWithContextCanceled verifies context cancellation.
func TestSendPipelineSignalWithContextCanceled(t *testing.T) {
	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}

	// Fill the channel.
	reg.Ch2 <- NewEndSignal()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if SendPipelineSignalWithContext(ctx, reg, NewEndSignal()) {
		t.Fatal("SendPipelineSignalWithContext succeeded with cancelled context")
	}

	select {
	case <-reg.Done():
		t.Fatal("cancelled End send closed Done before receiver observed the terminal")
	default:
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
	if EventAbort.String() != "Abort" {
		t.Fatalf("EventAbort.String() = %s, want Abort", EventAbort.String())
	}
}

// TestPipelineEdgeChannelFullFatalStillCloses verifies that fatal terminals
// close Done even if the terminal signal cannot be delivered because the
// channel is full.
func TestPipelineEdgeChannelFullFatalStillCloses(t *testing.T) {
	// Buffer size 1, but we'll fill it with a data signal first,
	// then TrySendError should hit the default branch.
	edge := NewPipelineEdge(1, 1)

	// Pre-fill channel with a data signal so the next send hits channel full.
	edge.Ch2 <- NewPipelineSignalToDirectly(nil, nil, nil)

	// Channel full => delivered = false, but Done() must still close for fatal.
	if edge.TrySendError(moerr.NewInternalErrorNoCtx("fatal")) {
		t.Fatal("TrySendError should fail to deliver on a full channel")
	}

	select {
	case <-edge.Done():
		// Correct: Done closed even though fatal delivery failed.
	default:
		t.Fatal("Done was not closed after TrySendError failed to deliver")
	}
}

// TestPipelineEdgeConcurrentEnd verifies that many goroutines calling
// SendEnd concurrently produce exactly one close of Done and no panics.
func TestPipelineEdgeConcurrentEnd(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	const goroutines = 50

	var wg sync.WaitGroup
	results := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- edge.SendEnd()
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
	if delivered != 1 {
		t.Fatalf("expected 1 delivery, got %d deliveries, %d no-ops", delivered, noop)
	}

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after concurrent SendEnd")
	}
}

// TestPipelineEdgeConcurrentAbort verifies concurrent Abort safety.
func TestPipelineEdgeConcurrentAbort(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	const goroutines = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			edge.Abort(moerr.NewInternalErrorNoCtx("abort"))
		}()
	}
	wg.Wait()

	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after concurrent Abort")
	}
	select {
	case <-edge.Aborted():
	default:
		t.Fatal("Aborted was not closed after concurrent Abort")
	}
}

// TestPipelineEdgeMixedConcurrentTerminal verifies that SendEnd and Abort
// called concurrently from different goroutines don't cause races.
func TestPipelineEdgeMixedConcurrentTerminal(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	const goroutines = 50

	var wg sync.WaitGroup

	// Half call SendEnd, half call Abort.
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			edge.SendEnd()
		}()
	}
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			edge.Abort(moerr.NewInternalErrorNoCtx("abort"))
		}()
	}
	wg.Wait()

	// Done must be closed.
	select {
	case <-edge.Done():
	default:
		t.Fatal("Done was not closed after mixed concurrent terminal calls")
	}

	// Aborted may or may not be closed depending on which won the Once.
}

// TestPipelineEdgeTimeoutFatalSendDoneClosesAfterTimeout verifies that the
// production terminal helper closes Done for fatal terminal signals even if the
// signal cannot be delivered because the channel is full.
func TestPipelineEdgeTimeoutFatalSendDoneClosesAfterTimeout(t *testing.T) {
	reg := NewPipelineEdge(1, 0)
	reg.Ch2 <- NewEndSignal() // fill it

	// With a very short timeout, this must fail.
	if SendPipelineSignalWithTimeout(reg, NewErrorSignal(moerr.NewInternalErrorNoCtx("fatal")), 10*time.Millisecond) {
		t.Fatal("SendPipelineSignalWithTimeout should fail on a full channel")
	}

	select {
	case <-reg.Done():
	default:
		t.Fatal("PipelineEdge Done was not closed")
	}
}

func TestPipelineEdgeTerminalRetryAfterFullChannelUsesFirstTerminal(t *testing.T) {
	edge := NewPipelineEdge(1, 0)
	edge.Ch2 <- NewPipelineSignalToDirectly(nil, nil, nil)
	firstErr := moerr.NewInternalErrorNoCtx("first terminal")
	secondErr := moerr.NewInternalErrorNoCtx("second terminal")

	if TrySendPipelineSignal(edge, NewErrorSignal(firstErr)) {
		t.Fatal("TrySendPipelineSignal should fail while the channel is full")
	}
	select {
	case <-edge.Done():
	default:
		t.Fatal("failed terminal try did not close Done")
	}
	if edge.Err() != firstErr {
		t.Fatal("edge did not preserve the first terminal error")
	}

	<-edge.Ch2
	if !SendPipelineSignalWithTimeout(edge, NewAbortSignal(secondErr), time.Second) {
		t.Fatal("terminal retry did not deliver after the channel drained")
	}
	signal := <-edge.Ch2
	if signal.EventType != EventError {
		t.Fatalf("retry delivered %s, want first EventError", signal.EventType)
	}
	if signal.TerminalErr() != firstErr {
		t.Fatal("retry did not deliver the first terminal error")
	}
}

func TestPipelineEdgeResetForReuseClearsTerminalState(t *testing.T) {
	edge := NewPipelineEdge(1, 0)
	if !edge.SendEnd() {
		t.Fatal("initial SendEnd failed")
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
	if !edge.SendError(testErr) {
		t.Fatal("SendError after reset failed")
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
	if !edge.SendEnd() {
		t.Fatal("initial SendEnd failed")
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

	if !edge.SendEnd() {
		t.Fatal("SendEnd after SetNilBatchCntForReuse failed")
	}
	select {
	case signal := <-edge.Ch2:
		if signal.EventType != EventEnd {
			t.Fatalf("signal after reuse = %s, want End", signal.EventType)
		}
	default:
		t.Fatal("SendEnd after reuse did not enqueue terminal signal")
	}
}
