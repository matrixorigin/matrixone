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

// TestPipelineEdgeAsWaitRegister verifies backward compatibility
// with WaitRegister-based code.
func TestPipelineEdgeAsWaitRegister(t *testing.T) {
	edge := NewPipelineEdge(1, 1)
	reg := edge.AsWaitRegister()

	if reg == nil {
		t.Fatal("AsWaitRegister returned nil")
	}
	if reg.Ch2 != edge.Ch2 {
		t.Fatal("AsWaitRegister Ch2 does not match edge Ch2")
	}

	// Send a signal through the WaitRegister channel.
	signal := NewEndSignal()
	reg.Ch2 <- signal

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

// TestPipelineEdgeChannelFullDoneStillCloses verifies the bugfix:
// Done() must close even if the terminal signal couldn't be delivered
// because the channel was full. The edge is terminal regardless of
// whether the signal reached the channel.
func TestPipelineEdgeChannelFullDoneStillCloses(t *testing.T) {
	// Buffer size 1, but we'll fill it with a data signal first,
	// then TrySendEnd should hit the default branch.
	edge := NewPipelineEdge(1, 1)

	// Pre-fill channel with a data signal so the next send hits channel full.
	edge.Ch2 <- NewPipelineSignalToDirectly(nil, nil, nil)

	// Channel full => delivered = false, but Done() must still close.
	if edge.TrySendEnd() {
		t.Fatal("TrySendEnd should fail to deliver on a full channel")
	}

	select {
	case <-edge.Done():
		// Correct: Done closed even though channel delivery failed.
	default:
		t.Fatal("Done was not closed after TrySendEnd failed to deliver")
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

// TestPipelineEdgeTimeoutSendDoneClosesAfterTimeout verifies that
// SendSignalWithTimeout on a full channel still marks the edge as
// terminal via the typed signal path in connector/dispatch. This is
// an integration-level invariant: the helper that sends with timeout
// does NOT close the edge's Done channel (it's a WaitRegister helper),
// but the edge itself must handle this case correctly.
func TestPipelineEdgeTimeoutSendDoneClosesAfterTimeout(t *testing.T) {
	// Create a WaitRegister with a full channel.
	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}
	reg.Ch2 <- NewEndSignal() // fill it

	// With a very short timeout, this must fail.
	if SendPipelineSignalWithTimeout(reg, NewEndSignal(), 10*time.Millisecond) {
		t.Fatal("SendPipelineSignalWithTimeout should fail on a full channel")
	}

	// The WaitRegister does NOT close Done — that's the edge's job.
	// This test documents the split responsibility.
	// PipelineEdge.sendTerminal always closes done regardless of delivery.
	edge := NewPipelineEdgeFromReg(reg)
	edge.SendEnd() // sendTerminal fires terminalOnce, closes done

	select {
	case <-edge.Done():
	default:
		t.Fatal("PipelineEdge Done was not closed")
	}
}
