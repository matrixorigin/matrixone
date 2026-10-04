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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

func TestPipelineSignalReceiverCancellationPreservesExecutionCause(t *testing.T) {
	tests := []struct {
		name      string
		cause     error
		wantError error
	}{
		{
			name:      "execution failure before terminal publication",
			cause:     moerr.NewDuplicateEntryNoCtx("1", "primary"),
			wantError: moerr.NewDuplicateEntryNoCtx("1", "primary"),
		},
		{
			name:  "graceful pipeline cancellation",
			cause: nil,
		},
		{
			name:      "joined execution failure and cancellation",
			cause:     errors.Join(context.Canceled, moerr.NewDuplicateEntryNoCtx("2", "primary")),
			wantError: moerr.NewDuplicateEntryNoCtx("2", "primary"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(test.cause)
			receiver := InitPipelineSignalReceiver(ctx, []*WaitRegister{NewPipelineEdge(1, 1)})

			got, err := receiver.GetNextBatch(nil)
			if got != nil {
				t.Fatal("canceled receiver returned a batch")
			}
			if test.wantError == nil {
				if err != nil {
					t.Fatalf("graceful cancellation returned error: %v", err)
				}
				return
			}
			var gotMoErr *moerr.Error
			var wantMoErr *moerr.Error
			if !errors.As(test.wantError, &wantMoErr) {
				t.Fatalf("test expectation is not a MatrixOne error: %v", test.wantError)
			}
			if !errors.As(err, &gotMoErr) || gotMoErr.ErrorCode() != wantMoErr.ErrorCode() {
				t.Fatalf("cancellation lost execution cause: got %v, want code %d", err, wantMoErr.ErrorCode())
			}
		})
	}
}

func TestPipelineSignalReceiverCancellationPreservesDeadlineClass(t *testing.T) {
	diagnostic := moerr.NewDuplicateEntryNoCtx("3", "primary")
	ctx, cancel := context.WithDeadlineCause(
		context.Background(), time.Now().Add(-time.Second), diagnostic)
	defer cancel()
	receiver := InitPipelineSignalReceiver(ctx, []*WaitRegister{NewPipelineEdge(1, 1)})

	got, err := receiver.GetNextBatch(nil)
	if got != nil {
		t.Fatal("deadline-canceled receiver returned a batch")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline cancellation returned %v, want context deadline exceeded", err)
	}
	if errors.Is(err, diagnostic) {
		t.Fatalf("deadline cancellation was replaced by diagnostic cause: %v", err)
	}
}

func TestPipelineSignalReceiverCancellationPreservesDurableEdgeFailure(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
	}{
		{name: "plain context cancellation"},
		{name: "query interrupted cancellation", cause: moerr.NewQueryInterrupted(context.Background())},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(test.cause)
			edge := NewPipelineEdge(1, 1)
			edge.Ch2 <- NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)
			duplicateErr := moerr.NewDuplicateEntryNoCtx("4", "primary")
			if edge.SendError(duplicateErr) {
				t.Fatal("error signal unexpectedly fit behind buffered data")
			}
			receiver := InitPipelineSignalReceiver(ctx, []*WaitRegister{edge})

			if err := receiver.contextDoneError(); !errors.Is(err, duplicateErr) {
				t.Fatalf("cancellation lost durable edge failure: got %v, want %v", err, duplicateErr)
			}
		})
	}
}

func TestPipelineSignalReceiverDurableFailureSelectionIsOrderIndependent(t *testing.T) {
	duplicateErr := moerr.NewDuplicateEntryNoCtx("5", "primary")
	interruptedErr := moerr.NewQueryInterrupted(context.Background())

	for _, test := range []struct {
		name               string
		errs               []error
		wantSubstantiveErr bool
	}{
		{
			name:               "cancellation before execution failure",
			errs:               []error{interruptedErr, duplicateErr},
			wantSubstantiveErr: true,
		},
		{
			name:               "execution failure before cancellation",
			errs:               []error{duplicateErr, interruptedErr},
			wantSubstantiveErr: true,
		},
		{
			name: "cancellation only",
			errs: []error{interruptedErr, context.Canceled},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			regs := make([]*WaitRegister, len(test.errs))
			for i, terminalErr := range test.errs {
				regs[i] = NewPipelineEdge(1, 1)
				regs[i].Ch2 <- NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)
				if regs[i].SendError(terminalErr) {
					t.Fatalf("error signal %d unexpectedly fit behind buffered data", i)
				}
			}

			receiver := InitPipelineSignalReceiver(ctx, regs)
			err := receiver.contextDoneError()
			if test.wantSubstantiveErr {
				if !errors.Is(err, duplicateErr) {
					t.Fatalf("durable failure selection depended on edge order: got %v, want %v", err, duplicateErr)
				}
			} else if !IsPipelineCancellationError(err) {
				t.Fatalf("cancellation-only edges returned non-cancellation error: %v", err)
			}
		})
	}
}

func TestPipelineSignalReceiverWaitingEndUsesCleanupTimeout(t *testing.T) {
	oldCleanupWaitTimeout := PipelineCleanupWaitTimeout
	PipelineCleanupWaitTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		PipelineCleanupWaitTimeout = oldCleanupWaitTimeout
	})

	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}
	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})
	done := make(chan struct{})
	go func() {
		receiver.WaitingEnd()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WaitingEnd did not return after its cleanup timeout")
	}
}

func TestPipelineSignalReceiverWaitingEndWithTimeoutReturnsWhenEndSignalIsMissing(t *testing.T) {
	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}
	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})

	done := make(chan bool)
	go func() {
		done <- receiver.WaitingEndWithTimeout(10 * time.Millisecond)
	}()

	select {
	case completed := <-done:
		if completed {
			t.Fatal("WaitingEndWithTimeout completed without receiving an end signal")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitingEndWithTimeout did not return after its timeout")
	}
}

func TestPipelineSignalReceiverWaitingEndWithTimeoutCompletesWhenEndSignalArrives(t *testing.T) {
	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}
	reg.Ch2 <- NewPipelineSignalToDirectly(nil, nil, nil)
	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})

	if !receiver.WaitingEndWithTimeout(time.Second) {
		t.Fatal("WaitingEndWithTimeout timed out after receiving an end signal")
	}
}

func TestPipelineSignalReceiverSharedEdgeContinuesAfterFirstEndSignal(t *testing.T) {
	reg := NewPipelineEdge(3, 2)
	reg.Ch2 <- NewEndSignal()
	reg.Ch2 <- NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)
	reg.Ch2 <- NewEndSignal()

	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})
	got, err := receiver.GetNextBatch(nil)
	if err != nil {
		t.Fatalf("GetNextBatch returned error: %v", err)
	}
	if got != batch.EmptyBatch {
		t.Fatal("receiver did not continue to data after the first shared End signal")
	}
	if !receiver.WaitingEndWithTimeout(time.Second) {
		t.Fatal("receiver did not complete after the second shared End signal")
	}
}

func TestPipelineSignalReceiverDonePreservesBufferedDataOrder(t *testing.T) {
	reg := NewPipelineEdge(2, 1)
	if !reg.SendDataDirect(context.Background(), batch.EmptyBatch, nil) {
		t.Fatal("failed to send buffered data")
	}
	if !reg.SendEnd() {
		t.Fatal("failed to send End")
	}

	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})
	got, err := receiver.GetNextBatch(nil)
	if err != nil {
		t.Fatalf("GetNextBatch returned error before buffered data: %v", err)
	}
	if got != batch.EmptyBatch {
		t.Fatal("Done notification bypassed buffered data")
	}

	got, err = receiver.GetNextBatch(nil)
	if got != nil || err != nil {
		t.Fatalf("unexpected result after buffered data and End: batch=%v err=%v", got, err)
	}
}

func TestPipelineSignalReceiverSharedFatalCompletesRemainingCount(t *testing.T) {
	reg := NewPipelineEdge(2, 2)
	if !reg.SendError(moerr.NewInternalErrorNoCtx("shared fatal")) {
		t.Fatal("failed to send shared fatal")
	}

	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})
	if !receiver.WaitingEndWithTimeout(time.Second) {
		t.Fatal("shared fatal did not complete the receiver terminal count")
	}
}

func TestPipelineSignalReceiverFailedCleanupWithoutCauseReturnsError(t *testing.T) {
	reg := NewPipelineEdge(1, 1)
	if !SendPipelineSignalWithTimeout(reg, BuildCleanupSignal(true, nil), time.Second) {
		t.Fatal("failed to send cleanup failure signal")
	}

	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})
	got, err := receiver.GetNextBatch(nil)
	if got != nil {
		t.Fatal("failure terminal returned a batch")
	}
	if err != ErrPipelineTerminalWithoutCause {
		t.Fatal("failure terminal without cause did not return ErrPipelineTerminalWithoutCause")
	}
}

func TestPipelineSignalReceiverWakesWhenFatalSignalCannotBeDelivered(t *testing.T) {
	testCases := []struct {
		name      string
		regCount  int
		targetIdx int
	}{
		{name: "one input", regCount: 1, targetIdx: 0},
		{name: "two inputs first", regCount: 2, targetIdx: 0},
		{name: "two inputs last", regCount: 2, targetIdx: 1},
		{name: "three inputs first", regCount: 3, targetIdx: 0},
		{name: "three inputs last", regCount: 3, targetIdx: 2},
		{name: "four inputs first", regCount: 4, targetIdx: 0},
		{name: "four inputs last", regCount: 4, targetIdx: 3},
		{name: "five inputs first", regCount: 5, targetIdx: 0},
		{name: "five inputs middle", regCount: 5, targetIdx: 2},
		{name: "five inputs last", regCount: 5, targetIdx: 4},
		{name: "six inputs first", regCount: 6, targetIdx: 0},
		{name: "six inputs middle", regCount: 6, targetIdx: 3},
		{name: "six inputs last", regCount: 6, targetIdx: 5},
		{name: "seven inputs first", regCount: 7, targetIdx: 0},
		{name: "seven inputs middle", regCount: 7, targetIdx: 2},
		{name: "seven inputs last", regCount: 7, targetIdx: 6},
		{name: "eight inputs first", regCount: 8, targetIdx: 0},
		{name: "eight inputs middle", regCount: 8, targetIdx: 4},
		{name: "eight inputs last", regCount: 8, targetIdx: 7},
		{name: "reflect select first input", regCount: 9, targetIdx: 0},
		{name: "reflect select middle input", regCount: 9, targetIdx: 4},
		{name: "reflect select last input", regCount: 9, targetIdx: 8},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			regs := make([]*WaitRegister, testCase.regCount)
			for i := range regs {
				regs[i] = NewPipelineEdge(1, 1)
			}
			target := regs[testCase.targetIdx]
			target.Ch2 <- NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)

			fatalErr := moerr.NewInternalErrorNoCtx("fatal signal delivery failed")
			sendCtx, cancelSend := context.WithCancel(context.Background())
			cancelSend()
			if SendPipelineSignalWithContext(sendCtx, target, NewErrorSignal(fatalErr)) {
				t.Fatal("fatal signal unexpectedly entered the full channel")
			}

			receiverCtx, cancelReceiver := context.WithCancel(context.Background())
			defer cancelReceiver()
			receiver := InitPipelineSignalReceiver(receiverCtx, regs)

			got, err := receiver.GetNextBatch(nil)
			if err != nil {
				t.Fatalf("GetNextBatch returned error before draining buffered data: %v", err)
			}
			if got != batch.EmptyBatch {
				t.Fatal("receiver did not drain the buffered data before reporting the fatal terminal")
			}

			type result struct {
				bat *batch.Batch
				err error
			}
			resultCh := make(chan result, 1)
			go func() {
				got, err := receiver.GetNextBatch(nil)
				resultCh <- result{bat: got, err: err}
			}()

			select {
			case result := <-resultCh:
				if result.bat != nil {
					t.Fatal("fatal terminal returned a batch")
				}
				if result.err != fatalErr {
					t.Fatalf("fatal terminal returned unexpected error: %v", result.err)
				}
			case <-time.After(100 * time.Millisecond):
				cancelReceiver()
				<-resultCh
				t.Fatal("receiver remained blocked after the full channel drained")
			}

			// A 9-input receiver starts in reflect.Select mode. Removing any
			// input drops it to the hand-written 8-way select; finish every
			// remaining edge to verify both the reflect case reindexing and
			// that mode transition.
			for i, reg := range regs {
				if i != testCase.targetIdx && !reg.SendEnd() {
					t.Fatalf("failed to end remaining input %d", i)
				}
			}
			got, err = receiver.GetNextBatch(nil)
			if got != nil || err != nil {
				t.Fatalf("receiver did not finish remaining inputs: batch=%v err=%v", got, err)
			}
		})
	}
}

func TestPipelineSignalReceiverSynthesizesSharedUndeliveredFatalCount(t *testing.T) {
	reg := NewPipelineEdge(1, 2)
	reg.Ch2 <- NewPipelineSignalToDirectly(batch.EmptyBatch, nil, nil)

	fatalErr := moerr.NewInternalErrorNoCtx("shared fatal signal delivery failed")
	sendCtx, cancelSend := context.WithCancel(context.Background())
	cancelSend()
	if SendPipelineSignalWithContext(sendCtx, reg, NewAbortSignal(fatalErr)) {
		t.Fatal("fatal signal unexpectedly entered the full channel")
	}

	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})
	got, err := receiver.GetNextBatch(nil)
	if err != nil || got != batch.EmptyBatch {
		t.Fatalf("unexpected buffered data result: batch=%v err=%v", got, err)
	}

	for i := 0; i < 2; i++ {
		got, err = receiver.GetNextBatch(nil)
		if got != nil || err != fatalErr {
			t.Fatalf("unexpected synthesized fatal %d: batch=%v err=%v", i, got, err)
		}
	}
	got, err = receiver.GetNextBatch(nil)
	if got != nil || err != nil {
		t.Fatalf("receiver did not finish after the shared fatal count: batch=%v err=%v", got, err)
	}
}

func TestSendPipelineSignalWithTimeoutReturnsWhenChannelIsFull(t *testing.T) {
	reg := &WaitRegister{Ch2: make(chan PipelineSignal, 1)}
	reg.Ch2 <- NewPipelineSignalToDirectly(nil, nil, nil)

	if SendPipelineSignalWithTimeout(reg, NewPipelineSignalToDirectly(nil, nil, nil), 10*time.Millisecond) {
		t.Fatal("SendPipelineSignalWithTimeout succeeded on a full channel with no receiver")
	}
}

func TestCleanupWarnLimiterSuppressesStorm(t *testing.T) {
	limiter := newCleanupWarnLimiter()

	for i := int64(1); i <= pipelineCleanupWarnBurstCount; i++ {
		allowed, occurrence, suppressed := limiter.allow("storm")
		if !allowed || occurrence != i || suppressed != 0 {
			t.Fatalf("unexpected burst decision: allowed=%t occurrence=%d suppressed=%d", allowed, occurrence, suppressed)
		}
	}

	for i := pipelineCleanupWarnBurstCount + 1; i < pipelineCleanupWarnSampleInterval; i++ {
		allowed, _, _ := limiter.allow("storm")
		if allowed {
			t.Fatalf("unexpected log allowed before sample interval at occurrence %d", i)
		}
	}

	allowed, occurrence, suppressed := limiter.allow("storm")
	if !allowed || occurrence != pipelineCleanupWarnSampleInterval {
		t.Fatalf("sample log was not allowed: allowed=%t occurrence=%d", allowed, occurrence)
	}
	wantSuppressed := pipelineCleanupWarnSampleInterval - pipelineCleanupWarnBurstCount - 1
	if suppressed != wantSuppressed {
		t.Fatalf("unexpected suppressed count: got %d, want %d", suppressed, wantSuppressed)
	}
}

func TestWarnPipelineCleanupfNilProcessIsSafe(t *testing.T) {
	WarnPipelineCleanupf(nil, "nil_proc_cleanup", "cleanup warning with nil process")
}
