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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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
	if !publishTerminalForTest(reg, NewEndSignal()) {
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
	if !publishTerminalForTest(reg, NewErrorSignal(moerr.NewInternalErrorNoCtx("shared fatal"))) {
		t.Fatal("failed to send shared fatal")
	}

	receiver := InitPipelineSignalReceiver(context.Background(), []*WaitRegister{reg})
	if !receiver.WaitingEndWithTimeout(time.Second) {
		t.Fatal("shared fatal did not complete the receiver terminal count")
	}
}

func TestPipelineSignalReceiverFailedCleanupWithoutCauseReturnsError(t *testing.T) {
	reg := NewPipelineEdge(1, 1)
	if !publishTerminalForTest(reg, BuildCleanupSignal(true, nil)) {
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
			if !publishTerminalForTest(target, NewErrorSignal(fatalErr)) {
				t.Fatal("terminal publication failed entered the full channel")
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
				if i != testCase.targetIdx && !publishTerminalForTest(reg, NewEndSignal()) {
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
	if !publishTerminalForTest(reg, NewErrorSignal(fatalErr)) {
		t.Fatal("terminal publication failed entered the full channel")
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

func publishTerminalForTest(edge *PipelineEdge, signal PipelineSignal) bool {
	_, ok := edge.PublishTerminal(signal)
	return ok
}

// A receiver cleanup may time out before a slow producer finishes. After all
// execution goroutines join, generation reset must discard late queued spool
// notifications as well as direct batches. Releasing only the current batch
// cannot discard a notification whose slot has never been received.
func TestPipelineEdgeReuseDiscardsPendingSpoolNotifications(t *testing.T) {
	for receivers := 1; receivers <= 2; receivers++ {
		for reset := 0; reset < 3; reset++ {
			t.Run(fmt.Sprintf("receivers_%d_reset_%d", receivers, reset), func(t *testing.T) {
				mp := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(mp)
				srcMP := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(srcMP)
				src := batch.NewWithSize(1)
				defer src.Clean(srcMP)
				src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(42), false, srcMP))
				src.SetRowCount(1)
				sp := pSpool.InitMyPipelineSpool(mp, uint32(receivers))
				defer sp.Abort(nil)
				edges := make([]*PipelineEdge, receivers)
				for i := range edges {
					edges[i] = NewPipelineEdge(2, 1)
				}
				r := InitPipelineSignalReceiver(context.Background(), edges)
				require.False(t, r.WaitingEndWithTimeout(time.Millisecond), "missing producer must time out during cleanup")
				for range 2 {
					done, err := sp.SendBatch(context.Background(), pSpool.SendToAllLocal, src, nil)
					require.NoError(t, err)
					require.False(t, done)
					for i, edge := range edges {
						require.True(t, edge.SendData(context.Background(), sp, i))
					}
				}
				for _, edge := range edges {
					require.True(t, publishTerminalForTest(edge, NewEndSignal()))
				}
				// This production cleanup intentionally retains pending batches
				// because another receiver can still own/read them at this point.
				sp.ForceCleanupAfterTerminalSignal()
				require.Positive(t, mp.CurrNB())
				// No old sender/receiver remains; reset is now allowed.
				for _, edge := range edges {
					switch reset {
					case 0:
						edge.ResetForReuse(1, 1)
					case 1:
						edge.SetNilBatchCntForReuse(1)
					case 2:
						edge.ResetTerminalStateForReuse()
					}
				}
				require.Zero(t, mp.CurrNB(), "reuse abandoned pending slot ownership")
			})
		}
	}
}

func TestPipelineReceiverQueuedSpoolHandoff(t *testing.T) {
	for _, next := range []string{"same_spool", "different_spool", "different_receiver", "direct", "current_direct", "current_direct_next_direct", "data_error", "end", "error", "cancel", "aborted_spool"} {
		t.Run(next, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			srcMP := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(srcMP)
			src := batch.NewWithSize(1)
			defer src.Clean(srcMP)
			src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(42), false, srcMP))
			src.SetRowCount(1)
			first := pSpool.InitMyPipelineSpool(mp, 2)
			defer first.Abort(nil)
			second := pSpool.InitMyPipelineSpool(mp, 1)
			defer second.Abort(nil)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			edge := NewPipelineEdge(2, 2)
			receiver := InitPipelineSignalReceiver(ctx, []*WaitRegister{edge})
			send := func(sp *pSpool.PipelineSpool) {
				done, err := sp.SendBatch(ctx, 0, src, nil)
				require.NoError(t, err)
				require.False(t, done)
				require.True(t, edge.SendData(ctx, sp, 0))
			}
			if next == "current_direct" || next == "current_direct_next_direct" {
				direct, err := src.Dup(mp)
				require.NoError(t, err)
				require.True(t, edge.SendDataDirect(ctx, direct, mp))
			} else {
				send(first)
			}
			current, err := receiver.GetNextBatch(nil)
			require.NoError(t, err)
			require.Equal(t, int64(42), vector.MustFixedColWithTypeCheck[int64](current.Vecs[0])[0])
			cause := errors.New("queued handoff failure")
			switch next {
			case "same_spool", "current_direct":
				send(first)
			case "different_spool":
				send(second)
			case "different_receiver":
				done, err := first.SendBatch(ctx, 1, src, nil)
				require.NoError(t, err)
				require.False(t, done)
				require.True(t, edge.SendData(ctx, first, 1))
			case "data_error":
				done, err := first.SendBatch(ctx, 0, src, cause)
				require.NoError(t, err)
				require.False(t, done)
				require.True(t, edge.SendData(ctx, first, 0))
			case "direct", "current_direct_next_direct":
				direct, err := src.Dup(mp)
				require.NoError(t, err)
				require.True(t, edge.SendDataDirect(ctx, direct, mp))
			case "end":
				publishTerminalForTest(edge, NewEndSignal())
				publishTerminalForTest(edge, NewEndSignal())
			case "error":
				publishTerminalForTest(edge, NewErrorSignal(cause))
			case "cancel":
				cancel()
			case "aborted_spool":
				send(first)
				first.Abort(cause)
				publishTerminalForTest(edge, NewErrorSignal(cause))
			}
			got, err := receiver.GetNextBatch(nil)
			switch next {
			case "same_spool", "different_spool", "different_receiver", "direct", "current_direct", "current_direct_next_direct", "data_error":
				if next == "data_error" {
					require.ErrorIs(t, err, cause)
				} else {
					require.NoError(t, err)
				}
				require.NotNil(t, got)
				require.Equal(t, 1, got.RowCount())
				require.Equal(t, int64(42), vector.MustFixedColWithTypeCheck[int64](got.Vecs[0])[0])
				publishTerminalForTest(edge, NewEndSignal())
				publishTerminalForTest(edge, NewEndSignal())
			case "error", "aborted_spool":
				require.Nil(t, got)
				require.ErrorIs(t, err, cause)
			case "end", "cancel":
				require.Nil(t, got)
				if next == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
				if next == "cancel" {
					publishTerminalForTest(edge, NewEndSignal())
					publishTerminalForTest(edge, NewEndSignal())
				}
			}
			require.True(t, receiver.WaitingEndWithTimeout(time.Second))
			first.ForceCleanupAfterTerminalSignal()
			second.ForceCleanupAfterTerminalSignal()
			require.Zero(t, mp.CurrNB())
		})
	}
}
