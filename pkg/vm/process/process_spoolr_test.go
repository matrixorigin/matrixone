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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
