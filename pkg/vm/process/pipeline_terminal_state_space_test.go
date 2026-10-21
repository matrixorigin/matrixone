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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Enumerate every length-six End/Error ordering against all queue
// occupancies for capacities 0..2 and expected sender counts 1..3. The oracle
// is the public contract: all senders end OR first fatal ends the whole edge;
// late terminals cannot replace the winner, and queued data precedes completion.
// No private terminal counters are used to predict the result. Concurrent races
// and larger/reflection fan-in are covered by separate tests.
func TestPipelineTerminalBoundedStateSpace(t *testing.T) {
	first := errors.New("first producer error")
	second := errors.New("second producer error")
	cases := 0
	for capacity := 0; capacity <= 2; capacity++ {
		for occupancy := 0; occupancy <= capacity; occupancy++ {
			for senders := 1; senders <= 3; senders++ {
				for sequence := 0; sequence < 729; sequence++ { // End and two distinct error causes: 3^6
					edge := &PipelineEdge{Ch2: make(chan PipelineSignal, capacity), NilBatchCnt: senders}
					for range occupancy {
						b := &batch.Batch{}
						b.SetRowCount(1)
						if !edge.SendDataDirect(context.Background(), b, nil) {
							t.Fatal("prefill failed")
						}
					}
					ends := 0
					var wantErr error
					encoded := sequence
					for step := 0; step < 6; step++ {
						action := encoded % 3
						encoded /= 3
						signal := NewEndSignal()
						if action == 1 {
							signal = NewErrorSignal(first)
						} else if action == 2 {
							signal = NewErrorSignal(second)
						}
						if ends < senders && wantErr == nil {
							if action == 0 {
								ends++
							} else {
								wantErr = signal.TerminalErr()
							}
						}
						_, _ = edge.PublishTerminal(signal)
						closed := false
						select {
						case <-edge.Done():
							closed = true
						default:
						}
						if closed != (ends == senders || wantErr != nil) || edge.Err() != wantErr {
							t.Fatalf("terminal contract: cap=%d occupancy=%d senders=%d sequence=%d step=%d closed=%v err=%v want=%v", capacity, occupancy, senders, sequence, step, closed, edge.Err(), wantErr)
						}
					}
					// Six End-only actions suffice for all sender counts. A
					// timeout here is a hang guard, not synchronization.
					ctx, stop := context.WithTimeout(context.Background(), time.Second)
					r := InitPipelineSignalReceiver(ctx, []*WaitRegister{edge})
					rows := 0
					var gotErr error
					for {
						b, err := r.GetNextBatch(nil)
						if err != nil || b == nil {
							gotErr = err
							break
						}
						rows += b.RowCount()
					}
					if ctx.Err() != nil || rows != occupancy || gotErr != wantErr {
						stop()
						t.Fatalf("receive contract: cap=%d occupancy=%d senders=%d sequence=%d rows=%d err=%v want=%v", capacity, occupancy, senders, sequence, rows, gotErr, wantErr)
					}
					if !r.WaitingEndWithTimeout(time.Second) {
						stop()
						t.Fatal("remaining fatal sender count did not drain")
					}
					stop()
					// Every terminal history must allow a clean next generation.
					edge.ResetForReuse(1, 1)
					if edge.Err() != nil || !publishTerminalForTest(edge, NewEndSignal()) {
						t.Fatal("reuse retained a previous terminal")
					}
					cases++
				}
			}
		}
	}
	t.Logf("exhaustively checked %d bounded terminal histories", cases)
}

// Continuously refill every healthy input. Ready-data polling must still reach
// another input's fatal terminal, including across the reflection threshold.
func TestPipelineReadyInputsDoNotStarveFatalTerminal(t *testing.T) {
	for inputs := 2; inputs <= 9; inputs++ {
		regs := make([]*WaitRegister, inputs)
		newData := func(idx int) *batch.Batch {
			b := &batch.Batch{}
			b.ShuffleIDX = int32(idx)
			b.SetRowCount(1)
			return b
		}
		for i := range regs {
			regs[i] = NewPipelineEdge(1, 1)
			if !regs[i].SendDataDirect(context.Background(), newData(i), nil) {
				t.Fatal("data admission failed")
			}
		}
		cause := errors.New("busy-input fatal terminal")
		publishTerminalForTest(regs[0], NewErrorSignal(cause))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		r := InitPipelineSignalReceiver(ctx, regs)
		seenError := false
		for step := 0; step <= inputs; step++ {
			got, err := r.GetNextBatch(nil)
			if err != nil {
				if err != cause {
					cancel()
					t.Fatalf("inputs=%d: original error was replaced: %v", inputs, err)
				}
				seenError = true
				break
			}
			if got == nil || ctx.Err() != nil {
				cancel()
				t.Fatalf("inputs=%d: premature termination", inputs)
			}
			idx := int(got.ShuffleIDX)
			if idx != 0 && !regs[idx].SendDataDirect(ctx, newData(idx), nil) {
				cancel()
				t.Fatal("healthy input could not refill")
			}
		}
		if !seenError {
			cancel()
			t.Fatalf("inputs=%d: busy data inputs starved fatal terminal", inputs)
		}
		for _, reg := range regs[1:] {
			publishTerminalForTest(reg, NewEndSignal())
		}
		if !r.WaitingEndWithTimeout(time.Second) {
			cancel()
			t.Fatal("healthy inputs did not finish cleanup")
		}
		cancel()
	}
}
