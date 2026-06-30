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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
)

// PipelineEdge is an explicit pipeline edge abstraction with typed lifecycle events.
// It wraps WaitRegister and adds idempotent terminal-state management.
//
// Invariants:
//  1. Terminal state (End/Error/Abort) is a protocol event, not an implicit nil batch.
//  2. End, Error, Abort are idempotent — calling them multiple times is safe.
//  3. Done() provides an observable terminal signal.
//  4. Every send/receive is cancelable via context, or bounded by the edge
//     timeout configuration.
type PipelineEdge struct {
	// Ch2 is the underlying data+terminal signal channel.
	// Exposed for direct select compatibility with PipelineSignalReceiver.
	Ch2 chan PipelineSignal

	// NilBatchCnt is the number of nil-batches this channel can receive before
	// it is considered done. Deprecated: prefer explicit terminal events.
	// Kept for backward compatibility with existing WaitRegister usage.
	NilBatchCnt int

	// --- terminal state ---
	done chan struct{}
	abrt chan struct{}

	terminalOnce sync.Once
	terminalErr  error
}

// NewPipelineEdge creates a new PipelineEdge.
// channelBufferSize is the buffer size for Ch2.
// nilBatchCnt is the NilBatchCnt value (0 defaults to 1, same as WaitRegister).
func NewPipelineEdge(channelBufferSize int, nilBatchCnt int) *PipelineEdge {
	if channelBufferSize <= 0 {
		channelBufferSize = 1
	}
	return &PipelineEdge{
		Ch2:         make(chan PipelineSignal, channelBufferSize),
		NilBatchCnt: nilBatchCnt,
		done:        make(chan struct{}),
		abrt:        make(chan struct{}),
	}
}

// NewPipelineEdgeFromReg wraps an existing WaitRegister as a PipelineEdge.
// If reg is nil, a new edge with buffer size 1 is created.
func NewPipelineEdgeFromReg(reg *WaitRegister) *PipelineEdge {
	if reg == nil {
		return NewPipelineEdge(1, 0)
	}
	return &PipelineEdge{
		Ch2:         reg.Ch2,
		NilBatchCnt: reg.NilBatchCnt,
		done:        make(chan struct{}),
		abrt:        make(chan struct{}),
	}
}

// AsWaitRegister returns the edge as a *WaitRegister for backward compat.
func (e *PipelineEdge) AsWaitRegister() *WaitRegister {
	if e == nil {
		return nil
	}
	return &WaitRegister{
		Ch2:         e.Ch2,
		NilBatchCnt: e.NilBatchCnt,
	}
}

// Done returns a channel that is closed when the edge reaches a terminal state
// (any of End, Error, or Abort). It never blocks.
func (e *PipelineEdge) Done() <-chan struct{} {
	if e == nil {
		c := make(chan struct{})
		close(c)
		return c
	}
	return e.done
}

// Aborted returns a channel that is closed when the edge is aborted (cancellation,
// remote failure, etc.). Unlike Done, this does NOT close on normal End.
func (e *PipelineEdge) Aborted() <-chan struct{} {
	if e == nil {
		c := make(chan struct{})
		return c
	}
	return e.abrt
}

// Err returns the terminal error, or nil if the edge ended without error.
func (e *PipelineEdge) Err() error {
	if e == nil {
		return nil
	}
	return e.terminalErr
}

// SendData sends a data batch via the edge. It returns true if the signal was
// successfully sent, false if the context was cancelled.
func (e *PipelineEdge) SendData(ctx context.Context, spool *pSpool.PipelineSpool, idx int) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return e.sendSignal(ctx, NewPipelineSignalToGetFromSpool(spool, idx))
}

// SendDataDirect sends a batch directly (not via spool) through the edge.
func (e *PipelineEdge) SendDataDirect(ctx context.Context, bat *batch.Batch, mp *mpool.MPool) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return e.sendSignal(ctx, NewPipelineSignalToDirectly(bat, nil, mp))
}

// SendEnd sends an End signal through the edge. It is idempotent: after the first
// call, subsequent calls are no-ops. It returns true if the End signal was delivered.
func (e *PipelineEdge) SendEnd() bool {
	return e.sendTerminal(NewEndSignal())
}

// SendError sends an Error signal through the edge. Idempotent.
func (e *PipelineEdge) SendError(err error) bool {
	return e.sendTerminal(NewErrorSignal(err))
}

// Abort sends an Abort signal through the edge and closes the abort channel.
// Idempotent. This is the strongest terminal signal — it indicates forced teardown.
func (e *PipelineEdge) Abort(err error) bool {
	return e.sendTerminal(NewAbortSignal(err))
}

// TrySendEnd attempts a non-blocking End send. Returns true if delivered.
func (e *PipelineEdge) TrySendEnd() bool {
	return e.trySend(NewEndSignal())
}

// TrySendError attempts a non-blocking Error send. Returns true if delivered.
func (e *PipelineEdge) TrySendError(err error) bool {
	return e.trySend(NewErrorSignal(err))
}

// TryAbort attempts a non-blocking Abort. Returns true if delivered.
func (e *PipelineEdge) TryAbort(err error) bool {
	return e.trySend(NewAbortSignal(err))
}

// --- internal ---

func (e *PipelineEdge) sendTerminal(signal PipelineSignal) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	delivered := false
	e.terminalOnce.Do(func() {
		e.terminalErr = signal.TerminalErr()
		// Best-effort non-blocking send; if channel is full or receiver gone,
		// we still mark the edge terminal so Done() unblocks.
		select {
		case e.Ch2 <- signal:
			delivered = true
		default:
		}
		// Always close done — even if signal was dropped, the state is terminal.
		close(e.done)
		if signal.EventType == EventAbort {
			close(e.abrt)
		}
	})
	return delivered
}

func (e *PipelineEdge) sendSignal(ctx context.Context, signal PipelineSignal) bool {
	if e == nil {
		return false
	}
	// Fast-path reject: if a terminal event was already dispatched,
	// refuse data sends so batches don't leak into a channel that
	// no receiver will drain.
	select {
	case <-e.done:
		return false
	default:
	}
	if ctx.Err() != nil {
		return false
	}
	select {
	case e.Ch2 <- signal:
		return true
	case <-ctx.Done():
	case <-e.abrt:
	}
	return false
}

// trySend is the non-blocking counterpart of sendTerminal.
// If the receiver is ready it delivers the signal; otherwise it drops the
// signal but still records the terminal state (done is always closed).
//
// This is safe because cleanup signals only need to reach the receiver once —
// timeout-based retry in the caller provides a fallback path.
//
// Return semantics:
//   - First call (fires terminalOnce): returns true if signal reached channel,
//     false if channel was full (signal dropped but state is terminal).
//   - Subsequent calls: returns false (done is already closed by the first call).
//
// The caller should use SendSignalWithTimeout as a fallback when trySend
// returns false and delivery confirmation is needed.
func (e *PipelineEdge) trySend(signal PipelineSignal) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	delivered := false
	e.terminalOnce.Do(func() {
		e.terminalErr = signal.TerminalErr()
		select {
		case e.Ch2 <- signal:
			delivered = true
		default:
		}
		// Always close done — even if signal was dropped, the state is terminal.
		close(e.done)
		if signal.EventType == EventAbort {
			close(e.abrt)
		}
	})
	return delivered
}

// --- send helpers compatible with existing WaitRegister-based code ---

// SendSignalWithTimeout sends a signal to the edge's channel with optional timeout.
func (e *PipelineEdge) SendSignalWithTimeout(signal PipelineSignal, timeout time.Duration) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return SendPipelineSignalWithTimeout(e.AsWaitRegister(), signal, timeout)
}

// SendSignalWithContext sends a signal to the edge's channel with context.
func (e *PipelineEdge) SendSignalWithContext(ctx context.Context, signal PipelineSignal) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return SendPipelineSignalWithContext(ctx, e.AsWaitRegister(), signal)
}

// TrySendSignal tries a non-blocking send to the edge's channel.
func (e *PipelineEdge) TrySendSignal(signal PipelineSignal) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return TrySendPipelineSignal(e.AsWaitRegister(), signal)
}

// ChannelState returns (len, cap) for the underlying channel.
func (e *PipelineEdge) ChannelState() (int, int) {
	if e == nil || e.Ch2 == nil {
		return 0, 0
	}
	return len(e.Ch2), cap(e.Ch2)
}
