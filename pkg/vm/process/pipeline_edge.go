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
// It owns both the signal channel and the idempotent terminal state.
//
// Invariants:
//  1. Terminal state (End/Error/Abort) is a protocol event, not an implicit nil batch.
//  2. End is counted per expected sender; Error and Abort are fatal first-wins
//     terminals that consume the edge's remaining sender count.
//  3. Done() provides an observable whole-edge terminal signal.
//  4. Every send/receive is cancelable via context, or bounded by the edge
//     timeout configuration.
type PipelineEdge struct {
	// Ch2 is the underlying data+terminal signal channel.
	// Exposed for direct select compatibility with PipelineSignalReceiver.
	Ch2 chan PipelineSignal

	// NilBatchCnt is the number of legacy nil-batches or typed End signals this
	// channel must receive before it is considered done. 0 defaults to 1.
	NilBatchCnt int

	// --- terminal state ---
	done chan struct{}
	abrt chan struct{}

	initOnce sync.Once

	terminalMu  sync.Mutex
	terminalErr error

	fatalSignal    PipelineSignal
	fatalTerminal  bool
	fatalDelivered int
	fatalRemaining int
	endDelivered   int
	doneClosed     bool
	abortClosed    bool
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

// ResetForReuse reinitializes the edge before it is wired into a newly compiled
// pipeline. It must not be called while senders or receivers can still access
// the edge.
func (e *PipelineEdge) ResetForReuse(channelBufferSize int, nilBatchCnt int) {
	if e == nil {
		return
	}
	if channelBufferSize <= 0 {
		channelBufferSize = 1
	}

	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()

	e.Ch2 = make(chan PipelineSignal, channelBufferSize)
	e.NilBatchCnt = nilBatchCnt
	e.resetTerminalStateLocked()
}

// SetNilBatchCntForReuse updates the legacy nil-batch count while preserving
// the channel buffer. It also drains buffered stale signals and resets terminal
// state. It is compile/reuse-time only and must not race with live senders or
// receivers.
func (e *PipelineEdge) SetNilBatchCntForReuse(nilBatchCnt int) {
	if e == nil {
		return
	}

	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()

	e.NilBatchCnt = nilBatchCnt
	e.drainChannelLocked()
	e.resetTerminalStateLocked()
}

func (e *PipelineEdge) drainChannelLocked() {
	for {
		select {
		case <-e.Ch2:
		default:
			return
		}
	}
}

func (e *PipelineEdge) resetTerminalStateLocked() {
	e.done = make(chan struct{})
	e.abrt = make(chan struct{})
	e.initOnce = sync.Once{}
	e.terminalErr = nil
	e.fatalSignal = PipelineSignal{}
	e.fatalTerminal = false
	e.fatalDelivered = 0
	e.fatalRemaining = 0
	e.endDelivered = 0
	e.doneClosed = false
	e.abortClosed = false
}

// NewPipelineEdgeFromReg returns the same edge object behind a WaitRegister name.
// If reg is nil, a new edge with buffer size 1 is created.
func NewPipelineEdgeFromReg(reg *WaitRegister) *PipelineEdge {
	if reg == nil {
		return NewPipelineEdge(1, 0)
	}
	return reg
}

// AsWaitRegister returns the same edge object under the historical type name.
func (e *PipelineEdge) AsWaitRegister() *WaitRegister {
	if e == nil {
		return nil
	}
	return e
}

// Done returns a channel that is closed once the whole edge is terminal:
// all expected End signals have been delivered, or the edge receives Error/Abort.
// It never blocks.
func (e *PipelineEdge) Done() <-chan struct{} {
	if e == nil {
		c := make(chan struct{})
		close(c)
		return c
	}
	e.initTerminalState()
	return e.done
}

// Aborted returns a channel that is closed when the edge is aborted (cancellation,
// remote failure, etc.). Unlike Done, this does NOT close on normal End.
func (e *PipelineEdge) Aborted() <-chan struct{} {
	if e == nil {
		c := make(chan struct{})
		return c
	}
	e.initTerminalState()
	return e.abrt
}

// Err returns the terminal error, or nil if the edge ended without error.
func (e *PipelineEdge) Err() error {
	if e == nil {
		return nil
	}
	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()
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

// SendEnd sends one sender's End signal. Done closes after the edge has
// delivered the expected number of End signals.
func (e *PipelineEdge) SendEnd() bool {
	return e.trySendTerminal(NewEndSignal())
}

// SendError marks the edge failed and tries to enqueue an Error signal.
func (e *PipelineEdge) SendError(err error) bool {
	return e.trySendTerminal(NewErrorSignal(err))
}

// Abort marks the edge aborted and tries to enqueue an Abort signal.
func (e *PipelineEdge) Abort(err error) bool {
	return e.trySendTerminal(NewAbortSignal(err))
}

// TrySendEnd attempts a non-blocking End send. Returns true if delivered.
func (e *PipelineEdge) TrySendEnd() bool {
	return e.trySendTerminal(NewEndSignal())
}

// TrySendError attempts a non-blocking Error send. Returns true if delivered.
func (e *PipelineEdge) TrySendError(err error) bool {
	return e.trySendTerminal(NewErrorSignal(err))
}

// TryAbort attempts a non-blocking Abort. Returns true if delivered.
func (e *PipelineEdge) TryAbort(err error) bool {
	return e.trySendTerminal(NewAbortSignal(err))
}

// --- internal ---

func (e *PipelineEdge) initTerminalState() {
	e.initOnce.Do(func() {
		if e.done == nil {
			e.done = make(chan struct{})
		}
		if e.abrt == nil {
			e.abrt = make(chan struct{})
		}
	})
}

func (e *PipelineEdge) expectedEndCountLocked() int {
	if e.NilBatchCnt <= 0 {
		return 1
	}
	return e.NilBatchCnt
}

func (e *PipelineEdge) closeDoneLocked() {
	if !e.doneClosed {
		close(e.done)
		e.doneClosed = true
	}
}

func (e *PipelineEdge) closeAbortLocked() {
	if !e.abortClosed {
		close(e.abrt)
		e.abortClosed = true
	}
}

func (e *PipelineEdge) canDeliverEndLocked() bool {
	return !e.fatalTerminal && e.endDelivered < e.expectedEndCountLocked()
}

func (e *PipelineEdge) recordEndDeliveredLocked() {
	if e.fatalTerminal || e.doneClosed {
		return
	}
	e.endDelivered++
	if e.endDelivered >= e.expectedEndCountLocked() {
		e.closeDoneLocked()
	}
}

func (e *PipelineEdge) recordFatalTerminalLocked(signal PipelineSignal) PipelineSignal {
	if !e.fatalTerminal {
		e.fatalTerminal = true
		e.fatalSignal = signal
		e.terminalErr = signal.TerminalErr()
		e.fatalRemaining = e.expectedEndCountLocked() - e.endDelivered
		if e.fatalRemaining <= 0 {
			e.fatalRemaining = 1
		}
		e.closeDoneLocked()
		if signal.EventType == EventAbort {
			e.closeAbortLocked()
		}
	}
	return e.fatalSignal
}

func (e *PipelineEdge) trySendTerminal(signal PipelineSignal) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	if !signal.EventType.IsTerminal() {
		return e.trySend(signal)
	}
	e.initTerminalState()

	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()

	if signal.EventType == EventEnd {
		if !e.canDeliverEndLocked() {
			return false
		}
		select {
		case e.Ch2 <- signal:
			e.recordEndDeliveredLocked()
			return true
		default:
			return false
		}
	}

	if e.doneClosed && !e.fatalTerminal {
		return false
	}
	signal = e.recordFatalTerminalLocked(signal)
	if e.fatalDelivered >= e.fatalRemaining {
		return false
	}
	for e.fatalDelivered < e.fatalRemaining {
		select {
		case e.Ch2 <- signal:
			e.fatalDelivered++
		default:
			return false
		}
	}
	return true
}

func (e *PipelineEdge) sendTerminalWithContext(ctx context.Context, signal PipelineSignal) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	if !signal.EventType.IsTerminal() {
		return e.sendSignal(ctx, signal)
	}
	e.initTerminalState()

	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()

	if signal.EventType == EventEnd {
		if !e.canDeliverEndLocked() {
			return false
		}
		select {
		case e.Ch2 <- signal:
			e.recordEndDeliveredLocked()
			return true
		case <-ctx.Done():
			return false
		}
	}

	if e.doneClosed && !e.fatalTerminal {
		return false
	}
	signal = e.recordFatalTerminalLocked(signal)
	if e.fatalDelivered >= e.fatalRemaining {
		return false
	}
	for e.fatalDelivered < e.fatalRemaining {
		select {
		case e.Ch2 <- signal:
			e.fatalDelivered++
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func (e *PipelineEdge) sendSignal(ctx context.Context, signal PipelineSignal) bool {
	if e == nil {
		return false
	}
	if signal.EventType.IsTerminal() {
		return e.sendTerminalWithContext(ctx, signal)
	}
	e.initTerminalState()
	e.terminalMu.Lock()
	closedForData := e.doneClosed || e.fatalTerminal
	e.terminalMu.Unlock()
	if closedForData {
		return false
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	if ctx.Err() != nil {
		return false
	}
	select {
	case e.Ch2 <- signal:
		return true
	case <-ctx.Done():
	case <-e.abrt:
	case <-e.done:
	}
	return false
}

// trySend is the non-blocking path for data signals. Terminal signals are
// routed through trySendTerminal so Done() and Err() are updated consistently.
func (e *PipelineEdge) trySend(signal PipelineSignal) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	if signal.EventType.IsTerminal() {
		return e.trySendTerminal(signal)
	}
	e.initTerminalState()
	e.terminalMu.Lock()
	closedForData := e.doneClosed || e.fatalTerminal
	e.terminalMu.Unlock()
	if closedForData {
		return false
	}
	delivered := false
	select {
	case e.Ch2 <- signal:
		delivered = true
	default:
	}
	return delivered
}

// --- send helpers ---

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
