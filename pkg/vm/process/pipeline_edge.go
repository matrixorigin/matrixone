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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
)

// PipelineEdge is an explicit pipeline edge abstraction with typed lifecycle events.
// It owns the signal channel and durable terminal state. Each sender publishes
// End once; a shared edge counts senders rather than deduplicating identities.
//
// Invariants:
//  1. Terminal state (End/Error) is a protocol event, not an implicit nil batch.
//  2. End is counted per expected sender; Error are fatal first-wins
//     terminals that consume the edge's remaining sender count.
//  3. Done() provides an observable whole-edge terminal signal.
//  4. Data waits observe context/Done; terminal publication never blocks.
type PipelineEdge struct {
	// Ch2 is the underlying data+terminal signal channel.
	// Exposed for direct select compatibility with PipelineSignalReceiver.
	Ch2 chan PipelineSignal

	// NilBatchCnt is the number of legacy nil-batches or typed End signals this
	// channel must receive before it is considered done. 0 defaults to 1.
	NilBatchCnt int

	// --- terminal state ---
	done chan struct{}

	initOnce sync.Once

	terminalMu  sync.Mutex
	terminalErr error

	fatalSignal    PipelineSignal
	fatalTerminal  bool
	fatalDelivered int
	fatalRemaining int
	endRecorded    int
	doneClosed     bool
	// Published by closeDoneLocked and reset only after execution quiesces.
	// Data admission need not acquire the terminal mutation mutex.
	closedForData atomic.Bool
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

	// Drain the old channel so buffered signals' resources (mpool batches,
	// spool references) are released before we replace it. Without this,
	// the abandoned channel would be GC'd but mpool memory would leak.
	e.drainChannelLocked()
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

// ResetTerminalStateForReuse drains buffered stale signals and clears the
// edge's terminal state (endRecorded/doneClosed/fatalTerminal) so a cached
// pipeline (e.g. a prepared statement's compile) can deliver data and End
// signals through the edge again on its next execution. The channel buffer
// and NilBatchCnt are preserved. It is reuse-time only and must not race
// with live senders or receivers.
func (e *PipelineEdge) ResetTerminalStateForReuse() {
	if e == nil {
		return
	}
	e.SetNilBatchCntForReuse(e.NilBatchCnt)
}

// DrainForCleanup releases buffered signals after every sender and receiver
// has stopped. It preserves the terminal state and is also safe on a nil edge.
// Like ResetForReuse, it must never race with execution on this edge.
func (e *PipelineEdge) DrainForCleanup() {
	if e == nil {
		return
	}
	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()
	e.drainChannelLocked()
}

func (e *PipelineEdge) drainChannelLocked() {
	for {
		select {
		case sig := <-e.Ch2:
			sig.release()
		default:
			return
		}
	}
}

// release frees the resources held by a data signal (direct batch or spool
// reference) so drainChannelLocked does not leak mpool/spool memory. Terminal
// signals (End/Error) carry no data and are no-ops.
func (signal *PipelineSignal) release() {
	if signal.EventType.IsTerminal() {
		return
	}
	if signal.typ == GetFromIndex && signal.source != nil {
		// This notification owns a pending spool entry, not necessarily the
		// receiver's current batch. Reuse happens after execution quiesces;
		// consume the pending entry before releasing it, otherwise a late
		// producer's unread batch is abandoned when the old channel is drained.
		_, _ = signal.source.ReceiveBatch(signal.index)
		signal.source.ReleaseCurrent(signal.index)
	} else if signal.typ == GetDirectly && signal.directly != nil {
		signal.directly.Clean(signal.mp)
	}
}

func (e *PipelineEdge) resetTerminalStateLocked() {
	e.done = make(chan struct{})
	e.initOnce = sync.Once{}
	e.terminalErr = nil
	e.fatalSignal = PipelineSignal{}
	e.fatalTerminal = false
	e.fatalDelivered = 0
	e.fatalRemaining = 0
	e.endRecorded = 0
	e.doneClosed = false
	e.closedForData.Store(false)
}

// Done returns a channel that is closed once the whole edge is terminal:
// all expected End signals have been recorded, or the edge receives Error.
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

// Err returns the terminal error, or nil if the edge ended without error.
func (e *PipelineEdge) Err() error {
	if e == nil {
		return nil
	}
	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()
	return e.terminalErr
}

// terminalSignalSnapshot returns the edge's terminal state after synchronizing
// with any in-flight terminal sender. It is used by PipelineSignalReceiver when
// Done has closed but the terminal signal itself could not enter a full Ch2.
func (e *PipelineEdge) terminalSignalSnapshot() PipelineSignal {
	if e == nil {
		return NewEndSignal()
	}
	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()
	if e.fatalTerminal {
		return e.fatalSignal
	}
	return NewEndSignal()
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

// --- internal ---

func (e *PipelineEdge) initTerminalState() {
	e.initOnce.Do(func() {
		if e.done == nil {
			e.done = make(chan struct{})
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
		e.closedForData.Store(true)
		close(e.done)
		e.doneClosed = true
	}
}

func (e *PipelineEdge) canDeliverEndLocked() bool {
	return !e.fatalTerminal && e.endRecorded < e.expectedEndCountLocked()
}

func (e *PipelineEdge) recordEndLocked() {
	if e.fatalTerminal || e.doneClosed {
		return
	}
	e.endRecorded++
	if e.endRecorded >= e.expectedEndCountLocked() {
		e.closeDoneLocked()
	}
}

func (e *PipelineEdge) recordFatalTerminalLocked(signal PipelineSignal) PipelineSignal {
	if !e.fatalTerminal {
		e.fatalTerminal = true
		e.fatalSignal = signal
		e.terminalErr = signal.TerminalErr()
		e.fatalRemaining = e.expectedEndCountLocked() - e.endRecorded
		if e.fatalRemaining <= 0 {
			e.fatalRemaining = 1
		}
		e.closeDoneLocked()
	}
	return e.fatalSignal
}

func (e *PipelineEdge) sendSignal(ctx context.Context, signal PipelineSignal) bool {
	if e == nil {
		return false
	}

	e.initTerminalState()
	if e.closedForData.Load() {
		return false
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	if ctx.Err() != nil {
		return false
	}
	// Most data notifications fit immediately. Keep terminal/cancel checks
	// above and use the full interruptible select only for backpressure.
	select {
	case e.Ch2 <- signal:
		return true
	default:
	}
	select {
	case e.Ch2 <- signal:
		return true
	case <-ctx.Done():
	case <-e.done:
	}
	return false
}

// --- send helpers ---

// PublishTerminal records cleanup independently of data-channel capacity.
// effective contains the first fatal cause or the established graceful End;
// ok means the terminal state is durable, even if its message did not fit.
func (e *PipelineEdge) PublishTerminal(signal PipelineSignal) (PipelineSignal, bool) {
	if e == nil || e.Ch2 == nil {
		return PipelineSignal{}, false
	}
	if !signal.EventType.IsTerminal() {
		return PipelineSignal{}, false
	}
	e.initTerminalState()

	e.terminalMu.Lock()
	defer e.terminalMu.Unlock()

	if signal.EventType == EventEnd {
		if !e.canDeliverEndLocked() {
			if e.fatalTerminal {
				return e.fatalSignal, true
			}
			return NewEndSignal(), true
		}
		// End is a durable edge state, not merely a best-effort channel
		// message.  Record it even when buffered data occupies Ch2.  Once all
		// expected senders have ended, Done wakes the receiver; the receiver
		// drains Ch2 first and then synthesizes any End that did not fit.
		select {
		case e.Ch2 <- signal:
		default:
		}
		e.recordEndLocked()
		return signal, true
	}

	if e.doneClosed && !e.fatalTerminal {
		return NewEndSignal(), true
	}
	signal = e.recordFatalTerminalLocked(signal)
	if e.fatalDelivered >= e.fatalRemaining {
		return signal, true
	}
	for e.fatalDelivered < e.fatalRemaining {
		select {
		case e.Ch2 <- signal:
			e.fatalDelivered++
		default:
			return signal, true
		}
	}
	return signal, true
}
