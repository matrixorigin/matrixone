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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

	// OrderedStream marks an edge whose single producer must preserve total
	// ordering across batch boundaries. It is compile/runtime topology metadata,
	// not a data signal property.
	OrderedStream bool

	// --- terminal state ---
	done chan struct{}
	abrt chan struct{}

	initOnce sync.Once

	terminalMu  sync.Mutex
	terminalErr error

	// readyMu protects one-shot continuation callbacks. A receiver registers a
	// callback when it observes no data; the next data/terminal publication
	// drains it without parking an execution goroutine. Capacity callbacks are
	// the dual signal used by producers under backpressure.
	readyMu           sync.Mutex
	readyCallbacks    []func()
	capacityCallbacks []func()

	fatalSignal    PipelineSignal
	fatalTerminal  bool
	fatalDelivered int
	fatalRemaining int
	endRecorded    int
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

	// Drain the old channel so buffered signals' resources (mpool batches,
	// spool references) are released before we replace it. Without this,
	// the abandoned channel would be GC'd but mpool memory would leak.
	for {
		select {
		case sig := <-e.Ch2:
			sig.release()
		default:
			goto done
		}
	}
done:
	e.Ch2 = make(chan PipelineSignal, channelBufferSize)
	e.NilBatchCnt = nilBatchCnt
	e.OrderedStream = false
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
// signals (End/Error/Abort) carry no data and are no-ops.
func (signal *PipelineSignal) release() {
	if signal.EventType.IsTerminal() {
		return
	}
	if signal.typ == GetFromIndex && signal.source != nil {
		signal.source.ReleaseCurrent(signal.index)
	} else if signal.typ == GetDirectly && signal.directly != nil {
		signal.directly.Clean(signal.mp)
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
	e.endRecorded = 0
	e.doneClosed = false
	e.abortClosed = false
	e.readyMu.Lock()
	e.readyCallbacks = nil
	e.capacityCallbacks = nil
	e.readyMu.Unlock()
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
// all expected End signals have been recorded, or the edge receives Error/Abort.
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

// TrySendData publishes a queued spool reference without waiting for edge
// capacity. The spool slot remains queued until this signal is delivered, so
// callers can safely yield and retry the same edge.
func (e *PipelineEdge) TrySendData(spool *pSpool.PipelineSpool, idx int) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return e.trySend(NewPipelineSignalToGetFromSpool(spool, idx))
}

// SendDataDirect sends a batch directly (not via spool) through the edge.
func (e *PipelineEdge) SendDataDirect(ctx context.Context, bat *batch.Batch, mp *mpool.MPool) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return e.sendSignal(ctx, NewPipelineSignalToDirectly(bat, nil, mp))
}

// TrySendDataDirect publishes a directly-owned batch without waiting for edge
// capacity. The caller retains ownership when it returns false and may retry
// after RegisterCapacityReady fires.
func (e *PipelineEdge) TrySendDataDirect(bat *batch.Batch, mp *mpool.MPool) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return e.trySend(NewPipelineSignalToDirectly(bat, nil, mp))
}

// SendEnd records one sender's End signal. It also enqueues the signal when
// capacity is available. Done closes after the expected number is recorded.
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

// TrySendEnd records one sender's End without blocking. It returns false only
// when this edge is already terminal or has recorded every expected End.
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
	if signal.EventType == EventEnd {
		if !e.canDeliverEndLocked() {
			e.terminalMu.Unlock()
			return false
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
		e.terminalMu.Unlock()
		e.notifyReady()
		e.notifyCapacity()
		return true
	}

	if e.doneClosed && !e.fatalTerminal {
		e.terminalMu.Unlock()
		return false
	}
	signal = e.recordFatalTerminalLocked(signal)
	if e.fatalDelivered >= e.fatalRemaining {
		e.terminalMu.Unlock()
		e.notifyReady()
		e.notifyCapacity()
		return false
	}
	delivered := true
fatalSendLoop:
	for e.fatalDelivered < e.fatalRemaining {
		select {
		case e.Ch2 <- signal:
			e.fatalDelivered++
		default:
			delivered = false
			break fatalSendLoop
		}
		if !delivered {
			break
		}
	}
	e.terminalMu.Unlock()
	e.notifyReady()
	e.notifyCapacity()
	return delivered
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
	if signal.EventType == EventEnd {
		if !e.canDeliverEndLocked() {
			e.terminalMu.Unlock()
			return false
		}
		// Terminal progress must not depend on spare data-channel capacity.
		// A non-blocking enqueue preserves the common fast path; durable state
		// plus Done is the fallback delivery path.
		select {
		case e.Ch2 <- signal:
		default:
		}
		e.recordEndLocked()
		e.terminalMu.Unlock()
		e.notifyReady()
		e.notifyCapacity()
		return true
	}

	if e.doneClosed && !e.fatalTerminal {
		e.terminalMu.Unlock()
		return false
	}
	signal = e.recordFatalTerminalLocked(signal)
	if e.fatalDelivered >= e.fatalRemaining {
		e.terminalMu.Unlock()
		e.notifyReady()
		e.notifyCapacity()
		return false
	}
	// Fatal state is durable and wakes PipelineSignalReceiver through Done.
	// Never make this control path wait behind the data channel it terminates;
	// enqueue as many fatal signals as fit and let the receiver synthesize any
	// missing remainder from the recorded state.
	delivered := true
fatalSendLoop:
	for e.fatalDelivered < e.fatalRemaining {
		select {
		case e.Ch2 <- signal:
			e.fatalDelivered++
		default:
			delivered = false
			break fatalSendLoop
		}
		if !delivered {
			break
		}
	}
	e.terminalMu.Unlock()
	e.notifyReady()
	e.notifyCapacity()
	return delivered
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
		e.notifyReady()
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
	if delivered {
		e.notifyReady()
	}
	return delivered
}

// RegisterReady arms a one-shot callback for the next readable signal or
// terminal transition. It never blocks and invokes the callback outside the
// edge lock. The immediate check closes the check/register race.
func (e *PipelineEdge) RegisterReady(callback func()) error {
	if callback == nil {
		return moerr.NewInvalidInputNoCtx("nil pipeline readiness callback")
	}
	if e == nil || e.Ch2 == nil {
		callback()
		return nil
	}
	e.terminalMu.Lock()
	ready := e.doneClosed || e.fatalTerminal || len(e.Ch2) > 0
	e.readyMu.Lock()
	if !ready {
		e.readyCallbacks = append(e.readyCallbacks, callback)
	}
	e.readyMu.Unlock()
	e.terminalMu.Unlock()
	if ready {
		callback()
	}
	return nil
}

// RegisterCapacityReady arms a one-shot callback for downstream channel
// capacity. It is the non-blocking replacement for polling
// WaitPipelineSignalCapacity.
func (e *PipelineEdge) RegisterCapacityReady(callback func()) error {
	if callback == nil {
		return moerr.NewInvalidInputNoCtx("nil pipeline capacity callback")
	}
	if e == nil || e.Ch2 == nil || cap(e.Ch2) == 0 {
		callback()
		return nil
	}
	// Keep the capacity check and callback publication under the same lock
	// used by notifyCapacity.  A receiver can drain Ch2 concurrently without
	// taking terminalMu; checking len(Ch2) first and only then acquiring
	// readyMu would therefore lose the wakeup between the check and append.
	// Once readyMu is held, either we observe the newly available slot or the
	// receiver's notifyCapacity waits until the callback is visible.
	e.terminalMu.Lock()
	e.readyMu.Lock()
	ready := e.doneClosed || e.fatalTerminal || len(e.Ch2) < cap(e.Ch2)
	if !ready {
		e.capacityCallbacks = append(e.capacityCallbacks, callback)
	}
	e.readyMu.Unlock()
	e.terminalMu.Unlock()
	if ready {
		callback()
	}
	return nil
}

func (e *PipelineEdge) notifyReady() {
	if e == nil {
		return
	}
	e.readyMu.Lock()
	callbacks := e.readyCallbacks
	e.readyCallbacks = nil
	e.readyMu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

func (e *PipelineEdge) notifyCapacity() {
	if e == nil {
		return
	}
	// Serialize notification with RegisterCapacityReady's terminal/capacity
	// check.  Without the terminal lock, a receiver could drain Ch2 after the
	// registration check but before the callback was appended, and the
	// notification would observe an empty callback list.  The producer would
	// then remain parked on an edge that already had capacity.
	e.terminalMu.Lock()
	e.readyMu.Lock()
	callbacks := e.capacityCallbacks
	e.capacityCallbacks = nil
	e.readyMu.Unlock()
	e.terminalMu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

// notifyCapacityOnReceive publishes the capacity transition caused by a
// receiver consuming one queued signal.
func (e *PipelineEdge) notifyCapacityOnReceive() {
	e.notifyCapacity()
}

// --- send helpers ---

// SendSignalWithTimeout sends data with an optional timeout. Terminal state is
// published without blocking; in particular, End does not wait for Ch2 space.
func (e *PipelineEdge) SendSignalWithTimeout(signal PipelineSignal, timeout time.Duration) bool {
	if e == nil || e.Ch2 == nil {
		return false
	}
	return SendPipelineSignalWithTimeout(e.AsWaitRegister(), signal, timeout)
}

// SendSignalWithContext sends data with context. Terminal publication is
// non-blocking, and End is recorded even when ctx has already been canceled.
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
