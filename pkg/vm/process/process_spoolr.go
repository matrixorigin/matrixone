// Copyright 2024 Matrix Origin
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
	"reflect"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
)

var (
	// PipelineCleanupWaitTimeout bounds cleanup-only waits for terminal
	// pipeline signals. Normal query execution does not use this timeout.
	PipelineCleanupWaitTimeout = 30 * time.Second

	// ErrPipelineEndSignalDeliveryFailed marks a successful cleanup path that
	// could not durably record its normal End and had to fall back to abort.
	ErrPipelineEndSignalDeliveryFailed = moerr.NewInternalErrorNoCtx("pipeline end signal delivery failed")

	// ErrPipelineTerminalWithoutCause marks a failure/abort terminal event whose
	// caller did not provide the original cause. Error and Abort terminal events
	// must always carry a non-nil error so receivers cannot confuse failure with
	// graceful End.
	ErrPipelineTerminalWithoutCause = moerr.NewInternalErrorNoCtx("pipeline terminal event missing cause")
)

type PipelineActionType uint8

const (
	GetFromIndex PipelineActionType = iota
	GetDirectly
)

// PipelineEventType is the typed terminal event for pipeline edges.
// It makes the termination protocol explicit instead of relying on nil-batch
// conventions and NilBatchCnt counting.
type PipelineEventType uint8

const (
	// EventData carries a normal data batch. A nil batch is also accepted
	// as a legacy per-sender end marker.
	EventData PipelineEventType = iota
	// EventEnd signals graceful pipeline completion with no error.
	EventEnd
	// EventError signals pipeline completion due to an error.
	EventError
)

func (t PipelineEventType) IsTerminal() bool {
	return t == EventEnd || t == EventError
}

func (t PipelineEventType) String() string {
	switch t {
	case EventData:
		return "Data"
	case EventEnd:
		return "End"
	case EventError:
		return "Error"
	default:
		return "Unknown"
	}
}

type PipelineSignal struct {
	typ PipelineActionType

	// EventType is set for typed terminal events (EventEnd/EventError).
	// When EventType != EventData, this signal carries a terminal event, not a data batch.
	EventType PipelineEventType

	// terminalErr carries the error for EventError.
	terminalErr error

	// for case: GetFromIndex
	index  int
	source *pSpool.PipelineSpool

	// for case: GetDirectly
	mp       *mpool.MPool
	directly *batch.Batch
	errInfo  error
}

// NewPipelineSignalToGetFromSpool return a signal indicate the receiver to get data from source by index.
func NewPipelineSignalToGetFromSpool(source *pSpool.PipelineSpool, index int) PipelineSignal {
	return PipelineSignal{
		typ:       GetFromIndex,
		EventType: EventData,
		source:    source,
		index:     index,
		mp:        nil,
		directly:  nil,
	}
}

// NewPipelineSignalToDirectly return a signal indicates the receiver to get data from signal directly.
func NewPipelineSignalToDirectly(data *batch.Batch, err error, mp *mpool.MPool) PipelineSignal {
	return PipelineSignal{
		typ:       GetDirectly,
		EventType: EventData,
		source:    nil,
		index:     0,
		directly:  data,
		mp:        mp,
		errInfo:   err,
	}
}

// NewEndSignal returns a PipelineSignal carrying an explicit EventEnd.
// Each sender publishes it once; a shared edge counts all expected senders.
func NewEndSignal() PipelineSignal {
	return PipelineSignal{
		typ:       GetDirectly,
		EventType: EventEnd,
	}
}

// NewErrorSignal returns a PipelineSignal carrying an explicit EventError.
// It is idempotent: receivers must treat multiple Error signals safely.
func NewErrorSignal(err error) PipelineSignal {
	if err == nil {
		err = ErrPipelineTerminalWithoutCause
	}
	return PipelineSignal{
		typ:         GetDirectly,
		EventType:   EventError,
		terminalErr: MarkPipelineFailure(err),
	}
}

func NormalizePipelineCleanupError(pipelineFailed bool, err error) error {
	if err != nil {
		return err
	}
	if pipelineFailed {
		return ErrPipelineTerminalWithoutCause
	}
	return nil
}

// ResolvePipelineSpoolAbortError preserves an error already recorded on a
// receiver edge when normal End delivery fails and the spool must be aborted.
// The synthetic delivery error is only used when none of the edges has a more
// specific terminal cause.
func ResolvePipelineSpoolAbortError(regs ...*WaitRegister) error {
	for _, reg := range regs {
		if reg == nil {
			continue
		}
		if err := reg.Err(); err != nil &&
			!errors.Is(err, ErrPipelineEndSignalDeliveryFailed) {
			return err
		}
	}
	return ErrPipelineEndSignalDeliveryFailed
}

// BuildCleanupSignal returns the appropriate terminal signal for pipeline cleanup.
// EventError is used when pipelineFailed=true or err!=nil; EventEnd otherwise.
func BuildCleanupSignal(pipelineFailed bool, err error) PipelineSignal {
	if cleanupErr := NormalizePipelineCleanupError(pipelineFailed, err); cleanupErr != nil {
		return NewErrorSignal(cleanupErr)
	}
	return NewEndSignal()
}

// TerminalErr returns the error carried by EventError.
func (signal PipelineSignal) TerminalErr() error {
	return signal.terminalErr
}

func WaitRegisterChannelState(reg *WaitRegister) (int, int) {
	if reg == nil || reg.Ch2 == nil {
		return 0, 0
	}
	return len(reg.Ch2), cap(reg.Ch2)
}

// Action will get the input batch from one place according to which type this signal is.
//
// For terminal events (EventEnd/EventError), nil batch is returned.
// The result batch of this function is an READ-ONLY one.
func (signal PipelineSignal) Action() (data *batch.Batch, info error) {
	// Terminal events never carry data.
	if signal.EventType.IsTerminal() {
		return nil, MarkPipelineFailure(signal.terminalErr)
	}

	if signal.typ == GetFromIndex {
		data, info = signal.source.ReceiveBatch(signal.index)
		return data, MarkPipelineFailure(info)
	}

	return signal.directly, MarkPipelineFailure(signal.errInfo)
}

type PipelineSignalReceiver struct {
	usrCtx context.Context
	srcReg []*WaitRegister
	doneCh []<-chan struct{}

	alive int
	// Rotate ready-edge polling so a busy input cannot starve another input's
	// data or durable terminal event.
	nextReady int

	// reflect-select cases for receivers with more than eight inputs. The first
	// case monitors usrCtx; each input then contributes a Ch2 case and a Done
	// case.
	regs []reflect.SelectCase
	// how many terminal signals each srcReg still expects.
	nbs []int

	// Keep the current signal inline: retaining the address of GetNextBatch's
	// local message would allocate one heap object for every delivered batch.
	currentSignal PipelineSignal
	hasCurrent    bool
}

type PipelineSignalReceiverState struct {
	Alive      int
	NilBatches []int
	ChannelLen []int
	ChannelCap []int
}

func InitPipelineSignalReceiver(runningCtx context.Context, regs []*WaitRegister) *PipelineSignalReceiver {
	nbs := make([]int, len(regs))
	srcRegs := make([]*WaitRegister, len(regs))
	doneCh := make([]<-chan struct{}, len(regs))

	for i, reg := range regs {
		// 0 is default number, it takes a same effect as 1.
		if reg.NilBatchCnt == 0 {
			nbs[i] = 1
		} else {
			nbs[i] = reg.NilBatchCnt
		}
		srcRegs[i] = reg
		doneCh[i] = reg.Done()
	}

	// if regs were not much, we will use an optimized method to receive msg.
	// and there is no need to init the `reflect.SelectCase`.
	var scs []reflect.SelectCase = nil
	if len(regs) > 8 {
		scs = make([]reflect.SelectCase, 0, len(regs)*2+1)
		scs = append(scs, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(runningCtx.Done())})
		for i := range regs {
			scs = append(scs, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(regs[i].Ch2)})
			scs = append(scs, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(doneCh[i])})
		}
	}

	return &PipelineSignalReceiver{
		usrCtx: runningCtx,
		srcReg: srcRegs,
		doneCh: doneCh,
		alive:  len(regs),
		regs:   scs,
		nbs:    nbs,
	}
}

func (receiver *PipelineSignalReceiver) setCurrent(current *PipelineSignal) {
	receiver.currentSignal = *current
	receiver.hasCurrent = true
}

func (receiver *PipelineSignalReceiver) releaseCurrent() {
	if receiver.hasCurrent {
		// Terminal events (End/Error) don't carry batch data,
		// so there is nothing to release.
		if receiver.currentSignal.EventType.IsTerminal() {
			receiver.currentSignal = PipelineSignal{}
			receiver.hasCurrent = false
			return
		}
		if receiver.currentSignal.typ == GetFromIndex {
			receiver.currentSignal.source.ReleaseCurrent(
				receiver.currentSignal.index)
		} else if receiver.currentSignal.typ == GetDirectly {
			if receiver.currentSignal.directly != nil {
				receiver.currentSignal.directly.Clean(receiver.currentSignal.mp)
			}
		}

		receiver.currentSignal = PipelineSignal{}
		receiver.hasCurrent = false
	}
}

func (receiver *PipelineSignalReceiver) GetNextBatch(
	analyzer Analyzer) (content *batch.Batch, info error) {
	var chosen int
	var msg PipelineSignal

	for {
		// When this input already has its next notification, ReceiveBatch can
		// release the preceding batch and acquire the next one under a single
		// spool lock. If the queue is empty we must release before waiting, so
		// a producer blocked on free capacity can make progress.
		queued := false
		if receiver.hasCurrent && receiver.currentSignal.typ == GetFromIndex &&
			len(receiver.srcReg) == 1 && receiver.usrCtx.Err() == nil {
			msg, queued = tryReceivePipelineSignal(receiver.srcReg[0].Ch2)
			if queued {
				chosen = 1
				if msg.EventType == EventData && msg.typ == GetFromIndex &&
					msg.source == receiver.currentSignal.source && msg.index == receiver.currentSignal.index {
					receiver.currentSignal = PipelineSignal{}
					receiver.hasCurrent = false
				} else {
					receiver.releaseCurrent()
				}
			}
		}
		if !queued {
			receiver.releaseCurrent()
		}

		if receiver.alive == 0 {
			return nil, nil
		}

		if !queued {
			if analyzer != nil {
				start := time.Now()
				chosen, msg = receiver.listenToAll()
				analyzer.WaitStop(start)
			} else {
				chosen, msg = receiver.listenToAll()
			}
			if chosen == 0 {
				return nil, receiver.contextDoneError()
			}
		}

		// Handle typed terminal events: End, Error.
		// Sender protocol guarantee: each sender sends exactly one terminal signal
		// per edge (either typed or legacy nil-batch, never both). This prevents
		// double-decrementing nbs[idx] in removeIdxReceiver.
		if msg.EventType.IsTerminal() {
			receiver.removeIdxReceiver(chosen)
			if msg.EventType == EventEnd {
				// Graceful end from one sender — continue draining others.
				continue
			}
			// EventError: propagate the error.
			return nil, msg.terminalErr
		}

		content, info = msg.Action()
		if content == nil {
			// Legacy nil-batch path: treat nil batch from GetFromIndex or
			// GetDirectly as a per-sender end signal.
			receiver.removeIdxReceiver(chosen)
			if info != nil {
				return nil, info
			}
			continue
		}

		receiver.setCurrent(&msg)
		if analyzer != nil {
			analyzer.Input(content)
		}
		return content, info
	}
}

// idx is start from 0, this is the index of receiver at the receiver.regs.
func (receiver *PipelineSignalReceiver) removeIdxReceiver(chosen int) {
	idx := chosen - 1

	receiver.nbs[idx]--
	if receiver.nbs[idx] == 0 {
		// remove the unused channel.
		receiver.srcReg = append(receiver.srcReg[:idx], receiver.srcReg[idx+1:]...)
		receiver.doneCh = append(receiver.doneCh[:idx], receiver.doneCh[idx+1:]...)
		receiver.nbs = append(receiver.nbs[:idx], receiver.nbs[idx+1:]...)

		if len(receiver.regs) > 0 {
			caseIdx := idx*2 + 1
			receiver.regs = append(receiver.regs[:caseIdx], receiver.regs[caseIdx+2:]...)
		}
		receiver.alive--
	}
}

func (receiver *PipelineSignalReceiver) State() PipelineSignalReceiverState {
	if receiver == nil {
		return PipelineSignalReceiverState{}
	}

	state := PipelineSignalReceiverState{
		Alive:      receiver.alive,
		NilBatches: slices.Clone(receiver.nbs),
		ChannelLen: make([]int, len(receiver.srcReg)),
		ChannelCap: make([]int, len(receiver.srcReg)),
	}
	for i, reg := range receiver.srcReg {
		if reg == nil || reg.Ch2 == nil {
			continue
		}
		state.ChannelLen[i] = len(reg.Ch2)
		state.ChannelCap[i] = cap(reg.Ch2)
	}
	return state
}

func (receiver *PipelineSignalReceiver) listenToAll() (int, PipelineSignal) {
	// Poll ready inputs before building a multi-way (or reflection) select.
	// Check both data and Done for each edge, with rotation across calls; data
	// retains priority over that edge's durable terminal, without starving a
	// different edge's failure behind a continuously busy producer.
	if count := len(receiver.srcReg); count > 1 {
		if receiver.usrCtx.Err() != nil {
			return 0, PipelineSignal{}
		}
		idx := receiver.nextReady
		if idx >= count {
			idx = 0
		}
		for range count {
			if signal, ok := tryReceivePipelineSignal(receiver.srcReg[idx].Ch2); ok {
				receiver.nextReady = idx + 1
				return idx + 1, signal
			}
			select {
			case <-receiver.doneCh[idx]:
				receiver.nextReady = idx + 1
				return receiver.receiveSignalOrTerminal(idx)
			default:
			}
			idx++
			if idx == count {
				idx = 0
			}
		}
	}
	// hard codes for less interface convert and less reflect.
	switch len(receiver.srcReg) {
	case 1:
		return receiver.listenToSingleEntry()
	case 2:
		return receiver.listenToTwoEntry()
	case 3:
		return receiver.listenToThreeEntry()
	case 4:
		return receiver.listenToFourEntry()
	case 5:
		return receiver.listenToFiveEntry()
	case 6:
		return receiver.listenToSixEntry()
	case 7:
		return receiver.listenToSevenEntry()
	case 8:
		return receiver.listenToEightEntry()
	}

	// common case.
	chosen, value, ok := reflect.Select(receiver.regs)
	if chosen == 0 {
		return 0, PipelineSignal{}
	}

	idx := (chosen - 1) / 2
	// Data cases occupy odd reflect indexes; Done cases occupy even indexes.
	if chosen%2 == 1 {
		if !ok {
			panic("unexpected sender close during GetNextBatch")
		}
		return idx + 1, value.Interface().(PipelineSignal)
	}
	return receiver.receiveSignalOrTerminal(idx)
}

// receiveSignalOrTerminal handles an edge whose Done channel is ready. Ch2
// remains the ordered source of truth: buffered data and successfully enqueued
// terminal signals must be consumed before a terminal recorded only in the
// edge's durable state is synthesized.
func (receiver *PipelineSignalReceiver) receiveSignalOrTerminal(idx int) (int, PipelineSignal) {
	reg := receiver.srcReg[idx]
	if signal, ok := tryReceivePipelineSignal(reg.Ch2); ok {
		return idx + 1, signal
	}

	// Synchronize with an in-flight terminal sender. Recheck Ch2 after taking
	// the snapshot to avoid synthesizing a duplicate terminal.
	terminal := reg.terminalSignalSnapshot()
	if signal, ok := tryReceivePipelineSignal(reg.Ch2); ok {
		return idx + 1, signal
	}
	return idx + 1, terminal
}

func tryReceivePipelineSignal(ch <-chan PipelineSignal) (PipelineSignal, bool) {
	select {
	case signal, ok := <-ch:
		if !ok {
			panic("unexpected sender close during GetNextBatch")
		}
		return signal, true
	default:
		return PipelineSignal{}, false
	}
}

func (receiver *PipelineSignalReceiver) WaitingEnd() {
	receiver.WaitingEndWithTimeout(PipelineCleanupWaitTimeout)
}

// WaitingEndWithTimeout is cleanup-only. It intentionally uses a detached
// cleanup context because Pipeline.Cleanup cancels the process context before
// operator Reset runs, but cleanup still needs a bounded chance to drain
// terminal pipeline signals and release spool references.
func (receiver *PipelineSignalReceiver) WaitingEndWithTimeout(timeout time.Duration) bool {
	cleanupCtx := context.TODO()
	cancel := func() {}
	if timeout > 0 {
		cleanupCtx, cancel = context.WithTimeout(context.TODO(), timeout)
	}
	defer cancel()

	if len(receiver.regs) > 0 {
		receiver.regs[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cleanupCtx.Done())}
	}

	receiver.usrCtx = cleanupCtx
	for {
		if receiver.alive == 0 {
			return true
		}
		_, _ = receiver.GetNextBatch(nil)
		if cleanupCtx.Err() != nil {
			receiver.releaseCurrent()
			return false
		}
	}
}

func (receiver *PipelineSignalReceiver) listenToSingleEntry() (chosen int, v PipelineSignal) {
	// Avoid a three-way select when the ordinary data path is already ready.
	// Cancellation is checked first; the blocking path still watches Done.
	if receiver.usrCtx.Err() != nil {
		return 0, v
	}
	if signal, ok := tryReceivePipelineSignal(receiver.srcReg[0].Ch2); ok {
		return 1, signal
	}
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	}
}

func (receiver *PipelineSignalReceiver) listenToTwoEntry() (chosen int, v PipelineSignal) {
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case v = <-receiver.srcReg[1].Ch2:
		return 2, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	case <-receiver.doneCh[1]:
		return receiver.receiveSignalOrTerminal(1)
	}
}

func (receiver *PipelineSignalReceiver) listenToThreeEntry() (chosen int, v PipelineSignal) {
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case v = <-receiver.srcReg[1].Ch2:
		return 2, v
	case v = <-receiver.srcReg[2].Ch2:
		return 3, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	case <-receiver.doneCh[1]:
		return receiver.receiveSignalOrTerminal(1)
	case <-receiver.doneCh[2]:
		return receiver.receiveSignalOrTerminal(2)
	}
}

func (receiver *PipelineSignalReceiver) listenToFourEntry() (chosen int, v PipelineSignal) {
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case v = <-receiver.srcReg[1].Ch2:
		return 2, v
	case v = <-receiver.srcReg[2].Ch2:
		return 3, v
	case v = <-receiver.srcReg[3].Ch2:
		return 4, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	case <-receiver.doneCh[1]:
		return receiver.receiveSignalOrTerminal(1)
	case <-receiver.doneCh[2]:
		return receiver.receiveSignalOrTerminal(2)
	case <-receiver.doneCh[3]:
		return receiver.receiveSignalOrTerminal(3)
	}
}

func (receiver *PipelineSignalReceiver) listenToFiveEntry() (chosen int, v PipelineSignal) {
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case v = <-receiver.srcReg[1].Ch2:
		return 2, v
	case v = <-receiver.srcReg[2].Ch2:
		return 3, v
	case v = <-receiver.srcReg[3].Ch2:
		return 4, v
	case v = <-receiver.srcReg[4].Ch2:
		return 5, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	case <-receiver.doneCh[1]:
		return receiver.receiveSignalOrTerminal(1)
	case <-receiver.doneCh[2]:
		return receiver.receiveSignalOrTerminal(2)
	case <-receiver.doneCh[3]:
		return receiver.receiveSignalOrTerminal(3)
	case <-receiver.doneCh[4]:
		return receiver.receiveSignalOrTerminal(4)
	}
}

func (receiver *PipelineSignalReceiver) listenToSixEntry() (chosen int, v PipelineSignal) {
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case v = <-receiver.srcReg[1].Ch2:
		return 2, v
	case v = <-receiver.srcReg[2].Ch2:
		return 3, v
	case v = <-receiver.srcReg[3].Ch2:
		return 4, v
	case v = <-receiver.srcReg[4].Ch2:
		return 5, v
	case v = <-receiver.srcReg[5].Ch2:
		return 6, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	case <-receiver.doneCh[1]:
		return receiver.receiveSignalOrTerminal(1)
	case <-receiver.doneCh[2]:
		return receiver.receiveSignalOrTerminal(2)
	case <-receiver.doneCh[3]:
		return receiver.receiveSignalOrTerminal(3)
	case <-receiver.doneCh[4]:
		return receiver.receiveSignalOrTerminal(4)
	case <-receiver.doneCh[5]:
		return receiver.receiveSignalOrTerminal(5)
	}
}

func (receiver *PipelineSignalReceiver) listenToSevenEntry() (chosen int, v PipelineSignal) {
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case v = <-receiver.srcReg[1].Ch2:
		return 2, v
	case v = <-receiver.srcReg[2].Ch2:
		return 3, v
	case v = <-receiver.srcReg[3].Ch2:
		return 4, v
	case v = <-receiver.srcReg[4].Ch2:
		return 5, v
	case v = <-receiver.srcReg[5].Ch2:
		return 6, v
	case v = <-receiver.srcReg[6].Ch2:
		return 7, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	case <-receiver.doneCh[1]:
		return receiver.receiveSignalOrTerminal(1)
	case <-receiver.doneCh[2]:
		return receiver.receiveSignalOrTerminal(2)
	case <-receiver.doneCh[3]:
		return receiver.receiveSignalOrTerminal(3)
	case <-receiver.doneCh[4]:
		return receiver.receiveSignalOrTerminal(4)
	case <-receiver.doneCh[5]:
		return receiver.receiveSignalOrTerminal(5)
	case <-receiver.doneCh[6]:
		return receiver.receiveSignalOrTerminal(6)
	}
}

func (receiver *PipelineSignalReceiver) listenToEightEntry() (chosen int, v PipelineSignal) {
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
	case v = <-receiver.srcReg[1].Ch2:
		return 2, v
	case v = <-receiver.srcReg[2].Ch2:
		return 3, v
	case v = <-receiver.srcReg[3].Ch2:
		return 4, v
	case v = <-receiver.srcReg[4].Ch2:
		return 5, v
	case v = <-receiver.srcReg[5].Ch2:
		return 6, v
	case v = <-receiver.srcReg[6].Ch2:
		return 7, v
	case v = <-receiver.srcReg[7].Ch2:
		return 8, v
	case <-receiver.doneCh[0]:
		return receiver.receiveSignalOrTerminal(0)
	case <-receiver.doneCh[1]:
		return receiver.receiveSignalOrTerminal(1)
	case <-receiver.doneCh[2]:
		return receiver.receiveSignalOrTerminal(2)
	case <-receiver.doneCh[3]:
		return receiver.receiveSignalOrTerminal(3)
	case <-receiver.doneCh[4]:
		return receiver.receiveSignalOrTerminal(4)
	case <-receiver.doneCh[5]:
		return receiver.receiveSignalOrTerminal(5)
	case <-receiver.doneCh[6]:
		return receiver.receiveSignalOrTerminal(6)
	case <-receiver.doneCh[7]:
		return receiver.receiveSignalOrTerminal(7)
	}
}

func (receiver *PipelineSignalReceiver) contextDoneError() error {
	if errors.Is(receiver.usrCtx.Err(), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	// A durable input failure must survive a concurrent successful stop.
	for _, reg := range receiver.srcReg {
		if err := reg.Err(); err != nil {
			return MarkPipelineFailure(err)
		}
	}
	cause := context.Cause(receiver.usrCtx)
	if cause == ErrPipelineStopped {
		return nil
	}
	return MarkPipelineFailure(cause)
}
