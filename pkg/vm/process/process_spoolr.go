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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"reflect"
	"time"
)

var (
	// PipelineCleanupWaitTimeout bounds cleanup-only waits for terminal
	// pipeline signals. Normal query execution does not use this timeout.
	PipelineCleanupWaitTimeout = 30 * time.Second

	// PipelineSignalSendTimeout bounds cleanup-only terminal signal sends.
	PipelineSignalSendTimeout = 30 * time.Second
)

type PipelineActionType uint8

const (
	GetFromIndex PipelineActionType = iota
	GetDirectly
)

type PipelineSignal struct {
	typ PipelineActionType

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
		typ:      GetFromIndex,
		source:   source,
		index:    index,
		mp:       nil,
		directly: nil,
	}
}

// NewPipelineSignalToDirectly return a signal indicates the receiver to get data from signal directly.
func NewPipelineSignalToDirectly(data *batch.Batch, err error, mp *mpool.MPool) PipelineSignal {
	return PipelineSignal{
		typ:      GetDirectly,
		source:   nil,
		index:    0,
		directly: data,
		mp:       mp,
		errInfo:  err,
	}
}

func SendPipelineSignalWithTimeout(reg *WaitRegister, signal PipelineSignal, timeout time.Duration) bool {
	if reg == nil || reg.Ch2 == nil {
		return false
	}
	if timeout <= 0 {
		reg.Ch2 <- signal
		return true
	}

	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()

	return SendPipelineSignalWithContext(ctx, reg, signal)
}

func SendPipelineSignalWithContext(ctx context.Context, reg *WaitRegister, signal PipelineSignal) bool {
	if reg == nil || reg.Ch2 == nil {
		return false
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	select {
	case reg.Ch2 <- signal:
		return true
	case <-ctx.Done():
		return false
	}
}

func TrySendPipelineSignal(reg *WaitRegister, signal PipelineSignal) bool {
	if reg == nil || reg.Ch2 == nil {
		return false
	}
	select {
	case reg.Ch2 <- signal:
		return true
	default:
		return false
	}
}

func WaitRegisterChannelState(reg *WaitRegister) (int, int) {
	if reg == nil || reg.Ch2 == nil {
		return 0, 0
	}
	return len(reg.Ch2), cap(reg.Ch2)
}

// Action will get the input batch from one place according to which type this signal is.
//
// the result batch of this function is an READ-ONLY one.
func (signal PipelineSignal) Action() (data *batch.Batch, info error) {
	if signal.typ == GetFromIndex {
		data, info = signal.source.ReceiveBatch(signal.index)
		return data, info
	}

	return signal.directly, signal.errInfo
}

type PipelineSignalReceiver struct {
	usrCtx context.Context
	srcReg []*WaitRegister

	alive int

	// receive data channel, first reg is the monitor for runningCtx.
	regs []reflect.SelectCase
	// how much nil batch should each reg wait. its length is 1 less than regs.
	nbs []int

	// currentSignal is the current signal this receiver was using.
	currentSignal *PipelineSignal
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

	for i, reg := range regs {
		// 0 is default number, it takes a same effect as 1.
		if reg.NilBatchCnt == 0 {
			nbs[i] = 1
		} else {
			nbs[i] = reg.NilBatchCnt
		}
		srcRegs[i] = reg
	}

	// if regs were not much, we will use an optimized method to receive msg.
	// and there is no need to init the `reflect.SelectCase`.
	var scs []reflect.SelectCase = nil
	if len(regs) > 8 {
		scs = make([]reflect.SelectCase, 0, len(regs)+1)
		scs = append(scs, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(runningCtx.Done())})
		for i := range regs {
			scs = append(scs, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(regs[i].Ch2)})
		}
	}

	return &PipelineSignalReceiver{
		usrCtx:        runningCtx,
		srcReg:        srcRegs,
		alive:         len(regs),
		regs:          scs,
		nbs:           nbs,
		currentSignal: nil,
	}
}

func (receiver *PipelineSignalReceiver) setCurrent(current *PipelineSignal) {
	receiver.currentSignal = current
}

func (receiver *PipelineSignalReceiver) releaseCurrent() {
	if receiver.currentSignal != nil {
		if receiver.currentSignal.typ == GetFromIndex {
			receiver.currentSignal.source.ReleaseCurrent(
				receiver.currentSignal.index)
		} else if receiver.currentSignal.typ == GetDirectly {
			if receiver.currentSignal.directly != nil {
				receiver.currentSignal.directly.Clean(receiver.currentSignal.mp)
			}
		}

		receiver.currentSignal = nil
	}
}

func (receiver *PipelineSignalReceiver) GetNextBatch(
	analyzer Analyzer) (content *batch.Batch, info error) {
	var chosen int
	var msg PipelineSignal

	for {
		receiver.releaseCurrent()

		if receiver.alive == 0 {
			return nil, nil
		}

		if analyzer != nil {
			start := time.Now()
			chosen, msg = receiver.listenToAll()
			analyzer.WaitStop(start)
			if chosen == 0 {
				return nil, nil
			}

		} else {
			chosen, msg = receiver.listenToAll()
			if chosen == 0 {
				return nil, nil
			}
		}

		content, info = msg.Action()
		if content == nil {
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
		receiver.nbs = append(receiver.nbs[:idx], receiver.nbs[idx+1:]...)

		if len(receiver.regs) > 0 {
			receiver.regs = append(receiver.regs[:chosen], receiver.regs[chosen+1:]...)
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
	if !ok {
		if chosen == 0 {
			return 0, PipelineSignal{}
		}
		panic("unexpected sender close during GetNextBatch")
	}
	return chosen, value.Interface().(PipelineSignal)
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
	select {
	case <-receiver.usrCtx.Done():
		return 0, v
	case v = <-receiver.srcReg[0].Ch2:
		return 1, v
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
	}
}
