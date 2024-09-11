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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"reflect"
	"time"
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
	source pSpool.PipelineCommunication

	// for case: GetDirectly
	mp       *mpool.MPool
	directly *batch.Batch
}

// NewPipelineSignalToGetFromSpool return a signal indicate the receiver to get data from source by index.
func NewPipelineSignalToGetFromSpool(source pSpool.PipelineCommunication, index int) PipelineSignal {
	return PipelineSignal{
		typ:      GetFromIndex,
		source:   source,
		index:    index,
		mp:       nil,
		directly: nil,
	}
}

// NewPipelineSignalToDirectly return a signal indicates the receiver to get data from signal directly.
// But users should watch that, the data shouldn't be allocated from mpool.
func NewPipelineSignalToDirectly(data *batch.Batch, mp *mpool.MPool) PipelineSignal {
	return PipelineSignal{
		typ:      GetDirectly,
		source:   nil,
		index:    0,
		directly: data,
		mp:       mp,
	}
}

// Action will get the input batch from one place according to which type this signal is.
//
// the result batch of this function is an READ-ONLY one.
func (signal PipelineSignal) Action() (data *batch.Batch, info error, skipThis bool) {
	if signal.typ == GetFromIndex {
		data, info = signal.source.ReceiveBatch(signal.index)
		return data, info, false
	}

	return signal.directly, nil, false
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
	var skipThisSignal bool
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

		content, info, skipThisSignal = msg.Action()
		if skipThisSignal {
			continue
		}
		if content == nil {
			receiver.removeIdxReceiver(chosen)
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
		panic("unexpected sender close during GetNextBatch")
	}
	return chosen, value.Interface().(PipelineSignal)
}

func (receiver *PipelineSignalReceiver) WaitingEnd() {
	if len(receiver.regs) > 0 {
		receiver.regs[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(context.TODO().Done())}
	}

	receiver.usrCtx = context.TODO()
	for {
		if receiver.alive == 0 {
			return
		}
		_, _ = receiver.GetNextBatch(nil)
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
