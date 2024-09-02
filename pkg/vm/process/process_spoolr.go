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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"reflect"
)

type PipelineActionType uint8

const (
	GetFromIndex PipelineActionType = iota
	GetDirectly
)

var _ = GetDirectly

type PipelineSignal struct {
	typ PipelineActionType

	// for case: GetFromIndex
	index  int
	source pSpool.PipelineCommunication

	// for case: GetDirectly
	directly *batch.Batch
}

// NewPipelineSignalToGetFromSpool return a signal indicate the receiver to get data from source by index.
func NewPipelineSignalToGetFromSpool(source pSpool.PipelineCommunication, index int) PipelineSignal {
	return PipelineSignal{
		typ:      GetFromIndex,
		source:   source,
		index:    index,
		directly: nil,
	}
}

// NewPipelineSignalToDirectly return a signal indicates the receiver to get data from signal directly.
// But users should watch that, the data shouldn't be allocated from mpool.
func NewPipelineSignalToDirectly(data *batch.Batch) PipelineSignal {
	return PipelineSignal{
		typ:      GetDirectly,
		source:   nil,
		index:    0,
		directly: data,
	}
}

// Action will get the input batch from one place according to which type this signal is.
//
// the result batch of this function is an READ-ONLY one.
func (signal PipelineSignal) Action() (data *batch.Batch, info error) {
	if signal.typ == GetFromIndex {
		return signal.source.ReceiveBatch(signal.index)
	}
	return signal.directly, nil
}

type PipelineSignalReceiver struct {
	alive int

	// receive data channel, first reg is the monitor for runningCtx.
	regs []reflect.SelectCase
	// how much nil batch should each reg wait. its length is 1 less than regs.
	nbs []int
}

func InitPipelineSignalReceiver(runningCtx context.Context, regs []*WaitRegister) *PipelineSignalReceiver {
	nbs := make([]int, len(regs))
	scs := make([]reflect.SelectCase, 0, len(regs))
	scs = append(scs, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(runningCtx.Done())})
	for i, reg := range regs {
		// 0 is default number, it takes a same effect as 1.
		if reg.NilBatchCnt == 0 {
			nbs[i] = 1
		} else {
			nbs[i] = reg.NilBatchCnt
		}

		scs = append(scs, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(reg.Ch2)})
	}

	return &PipelineSignalReceiver{
		alive: len(regs),
		regs:  scs,
		nbs:   nbs,
	}
}

func (receiver *PipelineSignalReceiver) GetNextBatch() (content *batch.Batch, info error) {
	for {
		chosen, v, ok := reflect.Select(receiver.regs)
		if chosen == 0 {
			// user was going to end.
			return nil, nil
		}
		if ok {
			msg := (v.Interface()).(PipelineSignal)
			content, info = msg.Action()
			if content == nil {
				idx := chosen - 1

				receiver.nbs[idx]--
				if receiver.nbs[idx] > 0 {
					continue
				}

				// remove the unused channel.
				receiver.regs = append(receiver.regs[:chosen], receiver.regs[chosen+1:]...)
				receiver.nbs = append(receiver.nbs[:idx], receiver.nbs[idx+1:]...)
				receiver.alive--
				continue
			}

			return content, info
		}
		break
	}
	panic("unexpected sender close during GetNextBatch")
}

func (receiver *PipelineSignalReceiver) WaitingEnd() {
	if len(receiver.regs) > 0 {
		receiver.regs[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(context.TODO().Done())}
	}
	for {
		if receiver.alive == 0 {
			return
		}
		_, _ = receiver.GetNextBatch()
	}
}
