// Copyright 2021 Matrix Origin
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

package sm

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type stateMachine struct {
	closed          common.Closable
	wg              *sync.WaitGroup
	receiveQueue    Queue
	checkpointQueue Queue
}

func NewStateMachine(wg *sync.WaitGroup, closed common.Closable, rQueue, ckpQueue Queue) *stateMachine {
	return &stateMachine{
		closed:          closed,
		wg:              wg,
		receiveQueue:    rQueue,
		checkpointQueue: ckpQueue,
	}
}

func (sm *stateMachine) EnqueueRecevied(item interface{}) (interface{}, error) {
	return sm.receiveQueue.Enqueue(item)
}

func (sm *stateMachine) EnqueueCheckpoint(item interface{}) (interface{}, error) {
	return sm.checkpointQueue.Enqueue(item)
}

func (sm *stateMachine) Start() {
	if sm.checkpointQueue != nil {
		sm.checkpointQueue.Start()
	}
	if sm.receiveQueue != nil {
		sm.receiveQueue.Start()
	}
}

func (sm *stateMachine) Stop() {
	if !sm.closed.TryClose() {
		return
	}
	if sm.checkpointQueue != nil {
		sm.checkpointQueue.Stop()
	}
	if sm.receiveQueue != nil {
		sm.receiveQueue.Stop()
	}
	sm.wg.Wait()
}
