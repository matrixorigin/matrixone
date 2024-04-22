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
)

type stateMachine struct {
	closed       Closable
	wg           *sync.WaitGroup
	receiveQueue Queue
	//checkpointQueue Queue
}

func NewStateMachine(wg *sync.WaitGroup, closed Closable, rQueue Queue) *stateMachine {
	return &stateMachine{
		closed:       closed,
		wg:           wg,
		receiveQueue: rQueue,
	}
}

func (sm *stateMachine) EnqueueRecevied(item any) (any, error) {
	return sm.receiveQueue.Enqueue(item)
}

func (sm *stateMachine) Start() {
	if sm.receiveQueue != nil {
		sm.receiveQueue.Start()
	}
}

func (sm *stateMachine) Stop() {
	if !sm.closed.TryClose() {
		return
	}
	if sm.receiveQueue != nil {
		sm.receiveQueue.Stop()
	}
	sm.wg.Wait()
}
