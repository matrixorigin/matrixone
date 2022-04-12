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
	sm.checkpointQueue.Start()
	sm.receiveQueue.Start()
}

func (sm *stateMachine) Stop() {
	if !sm.closed.TryClose() {
		return
	}
	sm.receiveQueue.Stop()
	sm.checkpointQueue.Stop()
	sm.wg.Wait()
}
