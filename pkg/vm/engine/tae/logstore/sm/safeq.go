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
	"context"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Created int32 = iota
	Running
	ReceiverStopped
	PrepareStop
	Stopped
)

type QueueNameType int

// DO NOT change the order
const (
	QueueNameForTest QueueNameType = 0

	GCManagerQueue QueueNameType = 1

	BaseStoreFlushQueue      QueueNameType = 2
	BaseStoreSyncQueue       QueueNameType = 3
	BaseStoreCommitQueue     QueueNameType = 4
	BaseStorePostCommitQueue QueueNameType = 5
	BaseStoreTruncateQueue   QueueNameType = 6

	TxnMgrPreparingRcvQueue QueueNameType = 7
	TxnMgrPreparingCkpQueue QueueNameType = 8
	TxnMgrFlushQueue        QueueNameType = 9

	IOPipelineWaitQueue     QueueNameType = 10
	IOPipelinePrefetchQueue QueueNameType = 11
	IOPipelineFetchQueue    QueueNameType = 12

	LogDriverPreAppendQueue QueueNameType = 13
	LogDriverTruncateQueue  QueueNameType = 14

	StoreImplDriverAppendQueue QueueNameType = 15
	StoreImplDoneWithErrQueue  QueueNameType = 16
	StoreImplLogInfoQueue      QueueNameType = 17
	StoreImplCheckpointQueue   QueueNameType = 18
	StoreImplTruncatingQueue   QueueNameType = 19
	StoreImplTruncateQueue     QueueNameType = 20

	LogTailCollectLogTailQueue QueueNameType = 21
	LogTailWaitCommitQueue     QueueNameType = 22

	CKPDirtyEntryQueue     QueueNameType = 23
	CKPWaitQueue           QueueNameType = 24
	CKPIncrementalCKPQueue QueueNameType = 25
	CKPGlobalCKPQueue      QueueNameType = 26
	CKPGCCheckpointQueue   QueueNameType = 27
	CKPPostCheckpointQueue QueueNameType = 28

	DiskCleanerProcessQueue QueueNameType = 29

	LogTailNotifierQueue QueueNameType = 30
	// new names adding here

	QueueNameMax QueueNameType = 31
)

var SafeQueueRegister [int(QueueNameMax)]struct {
	Queue *safeQueue
}

type safeQueue struct {
	name      QueueNameType
	queue     chan any
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	state     atomic.Int32
	pending   atomic.Int64
	batchSize int
	onItemsCB OnItemsCB
	// value is true by default
	blocking bool
}

// NewSafeQueue is blocking queue by default
func NewSafeQueue(name QueueNameType, queueSize, batchSize int, onItem OnItemsCB) *safeQueue {
	q := &safeQueue{
		name:      name,
		queue:     make(chan any, queueSize),
		batchSize: batchSize,
		onItemsCB: onItem,
	}
	q.blocking = true
	q.state.Store(Created)
	q.ctx, q.cancel = context.WithCancel(context.Background())

	SafeQueueRegister[name].Queue = q
	return q
}

func NewNonBlockingQueue(name QueueNameType, queueSize int, batchSize int, onItem OnItemsCB) *safeQueue {
	q := NewSafeQueue(name, queueSize, batchSize, onItem)
	q.blocking = false
	return q
}

func (q *safeQueue) Len() int {
	return len(q.queue)
}

func (q *safeQueue) Start() {
	q.state.Store(Running)
	q.wg.Add(1)
	items := make([]any, 0, q.batchSize)
	go func() {
		defer q.wg.Done()
		for {
			select {
			case <-q.ctx.Done():
				return
			case item := <-q.queue:
				if q.onItemsCB == nil {
					continue
				}
				items = append(items, item)
			Left:
				for i := 0; i < q.batchSize-1; i++ {
					select {
					case item = <-q.queue:
						items = append(items, item)
					default:
						break Left
					}
				}
				cnt := len(items)
				q.onItemsCB(items...)
				items = items[:0]
				q.pending.Add(-1 * int64(cnt))
			}
		}
	}()
}

func (q *safeQueue) Stop() {
	q.stopReceiver()
	q.waitStop()
	close(q.queue)
}

func (q *safeQueue) stopReceiver() {
	state := q.state.Load()
	if state >= ReceiverStopped {
		return
	}
	q.state.CompareAndSwap(state, ReceiverStopped)
}

func (q *safeQueue) waitStop() {
	if q.state.Load() <= Running {
		panic("logic error")
	}
	if q.state.Load() == Stopped {
		return
	}
	if q.state.CompareAndSwap(ReceiverStopped, PrepareStop) {
		// what if the pending is negative ?
		// if a queue processes item faster than the pending self-increment,
		// there could be a short moment that the pending is negative.
		// may be moving the pending.Add to the front of en-channel is a solution.
		for q.pending.Load() != 0 {
			runtime.Gosched()
		}
		q.cancel()
	}
	q.wg.Wait()
}

func (q *safeQueue) Enqueue(item any) (any, error) {
	if q.state.Load() != Running {
		return item, ErrClose
	}

	select {
	case q.queue <- item:
		q.pending.Add(1)
		return item, nil

	// if queue is full
	default:
		if q.blocking {
			start := time.Now()
			q.queue <- item
			q.pending.Add(1)

			// observe the blocking duration
			v2.TxnTNSideQueueBlockingHistograms[q.name].Observe(time.Since(start).Seconds())

			return item, nil
		} else {
			// if queue is non blocking
			return item, ErrFull
		}
	}
}

func (n QueueNameType) String() string {
	switch n {
	case GCManagerQueue:
		return "GCManagerQueue"
	case BaseStoreFlushQueue:
		return "BaseStoreFlushQueue"
	case BaseStoreSyncQueue:
		return "BaseStoreSyncQueue"
	case BaseStoreCommitQueue:
		return "BaseStoreCommitQueue"
	case BaseStorePostCommitQueue:
		return "BaseStorePostCommitQueue"
	case BaseStoreTruncateQueue:
		return "BaseStoreTruncateQueue"
	case TxnMgrPreparingRcvQueue:
		return "TxnMgrPreparingRcvQueue"
	case TxnMgrPreparingCkpQueue:
		return "TxnMgrPreparingCkpQueue"
	case TxnMgrFlushQueue:
		return "TxnMgrFlushQueue"
	case IOPipelineWaitQueue:
		return "IOPipelineWaitQueue"
	case IOPipelinePrefetchQueue:
		return "IOPipelinePrefetchQueue"
	case IOPipelineFetchQueue:
		return "IOPipelineFetchQueue"
	case LogDriverPreAppendQueue:
		return "LogDriverPreAppendQueue"
	case LogDriverTruncateQueue:
		return "LogDriverTruncateQueue"
	case StoreImplDriverAppendQueue:
		return "StoreImplDriverAppendQueue"
	case StoreImplDoneWithErrQueue:
		return "StoreImplDoneWithErrQueue"
	case StoreImplLogInfoQueue:
		return "StoreImplLogInfoQueue"
	case StoreImplCheckpointQueue:
		return "StoreImplCheckpointQueue"
	case StoreImplTruncatingQueue:
		return "StoreImplTruncatingQueue"
	case StoreImplTruncateQueue:
		return "StoreImplTruncateQueue"
	case LogTailCollectLogTailQueue:
		return "LogTailCollectLogTailQueue"
	case LogTailWaitCommitQueue:
		return "LogTailWaitCommitQueue"
	case CKPDirtyEntryQueue:
		return "CKPDirtyEntryQueue"
	case CKPWaitQueue:
		return "CKPWaitQueue"
	case CKPIncrementalCKPQueue:
		return "CKPIncrementalCKPQueue"
	case CKPGlobalCKPQueue:
		return "CKPGlobalCKPQueue"
	case CKPGCCheckpointQueue:
		return "CKPGCCheckpointQueue"
	case CKPPostCheckpointQueue:
		return "CKPPostCheckpointQueue"
	case DiskCleanerProcessQueue:
		return "DiskCleanerProcessQueue"
	case LogTailNotifierQueue:
		return "LogTailNotifierQueue"
	default:
		return "Unknown"
	}
}
