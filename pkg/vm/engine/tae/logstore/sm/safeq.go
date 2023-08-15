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
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	Created int32 = iota
	Running
	ReceiverStopped
	PrepareStop
	Stopped
)

type safeQueue struct {
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
func NewSafeQueue(queueSize, batchSize int, onItem OnItemsCB) *safeQueue {
	q := &safeQueue{
		queue:     make(chan any, queueSize),
		batchSize: batchSize,
		onItemsCB: onItem,
	}
	q.blocking = true
	q.state.Store(Created)
	q.ctx, q.cancel = context.WithCancel(context.Background())
	return q
}

func NewNonBlockingQueue(queueSize int, batchSize int, onItem OnItemsCB) *safeQueue {
	q := NewSafeQueue(queueSize, batchSize, onItem)
	q.blocking = false
	return q
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

	if q.blocking {
		q.pending.Add(1)
		q.queue <- item
		return item, nil
	} else {
		select {
		case q.queue <- item:
			q.pending.Add(1)
			return item, nil
		default:
			return item, ErrFull
		}
	}
}
