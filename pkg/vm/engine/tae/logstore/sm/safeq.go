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
	mu        sync.Mutex
	stopOnce  sync.Once
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
	q.mu.Lock()
	if q.state.Load() != Created {
		q.mu.Unlock()
		return
	}
	q.state.Store(Running)
	q.wg.Add(1)
	q.mu.Unlock()
	items := make([]any, 0, q.batchSize)
	go func() {
		defer q.wg.Done()
		for {
			select {
			case <-q.ctx.Done():
				return
			case item := <-q.queue:
				if q.onItemsCB == nil {
					q.pending.Add(-1)
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
	q.stopOnce.Do(func() {
		q.stopReceiver()
		q.waitStop()
		// waitStop observes pending == 0 only after every producer that passed
		// the second state check has sent and been handled. Producers that race
		// with Stop after this point withdraw before sending, so close releases
		// the queue buffer without racing a sender.
		close(q.queue)
		q.state.Store(Stopped)
	})
}

func (q *safeQueue) stopReceiver() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state.Load() < ReceiverStopped {
		q.state.Store(ReceiverStopped)
	}
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
		// Stop may begin after the first state check. Register before the
		// second check so Stop keeps the receiver alive until this producer
		// either sends or withdraws its pending item.
		if q.state.Load() != Running {
			q.pending.Add(-1)
			return item, ErrClose
		}
		q.queue <- item
		return item, nil
	} else {
		q.pending.Add(1)
		if q.state.Load() != Running {
			q.pending.Add(-1)
			return item, ErrClose
		}
		select {
		case q.queue <- item:
			return item, nil
		default:
			q.pending.Add(-1)
			return item, ErrFull
		}
	}
}
