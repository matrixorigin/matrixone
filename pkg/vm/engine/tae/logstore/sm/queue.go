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
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type waitableQueue struct {
	closed    common.Closable
	queue     chan interface{}
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	onFinCB   OnFinCB
	onItemsCB OnItemsCB
	enqueueOp EnqueueOp
	batchSize int
	externWg  *sync.WaitGroup
}

func NewWaitableQueue(queueSize, batchSize int, closed common.Closable, wg *sync.WaitGroup, enqueueOp EnqueueOp, onFin OnFinCB, onItem OnItemsCB) *waitableQueue {
	q := &waitableQueue{
		enqueueOp: enqueueOp,
		queue:     make(chan interface{}, queueSize),
		onFinCB:   onFin,
		onItemsCB: onItem,
		batchSize: batchSize,
		closed:    closed,
		externWg:  wg,
	}
	q.ctx, q.cancel = context.WithCancel(context.Background())
	return q
}

func (q *waitableQueue) loop() {
	defer q.externWg.Done()
	items := make([]interface{}, 0, q.batchSize)
	for {
		select {
		case <-q.ctx.Done():
			if q.onFinCB != nil {
				q.onFinCB()
			}
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
			q.wg.Add(-1 * cnt)
		}
	}
}

func (q *waitableQueue) Start() {
	q.externWg.Add(1)
	go q.loop()
}

func (q *waitableQueue) Enqueue(item interface{}) (interface{}, error) {
	if q.closed.IsClosed() {
		return nil, errors.New("closed")
	}
	q.wg.Add(1)
	if q.closed.IsClosed() {
		q.wg.Done()
		return nil, errors.New("closed")
	}
	if q.enqueueOp != nil {
		processed := q.enqueueOp(item)
		return processed, nil
	}
	q.queue <- item
	return item, nil
}

func (q *waitableQueue) Stop() {
	q.wg.Wait()
	q.cancel()
}
