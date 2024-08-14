// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"time"
)

type queue interface {
	enqueue(context.Context, *wrappedData)
	dequeue(ctx context.Context) (*wrappedData, error)
	close()
}

type dataQueue struct {
	queue     chan *wrappedData
	mayIgnore func(*wrappedData) bool
}

func newDataQueue(queueSize int, mayIgnore func(*wrappedData) bool) queue {
	return &dataQueue{
		queue:     make(chan *wrappedData, queueSize),
		mayIgnore: mayIgnore,
	}
}

func (q *dataQueue) enqueue(ctx context.Context, w *wrappedData) {
	if w == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return

		case q.queue <- w:
			return

		default:
			if q.mayIgnore != nil && q.mayIgnore(w) {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (q *dataQueue) dequeue(ctx context.Context) (*wrappedData, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case v := <-q.queue:
		return v, nil
	}

}

func (q *dataQueue) close() {
	close(q.queue)
}
