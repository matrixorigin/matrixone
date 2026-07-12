// Copyright 2026 Matrix Origin
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

package client

import (
	"context"
	"sync"
)

// activeTxnWaiter is the admission gate for one max-active queued transaction.
// It has one terminal result: admitted, client closed, or another terminal
// failure. Caller cancellation is deliberately not a terminal result for the
// gate; closeTxn removes the abandoned entry from the client-owned queue.
type activeTxnWaiter struct {
	doneC chan struct{}
	once  sync.Once
	err   error
}

func newActiveTxnWaiter() *activeTxnWaiter {
	return &activeTxnWaiter{doneC: make(chan struct{})}
}

func (w *activeTxnWaiter) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.doneC:
		return w.err
	}
}

func (w *activeTxnWaiter) complete(err error) {
	w.once.Do(func() {
		w.err = err
		close(w.doneC)
	})
}

// withFailureContext stops pre-admission work when the queue is failed by
// client shutdown. It is used only by queued transactions that can block while
// obtaining a snapshot; admitted and queue-less transactions stay on the
// allocation-free fast path.
func (w *activeTxnWaiter) withFailureContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-w.doneC:
			if w.err != nil {
				cancel()
			}
		}
	}()
	return ctx, cancel
}

func (w *activeTxnWaiter) result() (error, bool) {
	select {
	case <-w.doneC:
		return w.err, true
	default:
		return nil, false
	}
}
