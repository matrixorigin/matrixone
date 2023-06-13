// Copyright 2023 Matrix Origin
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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
)

const (
	maxNotifiedCount = 64
)

type timestampWaiter struct {
	stopper   stopper.Stopper
	notifiedC chan timestamp.Timestamp
	latestTS  atomic.Pointer[timestamp.Timestamp]
	mu        struct {
		sync.Mutex
		waiters      []*waiter
		lastNotified timestamp.Timestamp
	}
}

// NewTimestampWaiter create timestamp waiter
func NewTimestampWaiter() TimestampWaiter {
	tw := &timestampWaiter{
		stopper: *stopper.NewStopper("timestamp-waiter",
			stopper.WithLogger(util.GetLogger().RawLogger())),
		notifiedC: make(chan timestamp.Timestamp, maxNotifiedCount),
	}
	if err := tw.stopper.RunTask(tw.handleNotify); err != nil {
		panic(err)
	}
	return tw
}

func (tw *timestampWaiter) GetTimestamp(
	ctx context.Context,
	ts timestamp.Timestamp) (timestamp.Timestamp, error) {
	latest := tw.latestTS.Load()
	if latest != nil && latest.GreaterEq(ts) {
		return latest.Next(), nil
	}

	w := tw.addToWait(ts)
	if w != nil {
		defer w.close()
		if err := w.wait(ctx); err != nil {
			return timestamp.Timestamp{}, err
		}
	}
	v := tw.latestTS.Load()
	return v.Next(), nil
}

func (tw *timestampWaiter) NotifyLatestCommitTS(ts timestamp.Timestamp) {
	util.LogTxnPushedTimestampUpdated(ts)
	tw.notifiedC <- ts
}

func (tw *timestampWaiter) Close() {
	tw.stopper.Stop()
}

func (tw *timestampWaiter) addToWait(ts timestamp.Timestamp) *waiter {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if !tw.mu.lastNotified.IsEmpty() &&
		tw.mu.lastNotified.GreaterEq(ts) {
		return nil
	}

	w := newWaiter(ts)
	w.ref()
	tw.mu.waiters = append(tw.mu.waiters, w)
	return w
}

func (tw *timestampWaiter) notifyWaiters(ts timestamp.Timestamp) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	waiters := tw.mu.waiters[:0]
	for idx, w := range tw.mu.waiters {
		if w != nil && ts.GreaterEq(w.waitAfter) {
			w.notify()
			w.unref()
			tw.mu.waiters[idx] = nil
			w = nil
		}
		if w != nil {
			waiters = append(waiters, w)
		}
	}

	tw.mu.waiters = waiters
	tw.mu.lastNotified = ts
}

func (tw *timestampWaiter) handleNotify(ctx context.Context) {
	var latest timestamp.Timestamp
	for {
		select {
		case <-ctx.Done():
			return
		case ts := <-tw.notifiedC:
			ts = tw.fetchLatestTS(ts)
			if latest.Less(ts) {
				latest = ts
				tw.latestTS.Store(&ts)
				tw.notifyWaiters(ts)
			}
		}
	}
}

func (tw *timestampWaiter) fetchLatestTS(ts timestamp.Timestamp) timestamp.Timestamp {
	for {
		select {
		case v := <-tw.notifiedC:
			if v.Greater(ts) {
				ts = v
			}
		default:
			return ts
		}
	}
}

var (
	waitersPool = sync.Pool{
		New: func() any {
			w := &waiter{
				c: make(chan struct{}, 1),
			}
			runtime.SetFinalizer(w, func(w *waiter) {
				close(w.c)
			})
			return w
		},
	}
)

type waiter struct {
	waitAfter timestamp.Timestamp
	c         chan struct{}
	refCount  atomic.Int32
}

func newWaiter(ts timestamp.Timestamp) *waiter {
	w := waitersPool.Get().(*waiter)
	w.init(ts)
	return w
}

func (w *waiter) init(ts timestamp.Timestamp) {
	w.waitAfter = ts
	if w.ref() != 1 {
		panic("BUG: waiter init must has ref count 1")
	}
}

func (w *waiter) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.c:
		return nil
	}
}

func (w *waiter) notify() {
	w.c <- struct{}{}
}

func (w *waiter) ref() int32 {
	return w.refCount.Add(1)
}

func (w *waiter) unref() {
	n := w.refCount.Add(-1)
	if n < 0 {
		panic("BUG: negative ref count")
	}
	if n == 0 {
		w.reset()
		waitersPool.Put(w)
	}
}

func (w *waiter) close() {
	w.unref()
}

func (w *waiter) reset() {
	for {
		select {
		case <-w.c:
		default:
			return
		}
	}
}
