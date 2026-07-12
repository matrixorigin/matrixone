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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
)

const (
	maxNotifiedCount = 64
)

type timestampWaiter struct {
	logger    *log.MOLogger
	stopper   stopper.Stopper
	notifiedC chan struct{}
	latestTS  atomic.Pointer[timestamp.Timestamp]
	notified  atomic.Pointer[timestamp.Timestamp]
	mu        struct {
		sync.Mutex
		waiters      []*waiter
		lastNotified timestamp.Timestamp
	}
}

// NewTimestampWaiter create timestamp waiter
func NewTimestampWaiter(
	logger *log.MOLogger,
) TimestampWaiter {
	tw := &timestampWaiter{
		logger: logger,
		stopper: *stopper.NewStopper("timestamp-waiter",
			stopper.WithLogger(logger.RawLogger())),
		notifiedC: make(chan struct{}, maxNotifiedCount),
	}
	if err := tw.stopper.RunTask(tw.handleEvent); err != nil {
		panic(err)
	}
	return tw
}

func (tw *timestampWaiter) GetTimestamp(ctx context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error) {
	latest := tw.latestTS.Load()
	if latest != nil && latest.GreaterEq(ts) {
		return latest.Next(), nil
	}

	w, err := tw.addToWait(ts)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	if w != nil {
		defer tw.removeWaiter(w)
		defer w.close()
		if err := w.wait(ctx); err != nil {
			return timestamp.Timestamp{}, err
		}
	}
	v := tw.latestTS.Load()
	return v.Next(), nil
}

// removeWaiter drops the waiter's list reference when its caller stops
// waiting. Without this, canceled waits remain reachable until an unrelated
// future timestamp notification arrives.
func (tw *timestampWaiter) removeWaiter(target *waiter) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	i := target.index
	if i < 0 || i >= len(tw.mu.waiters) || tw.mu.waiters[i] != target {
		return
	}
	last := len(tw.mu.waiters) - 1
	if i != last {
		tw.mu.waiters[i] = tw.mu.waiters[last]
		tw.mu.waiters[i].index = i
	}
	tw.mu.waiters[last] = nil
	tw.mu.waiters = tw.mu.waiters[:last]
	target.index = -1
	target.unref()
}

func (tw *timestampWaiter) NotifyLatestCommitTS(ts timestamp.Timestamp) {
	util.LogTxnPushedTimestampUpdated(tw.logger, ts)
	if !tw.storeNotified(ts) {
		return
	}
	select {
	case tw.notifiedC <- struct{}{}:
	default:
	}
}

func (tw *timestampWaiter) Close() {
	tw.stopper.Stop()
}

func (tw *timestampWaiter) LatestTS() timestamp.Timestamp {
	if tw == nil {
		return timestamp.Timestamp{}
	}
	ts := tw.latestTS.Load()
	if ts == nil {
		return timestamp.Timestamp{}
	}
	return *ts
}

func (tw *timestampWaiter) addToWait(ts timestamp.Timestamp) (*waiter, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if !tw.mu.lastNotified.IsEmpty() &&
		tw.mu.lastNotified.GreaterEq(ts) {
		return nil, nil
	}

	w := newWaiter(ts)
	w.ref()
	w.index = len(tw.mu.waiters)
	tw.mu.waiters = append(tw.mu.waiters, w)
	return w, nil
}

func (tw *timestampWaiter) notifyWaiters(ts timestamp.Timestamp) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	waiters := tw.mu.waiters[:0]
	for _, w := range tw.mu.waiters {
		if w != nil && ts.GreaterEq(w.waitAfter) {
			w.notify()
			w.index = -1
			w.unref()
			w = nil
		}
		if w != nil {
			w.index = len(waiters)
			waiters = append(waiters, w)
		}
	}

	tw.mu.waiters = waiters
	tw.mu.lastNotified = ts
}

func (tw *timestampWaiter) handleEvent(ctx context.Context) {
	var latest timestamp.Timestamp
	for {
		select {
		case <-ctx.Done():
			return
		case <-tw.notifiedC:
			ts := tw.fetchLatestTS()
			if latest.Less(ts) {
				latest = ts
				tw.latestTS.Store(&ts)
				tw.notifyWaiters(ts)
			}
		}
	}
}

func (tw *timestampWaiter) storeNotified(ts timestamp.Timestamp) bool {
	for {
		latest := tw.notified.Load()
		if latest != nil && latest.GreaterEq(ts) {
			return false
		}
		v := ts
		if tw.notified.CompareAndSwap(latest, &v) {
			return true
		}
	}
}

func (tw *timestampWaiter) fetchLatestTS() timestamp.Timestamp {
	ts := tw.takeNotified()
	for i := 0; i < maxNotifiedCount; i++ {
		select {
		case <-tw.notifiedC:
			v := tw.takeNotified()
			if ts.Less(v) {
				ts = v
			}
		default:
			return ts
		}
	}
	return ts
}

func (tw *timestampWaiter) takeNotified() timestamp.Timestamp {
	for {
		latest := tw.notified.Load()
		if latest == nil {
			return timestamp.Timestamp{}
		}
		if tw.notified.CompareAndSwap(latest, nil) {
			return *latest
		}
	}
}

var (
	waitersPool = sync.Pool{
		New: func() any {
			w := &waiter{
				notifyC: make(chan struct{}, 1),
			}
			runtime.SetFinalizer(w, func(w *waiter) {
				close(w.notifyC)
			})
			return w
		},
	}
)

type waiter struct {
	waitAfter timestamp.Timestamp
	notifyC   chan struct{}
	refCount  atomic.Int32
	// index is guarded by timestampWaiter.mu while the waiter is registered.
	index int
}

func newWaiter(ts timestamp.Timestamp) *waiter {
	w := waitersPool.Get().(*waiter)
	w.init(ts)
	return w
}

func (w *waiter) init(ts timestamp.Timestamp) {
	w.waitAfter = ts
	w.index = -1
	if w.ref() != 1 {
		panic("BUG: waiter init must has ref count 1")
	}
}

func (w *waiter) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.notifyC:
		return nil
	}
}

func (w *waiter) notify() {
	w.notifyC <- struct{}{}
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
		case <-w.notifyC:
		default:
			return
		}
	}
}
