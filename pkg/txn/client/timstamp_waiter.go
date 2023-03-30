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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	maxNotifiedCount = 64
	maxWaitersCount  = 10000
)

type timestampWaiter struct {
	stopper   stopper.Stopper
	notifiedC chan timestamp.Timestamp
	latestTS  atomic.Pointer[timestamp.Timestamp]

	mu struct {
		sync.RWMutex
		waiters []*waiter
	}
}

// NewTimestampWaiter create timestamp waiter
func NewTimestampWaiter() TimestampWaiter {
	tw := &timestampWaiter{
		stopper: *stopper.NewStopper("timestamp-waiter",
			stopper.WithLogger(getLogger().RawLogger())),
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
	if latest != nil && latest.Greater(ts) {
		return *latest, nil
	}

	w := tw.addToWait(ts)
	defer w.close()
	if err := w.wait(ctx); err != nil {
		return timestamp.Timestamp{}, err
	}
	v := tw.latestTS.Load()
	return *v, nil
}

func (tw *timestampWaiter) NotifyLatestCommitTS(ts timestamp.Timestamp) {
	tw.notifiedC <- ts
}

func (tw *timestampWaiter) Close() {
	tw.stopper.Stop()
}

func (tw *timestampWaiter) addToWait(ts timestamp.Timestamp) *waiter {
	w := newWaiter(ts)
	w.ref()

	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.mu.waiters = append(tw.mu.waiters, w)
	sort.Slice(
		tw.mu.waiters,
		func(i, j int) bool {
			return tw.mu.waiters[i].waitAfter.Less(tw.mu.waiters[j].waitAfter)
		})
	return w
}

func (tw *timestampWaiter) notifyWaiters(ts timestamp.Timestamp) {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	for i := len(tw.mu.waiters) - 1; i >= 0; i-- {
		w := tw.mu.waiters[i]
		if ts.Greater(w.waitAfter) {
			w.notify()
			w.unref()
		}
	}
}

func (tw *timestampWaiter) handleNotify(ctx context.Context) {
	var latest timestamp.Timestamp
	for {
		select {
		case <-ctx.Done():
			return
		case ts := <-tw.notifiedC:
			ts = tw.fetchLatestTS(ts)
			if latest.GreaterEq(ts) {
				break
			}
			latest = ts
			tw.latestTS.Store(&ts)

			n := tw.waiters.Len()
			for i := uint64(0); i < n; i++ {
				w := tw.waiters.MustGet()
				if ts.Greater(w.waitAfter) {
					w.notify()
					w.unref()
				} else {
					tw.waiters.Put(w)
				}
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
				c: make(chan struct{}),
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
	w.ref()
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

func (w *waiter) ref() {
	w.refCount.Add(1)
}

func (w *waiter) unref() {
	if w.refCount.Add(-1) == 0 {
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
