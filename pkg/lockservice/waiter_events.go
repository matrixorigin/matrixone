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

package lockservice

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

var (
	defaultLazyCheckDuration atomic.Value
)

func init() {
	defaultLazyCheckDuration.Store(time.Second * 5)
}

type lockContext struct {
	ctx      context.Context
	txn      *activeTxn
	waitTxn  pb.WaitTxn
	rows     [][]byte
	opts     LockOptions
	offset   int
	idx      int
	lockedTS timestamp.Timestamp
	result   pb.Result
	cb       func(pb.Result, error)
	lockFunc func(*lockContext, bool)
	w        *waiter
	createAt time.Time
}

func (l *localLockTable) newLockContext(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	cb func(pb.Result, error),
	bind pb.LockTable) *lockContext {
	c := reuse.Alloc[lockContext](nil)
	c.ctx = ctx
	c.txn = txn
	c.rows = rows
	c.waitTxn = txn.toWaitTxn(l.bind.ServiceID, true)
	c.opts = opts
	c.cb = cb
	c.result = pb.Result{LockedOn: bind}
	c.createAt = time.Now()
	return c
}

func (c *lockContext) done(err error) {
	c.cb(c.result, err)
	c.release()
}

func (c *lockContext) release() {
	reuse.Free(c, nil)
}

func (c *lockContext) doLock() {
	if c.lockFunc == nil {
		panic("missing lock")
	}
	c.lockFunc(c, true)
}

type event struct {
	c      *lockContext
	eventC chan *lockContext
}

func (e event) notified() {
	if e.eventC != nil {
		e.eventC <- e.c
	}
}

// waiterEvents is used to handle all notified waiters. And use a pool to retry the lock op,
// to avoid too many goroutine blocked.
type waiterEvents struct {
	n        int
	detector *detector
	eventC   chan *lockContext
	stopper  *stopper.Stopper

	mu struct {
		sync.RWMutex
		blockedWaiters []*waiter
	}
}

func newWaiterEvents(
	n int,
	detector *detector) *waiterEvents {
	return &waiterEvents{
		n:        n,
		detector: detector,
		eventC:   make(chan *lockContext, 10000),
		stopper:  stopper.NewStopper("waiter-events", stopper.WithLogger(getLogger().RawLogger())),
	}
}

func (mw *waiterEvents) start() {
	for i := 0; i < mw.n; i++ {
		if err := mw.stopper.RunTask(mw.handle); err != nil {
			panic(err)
		}
	}
}

func (mw *waiterEvents) close() {
	mw.stopper.Stop()
	close(mw.eventC)
	mw.mu.Lock()
	for _, w := range mw.mu.blockedWaiters {
		w.close()
	}
	mw.mu.Unlock()
}

func (mw *waiterEvents) add(c *lockContext) {
	if c.opts.async {
		c.w.event = event{
			eventC: mw.eventC,
			c:      c,
		}
	}
	c.w.startWait()
	mw.addToLazyCheckDeadlockC(c.w)
}

func (mw *waiterEvents) addToLazyCheckDeadlockC(w *waiter) {
	w.ref()
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.mu.blockedWaiters = append(mw.mu.blockedWaiters, w)
}

func (mw *waiterEvents) handle(ctx context.Context) {
	timeout := defaultLazyCheckDuration.Load().(time.Duration)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case c := <-mw.eventC:
			txn := c.txn
			txn.Lock()
			c.doLock()
			txn.Unlock()
		case <-timer.C:
			mw.check(timeout)
			timer.Reset(timeout)
		}
	}
}

func (mw *waiterEvents) check(timeout time.Duration) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	if len(mw.mu.blockedWaiters) == 0 {
		return
	}

	stopAt := -1
	now := time.Now()
	for i, w := range mw.mu.blockedWaiters {
		if now.Sub(w.waitAt.Load().(time.Time)) < timeout {
			stopAt = i
			break
		}

		// already completed
		if w.getStatus() != blocking {
			continue
		}

		// deadlock check busy, retry later
		if err := mw.addToDeadlockCheck(w); err != nil {
			stopAt = i
			break
		}
	}
	if stopAt == -1 {
		stopAt = len(mw.mu.blockedWaiters)
	}
	for i := 0; i < stopAt; i++ {
		mw.mu.blockedWaiters[i].close()
		mw.mu.blockedWaiters[i] = nil
	}

	mw.mu.blockedWaiters = append(mw.mu.blockedWaiters[:0], mw.mu.blockedWaiters[stopAt:]...)
}

func (mw *waiterEvents) addToDeadlockCheck(w *waiter) error {
	for _, holder := range w.waitFor {
		if err := mw.detector.check(holder, w.txn); err != nil {
			return err
		}
	}
	return nil
}

func (c lockContext) TypeName() string {
	return "lockservice.lockContext"
}
