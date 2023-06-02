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

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type lockContext struct {
	ctx      context.Context
	txn      *activeTxn
	rows     [][]byte
	opts     LockOptions
	offset   int
	w        *waiter
	idx      int
	lockedTS timestamp.Timestamp
	result   pb.Result
	cb       func(pb.Result, error)
	lockFunc func(lockContext, bool)
}

func newLockContext(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	cb func(pb.Result, error),
	bind pb.LockTable) lockContext {
	return lockContext{
		ctx:    ctx,
		txn:    txn,
		rows:   rows,
		opts:   opts,
		cb:     cb,
		result: pb.Result{LockedOn: bind},
	}
}

func (c lockContext) done(err error) {
	c.cb(c.result, err)
}

func (c lockContext) doLock() {
	if c.lockFunc == nil {
		panic("missing lock")
	}
	c.lockFunc(c, true)
}

type event struct {
	c      lockContext
	eventC chan lockContext
}

func (e event) notified() {
	if e.eventC != nil {
		e.eventC <- e.c
	}
}

// waiterEvents is used to handle all notified waiters. And use a pool to retry the lock op,
// to avoid too many goroutine blocked.
type waiterEvents struct {
	n       int
	eventC  chan lockContext
	stopper *stopper.Stopper
}

func newWaiterEvents(n int) *waiterEvents {
	return &waiterEvents{
		n:       n,
		eventC:  make(chan lockContext, 10000),
		stopper: stopper.NewStopper("waiter-events", stopper.WithLogger(getLogger().RawLogger())),
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
}

func (mw *waiterEvents) add(c lockContext) {
	c.w.event = event{
		eventC: mw.eventC,
		c:      c,
	}
}

func (mw *waiterEvents) handle(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-mw.eventC:
			c.doLock()
		}
	}
}
