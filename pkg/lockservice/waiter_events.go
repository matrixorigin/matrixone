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
	eventC  chan *waiter
	stopper *stopper.Stopper
}

func newWaiterEvents(n int) *waiterEvents {
	return &waiterEvents{
		n:       n,
		eventC:  make(chan *waiter, 10000),
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

func (mw *waiterEvents) handle(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-mw.eventC:
			// w.wait()
		}
	}
}
