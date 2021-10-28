package sm

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

type State = int32

const (
	Created State = iota
	Running
	ReceiverStopped
	PrepareStop
	Stopped
)

type safeQueue struct {
	queue     chan interface{}
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	state     State
	pending   int64
	batchSize int
	onItemsCB OnItemsCB
}

func NewSafeQueue(queueSize, batchSize int, onItem OnItemsCB) *safeQueue {
	q := &safeQueue{
		queue:     make(chan interface{}, queueSize),
		state:     Created,
		batchSize: batchSize,
		onItemsCB: onItem,
	}
	q.ctx, q.cancel = context.WithCancel(context.Background())
	return q
}

func (q *safeQueue) Start() {
	q.state = Running
	q.wg.Add(1)
	items := make([]interface{}, 0, q.batchSize)
	go func() {
		defer q.wg.Done()
		for {
			select {
			case <-q.ctx.Done():
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
				atomic.AddInt64(&q.pending, int64(cnt)*(-1))
			}
		}
	}()
}

func (q *safeQueue) Stop() {
	q.stopReceiver()
	q.waitStop()
}

func (q *safeQueue) stopReceiver() {
	state := atomic.LoadInt32(&q.state)
	if state >= ReceiverStopped {
		return
	}
	if atomic.CompareAndSwapInt32(&q.state, state, ReceiverStopped) {
		return
	}
}

func (q *safeQueue) waitStop() {
	state := atomic.LoadInt32(&q.state)
	if state <= Running {
		panic("logic error")
	}
	if state == Stopped {
		return
	}
	if atomic.CompareAndSwapInt32(&q.state, ReceiverStopped, PrepareStop) {
		pending := atomic.LoadInt64(&q.pending)
		for {
			if pending == 0 {
				break
			}
			runtime.Gosched()
			pending = atomic.LoadInt64(&q.pending)
		}
		q.cancel()
	}
	q.wg.Wait()
}

func (q *safeQueue) Enqueue(item interface{}) (interface{}, error) {
	state := atomic.LoadInt32(&q.state)
	if state != Running {
		return item, errors.New("closed")
	}
	atomic.AddInt64(&q.pending, int64(1))
	if atomic.LoadInt32(&q.state) != Running {
		atomic.AddInt64(&q.pending, int64(-1))
		return item, errors.New("closed")
	}
	q.queue <- item
	return item, nil
}
