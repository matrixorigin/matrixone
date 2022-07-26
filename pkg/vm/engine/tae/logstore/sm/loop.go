package sm

import (
	"context"
	"sync"
)

type Loop struct {
	queue        chan any
	fn           func([]any, chan any)
	nextQueue    chan any
	queueCtx     context.Context
	cancelFn     context.CancelFunc
	wg           *sync.WaitGroup
	maxBatchSize int

	Itemcount int
	Itemtimes int
}

func NewLoop(queue, nextQueue chan any, fn func([]any, chan any), maxBatchSize int) *Loop {
	return &Loop{
		queue:        queue,
		fn:           fn,
		nextQueue:    nextQueue,
		maxBatchSize: maxBatchSize,
	}
}

func (l *Loop) Start() {
	l.queueCtx, l.cancelFn = context.WithCancel(context.Background())
	l.wg = &sync.WaitGroup{}
	l.wg.Add(1)
	go l.loop()
}

func (l *Loop) loop() {
	defer l.wg.Done()
	for {
		batch := make([]any, 0)
		select {
		case <-l.queueCtx.Done():
			return
		case item := <-l.queue:
			batch = append(batch, item)
		Left:
			for i := 0; i < l.maxBatchSize; i++ {
				select {
				case item = <-l.queue:
					batch = append(batch, item)
				default:
					break Left
				}
			}
		}
		l.Itemcount+=len(batch)
		l.Itemtimes++
		l.fn(batch, l.nextQueue)
	}
}

func (l *Loop) Stop() {
	l.cancelFn()
	l.wg.Wait()
}
