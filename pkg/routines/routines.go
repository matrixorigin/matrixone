package routines

import (
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/routines/task"
	"matrixbase/pkg/routines/worker"
	"sync"
	"sync/atomic"
)

func New(num int, ms []*mempool.Mempool) Routines {
	r := &routines{
		num: uint64(num),
		ch:  make(chan struct{}),
		ws:  make([]worker.Worker, num),
	}
	for i := 0; i < num; i++ {
		r.ws[i] = worker.New(ms[i])
	}
	return r
}

func (r *routines) Run() {
	var wg sync.WaitGroup

	for i, j := 0, len(r.ws); i < j; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r.ws[idx].Run()
		}(i)
	}
	for {
		select {
		case <-r.ch:
			for _, w := range r.ws {
				w.Stop()
			}
			wg.Wait()
			r.ch <- struct{}{}
			return
		}
	}
}

func (r *routines) Stop() {
	r.ch <- struct{}{}
	<-r.ch
}

func (r *routines) AddTask(t task.Task) {
	r.ws[atomic.AddUint64(&r.cnt, 1)%r.num].AddTask(t)
}
