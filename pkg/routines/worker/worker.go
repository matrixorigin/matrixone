package worker

import (
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/routines/task"
)

func New(mp *mempool.Mempool) Worker {
	return &worker{
		mp: mp,
		ch: make(chan struct{}),
		ts: make(chan task.Task),
	}
}

func (w *worker) Run() {
	for {
		select {
		case <-w.ch:
			w.ch <- struct{}{}
			return
		case t := <-w.ts:
			t.Stop(t.Execute(w.mp))
		}
	}
}

func (w *worker) Stop() {
	w.ch <- struct{}{}
	<-w.ch
	close(w.ch)
	close(w.ts)
}

func (w *worker) AddTask(t task.Task) {
	w.ts <- t
}
