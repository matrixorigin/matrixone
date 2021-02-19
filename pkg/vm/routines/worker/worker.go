package worker

import (
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/routines/task"
)

func New(proc *process.Process) Worker {
	return &worker{
		proc: proc,
		ch:   make(chan struct{}),
		ts:   make(chan task.Task),
	}
}

func (w *worker) Run() {
	for {
		select {
		case <-w.ch:
			w.ch <- struct{}{}
			return
		case t := <-w.ts:
			t.Stop(t.Execute(w.proc))
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
