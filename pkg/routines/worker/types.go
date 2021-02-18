package worker

import (
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/routines/task"
)

type Worker interface {
	Run()
	Stop()
	AddTask(task.Task)
}

type worker struct {
	ch chan struct{}
	ts chan task.Task
	mp *mempool.Mempool
}
