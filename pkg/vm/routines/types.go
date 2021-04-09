package routines

import (
	"matrixone/pkg/vm/routines/task"
	"matrixone/pkg/vm/routines/worker"
)

type Routines interface {
	Run()
	Stop()
	AddTask(task.Task)
}

type routines struct {
	cnt uint64
	num uint64
	ch  chan struct{}
	ws  []worker.Worker
}
