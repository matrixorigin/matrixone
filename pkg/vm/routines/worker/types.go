package worker

import (
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/routines/task"
)

type Worker interface {
	Run()
	Stop()
	AddTask(task.Task)
}

type worker struct {
	ch   chan struct{}
	ts   chan task.Task
	proc *process.Process
}
