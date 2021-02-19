package worker

import (
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/routines/task"
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
