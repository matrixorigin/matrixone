package task

import "matrixone/pkg/vm/process"

type Task interface {
	Stop(TaskResult)
	Execute(*process.Process) TaskResult
}

type TaskResult interface {
	Error() error
	Result() interface{}
}
